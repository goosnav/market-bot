"""Campaign builder and exact queue preview services for Sprint 5."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
import hashlib
import json
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from backend.app.core.logging import utc_now
from backend.app.domain.enums import CampaignStatus, EntityType, GenerationValidationStatus, QueuedMessageState
from backend.app.domain.models import (
    AuditEventCreate,
    CampaignAudienceSnapshotCreate,
    CampaignCreate,
    ProviderAccountCreate,
    QueuedMessageCreate,
    SequenceCreate,
    SequenceStepCreate,
)
from backend.app.repositories.audit import AuditEventRepository
from backend.app.repositories.builder import (
    CampaignAudienceSnapshotRepository,
    CampaignPreviewRepository,
    ProviderAccountRepository,
    SequenceRepository,
)
from backend.app.repositories.leads import CompanyRepository, LeadRepository
from backend.app.services.campaigns import CampaignService
from backend.app.services.lead_warehouse import LeadWarehouseService
from backend.app.services.queue import QueuedMessageService
from backend.app.services.template_studio import TemplateStudioService

WEEKDAY_NAMES = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


class CampaignBuilderError(ValueError):
    """Raised when a campaign preview request is invalid."""


class CampaignBuilderService:
    """Own provider pools, campaign assembly, and exact queue preview behavior."""

    def __init__(self, connection) -> None:
        self.audit_events = AuditEventRepository(connection)
        self.campaigns = CampaignService(connection)
        self.companies = CompanyRepository(connection)
        self.leads = LeadRepository(connection)
        self.provider_accounts = ProviderAccountRepository(connection)
        self.preview = CampaignPreviewRepository(connection)
        self.queue = QueuedMessageService(connection)
        self.sequence_repository = SequenceRepository(connection)
        self.snapshots = CampaignAudienceSnapshotRepository(connection)
        self.studio = TemplateStudioService(connection)
        self.warehouse = LeadWarehouseService(connection)

    def create_provider_account(self, payload: dict[str, object], actor: str) -> dict[str, object]:
        provider_name = string_value(payload.get("provider_name"), "manual") or "manual"
        display_name = required_text(payload.get("display_name"), "Provider account display name")
        email_address = required_text(payload.get("email_address"), "Provider account email")
        provider_account_id = self.provider_accounts.create(
            ProviderAccountCreate(
                provider_name=provider_name,
                display_name=display_name,
                email_address=email_address,
                sending_domain=string_value(payload.get("sending_domain")) or domain_from_email(email_address),
                external_account_id=string_value(payload.get("external_account_id")) or None,
                daily_cap=max(int(payload.get("daily_cap", 0)), 0),
                warmup_cap=max(int(payload.get("warmup_cap", 0)), 0),
                status=string_value(payload.get("status"), "active") or "active",
            )
        )
        provider_account = self.provider_accounts.get(provider_account_id)
        if provider_account is None:
            raise LookupError(f"Provider account {provider_account_id} could not be reloaded.")
        self._record_audit(
            entity_type=EntityType.PROVIDER_ACCOUNT.value,
            entity_id=provider_account_id,
            event_type="provider_account.created",
            actor=actor,
            summary=f"Provider account '{display_name}' created.",
            payload={"provider_name": provider_name, "email_address": email_address},
        )
        return provider_account

    def list_provider_accounts(self) -> list[dict[str, object]]:
        return self.provider_accounts.list_all()

    def get_summary(self) -> dict[str, object]:
        campaigns = [hydrate_campaign_row(item) for item in self.preview.list_campaigns()]
        latest_campaign_id = campaigns[0]["id"] if campaigns else None
        return {
            "campaign_count": len(campaigns),
            "provider_account_count": len(self.provider_accounts.list_all()),
            "campaigns": campaigns,
            "latest_campaign_id": latest_campaign_id,
            "provider_accounts": self.provider_accounts.list_all(),
            "templates": self.studio.list_templates(),
            "offer_profiles": self.studio.list_offer_profiles(),
            "vertical_playbooks": self.studio.list_vertical_playbooks(),
            "saved_filters": self.warehouse.list_saved_filters(),
            "lists": self.warehouse.lists.list_all(),
            "lead_preview": self.warehouse.list_leads(limit=12),
        }

    def build_campaign_preview(self, payload: dict[str, object], actor: str) -> dict[str, object]:
        campaign_name = required_text(payload.get("name"), "Campaign name")
        steps_payload = payload.get("steps")
        if not isinstance(steps_payload, list) or not steps_payload:
            raise CampaignBuilderError("Campaign preview requires at least one sequence step.")

        provider_account_ids = [int(account_id) for account_id in list_value(payload.get("provider_account_ids"))]
        if not provider_account_ids:
            raise CampaignBuilderError("Campaign preview requires at least one provider account.")
        provider_accounts = self.provider_accounts.list_by_ids(provider_account_ids)
        if len(provider_accounts) != len(set(provider_account_ids)):
            raise CampaignBuilderError("One or more provider accounts could not be found.")

        audience_filters = resolve_audience_filters(payload.get("audience"))
        leads = self.warehouse.list_leads(audience_filters, limit=int(payload.get("audience_limit", 250)))
        if not leads:
            raise CampaignBuilderError("Campaign audience resolved to zero leads.")

        send_window = normalize_send_window(payload.get("send_window"))
        quiet_hours = normalize_quiet_hours(payload.get("quiet_hours"))
        timezone_name = string_value(payload.get("timezone"), send_window["timezone"]) or send_window["timezone"]
        provider_name = string_value(payload.get("provider_name"), provider_accounts[0]["provider_name"]) or provider_accounts[0]["provider_name"]

        campaign = self.campaigns.create_campaign(
            CampaignCreate(
                name=campaign_name,
                description=string_value(payload.get("description")),
                offer_profile_id=int(payload["offer_profile_id"]) if payload.get("offer_profile_id") else None,
                vertical_playbook_id=int(payload["vertical_playbook_id"]) if payload.get("vertical_playbook_id") else None,
                provider_name=provider_name,
                approval_mode=string_value(payload.get("approval_mode"), "manual") or "manual",
                reply_mode=string_value(payload.get("reply_mode"), "manual") or "manual",
                send_window_config_json=json.dumps(send_window, sort_keys=True),
                quiet_hours_config_json=json.dumps(quiet_hours, sort_keys=True),
            ),
            actor=actor,
        )
        sequence_id = self.sequence_repository.create(
            SequenceCreate(
                campaign_id=int(campaign["id"]),
                name=string_value(payload.get("sequence_name"), f"{campaign_name} Sequence") or f"{campaign_name} Sequence",
                description=string_value(payload.get("sequence_description")),
                timezone=timezone_name,
            )
        )

        step_records = []
        for index, step_payload in enumerate(steps_payload):
            if not isinstance(step_payload, dict):
                raise CampaignBuilderError("Sequence steps must be objects.")
            template_id = int(step_payload.get("template_id", 0))
            template = self.studio.get_template(template_id)
            variant = resolve_step_variant(template, step_payload.get("template_variant_id"))
            step_id = self.sequence_repository.create_step(
                SequenceStepCreate(
                    sequence_id=sequence_id,
                    step_order=index,
                    step_type=string_value(step_payload.get("step_type"), "email") or "email",
                    delay_days=max(int(step_payload.get("delay_days", 0)), 0),
                    template_id=template_id,
                    template_variant_id=int(variant["id"]),
                    subject_override=string_value(step_payload.get("subject_override")),
                    body_override=string_value(step_payload.get("body_override")),
                )
            )
            step_record = {
                "id": step_id,
                "step_order": index,
                "template_id": template_id,
                "template_variant_id": int(variant["id"]),
                "step_type": string_value(step_payload.get("step_type"), "email") or "email",
                "delay_days": max(int(step_payload.get("delay_days", 0)), 0),
                "subject_override": string_value(step_payload.get("subject_override")),
                "body_override": string_value(step_payload.get("body_override")),
                "disabled_block_keys": [str(key) for key in list_value(step_payload.get("disabled_block_keys")) if str(key).strip()],
            }
            step_records.append(step_record)

        base_time = parse_base_time(payload.get("start_at"))
        usage_by_day: dict[tuple[int, str], int] = defaultdict(int)
        queue_rows: list[dict[str, object]] = []
        for lead_index, lead in enumerate(leads):
            snapshot_payload = {
                "lead_id": lead["id"],
                "full_name": lead["full_name"],
                "email": lead.get("email"),
                "company_name": lead.get("company_name") or lead.get("company_name_snapshot"),
                "company_domain": lead.get("company_domain") or lead.get("company_domain_snapshot"),
                "title": lead.get("title"),
                "suppression_status": lead.get("suppression_status"),
                "source": lead.get("source"),
            }
            self.snapshots.create(
                CampaignAudienceSnapshotCreate(
                    campaign_id=int(campaign["id"]),
                    lead_id=int(lead["id"]),
                    company_id=int(lead["company_id"]) if lead.get("company_id") else None,
                    snapshot_json=json.dumps(snapshot_payload, sort_keys=True),
                )
            )
            for step_record in step_records:
                provider_account = choose_provider_account(
                    provider_accounts,
                    usage_by_day,
                    base_time=base_time,
                    step_delay_days=int(step_record["delay_days"]),
                    row_index=len(queue_rows),
                    send_window=send_window,
                    quiet_hours=quiet_hours,
                    timezone_name=timezone_name,
                )
                artifact = self.studio.render_template(
                    template_id=int(step_record["template_id"]),
                    template_variant_id=int(step_record["template_variant_id"]),
                    lead_id=int(lead["id"]),
                    offer_profile_id=int(campaign["offer_profile_id"]) if campaign.get("offer_profile_id") else None,
                    vertical_playbook_id=int(campaign["vertical_playbook_id"]) if campaign.get("vertical_playbook_id") else None,
                    actor=actor,
                    deterministic_mode=bool(payload.get("deterministic_mode", False)),
                    disabled_block_keys=step_record["disabled_block_keys"],
                    generation_seed=lead_index * 100 + int(step_record["step_order"]),
                )
                if step_record["subject_override"] or step_record["body_override"]:
                    artifact = self.studio.create_manual_edit_artifact(
                        artifact["id"],
                        actor=actor,
                        edited_subject=step_record["subject_override"] or None,
                        edited_body=step_record["body_override"] or None,
                    )
                scheduled_for = compute_scheduled_time(
                    base_time=base_time,
                    row_index=len(queue_rows),
                    step_delay_days=int(step_record["delay_days"]),
                    send_window=send_window,
                    quiet_hours=quiet_hours,
                    timezone_name=timezone_name,
                )
                extra_flags = build_queue_validation_flags(lead, provider_account)
                all_risk_flags = list(artifact["risk_flags"]) + extra_flags
                validation_status = artifact["validation_status"]
                queued_state = derive_queue_state(validation_status, all_risk_flags)
                render_hash = build_render_hash(artifact["subject"], artifact["body"], scheduled_for, provider_account["id"])
                queued_message = self.queue.create_message(
                    QueuedMessageCreate(
                        lead_id=int(lead["id"]),
                        company_id=int(lead["company_id"]) if lead.get("company_id") else None,
                        campaign_id=int(campaign["id"]),
                        sequence_id=sequence_id,
                        sequence_step_id=int(step_record["id"]),
                        template_id=int(step_record["template_id"]),
                        template_variant_id=int(step_record["template_variant_id"]),
                        generation_artifact_id=int(artifact["id"]),
                        provider_account_id=int(provider_account["id"]),
                        scheduled_for=scheduled_for,
                        state=queued_state,
                        rendered_subject=artifact["subject"],
                        rendered_body=artifact["body"],
                        render_hash=render_hash,
                        risk_flags_json=json.dumps(all_risk_flags, sort_keys=True),
                    ),
                    actor=actor,
                )
                row_local_day = scheduled_for[:10]
                usage_by_day[(int(provider_account["id"]), row_local_day)] += 1
                queue_rows.append(
                    {
                        "queued_message_id": queued_message["id"],
                        "step_order": step_record["step_order"],
                    }
                )

        self.campaigns.transition_status(int(campaign["id"]), CampaignStatus.PREVIEW_READY, actor=actor, reason="Queue preview generated")
        self._record_audit(
            entity_type=EntityType.CAMPAIGN.value,
            entity_id=int(campaign["id"]),
            event_type="campaign.preview_generated",
            actor=actor,
            summary=f"Campaign '{campaign_name}' preview generated.",
            payload={
                "queued_count": len(queue_rows),
                "provider_account_ids": provider_account_ids,
                "audience_count": len(leads),
            },
        )
        return self.get_preview(int(campaign["id"]))

    def get_preview(self, campaign_id: int, limit: int = 500) -> dict[str, object]:
        campaign = self.preview.get_campaign(campaign_id)
        if campaign is None:
            raise LookupError(f"Campaign {campaign_id} does not exist.")
        rows = [hydrate_preview_row(row) for row in self.preview.list_preview_rows(campaign_id, limit=limit)]
        return {
            "campaign": hydrate_campaign_row(campaign),
            "items": rows,
            "queue_counts": summarize_queue_counts(rows),
            "launch_ready": bool(rows) and all(item["state"] == QueuedMessageState.APPROVED.value for item in rows),
        }

    def manual_edit_preview(
        self,
        queued_message_id: int,
        *,
        actor: str,
        edited_subject: str | None = None,
        edited_body: str | None = None,
    ) -> dict[str, object]:
        row = self.preview.get_preview_row(queued_message_id)
        if row is None:
            raise LookupError(f"Queued message {queued_message_id} does not exist.")
        artifact = self.studio.create_manual_edit_artifact(
            int(row["generation_artifact_id"]),
            actor=actor,
            edited_subject=edited_subject,
            edited_body=edited_body,
        )
        next_state = derive_updated_state(row["state"], artifact["validation_status"], artifact["risk_flags"])
        self.preview.update_render_output(
            queued_message_id,
            generation_artifact_id=int(artifact["id"]),
            rendered_subject=artifact["subject"],
            rendered_body=artifact["body"],
            render_hash=build_render_hash(artifact["subject"], artifact["body"], string_value(row["scheduled_for"]), int(row["provider_account_id"])),
            risk_flags_json=json.dumps(artifact["risk_flags"], sort_keys=True),
            state=next_state,
        )
        self._record_audit(
            entity_type=EntityType.QUEUED_MESSAGE.value,
            entity_id=queued_message_id,
            event_type="queued_message.manually_edited",
            actor=actor,
            summary=f"Queued message {queued_message_id} manually edited.",
            payload={"edited_subject": edited_subject is not None, "edited_body": edited_body is not None},
        )
        hydrated = self.preview.get_preview_row(queued_message_id)
        if hydrated is None:
            raise LookupError(f"Queued message {queued_message_id} could not be reloaded.")
        return hydrate_preview_row(hydrated)

    def regenerate_preview(
        self,
        queued_message_id: int,
        *,
        actor: str,
        regenerate_block_keys: list[str] | None = None,
    ) -> dict[str, object]:
        row = self.preview.get_preview_row(queued_message_id)
        if row is None:
            raise LookupError(f"Queued message {queued_message_id} does not exist.")
        artifact = self.studio.regenerate_artifact(
            int(row["generation_artifact_id"]),
            actor=actor,
            regenerate_block_keys=regenerate_block_keys,
        )
        next_state = derive_updated_state(row["state"], artifact["validation_status"], artifact["risk_flags"])
        self.preview.update_render_output(
            queued_message_id,
            generation_artifact_id=int(artifact["id"]),
            rendered_subject=artifact["subject"],
            rendered_body=artifact["body"],
            render_hash=build_render_hash(artifact["subject"], artifact["body"], string_value(row["scheduled_for"]), int(row["provider_account_id"])),
            risk_flags_json=json.dumps(artifact["risk_flags"], sort_keys=True),
            state=next_state,
        )
        self._record_audit(
            entity_type=EntityType.QUEUED_MESSAGE.value,
            entity_id=queued_message_id,
            event_type="queued_message.ai_regenerated",
            actor=actor,
            summary=f"Queued message {queued_message_id} AI blocks regenerated.",
            payload={"regenerate_block_keys": list(regenerate_block_keys or [])},
        )
        hydrated = self.preview.get_preview_row(queued_message_id)
        if hydrated is None:
            raise LookupError(f"Queued message {queued_message_id} could not be reloaded.")
        return hydrate_preview_row(hydrated)

    def approve_preview(self, campaign_id: int, actor: str) -> dict[str, object]:
        preview = self.get_preview(campaign_id)
        items = preview["items"]
        if not items:
            raise CampaignBuilderError("Campaign preview is empty and cannot be approved.")
        blocked = [item for item in items if item["state"] == QueuedMessageState.BLOCKED.value]
        if blocked:
            raise CampaignBuilderError("Blocked queue rows must be fixed before approval.")
        for item in items:
            if item["state"] == QueuedMessageState.RENDERED.value:
                self.queue.transition_state(int(item["id"]), QueuedMessageState.APPROVED, actor=actor, reason="Preview approved")
        self.campaigns.transition_status(campaign_id, CampaignStatus.SCHEDULED, actor=actor, reason="Preview approved and launch-ready")
        self._record_audit(
            entity_type=EntityType.CAMPAIGN.value,
            entity_id=campaign_id,
            event_type="campaign.preview_approved",
            actor=actor,
            summary=f"Campaign {campaign_id} preview approved.",
            payload={"queued_count": len(items)},
        )
        return self.get_preview(campaign_id)

    def _record_audit(
        self,
        *,
        entity_type: str,
        entity_id: int,
        event_type: str,
        actor: str,
        summary: str,
        payload: dict[str, object],
    ) -> None:
        self.audit_events.record(
            AuditEventCreate(
                entity_type=entity_type,
                entity_id=entity_id,
                event_type=event_type,
                actor=actor,
                summary=summary,
                payload_json=json.dumps(payload, sort_keys=True),
            )
        )


def resolve_audience_filters(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {}
    filters = dict(value)
    saved_filter_id = filters.get("saved_filter_id")
    list_id = filters.get("list_id")
    if saved_filter_id:
        filters["saved_filter_id"] = str(saved_filter_id)
    if list_id:
        filters["list_id"] = int(list_id)
    return filters


def normalize_send_window(value: object) -> dict[str, object]:
    payload = dict(value) if isinstance(value, dict) else {}
    start_hour = int(payload.get("start_hour", 9))
    end_hour = int(payload.get("end_hour", 17))
    if not 0 <= start_hour <= 23 or not 1 <= end_hour <= 24:
        raise CampaignBuilderError("Send window hours must be within 0-24.")
    allowed_weekdays = payload.get("allowed_weekdays")
    weekdays = [str(day).lower()[:3] for day in allowed_weekdays] if isinstance(allowed_weekdays, list) and allowed_weekdays else WEEKDAY_NAMES[:5]
    return {
        "start_hour": start_hour,
        "end_hour": end_hour,
        "interval_minutes": max(int(payload.get("interval_minutes", 15)), 1),
        "allowed_weekdays": weekdays,
        "timezone": string_value(payload.get("timezone"), "UTC") or "UTC",
    }


def normalize_quiet_hours(value: object) -> dict[str, object]:
    payload = dict(value) if isinstance(value, dict) else {}
    start_hour = int(payload.get("start_hour", 20))
    end_hour = int(payload.get("end_hour", 8))
    if not 0 <= start_hour <= 23 or not 0 <= end_hour <= 23:
        raise CampaignBuilderError("Quiet hour bounds must be within 0-23.")
    return {
        "start_hour": start_hour,
        "end_hour": end_hour,
    }


def parse_base_time(value: object) -> datetime:
    if isinstance(value, str) and value.strip():
        try:
            dt = datetime.fromisoformat(value)
        except ValueError as exc:
            raise CampaignBuilderError(f"Invalid start_at value '{value}'.") from exc
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
    return datetime.now(tz=UTC).replace(minute=0, second=0, microsecond=0)


def resolve_step_variant(template: dict[str, object], value: object) -> dict[str, object]:
    variants = template["variants"] if isinstance(template.get("variants"), list) else []
    if value:
        requested = int(value)
        for variant in variants:
            if int(variant["id"]) == requested:
                return variant
        raise CampaignBuilderError(f"Template variant {requested} does not belong to template {template['id']}.")
    for variant in variants:
        if bool(variant.get("is_default")):
            return variant
    if variants:
        return variants[0]
    raise CampaignBuilderError(f"Template {template['id']} has no variants.")


def build_queue_validation_flags(lead: dict[str, object], provider_account: dict[str, object]) -> list[dict[str, object]]:
    flags: list[dict[str, object]] = []
    if not string_value(lead.get("email")).strip():
        flags.append(
            {
                "code": "missing_email",
                "severity": "error",
                "message": f"Lead {lead['full_name']} is missing an email address.",
            }
        )
    if string_value(lead.get("suppression_status"), "clear") != "clear":
        flags.append(
            {
                "code": "suppressed_lead",
                "severity": "error",
                "message": f"Lead {lead['full_name']} is currently suppressed.",
            }
        )
    if string_value(provider_account.get("status"), "active") != "active":
        flags.append(
            {
                "code": "provider_unavailable",
                "severity": "error",
                "message": f"Provider account {provider_account['display_name']} is not active.",
            }
        )
    return flags


def derive_queue_state(validation_status: str, risk_flags: list[dict[str, object]]) -> QueuedMessageState:
    if validation_status == GenerationValidationStatus.BLOCKED.value or any(flag.get("severity") == "error" for flag in risk_flags):
        return QueuedMessageState.BLOCKED
    return QueuedMessageState.RENDERED


def derive_updated_state(current_state: str, validation_status: str, risk_flags: list[dict[str, object]]) -> str:
    if validation_status == GenerationValidationStatus.BLOCKED.value or any(flag.get("severity") == "error" for flag in risk_flags):
        return QueuedMessageState.BLOCKED.value
    if current_state == QueuedMessageState.APPROVED.value:
        return QueuedMessageState.APPROVED.value
    if current_state == QueuedMessageState.SCHEDULED.value:
        return QueuedMessageState.SCHEDULED.value
    return QueuedMessageState.RENDERED.value


def choose_provider_account(
    provider_accounts: list[dict[str, object]],
    usage_by_day: dict[tuple[int, str], int],
    *,
    base_time: datetime,
    step_delay_days: int,
    row_index: int,
    send_window: dict[str, object],
    quiet_hours: dict[str, object],
    timezone_name: str,
) -> dict[str, object]:
    if not provider_accounts:
        raise CampaignBuilderError("No provider accounts available.")
    attempts = len(provider_accounts)
    for offset in range(attempts):
        account = provider_accounts[(row_index + offset) % len(provider_accounts)]
        scheduled_for = compute_scheduled_time(
            base_time=base_time,
            row_index=row_index,
            step_delay_days=step_delay_days,
            send_window=send_window,
            quiet_hours=quiet_hours,
            timezone_name=timezone_name,
        )
        local_day = scheduled_for[:10]
        daily_cap = int(account.get("daily_cap") or 0)
        current_usage = usage_by_day[(int(account["id"]), local_day)]
        if daily_cap > 0 and current_usage >= daily_cap:
            continue
        return account
    return provider_accounts[row_index % len(provider_accounts)]


def compute_scheduled_time(
    *,
    base_time: datetime,
    row_index: int,
    step_delay_days: int,
    send_window: dict[str, object],
    quiet_hours: dict[str, object],
    timezone_name: str,
) -> str:
    interval_minutes = int(send_window["interval_minutes"])
    candidate = base_time + timedelta(days=step_delay_days, minutes=row_index * interval_minutes)
    zone = resolve_zone(timezone_name)
    local = candidate.astimezone(zone).replace(second=0, microsecond=0)
    local = align_to_window(local, send_window, quiet_hours)
    return local.astimezone(UTC).isoformat()


def align_to_window(local: datetime, send_window: dict[str, object], quiet_hours: dict[str, object]) -> datetime:
    start_hour = int(send_window["start_hour"])
    end_hour = int(send_window["end_hour"])
    interval_minutes = int(send_window["interval_minutes"])
    allowed_days = {str(day).lower()[:3] for day in send_window.get("allowed_weekdays", WEEKDAY_NAMES[:5])}

    while True:
        weekday = WEEKDAY_NAMES[local.weekday()]
        if weekday not in allowed_days:
            local = next_day_at_hour(local, start_hour)
            continue
        if within_quiet_hours(local, quiet_hours):
            local = end_of_quiet_hours(local, quiet_hours)
            continue
        if local.hour < start_hour:
            local = local.replace(hour=start_hour, minute=0, second=0, microsecond=0)
            continue
        if local.hour >= end_hour:
            local = next_day_at_hour(local, start_hour)
            continue
        minute = local.minute
        rounded_minute = ((minute + interval_minutes - 1) // interval_minutes) * interval_minutes
        if rounded_minute >= 60:
            local = (local + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            continue
        local = local.replace(minute=rounded_minute, second=0, microsecond=0)
        if local.hour >= end_hour:
            local = next_day_at_hour(local, start_hour)
            continue
        if within_quiet_hours(local, quiet_hours):
            local = end_of_quiet_hours(local, quiet_hours)
            continue
        return local


def within_quiet_hours(local: datetime, quiet_hours: dict[str, object]) -> bool:
    start_hour = int(quiet_hours["start_hour"])
    end_hour = int(quiet_hours["end_hour"])
    hour = local.hour
    if start_hour == end_hour:
        return False
    if start_hour < end_hour:
        return start_hour <= hour < end_hour
    return hour >= start_hour or hour < end_hour


def end_of_quiet_hours(local: datetime, quiet_hours: dict[str, object]) -> datetime:
    start_hour = int(quiet_hours["start_hour"])
    end_hour = int(quiet_hours["end_hour"])
    if start_hour < end_hour:
        return local.replace(hour=end_hour, minute=0, second=0, microsecond=0)
    if local.hour >= start_hour:
        local = local + timedelta(days=1)
    return local.replace(hour=end_hour, minute=0, second=0, microsecond=0)


def next_day_at_hour(local: datetime, hour: int) -> datetime:
    next_day = local + timedelta(days=1)
    return next_day.replace(hour=hour, minute=0, second=0, microsecond=0)


def resolve_zone(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def build_render_hash(subject: str, body: str, scheduled_for: str, provider_account_id: int) -> str:
    payload = f"{subject}\n{body}\n{scheduled_for}\n{provider_account_id}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def hydrate_campaign_row(row: dict[str, object]) -> dict[str, object]:
    return {
        "id": int(row["id"]),
        "name": string_value(row.get("name")),
        "status": string_value(row.get("status")),
        "approval_mode": string_value(row.get("approval_mode")),
        "reply_mode": string_value(row.get("reply_mode")),
        "provider_name": string_value(row.get("provider_name")),
        "offer_profile_id": int(row["offer_profile_id"]) if row.get("offer_profile_id") is not None else None,
        "offer_profile_name": string_value(row.get("offer_profile_name")),
        "vertical_playbook_id": int(row["vertical_playbook_id"]) if row.get("vertical_playbook_id") is not None else None,
        "vertical_playbook_name": string_value(row.get("vertical_playbook_name")),
        "audience_count": int(row.get("audience_count") or 0),
        "queued_count": int(row.get("queued_count") or 0),
        "blocked_count": int(row.get("blocked_count") or 0),
        "approved_count": int(row.get("approved_count") or 0),
        "rendered_count": int(row.get("rendered_count") or 0),
        "send_window_config": parse_json_object(row.get("send_window_config_json")),
        "quiet_hours_config": parse_json_object(row.get("quiet_hours_config_json")),
        "created_at": string_value(row.get("created_at")),
        "updated_at": string_value(row.get("updated_at")),
        "launch_ready": string_value(row.get("status")) == CampaignStatus.SCHEDULED.value,
    }


def hydrate_preview_row(row: dict[str, object]) -> dict[str, object]:
    artifact_output = parse_json_object(row.get("artifact_output_json"))
    prompt_input = parse_json_object(row.get("artifact_prompt_input_json"))
    artifact_blocks = artifact_output.get("blocks") if isinstance(artifact_output, dict) else []
    if not isinstance(artifact_blocks, list):
        artifact_blocks = []
    static_sections = [block for block in artifact_blocks if str(block.get("block_type")) != "ai_generated"]
    ai_sections = [block for block in artifact_blocks if str(block.get("block_type")) == "ai_generated"]
    risk_flags = parse_json_list(row.get("risk_flags_json"))
    return {
        "id": int(row["id"]),
        "campaign_id": int(row["campaign_id"]),
        "lead_id": int(row["lead_id"]),
        "lead_name": string_value(row.get("lead_name")),
        "lead_email": string_value(row.get("lead_email")),
        "company_id": int(row["company_id"]) if row.get("company_id") is not None else None,
        "company_name": string_value(row.get("company_name")),
        "company_domain": string_value(row.get("company_domain")),
        "provider_account_id": int(row["provider_account_id"]),
        "provider_account_name": string_value(row.get("provider_account_name")),
        "provider_account_email": string_value(row.get("provider_account_email")),
        "sequence_step_id": int(row["sequence_step_id"]),
        "step_order": int(row.get("step_order") or 0),
        "delay_days": int(row.get("delay_days") or 0),
        "step_type": string_value(row.get("step_type")),
        "template_id": int(row["template_id"]),
        "template_name": string_value(row.get("template_name")),
        "template_variant_id": int(row["template_variant_id"]),
        "template_variant_name": string_value(row.get("template_variant_name")),
        "generation_artifact_id": int(row["generation_artifact_id"]),
        "source_artifact_id": int(row["source_artifact_id"]) if row.get("source_artifact_id") is not None else None,
        "state": string_value(row.get("state")),
        "scheduled_for": string_value(row.get("scheduled_for")),
        "subject": string_value(row.get("rendered_subject")),
        "body": string_value(row.get("rendered_body")),
        "render_hash": string_value(row.get("render_hash")),
        "risk_flags": risk_flags,
        "artifact_validation_status": string_value(row.get("artifact_validation_status")),
        "artifact_prompt_input": prompt_input,
        "static_sections": static_sections,
        "ai_sections": ai_sections,
    }


def summarize_queue_counts(items: list[dict[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for item in items:
        counts[item["state"]] += 1
    return dict(counts)


def parse_json_object(value: object) -> dict[str, object]:
    if value in (None, ""):
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def parse_json_list(value: object) -> list[dict[str, object]]:
    if value in (None, ""):
        return []
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def required_text(value: object, label: str) -> str:
    text = string_value(value).strip()
    if not text:
        raise CampaignBuilderError(f"{label} is required.")
    return text


def string_value(value: object, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def domain_from_email(email_address: str) -> str | None:
    if "@" not in email_address:
        return None
    return email_address.split("@", 1)[1].strip().lower() or None


def list_value(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]
