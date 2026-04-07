"""Template studio services for bounded message generation and validation."""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import json
import re

from backend.app.core.logging import utc_now
from backend.app.domain.enums import EntityType, GenerationValidationStatus, TemplateBlockType
from backend.app.domain.models import (
    AuditEventCreate,
    GenerationArtifactCreate,
    OfferProfileCreate,
    TemplateBlockCreate,
    TemplateCreate,
    TemplateVariantCreate,
    VerticalPlaybookCreate,
)
from backend.app.repositories.audit import AuditEventRepository
from backend.app.repositories.leads import CompanyRepository, LeadRepository
from backend.app.repositories.studio import (
    GenerationArtifactRepository,
    OfferProfileRepository,
    TemplateBlockRepository,
    TemplateRepository,
    TemplateVariantRepository,
    VerticalPlaybookRepository,
)

PROMPT_VERSION = "sprint-4-local-generator-v1"
TOKEN_PATTERN = re.compile(r"{{\s*([a-zA-Z0-9_.]+)\s*}}")


class TemplateStudioError(ValueError):
    """Raised when a template or generation request is invalid."""


class TemplateStudioService:
    """Own template CRUD, profile/playbook CRUD, render preview, and artifact storage."""

    def __init__(self, connection) -> None:
        self.audit_events = AuditEventRepository(connection)
        self.artifacts = GenerationArtifactRepository(connection)
        self.companies = CompanyRepository(connection)
        self.leads = LeadRepository(connection)
        self.offer_profiles = OfferProfileRepository(connection)
        self.playbooks = VerticalPlaybookRepository(connection)
        self.template_blocks = TemplateBlockRepository(connection)
        self.templates = TemplateRepository(connection)
        self.template_variants = TemplateVariantRepository(connection)

    def save_offer_profile(self, payload: dict[str, object], actor: str) -> dict[str, object]:
        profile_create = OfferProfileCreate(
            name=required_name(payload.get("name"), "Offer profile name"),
            description=string_value(payload.get("description")),
            target_verticals_json=json.dumps(string_list(payload.get("target_verticals")), sort_keys=True),
            target_pains_json=json.dumps(string_list(payload.get("target_pains")), sort_keys=True),
            value_proposition=string_value(payload.get("value_proposition")),
            standard_cta=string_value(payload.get("standard_cta")),
            booking_link_id=int(payload["booking_link_id"]) if payload.get("booking_link_id") else None,
            allowed_claims_json=json.dumps(string_list(payload.get("allowed_claims")), sort_keys=True),
            disallowed_claims_json=json.dumps(string_list(payload.get("disallowed_claims")), sort_keys=True),
            pricing_framing_snippets_json=json.dumps(string_list(payload.get("pricing_framing_snippets")), sort_keys=True),
            objection_handling_snippets_json=json.dumps(string_list(payload.get("objection_handling_snippets")), sort_keys=True),
        )
        profile_id = int(payload["id"]) if payload.get("id") else None
        if profile_id is None:
            profile_id = self.offer_profiles.create(profile_create)
            event_type = "offer_profile.created"
            summary = f"Offer profile '{profile_create.name}' created."
        else:
            if self.offer_profiles.get(profile_id) is None:
                raise LookupError(f"Offer profile {profile_id} does not exist.")
            self.offer_profiles.update(profile_id, profile_create)
            event_type = "offer_profile.updated"
            summary = f"Offer profile '{profile_create.name}' updated."

        profile = self.offer_profiles.get(profile_id)
        if profile is None:
            raise LookupError(f"Offer profile {profile_id} could not be reloaded.")
        hydrated = hydrate_offer_profile(profile)
        self._record_audit(
            entity_type=EntityType.OFFER_PROFILE.value,
            entity_id=profile_id,
            event_type=event_type,
            actor=actor,
            summary=summary,
            payload={"name": hydrated["name"]},
        )
        return hydrated

    def list_offer_profiles(self) -> list[dict[str, object]]:
        return [hydrate_offer_profile(profile) for profile in self.offer_profiles.list_all()]

    def save_vertical_playbook(self, payload: dict[str, object], actor: str) -> dict[str, object]:
        playbook_create = VerticalPlaybookCreate(
            name=required_name(payload.get("name"), "Vertical playbook name"),
            target_pains_json=json.dumps(string_list(payload.get("target_pains")), sort_keys=True),
            acceptable_language_json=json.dumps(string_list(payload.get("acceptable_language")), sort_keys=True),
            disallowed_language_json=json.dumps(string_list(payload.get("disallowed_language")), sort_keys=True),
            personalization_strategy=string_value(payload.get("personalization_strategy")),
            tone_profile=string_value(payload.get("tone_profile")),
            sample_subject_patterns_json=json.dumps(string_list(payload.get("sample_subject_patterns")), sort_keys=True),
            standard_objections_json=json.dumps(string_list(payload.get("standard_objections")), sort_keys=True),
            escalation_rules_json=json.dumps(string_list(payload.get("escalation_rules")), sort_keys=True),
        )
        playbook_id = int(payload["id"]) if payload.get("id") else None
        if playbook_id is None:
            playbook_id = self.playbooks.create(playbook_create)
            event_type = "vertical_playbook.created"
            summary = f"Vertical playbook '{playbook_create.name}' created."
        else:
            if self.playbooks.get(playbook_id) is None:
                raise LookupError(f"Vertical playbook {playbook_id} does not exist.")
            self.playbooks.update(playbook_id, playbook_create)
            event_type = "vertical_playbook.updated"
            summary = f"Vertical playbook '{playbook_create.name}' updated."

        playbook = self.playbooks.get(playbook_id)
        if playbook is None:
            raise LookupError(f"Vertical playbook {playbook_id} could not be reloaded.")
        hydrated = hydrate_playbook(playbook)
        self._record_audit(
            entity_type=EntityType.VERTICAL_PLAYBOOK.value,
            entity_id=playbook_id,
            event_type=event_type,
            actor=actor,
            summary=summary,
            payload={"name": hydrated["name"]},
        )
        return hydrated

    def list_vertical_playbooks(self) -> list[dict[str, object]]:
        return [hydrate_playbook(playbook) for playbook in self.playbooks.list_all()]

    def save_template(self, payload: dict[str, object], actor: str) -> dict[str, object]:
        variants_payload = payload.get("variants")
        if not isinstance(variants_payload, list) or not variants_payload:
            raise TemplateStudioError("Template must include at least one variant.")

        template_create = TemplateCreate(
            name=required_name(payload.get("name"), "Template name"),
            description=string_value(payload.get("description")),
            channel=string_value(payload.get("channel"), "email") or "email",
            is_active=bool(payload.get("is_active", True)),
        )
        template_id = int(payload["id"]) if payload.get("id") else None
        if template_id is None:
            template_id = self.templates.create(template_create)
            event_type = "template.created"
            summary = f"Template '{template_create.name}' created."
        else:
            if self.templates.get(template_id) is None:
                raise LookupError(f"Template {template_id} does not exist.")
            self.templates.update(template_id, template_create)
            self.template_blocks.delete_for_template(template_id)
            self.template_variants.delete_for_template(template_id)
            event_type = "template.updated"
            summary = f"Template '{template_create.name}' updated."

        base_blocks_payload = payload.get("blocks")
        if isinstance(base_blocks_payload, list):
            self._store_blocks(template_id, None, base_blocks_payload)

        default_assigned = any(bool(variant.get("is_default")) for variant in variants_payload if isinstance(variant, dict))
        for index, variant_payload in enumerate(variants_payload):
            if not isinstance(variant_payload, dict):
                raise TemplateStudioError("Template variants must be objects.")
            variant_id = self.template_variants.create(
                TemplateVariantCreate(
                    template_id=template_id,
                    name=required_name(variant_payload.get("name"), "Template variant name"),
                    variant_label=string_value(variant_payload.get("variant_label")),
                    is_default=bool(variant_payload.get("is_default", not default_assigned and index == 0)),
                )
            )
            variant_blocks = variant_payload.get("blocks")
            if not isinstance(variant_blocks, list) or not variant_blocks:
                raise TemplateStudioError("Each template variant must include at least one block.")
            self._store_blocks(template_id, variant_id, variant_blocks)

        hydrated = self.get_template(template_id)
        self._record_audit(
            entity_type=EntityType.TEMPLATE.value,
            entity_id=template_id,
            event_type=event_type,
            actor=actor,
            summary=summary,
            payload={"name": hydrated["name"], "variant_count": len(hydrated["variants"])},
        )
        return hydrated

    def list_templates(self) -> list[dict[str, object]]:
        templates = []
        for template in self.templates.list_all():
            templates.append(self.get_template(int(template["id"])))
        return templates

    def get_template(self, template_id: int) -> dict[str, object]:
        template = self.templates.get(template_id)
        if template is None:
            raise LookupError(f"Template {template_id} does not exist.")
        variants = [hydrate_variant(variant) for variant in self.template_variants.list_for_template(template_id)]
        blocks = [hydrate_block(block) for block in self.template_blocks.list_for_template(template_id)]

        base_blocks = [block for block in blocks if block["template_variant_id"] is None]
        blocks_by_variant: dict[int, list[dict[str, object]]] = {}
        for block in blocks:
            variant_id = block["template_variant_id"]
            if variant_id is None:
                continue
            blocks_by_variant.setdefault(int(variant_id), []).append(block)

        hydrated = hydrate_template(template)
        hydrated["blocks"] = sorted(base_blocks, key=block_sort_key)
        hydrated["variants"] = []
        for variant in variants:
            item = dict(variant)
            item["blocks"] = sorted(blocks_by_variant.get(int(variant["id"]), []), key=block_sort_key)
            hydrated["variants"].append(item)
        hydrated["variant_count"] = len(hydrated["variants"])
        hydrated["block_count"] = len(hydrated["blocks"]) + sum(len(variant["blocks"]) for variant in hydrated["variants"])
        return hydrated

    def list_artifacts(self, limit: int = 20) -> list[dict[str, object]]:
        return [hydrate_artifact(artifact) for artifact in self.artifacts.list_recent(limit=limit)]

    def get_artifact(self, artifact_id: int) -> dict[str, object]:
        artifact = self.artifacts.get(artifact_id)
        if artifact is None:
            raise LookupError(f"Generation artifact {artifact_id} does not exist.")
        return hydrate_artifact(artifact)

    def get_summary(self) -> dict[str, object]:
        return {
            "template_count": self.templates.count_all(),
            "offer_profile_count": self.offer_profiles.count_all(),
            "vertical_playbook_count": self.playbooks.count_all(),
            "artifact_count": self.artifacts.count_all(),
            "templates": self.list_templates(),
            "offer_profiles": self.list_offer_profiles(),
            "vertical_playbooks": self.list_vertical_playbooks(),
            "recent_artifacts": self.list_artifacts(limit=8),
            "lead_preview": self.leads.list_filtered({}, limit=12),
        }

    def render_template(
        self,
        *,
        template_id: int,
        lead_id: int,
        actor: str,
        template_variant_id: int | None = None,
        offer_profile_id: int | None = None,
        vertical_playbook_id: int | None = None,
        deterministic_mode: bool = False,
        disabled_block_keys: list[str] | None = None,
        generation_seed: int = 0,
        source_artifact_id: int | None = None,
        preserved_ai_blocks: dict[str, dict[str, object]] | None = None,
        regenerated_block_keys: list[str] | None = None,
    ) -> dict[str, object]:
        template = self.get_template(template_id)
        variant = self._resolve_variant(template, template_variant_id)
        lead_context = self._load_lead_context(lead_id)
        offer_profile = self._load_offer_profile(offer_profile_id)
        playbook = self._load_playbook(vertical_playbook_id)
        context = build_render_context(lead_context, offer_profile, playbook)
        selected_variant_id = int(variant["id"])
        blocks = resolve_render_blocks(self.template_blocks.list_for_render(template_id, selected_variant_id))
        disabled_keys = {key.strip() for key in disabled_block_keys or [] if key and str(key).strip()}

        rendered_blocks: list[dict[str, object]] = []
        risk_flags: list[dict[str, object]] = []
        ai_activity = False
        preserved_lookup = preserved_ai_blocks or {}

        for block in blocks:
            block_risk_flags, rendered_block = self._render_block(
                block,
                context=context,
                deterministic_mode=deterministic_mode,
                generation_seed=generation_seed,
                disabled_block_keys=disabled_keys,
                preserved_ai_blocks=preserved_lookup,
            )
            risk_flags.extend(block_risk_flags)
            if rendered_block is None:
                continue
            rendered_blocks.append(rendered_block)
            if rendered_block["source"] in {"local_ai", "preserved_ai"}:
                ai_activity = True

        subject = " ".join(block["rendered_text"] for block in rendered_blocks if block["section"] == "subject").strip()
        body = "\n\n".join(block["rendered_text"] for block in rendered_blocks if block["section"] == "body").strip()

        validation_flags = validate_rendered_message(
            subject=subject,
            body=body,
            blocks=rendered_blocks,
            offer_profile=offer_profile,
            playbook=playbook,
        )
        risk_flags.extend(validation_flags)
        validation_status = classify_validation_status(risk_flags)
        model_name = "deterministic" if not ai_activity else "local_heuristic_v1"

        prompt_input = {
            "template_id": template_id,
            "template_variant_id": selected_variant_id,
            "lead_id": lead_id,
            "offer_profile_id": offer_profile_id,
            "vertical_playbook_id": vertical_playbook_id,
            "deterministic_mode": deterministic_mode,
            "disabled_block_keys": sorted(disabled_keys),
            "generation_seed": generation_seed,
            "regenerated_block_keys": sorted(regenerated_block_keys or []),
            "context_snapshot": context,
        }
        output = {
            "template": {"id": template["id"], "name": template["name"]},
            "variant": {"id": variant["id"], "name": variant["name"], "label": variant["variant_label"]},
            "lead": {
                "id": lead_context["lead"]["id"],
                "full_name": lead_context["lead"]["full_name"],
                "email": lead_context["lead"]["email"],
                "company_name": lead_context["company"]["name"],
            },
            "subject": subject,
            "body": body,
            "blocks": rendered_blocks,
            "validation": {
                "status": validation_status.value,
                "risk_flags": risk_flags,
            },
        }
        output_text = f"Subject: {subject}\n\n{body}".strip()
        artifact_id = self.artifacts.create(
            GenerationArtifactCreate(
                kind="message_render",
                prompt_version=PROMPT_VERSION,
                prompt_input_json=json.dumps(prompt_input, sort_keys=True),
                output_text=output_text,
                output_json=json.dumps(output, sort_keys=True),
                validation_status=validation_status,
                risk_flags_json=json.dumps(risk_flags, sort_keys=True),
                model_name=model_name,
            ),
            template_id=template_id,
            template_variant_id=selected_variant_id,
            lead_id=lead_id,
            source_artifact_id=source_artifact_id,
        )
        self._record_audit(
            entity_type=EntityType.GENERATION_ARTIFACT.value,
            entity_id=artifact_id,
            event_type="generation_artifact.created",
            actor=actor,
            summary=f"Render artifact {artifact_id} created for template '{template['name']}'.",
            payload={
                "template_id": template_id,
                "template_variant_id": selected_variant_id,
                "lead_id": lead_id,
                "validation_status": validation_status.value,
                "risk_flag_count": len(risk_flags),
            },
        )
        artifact = self.artifacts.get(artifact_id)
        if artifact is None:
            raise LookupError(f"Generation artifact {artifact_id} could not be reloaded.")
        return hydrate_artifact(artifact)

    def regenerate_artifact(
        self,
        artifact_id: int,
        *,
        actor: str,
        regenerate_block_keys: list[str] | None = None,
        deterministic_mode: bool | None = None,
    ) -> dict[str, object]:
        artifact = self.artifacts.get(artifact_id)
        if artifact is None:
            raise LookupError(f"Generation artifact {artifact_id} does not exist.")
        hydrated = hydrate_artifact(artifact)
        prompt_input = hydrated["prompt_input"]
        output = hydrated["output"]
        block_payloads = output.get("blocks") if isinstance(output, dict) else []
        if not isinstance(block_payloads, list):
            block_payloads = []

        target_keys = [key.strip() for key in (regenerate_block_keys or []) if key and key.strip()]
        if not target_keys:
            target_keys = [str(block["block_key"]) for block in block_payloads if block.get("block_type") == TemplateBlockType.AI_GENERATED.value]

        preserved_ai_blocks = {
            str(block["block_key"]): block
            for block in block_payloads
            if block.get("block_type") == TemplateBlockType.AI_GENERATED.value and str(block.get("block_key")) not in target_keys
        }
        return self.render_template(
            template_id=int(prompt_input["template_id"]),
            template_variant_id=int(prompt_input["template_variant_id"]),
            lead_id=int(prompt_input["lead_id"]),
            offer_profile_id=int(prompt_input["offer_profile_id"]) if prompt_input.get("offer_profile_id") else None,
            vertical_playbook_id=int(prompt_input["vertical_playbook_id"]) if prompt_input.get("vertical_playbook_id") else None,
            actor=actor,
            deterministic_mode=bool(prompt_input["deterministic_mode"]) if deterministic_mode is None else deterministic_mode,
            disabled_block_keys=string_list(prompt_input.get("disabled_block_keys")),
            generation_seed=int(prompt_input.get("generation_seed", 0)) + 1,
            source_artifact_id=artifact_id,
            preserved_ai_blocks=preserved_ai_blocks,
            regenerated_block_keys=target_keys,
        )

    def create_manual_edit_artifact(
        self,
        artifact_id: int,
        *,
        actor: str,
        edited_subject: str | None = None,
        edited_body: str | None = None,
    ) -> dict[str, object]:
        hydrated = self.get_artifact(artifact_id)
        prompt_input = hydrated["prompt_input"]
        output = hydrated["output"]
        block_payloads = output.get("blocks") if isinstance(output, dict) else []
        if not isinstance(block_payloads, list):
            block_payloads = []

        final_subject = string_value(edited_subject, hydrated["subject"]).strip() or hydrated["subject"]
        final_body = string_value(edited_body, hydrated["body"]).strip() or hydrated["body"]

        next_blocks: list[dict[str, object]] = []
        if edited_subject is not None:
            next_blocks.append(
                {
                    "block_key": "subject_manual_override",
                    "block_type": TemplateBlockType.STATIC.value,
                    "section": "subject",
                    "position": -1,
                    "source": "manual_override",
                    "rendered_text": final_subject,
                    "template_variant_id": hydrated["template_variant_id"],
                    "missing_variables": [],
                    "rules": {},
                }
            )
        if edited_body is not None:
            next_blocks.append(
                {
                    "block_key": "body_manual_override",
                    "block_type": TemplateBlockType.STATIC.value,
                    "section": "body",
                    "position": -1,
                    "source": "manual_override",
                    "rendered_text": final_body,
                    "template_variant_id": hydrated["template_variant_id"],
                    "missing_variables": [],
                    "rules": {},
                }
            )
        for block in block_payloads:
            section = string_value(block.get("section"))
            if section == "subject" and edited_subject is not None:
                continue
            if section == "body" and edited_body is not None:
                continue
            next_blocks.append(dict(block))

        offer_profile = self._load_offer_profile(int(prompt_input["offer_profile_id"])) if prompt_input.get("offer_profile_id") else None
        playbook = self._load_playbook(int(prompt_input["vertical_playbook_id"])) if prompt_input.get("vertical_playbook_id") else None
        risk_flags = validate_rendered_message(
            subject=final_subject,
            body=final_body,
            blocks=next_blocks,
            offer_profile=offer_profile,
            playbook=playbook,
        )
        validation_status = classify_validation_status(risk_flags)
        next_output = dict(output)
        next_output["subject"] = final_subject
        next_output["body"] = final_body
        next_output["blocks"] = next_blocks
        next_output["validation"] = {
            "status": validation_status.value,
            "risk_flags": risk_flags,
        }

        artifact_id_new = self.artifacts.create(
            GenerationArtifactCreate(
                kind="message_render_manual_edit",
                prompt_version=f"{PROMPT_VERSION}-manual-edit",
                prompt_input_json=json.dumps(prompt_input, sort_keys=True),
                output_text=f"Subject: {final_subject}\n\n{final_body}".strip(),
                output_json=json.dumps(next_output, sort_keys=True),
                validation_status=validation_status,
                risk_flags_json=json.dumps(risk_flags, sort_keys=True),
                model_name="manual_edit",
            ),
            template_id=int(prompt_input["template_id"]) if prompt_input.get("template_id") else None,
            template_variant_id=int(prompt_input["template_variant_id"]) if prompt_input.get("template_variant_id") else None,
            lead_id=int(prompt_input["lead_id"]) if prompt_input.get("lead_id") else None,
            source_artifact_id=artifact_id,
        )
        self._record_audit(
            entity_type=EntityType.GENERATION_ARTIFACT.value,
            entity_id=artifact_id_new,
            event_type="generation_artifact.manual_edit_created",
            actor=actor,
            summary=f"Manual edit artifact {artifact_id_new} created from artifact {artifact_id}.",
            payload={
                "source_artifact_id": artifact_id,
                "edited_subject": edited_subject is not None,
                "edited_body": edited_body is not None,
                "validation_status": validation_status.value,
            },
        )
        return self.get_artifact(artifact_id_new)

    def _store_blocks(self, template_id: int, template_variant_id: int | None, blocks_payload: list[object]) -> None:
        for position, block_payload in enumerate(blocks_payload):
            if not isinstance(block_payload, dict):
                raise TemplateStudioError("Template blocks must be objects.")
            block_type_value = string_value(block_payload.get("block_type"), TemplateBlockType.STATIC.value) or TemplateBlockType.STATIC.value
            try:
                block_type = TemplateBlockType(block_type_value)
            except ValueError as exc:
                raise TemplateStudioError(f"Unsupported block_type '{block_type_value}'.") from exc
            section = string_value(block_payload.get("section"), "body") or "body"
            if section not in {"subject", "body"}:
                raise TemplateStudioError(f"Unsupported block section '{section}'.")
            self.template_blocks.create(
                TemplateBlockCreate(
                    template_id=template_id,
                    template_variant_id=template_variant_id,
                    block_key=required_name(block_payload.get("block_key"), "Template block key"),
                    block_type=block_type,
                    section=section,
                    content=string_value(block_payload.get("content")),
                    fallback_content=string_value(block_payload.get("fallback_content")),
                    rules_json=json.dumps(dict_value(block_payload.get("rules")), sort_keys=True),
                    position=int(block_payload.get("position", position)),
                    is_required=bool(block_payload.get("is_required", True)),
                )
            )

    def _resolve_variant(self, template: dict[str, object], template_variant_id: int | None) -> dict[str, object]:
        variants = template["variants"] if isinstance(template.get("variants"), list) else []
        if template_variant_id is not None:
            for variant in variants:
                if int(variant["id"]) == template_variant_id:
                    return variant
            raise LookupError(f"Template variant {template_variant_id} does not belong to template {template['id']}.")
        for variant in variants:
            if bool(variant["is_default"]):
                return variant
        if variants:
            return variants[0]
        raise TemplateStudioError(f"Template {template['id']} has no variants.")

    def _load_lead_context(self, lead_id: int) -> dict[str, dict[str, object]]:
        lead = self.leads.get(lead_id)
        if lead is None:
            raise LookupError(f"Lead {lead_id} does not exist.")
        company = self.companies.get(int(lead["company_id"])) if lead["company_id"] else None
        return {
            "lead": {
                "id": int(lead["id"]),
                "first_name": string_value(lead.get("first_name")),
                "last_name": string_value(lead.get("last_name")),
                "full_name": string_value(lead.get("full_name")),
                "email": string_value(lead.get("email")),
                "title": string_value(lead.get("title")),
                "city": string_value(lead.get("city")),
                "state": string_value(lead.get("state")),
                "country": string_value(lead.get("country")),
            },
            "company": {
                "id": int(company["id"]) if company is not None else None,
                "name": string_value(company.get("name") if company is not None else lead.get("company_name_snapshot")),
                "domain": string_value(company.get("domain") if company is not None else lead.get("company_domain_snapshot")),
                "vertical": string_value(company.get("vertical") if company is not None else ""),
                "website": string_value(company.get("website") if company is not None else ""),
            },
        }

    def _load_offer_profile(self, offer_profile_id: int | None) -> dict[str, object] | None:
        if offer_profile_id is None:
            return None
        profile = self.offer_profiles.get(offer_profile_id)
        if profile is None:
            raise LookupError(f"Offer profile {offer_profile_id} does not exist.")
        return hydrate_offer_profile(profile)

    def _load_playbook(self, vertical_playbook_id: int | None) -> dict[str, object] | None:
        if vertical_playbook_id is None:
            return None
        playbook = self.playbooks.get(vertical_playbook_id)
        if playbook is None:
            raise LookupError(f"Vertical playbook {vertical_playbook_id} does not exist.")
        return hydrate_playbook(playbook)

    def _render_block(
        self,
        block: dict[str, object],
        *,
        context: dict[str, dict[str, object]],
        deterministic_mode: bool,
        generation_seed: int,
        disabled_block_keys: set[str],
        preserved_ai_blocks: dict[str, dict[str, object]],
    ) -> tuple[list[dict[str, object]], dict[str, object] | None]:
        risk_flags: list[dict[str, object]] = []
        rules = dict_value(block.get("rules"))
        block_type = TemplateBlockType(str(block["block_type"]))
        block_key = str(block["block_key"])

        if block_type == TemplateBlockType.CONDITIONAL and not should_include_conditional_block(rules, context):
            return risk_flags, None

        source = block_type.value
        missing_variables: list[str] = []
        if block_type == TemplateBlockType.STATIC:
            rendered_text = string_value(block.get("content"))
        elif block_type == TemplateBlockType.MERGED:
            rendered_text, missing_variables = render_text_template(string_value(block.get("content")), context)
            source = "merged"
        elif block_type == TemplateBlockType.CONDITIONAL:
            rendered_text, missing_variables = render_text_template(string_value(block.get("content")), context)
            source = "conditional"
        else:
            if block_key in preserved_ai_blocks:
                rendered_text = string_value(preserved_ai_blocks[block_key].get("rendered_text"))
                source = "preserved_ai"
            elif deterministic_mode or block_key in disabled_block_keys:
                fallback_text = string_value(block.get("fallback_content")) or string_value(block.get("content"))
                rendered_text, missing_variables = render_text_template(fallback_text, context)
                rendered_text = truncate_words(rendered_text, int(rules.get("max_words", 22 if block["section"] == "body" else 8)))
                source = "deterministic_fallback"
            else:
                instruction, instruction_missing = render_text_template(string_value(block.get("content")), context)
                missing_variables.extend(instruction_missing)
                rendered_text = generate_ai_text(block, context, instruction, generation_seed)
                rendered_text = truncate_words(rendered_text, int(rules.get("max_words", 22 if block["section"] == "body" else 8)))
                source = "local_ai"

        rendered_text = normalize_rendered_text(rendered_text, section=str(block["section"]))
        for token in sorted(set(missing_variables)):
            if rendered_text or bool(block["is_required"]):
                risk_flags.append(
                    {
                        "code": "missing_variable",
                        "severity": "warning",
                        "block_key": block_key,
                        "message": f"Missing variable '{token}' while rendering block '{block_key}'.",
                    }
                )

        if not rendered_text:
            if bool(block["is_required"]):
                risk_flags.append(
                    {
                        "code": "empty_required_block",
                        "severity": "error",
                        "block_key": block_key,
                        "message": f"Required block '{block_key}' rendered empty.",
                    }
                )
            return risk_flags, None

        return risk_flags, {
            "block_key": block_key,
            "block_type": block_type.value,
            "section": str(block["section"]),
            "position": int(block["position"]),
            "source": source,
            "rendered_text": rendered_text,
            "template_variant_id": block["template_variant_id"],
            "missing_variables": sorted(set(missing_variables)),
            "rules": rules,
        }

    def _record_audit(
        self,
        *,
        entity_type: str,
        entity_id: int,
        event_type: str,
        actor: str,
        summary: str,
        payload: Mapping[str, object],
    ) -> None:
        self.audit_events.record(
            AuditEventCreate(
                entity_type=entity_type,
                entity_id=entity_id,
                event_type=event_type,
                actor=actor,
                summary=summary,
                payload_json=json.dumps(dict(payload), sort_keys=True),
            ),
            created_at=utc_now(),
        )


def hydrate_template(template: Mapping[str, object]) -> dict[str, object]:
    return {
        "id": int(template["id"]),
        "name": string_value(template.get("name")),
        "description": string_value(template.get("description")),
        "channel": string_value(template.get("channel")),
        "is_active": bool(template.get("is_active")),
        "created_at": string_value(template.get("created_at")),
        "updated_at": string_value(template.get("updated_at")),
        "variant_count": int(template.get("variant_count", 0)),
        "block_count": int(template.get("block_count", 0)),
    }


def hydrate_variant(variant: Mapping[str, object]) -> dict[str, object]:
    return {
        "id": int(variant["id"]),
        "template_id": int(variant["template_id"]),
        "name": string_value(variant.get("name")),
        "variant_label": string_value(variant.get("variant_label")),
        "is_default": bool(variant.get("is_default")),
        "created_at": string_value(variant.get("created_at")),
        "updated_at": string_value(variant.get("updated_at")),
    }


def hydrate_block(block: Mapping[str, object]) -> dict[str, object]:
    return {
        "id": int(block["id"]),
        "template_id": int(block["template_id"]),
        "template_variant_id": int(block["template_variant_id"]) if block.get("template_variant_id") is not None else None,
        "block_key": string_value(block.get("block_key")),
        "block_type": string_value(block.get("block_type")),
        "section": string_value(block.get("section"), "body") or "body",
        "content": string_value(block.get("content")),
        "fallback_content": string_value(block.get("fallback_content")),
        "rules": parse_json_object(block.get("rules_json")),
        "position": int(block.get("position", 0)),
        "is_required": bool(block.get("is_required")),
        "created_at": string_value(block.get("created_at")),
        "updated_at": string_value(block.get("updated_at")),
    }


def hydrate_offer_profile(profile: Mapping[str, object]) -> dict[str, object]:
    return {
        "id": int(profile["id"]),
        "name": string_value(profile.get("name")),
        "description": string_value(profile.get("description")),
        "target_verticals": parse_json_list(profile.get("target_verticals_json")),
        "target_pains": parse_json_list(profile.get("target_pains_json")),
        "value_proposition": string_value(profile.get("value_proposition")),
        "standard_cta": string_value(profile.get("standard_cta")),
        "booking_link_id": int(profile["booking_link_id"]) if profile.get("booking_link_id") is not None else None,
        "allowed_claims": parse_json_list(profile.get("allowed_claims_json")),
        "disallowed_claims": parse_json_list(profile.get("disallowed_claims_json")),
        "pricing_framing_snippets": parse_json_list(profile.get("pricing_framing_snippets_json")),
        "objection_handling_snippets": parse_json_list(profile.get("objection_handling_snippets_json")),
        "created_at": string_value(profile.get("created_at")),
        "updated_at": string_value(profile.get("updated_at")),
    }


def hydrate_playbook(playbook: Mapping[str, object]) -> dict[str, object]:
    return {
        "id": int(playbook["id"]),
        "name": string_value(playbook.get("name")),
        "target_pains": parse_json_list(playbook.get("target_pains_json")),
        "acceptable_language": parse_json_list(playbook.get("acceptable_language_json")),
        "disallowed_language": parse_json_list(playbook.get("disallowed_language_json")),
        "personalization_strategy": string_value(playbook.get("personalization_strategy")),
        "tone_profile": string_value(playbook.get("tone_profile")),
        "sample_subject_patterns": parse_json_list(playbook.get("sample_subject_patterns_json")),
        "standard_objections": parse_json_list(playbook.get("standard_objections_json")),
        "escalation_rules": parse_json_list(playbook.get("escalation_rules_json")),
        "created_at": string_value(playbook.get("created_at")),
        "updated_at": string_value(playbook.get("updated_at")),
    }


def hydrate_artifact(artifact: Mapping[str, object]) -> dict[str, object]:
    output = parse_json_object(artifact.get("output_json"))
    prompt_input = parse_json_object(artifact.get("prompt_input_json"))
    risk_flags = parse_json_list(artifact.get("risk_flags_json"))
    return {
        "id": int(artifact["id"]),
        "kind": string_value(artifact.get("kind")),
        "prompt_version": string_value(artifact.get("prompt_version")),
        "validation_status": string_value(artifact.get("validation_status")),
        "risk_flags": risk_flags,
        "model_name": string_value(artifact.get("model_name")),
        "template_id": int(artifact["template_id"]) if artifact.get("template_id") is not None else None,
        "template_name": string_value(artifact.get("template_name")),
        "template_variant_id": int(artifact["template_variant_id"]) if artifact.get("template_variant_id") is not None else None,
        "template_variant_name": string_value(artifact.get("template_variant_name")),
        "lead_id": int(artifact["lead_id"]) if artifact.get("lead_id") is not None else None,
        "lead_name": string_value(artifact.get("lead_name")),
        "source_artifact_id": int(artifact["source_artifact_id"]) if artifact.get("source_artifact_id") is not None else None,
        "subject": string_value(output.get("subject")),
        "body": string_value(output.get("body")),
        "output": output,
        "prompt_input": prompt_input,
        "output_text": string_value(artifact.get("output_text")),
        "created_at": string_value(artifact.get("created_at")),
        "updated_at": string_value(artifact.get("updated_at")),
    }


def build_render_context(
    lead_context: Mapping[str, Mapping[str, object]],
    offer_profile: Mapping[str, object] | None,
    playbook: Mapping[str, object] | None,
) -> dict[str, dict[str, object]]:
    return {
        "lead": dict(lead_context["lead"]),
        "company": dict(lead_context["company"]),
        "offer": {
            "id": offer_profile.get("id") if offer_profile else None,
            "name": string_value(offer_profile.get("name") if offer_profile else ""),
            "value_proposition": string_value(offer_profile.get("value_proposition") if offer_profile else ""),
            "standard_cta": string_value(offer_profile.get("standard_cta") if offer_profile else ""),
            "target_verticals": string_list(offer_profile.get("target_verticals") if offer_profile else []),
            "target_pains": string_list(offer_profile.get("target_pains") if offer_profile else []),
            "allowed_claims": string_list(offer_profile.get("allowed_claims") if offer_profile else []),
            "disallowed_claims": string_list(offer_profile.get("disallowed_claims") if offer_profile else []),
        },
        "playbook": {
            "id": playbook.get("id") if playbook else None,
            "name": string_value(playbook.get("name") if playbook else ""),
            "target_pains": string_list(playbook.get("target_pains") if playbook else []),
            "acceptable_language": string_list(playbook.get("acceptable_language") if playbook else []),
            "disallowed_language": string_list(playbook.get("disallowed_language") if playbook else []),
            "tone_profile": string_value(playbook.get("tone_profile") if playbook else ""),
            "sample_subject_patterns": string_list(playbook.get("sample_subject_patterns") if playbook else []),
            "personalization_strategy": string_value(playbook.get("personalization_strategy") if playbook else ""),
        },
    }


def resolve_render_blocks(block_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    resolved: dict[str, dict[str, object]] = {}
    for row in block_rows:
        hydrated = hydrate_block(row)
        resolved[hydrated["block_key"]] = hydrated
    return sorted(resolved.values(), key=block_sort_key)


def block_sort_key(block: Mapping[str, object]) -> tuple[int, int, str]:
    section_order = 0 if str(block.get("section")) == "subject" else 1
    return (section_order, int(block.get("position", 0)), string_value(block.get("block_key")))


def parse_json_list(value: object) -> list[object]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def parse_json_object(value: object) -> dict[str, object]:
    if value in (None, ""):
        return {}
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def render_text_template(template_text: str, context: Mapping[str, object]) -> tuple[str, list[str]]:
    missing: list[str] = []

    def replace(match: re.Match[str]) -> str:
        token = match.group(1)
        value = lookup_context_value(context, token)
        if value in (None, ""):
            missing.append(token)
            return ""
        if isinstance(value, list):
            return ", ".join(str(item) for item in value if item not in (None, ""))
        return str(value)

    rendered = TOKEN_PATTERN.sub(replace, template_text or "")
    return normalize_spacing(rendered), missing


def lookup_context_value(context: Mapping[str, object], token: str) -> object:
    current: object = context
    for segment in token.split("."):
        if not isinstance(current, Mapping) or segment not in current:
            return None
        current = current[segment]
    return current


def should_include_conditional_block(rules: Mapping[str, object], context: Mapping[str, object]) -> bool:
    if_present = rules.get("if_present")
    if isinstance(if_present, str):
        if lookup_context_value(context, if_present) in (None, ""):
            return False
    elif isinstance(if_present, list):
        for token in if_present:
            if lookup_context_value(context, str(token)) in (None, ""):
                return False

    when = rules.get("when")
    if isinstance(when, Mapping):
        for token, expected in when.items():
            actual = lookup_context_value(context, str(token))
            if normalize_casefold(actual) != normalize_casefold(expected):
                return False
    return True


def generate_ai_text(block: Mapping[str, object], context: Mapping[str, object], instruction: str, generation_seed: int) -> str:
    company_name = string_value(lookup_context_value(context, "company.name"), "your team") or "your team"
    lead_first_name = string_value(lookup_context_value(context, "lead.first_name"), company_name) or company_name
    tone = string_value(lookup_context_value(context, "playbook.tone_profile"), "clear") or "clear"
    pain = first_non_empty(
        string_list(lookup_context_value(context, "playbook.target_pains")),
        string_list(lookup_context_value(context, "offer.target_pains")),
        ["slower outbound follow-up"],
    )
    value_prop = string_value(lookup_context_value(context, "offer.value_proposition"), "a tighter outbound workflow") or "a tighter outbound workflow"
    focus = summarize_instruction(instruction) or value_prop
    section = string_value(block.get("section"), "body") or "body"
    risky_focus = bool(re.search(r"\b\d+%|\bguarante(?:e|ed|es)\b|\bpromise\b|\bdouble\b|\btriple\b", focus.casefold()))

    if section == "subject":
        subject_patterns = string_list(lookup_context_value(context, "playbook.sample_subject_patterns"))
        candidates = [
            f"{company_name}: {focus}",
            f"{lead_first_name}, {focus}",
            f"{company_name} and {pain}",
            f"Quick idea for {company_name}",
        ]
        for pattern in subject_patterns:
            rendered_pattern, _ = render_text_template(str(pattern), context)
            if rendered_pattern:
                candidates.append(rendered_pattern)
    else:
        if risky_focus:
            return f"Saw {company_name} and thought {focus} could help with {pain}."
        candidates = [
            f"Saw {company_name} and thought {focus} could help with {pain}.",
            f"{company_name} looked like a fit for a {tone} note about {focus}.",
            f"Teams like {company_name} often lose time to {pain}; {focus} felt relevant.",
            f"One reason I reached out: {focus} might make {value_prop} easier for {company_name}.",
        ]

    index = stable_choice_index(
        seed_parts=[
            block.get("block_key"),
            section,
            instruction,
            company_name,
            lead_first_name,
            value_prop,
            str(generation_seed),
        ],
        total=len(candidates),
    )
    return candidates[index]


def validate_rendered_message(
    *,
    subject: str,
    body: str,
    blocks: list[dict[str, object]],
    offer_profile: Mapping[str, object] | None,
    playbook: Mapping[str, object] | None,
) -> list[dict[str, object]]:
    full_text = "\n".join(part for part in [subject, body] if part).strip()
    full_text_lower = full_text.casefold()
    risk_flags: list[dict[str, object]] = []

    banned_phrases = []
    if offer_profile is not None:
        banned_phrases.extend(string_list(offer_profile.get("disallowed_claims")))
    if playbook is not None:
        banned_phrases.extend(string_list(playbook.get("disallowed_language")))
    for block in blocks:
        banned_phrases.extend(string_list(dict_value(block.get("rules")).get("banned_phrases")))

    allowed_claims = [item.casefold() for item in string_list(offer_profile.get("allowed_claims") if offer_profile else [])]
    for phrase in sorted({item.strip() for item in banned_phrases if item and item.strip()}):
        if phrase.casefold() in full_text_lower:
            risk_flags.append(
                {
                    "code": "banned_phrase",
                    "severity": "error",
                    "message": f"Rendered output contains banned phrase '{phrase}'.",
                }
            )

    unsupported_patterns = [
        (r"\bguarante(?:e|ed|es)\b", "guarantee"),
        (r"\bpromise\b", "promise language"),
        (r"\b\d+(?:\.\d+)?%", "percentage claim"),
        (r"\bdouble\b|\btriple\b", "growth multiplier"),
        (r"\bcase stud(?:y|ies)\b", "case study reference"),
    ]
    for pattern, label in unsupported_patterns:
        if not re.search(pattern, full_text_lower):
            continue
        if any(re.search(pattern, claim) for claim in allowed_claims):
            continue
        risk_flags.append(
            {
                "code": "unsupported_claim",
                "severity": "error",
                "message": f"Rendered output includes unsupported claim category '{label}'.",
            }
        )

    if not subject:
        risk_flags.append(
            {
                "code": "missing_subject",
                "severity": "error",
                "message": "Rendered output is missing a subject line.",
            }
        )
    if not body:
        risk_flags.append(
            {
                "code": "missing_body",
                "severity": "error",
                "message": "Rendered output is missing a message body.",
            }
        )
    return risk_flags


def classify_validation_status(risk_flags: list[dict[str, object]]) -> GenerationValidationStatus:
    if any(flag.get("severity") == "error" for flag in risk_flags):
        return GenerationValidationStatus.BLOCKED
    if risk_flags:
        return GenerationValidationStatus.WARNING
    return GenerationValidationStatus.PASSED


def normalize_rendered_text(value: str, *, section: str) -> str:
    if section == "subject":
        return " ".join(value.split()).strip()
    lines = [line.strip() for line in value.splitlines()]
    return "\n".join(line for line in lines if line).strip()


def normalize_spacing(value: str) -> str:
    return re.sub(r"[ \t]+", " ", value).strip()


def summarize_instruction(instruction: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9\s%-]", " ", instruction or "")
    words = [word for word in cleaned.split() if word]
    return " ".join(words[:8]).strip()


def truncate_words(text: str, max_words: int) -> str:
    if max_words <= 0:
        return ""
    words = text.split()
    if len(words) <= max_words:
        return text.strip()
    return " ".join(words[:max_words]).strip()


def stable_choice_index(seed_parts: list[object], total: int) -> int:
    if total <= 0:
        return 0
    digest = hashlib.sha256("|".join(str(part) for part in seed_parts).encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % total


def string_value(value: object, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in re.split(r"[\n,]", value) if item.strip()]
    return [str(value).strip()] if str(value).strip() else []


def dict_value(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def required_name(value: object, label: str) -> str:
    text = string_value(value).strip()
    if not text:
        raise TemplateStudioError(f"{label} is required.")
    return text


def first_non_empty(*groups: list[str]) -> str:
    for group in groups:
        for item in group:
            if item:
                return item
    return ""


def normalize_casefold(value: object) -> str:
    return string_value(value).casefold()
