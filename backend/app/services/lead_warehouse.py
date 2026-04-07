"""Lead ingestion, normalization, dedupe, tagging, and saved-filter services."""

from __future__ import annotations

import csv
from difflib import SequenceMatcher
import io
import json
import re
import zipfile
from xml.etree import ElementTree

from backend.app.adapters.apollo import ApolloLeadAdapter
from backend.app.core.logging import utc_now
from backend.app.domain.enums import EntityType, ImportJobStatus, ImportRowResolution
from backend.app.domain.models import AuditEventCreate, ImportJobCreate, LeadCreate, SavedFilterCreate
from backend.app.repositories.audit import AuditEventRepository
from backend.app.repositories.leads import CompanyRepository, LeadRepository
from backend.app.repositories.warehouse import ImportJobRepository, ListRepository, SavedFilterRepository, TagRepository


class LeadImportError(ValueError):
    """Raised when an import payload cannot be parsed into rows."""


class LeadWarehouseService:
    """Own lead ingestion, dedupe, list membership, and saved-filter behavior."""

    def __init__(self, connection) -> None:
        self.connection = connection
        self.audit_events = AuditEventRepository(connection)
        self.companies = CompanyRepository(connection)
        self.import_jobs = ImportJobRepository(connection)
        self.leads = LeadRepository(connection)
        self.lists = ListRepository(connection)
        self.saved_filters = SavedFilterRepository(connection)
        self.tags = TagRepository(connection)

    def import_csv_text(self, csv_text: str, actor: str, *, list_name: str = "Imported CSV", source: str = "csv") -> dict[str, object]:
        try:
            rows = parse_csv_text(csv_text)
        except LeadImportError as exc:
            return self._record_parse_failure(
                source=source,
                import_format="csv",
                actor=actor,
                list_name=list_name,
                error_message=str(exc),
            )
        return self._run_import(
            source=source,
            import_format="csv",
            actor=actor,
            list_name=list_name,
            rows=rows,
        )

    def import_xlsx_bytes(self, xlsx_bytes: bytes, actor: str, *, list_name: str = "Imported XLSX", source: str = "xlsx") -> dict[str, object]:
        try:
            rows = parse_xlsx_bytes(xlsx_bytes)
        except LeadImportError as exc:
            return self._record_parse_failure(
                source=source,
                import_format="xlsx",
                actor=actor,
                list_name=list_name,
                error_message=str(exc),
            )
        return self._run_import(
            source=source,
            import_format="xlsx",
            actor=actor,
            list_name=list_name,
            rows=rows,
        )

    def import_apollo_people(self, records: list[dict[str, object]], actor: str, *, list_name: str = "Apollo Import") -> dict[str, object]:
        normalized_rows = ApolloLeadAdapter.normalize_people_records(records)
        return self._run_import(
            source="apollo",
            import_format="apollo_people",
            actor=actor,
            list_name=list_name,
            rows=normalized_rows,
        )

    def create_manual_lead(self, payload: dict[str, object], actor: str, *, list_name: str = "Manual Leads") -> dict[str, object]:
        return self._run_import(
            source="manual",
            import_format="manual",
            actor=actor,
            list_name=list_name,
            rows=[payload],
        )

    def save_filter(self, name: str, filters: dict[str, object], description: str = "") -> dict[str, object]:
        saved_filter_id = self.saved_filters.create(
            SavedFilterCreate(
                name=name,
                description=description,
                filter_json=json.dumps(filters, sort_keys=True),
            )
        )
        saved_filter = self.saved_filters.get(saved_filter_id)
        if saved_filter is None:
            raise LookupError(f"Saved filter {saved_filter_id} was created but could not be reloaded.")
        return saved_filter

    def list_saved_filters(self) -> list[dict[str, object]]:
        return self.saved_filters.list_all()

    def list_leads(self, filters: dict[str, object] | None = None, limit: int = 50) -> list[dict[str, object]]:
        resolved_filters = dict(filters or {})
        saved_filter_id = resolved_filters.pop("saved_filter_id", None)
        if saved_filter_id:
            saved_filter = self.saved_filters.get(int(saved_filter_id))
            if saved_filter is not None:
                saved_filters = json.loads(saved_filter["filter_json"])
                resolved_filters.update(saved_filters)
        leads = self.leads.list_filtered(resolved_filters, limit=limit)
        for lead in leads:
            lead["tags"] = [tag["name"] for tag in self.tags.list_for_entity(EntityType.LEAD.value, int(lead["id"]))]
        return leads

    def assign_tag(self, lead_id: int, tag_name: str, color: str = "") -> list[dict[str, object]]:
        tag_id = self.tags.get_or_create(tag_name, color=color)
        self.tags.assign(tag_id, EntityType.LEAD.value, lead_id)
        return self.tags.list_for_entity(EntityType.LEAD.value, lead_id)

    def get_summary(self) -> dict[str, object]:
        return {
            "lead_count": self.leads.count_all(),
            "company_count": self.companies.count_all(),
            "list_count": self.lists.count_all(),
            "saved_filter_count": len(self.saved_filters.list_all()),
            "recent_imports": self.import_jobs.list_recent(limit=8),
            "lead_preview": self.list_leads(limit=10),
            "lists": self.lists.list_all(),
            "saved_filters": self.saved_filters.list_all(),
        }

    def _run_import(
        self,
        *,
        source: str,
        import_format: str,
        actor: str,
        list_name: str,
        rows: list[dict[str, object]],
    ) -> dict[str, object]:
        timestamp = utc_now()
        list_id = self.lists.get_or_create(list_name) if list_name else None
        import_job_id = self.import_jobs.create(
            ImportJobCreate(
                source=source,
                import_format=import_format,
                requested_by=actor,
                list_id=list_id,
                status=ImportJobStatus.RUNNING,
            ),
            created_at=timestamp,
        )
        summary = build_empty_import_summary(import_job_id, source, import_format, list_id)

        try:
            for row_number, raw_row in enumerate(rows, start=1):
                summary["total_read"] += 1
                normalized = normalize_import_row(raw_row, source=source)
                if normalized["error"]:
                    self._record_import_outcome(
                        import_job_id,
                        row_number=row_number,
                        resolution=ImportRowResolution.SKIPPED,
                        dedupe_rule="validation_failed",
                        raw_row=raw_row,
                        normalized=normalized,
                        message=str(normalized["error"]),
                        actor=actor,
                        counters=summary,
                    )
                    continue

                company_id = self._resolve_company_id(normalized)
                resolution = self._resolve_dedupe(normalized)
                if resolution["resolution"] == ImportRowResolution.MANUAL_REVIEW_REQUIRED:
                    self._record_import_outcome(
                        import_job_id,
                        row_number=row_number,
                        resolution=ImportRowResolution.MANUAL_REVIEW_REQUIRED,
                        dedupe_rule=str(resolution["dedupe_rule"]),
                        raw_row=raw_row,
                        normalized=normalized,
                        message=str(resolution["message"]),
                        actor=actor,
                        counters=summary,
                        existing_lead_id=resolution["existing_lead_id"],
                        company_id=company_id,
                    )
                    continue

                if resolution["resolution"] == ImportRowResolution.CONFLICTING:
                    self._record_import_outcome(
                        import_job_id,
                        row_number=row_number,
                        resolution=ImportRowResolution.CONFLICTING,
                        dedupe_rule=str(resolution["dedupe_rule"]),
                        raw_row=raw_row,
                        normalized=normalized,
                        message=str(resolution["message"]),
                        actor=actor,
                        counters=summary,
                        existing_lead_id=resolution["existing_lead_id"],
                        company_id=company_id,
                    )
                    continue

                normalized["company_id"] = company_id
                if resolution["resolution"] == ImportRowResolution.MERGED:
                    lead_id = int(resolution["existing_lead_id"])
                    self._merge_into_existing_lead(lead_id, normalized)
                    event_type = "lead.merged_from_import"
                    event_summary = f"Lead merged from {source} import."
                else:
                    lead_id = self.leads.create(build_lead_create(normalized))
                    event_type = "lead.imported"
                    event_summary = f"Lead imported from {source}."

                if list_id is not None:
                    self.lists.add_membership(list_id, lead_id)

                self._record_import_outcome(
                    import_job_id,
                    row_number=row_number,
                    resolution=resolution["resolution"],
                    dedupe_rule=str(resolution["dedupe_rule"]),
                    raw_row=raw_row,
                    normalized=normalized,
                    message=str(resolution["message"]),
                    actor=actor,
                    counters=summary,
                    lead_id=lead_id,
                    existing_lead_id=resolution["existing_lead_id"],
                    company_id=company_id,
                    audit_event_type=event_type,
                    audit_summary=event_summary,
                )
        except LeadImportError as exc:
            return self._finalize_failed_import(import_job_id, summary, str(exc))
        except Exception as exc:
            return self._finalize_failed_import(import_job_id, summary, f"Unexpected import failure: {exc}")

        completed_at = utc_now()
        summary["status"] = ImportJobStatus.COMPLETED.value
        self.import_jobs.update_job(
            import_job_id,
            status=ImportJobStatus.COMPLETED,
            total_read=int(summary["total_read"]),
            inserted_count=int(summary["inserted"]),
            merged_count=int(summary["merged"]),
            skipped_count=int(summary["skipped"]),
            conflicting_count=int(summary["conflicting"]),
            manual_review_required_count=int(summary["manual_review_required"]),
            summary_json=json.dumps(summary, sort_keys=True),
            completed_at=completed_at,
            updated_at=completed_at,
        )
        self.audit_events.record(
            AuditEventCreate(
                entity_type=EntityType.IMPORT_JOB.value,
                entity_id=import_job_id,
                event_type="import.completed",
                actor=actor,
                summary=f"{source} import completed.",
                payload_json=json.dumps(summary, sort_keys=True),
            ),
            created_at=completed_at,
        )
        return summary

    def _finalize_failed_import(self, import_job_id: int, summary: dict[str, object], error_message: str) -> dict[str, object]:
        completed_at = utc_now()
        summary["status"] = ImportJobStatus.FAILED.value
        summary["error"] = error_message
        self.import_jobs.update_job(
            import_job_id,
            status=ImportJobStatus.FAILED,
            total_read=int(summary["total_read"]),
            inserted_count=int(summary["inserted"]),
            merged_count=int(summary["merged"]),
            skipped_count=int(summary["skipped"]),
            conflicting_count=int(summary["conflicting"]),
            manual_review_required_count=int(summary["manual_review_required"]),
            summary_json=json.dumps(summary, sort_keys=True),
            error_message=error_message,
            completed_at=completed_at,
            updated_at=completed_at,
        )
        return summary

    def _record_parse_failure(
        self,
        *,
        source: str,
        import_format: str,
        actor: str,
        list_name: str,
        error_message: str,
    ) -> dict[str, object]:
        timestamp = utc_now()
        list_id = self.lists.get_or_create(list_name) if list_name else None
        import_job_id = self.import_jobs.create(
            ImportJobCreate(
                source=source,
                import_format=import_format,
                requested_by=actor,
                list_id=list_id,
                status=ImportJobStatus.FAILED,
            ),
            created_at=timestamp,
        )
        summary = build_empty_import_summary(import_job_id, source, import_format, list_id)
        summary["status"] = ImportJobStatus.FAILED.value
        summary["error"] = error_message
        self.import_jobs.update_job(
            import_job_id,
            status=ImportJobStatus.FAILED,
            total_read=0,
            inserted_count=0,
            merged_count=0,
            skipped_count=0,
            conflicting_count=0,
            manual_review_required_count=0,
            summary_json=json.dumps(summary, sort_keys=True),
            error_message=error_message,
            completed_at=timestamp,
            updated_at=timestamp,
        )
        self.audit_events.record(
            AuditEventCreate(
                entity_type=EntityType.IMPORT_JOB.value,
                entity_id=import_job_id,
                event_type="import.failed",
                actor=actor,
                summary=f"{source} import failed before processing.",
                payload_json=json.dumps(summary, sort_keys=True),
            ),
            created_at=timestamp,
        )
        return summary

    def _resolve_company_id(self, normalized: dict[str, object]) -> int | None:
        domain = str(normalized.get("company_domain_snapshot") or "")
        company_name = str(normalized.get("company_name_snapshot") or "")
        existing_company = None
        if domain:
            existing_company = self.companies.find_by_domain(domain)
        if existing_company is None and company_name:
            existing_company = self.companies.find_by_name(company_name)
        if existing_company is not None:
            company_updates = fill_missing_fields(
                existing_company,
                {
                    "website": str(normalized.get("website") or ""),
                    "phone": str(normalized.get("company_phone") or ""),
                    "city": str(normalized.get("city") or ""),
                    "state": str(normalized.get("state") or ""),
                    "country": str(normalized.get("country") or ""),
                },
            )
            self.companies.update_fields(int(existing_company["id"]), company_updates)
            return int(existing_company["id"])
        if not company_name:
            return None
        return self.companies.create(
            source=str(normalized["source"]),
            external_source_id=normalized.get("company_external_source_id") or None,
            name=company_name,
            domain=domain or None,
            website=str(normalized.get("website") or ""),
            phone=str(normalized.get("company_phone") or ""),
            city=str(normalized.get("city") or ""),
            state=str(normalized.get("state") or ""),
            country=str(normalized.get("country") or ""),
        )

    def _resolve_dedupe(self, normalized: dict[str, object]) -> dict[str, object]:
        source = str(normalized["source"])
        external_source_id = str(normalized.get("external_source_id") or "")
        email = str(normalized.get("email") or "")
        full_name = str(normalized.get("full_name") or "")
        company_name = str(normalized.get("company_name_snapshot") or "")
        company_domain = str(normalized.get("company_domain_snapshot") or "")

        if external_source_id:
            existing_by_source = self.leads.find_by_source_external_id(source, external_source_id)
            if existing_by_source is not None:
                if email and existing_by_source["email"] and str(existing_by_source["email"]).lower() != email.lower():
                    return {
                        "resolution": ImportRowResolution.CONFLICTING,
                        "existing_lead_id": int(existing_by_source["id"]),
                        "dedupe_rule": "source_external_id_conflict",
                        "message": "External source ID matched an existing lead with a different email.",
                    }
                return {
                    "resolution": ImportRowResolution.MERGED,
                    "existing_lead_id": int(existing_by_source["id"]),
                    "dedupe_rule": "source_external_id",
                    "message": "Matched by source and external source ID.",
                }

        if email:
            existing_by_email = self.leads.find_by_email(email)
            if existing_by_email is not None:
                return {
                    "resolution": ImportRowResolution.MERGED,
                    "existing_lead_id": int(existing_by_email["id"]),
                    "dedupe_rule": "exact_email",
                    "message": "Matched by exact email.",
                }

        if full_name and company_name:
            exact_name_company = self.leads.find_exact_name_company_match(full_name, company_name)
            if exact_name_company is not None:
                return {
                    "resolution": ImportRowResolution.MERGED,
                    "existing_lead_id": int(exact_name_company["id"]),
                    "dedupe_rule": "exact_name_company",
                    "message": "Matched by exact name and company.",
                }

        if full_name and (company_name or company_domain):
            for candidate in self.leads.list_company_candidates(company_name, company_domain):
                candidate_name = str(candidate["full_name"] or "")
                if not candidate_name:
                    continue
                similarity = SequenceMatcher(None, canonicalize_name(candidate_name), canonicalize_name(full_name)).ratio()
                if similarity >= 0.81:
                    return {
                        "resolution": ImportRowResolution.MANUAL_REVIEW_REQUIRED,
                        "existing_lead_id": int(candidate["id"]),
                        "dedupe_rule": "name_company_similarity",
                        "message": f"Similar lead detected for manual review (similarity {similarity:.2f}).",
                    }

        return {
            "resolution": ImportRowResolution.INSERTED,
            "existing_lead_id": None,
            "dedupe_rule": "no_duplicate_match",
            "message": "Inserted as a new lead.",
        }

    def _merge_into_existing_lead(self, lead_id: int, normalized: dict[str, object]) -> None:
        existing = self.leads.get(lead_id)
        if existing is None:
            raise LookupError(f"Lead {lead_id} does not exist for merge.")
        updates = fill_missing_fields(
            existing,
            {
                "first_name": normalized.get("first_name", ""),
                "last_name": normalized.get("last_name", ""),
                "full_name": normalized.get("full_name", ""),
                "email": normalized.get("email"),
                "phone": normalized.get("phone", ""),
                "title": normalized.get("title", ""),
                "linkedin_url": normalized.get("linkedin_url", ""),
                "company_id": normalized.get("company_id"),
                "company_name_snapshot": normalized.get("company_name_snapshot", ""),
                "company_domain_snapshot": normalized.get("company_domain_snapshot", ""),
                "city": normalized.get("city", ""),
                "state": normalized.get("state", ""),
                "country": normalized.get("country", ""),
                "external_source_id": normalized.get("external_source_id"),
            },
        )
        self.leads.update_fields(lead_id, updates)

    def _record_import_outcome(
        self,
        import_job_id: int,
        *,
        row_number: int,
        resolution: ImportRowResolution,
        dedupe_rule: str,
        raw_row: dict[str, object],
        normalized: dict[str, object],
        message: str,
        actor: str,
        counters: dict[str, object],
        lead_id: int | None = None,
        company_id: int | None = None,
        existing_lead_id: int | None = None,
        audit_event_type: str | None = None,
        audit_summary: str | None = None,
    ) -> None:
        counter_key = resolution.value
        counters[counter_key] += 1
        self.import_jobs.record_row(
            import_job_id,
            row_number=row_number,
            resolution=resolution,
            dedupe_rule=dedupe_rule,
            lead_id=lead_id,
            company_id=company_id,
            existing_lead_id=existing_lead_id,
            raw_payload_json=json.dumps(raw_row, sort_keys=True),
            normalized_payload_json=json.dumps(normalized, sort_keys=True),
            message=message,
        )
        entity_type = EntityType.LEAD.value if lead_id or existing_lead_id else EntityType.IMPORT_JOB.value
        entity_id = lead_id or existing_lead_id or import_job_id
        self.audit_events.record(
            AuditEventCreate(
                entity_type=entity_type,
                entity_id=entity_id,
                event_type=audit_event_type or f"lead.import_{resolution.value}",
                actor=actor,
                summary=audit_summary or message,
                payload_json=json.dumps(
                    {
                        "import_job_id": import_job_id,
                        "resolution": resolution.value,
                        "dedupe_rule": dedupe_rule,
                        "message": message,
                    },
                    sort_keys=True,
                ),
            )
        )


def build_empty_import_summary(import_job_id: int, source: str, import_format: str, list_id: int | None) -> dict[str, object]:
    return {
        "import_job_id": import_job_id,
        "status": ImportJobStatus.RUNNING.value,
        "source": source,
        "import_format": import_format,
        "list_id": list_id,
        "total_read": 0,
        "inserted": 0,
        "merged": 0,
        "skipped": 0,
        "conflicting": 0,
        "manual_review_required": 0,
    }


def build_lead_create(normalized: dict[str, object]) -> LeadCreate:
    payload = {
        "source": str(normalized["source"]),
        "external_source_id": normalized.get("external_source_id") or None,
        "first_name": str(normalized.get("first_name") or ""),
        "last_name": str(normalized.get("last_name") or ""),
        "full_name": str(normalized.get("full_name") or ""),
        "email": normalized.get("email") or None,
        "phone": str(normalized.get("phone") or ""),
        "title": str(normalized.get("title") or ""),
        "linkedin_url": str(normalized.get("linkedin_url") or ""),
        "company_id": normalized.get("company_id"),
        "company_name_snapshot": str(normalized.get("company_name_snapshot") or ""),
        "company_domain_snapshot": str(normalized.get("company_domain_snapshot") or ""),
        "city": str(normalized.get("city") or ""),
        "state": str(normalized.get("state") or ""),
        "country": str(normalized.get("country") or ""),
        "enrichment_status": str(normalized.get("enrichment_status") or "pending"),
        "verification_status": str(normalized.get("verification_status") or "unverified"),
        "suppression_status": str(normalized.get("suppression_status") or "clear"),
        "fit_score": float(normalized.get("fit_score") or 0),
        "personalization_json": json.dumps(normalized.get("personalization") or {}, sort_keys=True),
        "notes": str(normalized.get("notes") or ""),
    }
    return LeadCreate(**payload)


def normalize_import_row(raw_row: dict[str, object], *, source: str) -> dict[str, object]:
    mapped = lower_key_map(raw_row)
    full_name = normalize_whitespace(first_present(mapped, "full_name", "name"))
    first_name = normalize_whitespace(first_present(mapped, "first_name", "firstname"))
    last_name = normalize_whitespace(first_present(mapped, "last_name", "lastname"))
    if not full_name and (first_name or last_name):
        full_name = normalize_whitespace(" ".join(part for part in [first_name, last_name] if part))
    if full_name and not first_name and not last_name:
        split_first, split_last = split_full_name(full_name)
        first_name = split_first
        last_name = split_last

    email = normalize_email(first_present(mapped, "email", "work_email"))
    website = normalize_url(first_present(mapped, "website", "website_url"))
    company_name = normalize_whitespace(first_present(mapped, "company", "company_name", "organization_name"))
    company_domain = normalize_domain(
        first_present(mapped, "company_domain", "domain", "primary_domain") or website or infer_domain_from_email(email)
    )

    normalized = {
        "source": source,
        "external_source_id": normalize_whitespace(first_present(mapped, "external_source_id", "source_id", "apollo_id", "id")),
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "email": email,
        "phone": normalize_whitespace(first_present(mapped, "phone", "mobile_phone", "work_phone")),
        "title": normalize_whitespace(first_present(mapped, "title", "job_title")),
        "linkedin_url": normalize_url(first_present(mapped, "linkedin_url", "linkedin")),
        "company_name_snapshot": company_name,
        "company_domain_snapshot": company_domain,
        "website": website,
        "city": normalize_whitespace(first_present(mapped, "city")),
        "state": normalize_whitespace(first_present(mapped, "state", "region")),
        "country": normalize_whitespace(first_present(mapped, "country")),
        "notes": normalize_whitespace(first_present(mapped, "notes")),
        "enrichment_status": normalize_whitespace(first_present(mapped, "enrichment_status")) or "pending",
        "verification_status": normalize_whitespace(first_present(mapped, "verification_status")) or "unverified",
        "suppression_status": normalize_whitespace(first_present(mapped, "suppression_status")) or "clear",
        "fit_score": safe_float(first_present(mapped, "fit_score")),
        "personalization": {},
        "error": "",
    }

    if not normalized["full_name"]:
        normalized["error"] = "Lead is missing a usable full name."
    elif not (normalized["email"] or normalized["phone"] or normalized["linkedin_url"]):
        normalized["error"] = "Lead is missing email, phone, and LinkedIn URL."
    elif not (company_name or company_domain):
        normalized["error"] = "Lead is missing company name and domain."
    return normalized


def parse_csv_text(csv_text: str) -> list[dict[str, object]]:
    if not csv_text.strip():
        raise LeadImportError("CSV import is empty.")
    reader = csv.DictReader(io.StringIO(csv_text), skipinitialspace=True, restval="", restkey="__extra__", strict=True)
    if reader.fieldnames is None:
        raise LeadImportError("CSV import is missing a header row.")
    rows: list[dict[str, object]] = []
    try:
        for row in reader:
            if "__extra__" in row and row["__extra__"]:
                raise LeadImportError("CSV row has more values than the header row defines.")
            rows.append(row)
    except csv.Error as exc:
        raise LeadImportError(f"Malformed CSV import: {exc}") from exc
    return rows


def parse_xlsx_bytes(xlsx_bytes: bytes) -> list[dict[str, object]]:
    if not xlsx_bytes:
        raise LeadImportError("XLSX import is empty.")
    try:
        with zipfile.ZipFile(io.BytesIO(xlsx_bytes)) as workbook:
            shared_strings = load_shared_strings(workbook)
            sheet_xml = workbook.read("xl/worksheets/sheet1.xml")
    except (KeyError, zipfile.BadZipFile) as exc:
        raise LeadImportError(f"Malformed XLSX import: {exc}") from exc

    root = ElementTree.fromstring(sheet_xml)
    namespace = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    rows = root.findall(".//main:sheetData/main:row", namespace)
    if not rows:
        raise LeadImportError("XLSX import does not contain any worksheet rows.")

    header_cells = extract_sheet_row(rows[0], shared_strings, namespace)
    headers = [str(cell or "").strip() for cell in header_cells]
    if not any(headers):
        raise LeadImportError("XLSX import is missing a usable header row.")

    records: list[dict[str, object]] = []
    for row in rows[1:]:
        values = extract_sheet_row(row, shared_strings, namespace)
        if not any(str(value).strip() for value in values):
            continue
        record = {}
        for index, header in enumerate(headers):
            if not header:
                continue
            record[header] = values[index] if index < len(values) else ""
        records.append(record)
    return records


def load_shared_strings(workbook: zipfile.ZipFile) -> list[str]:
    try:
        shared_strings_xml = workbook.read("xl/sharedStrings.xml")
    except KeyError:
        return []
    namespace = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    root = ElementTree.fromstring(shared_strings_xml)
    return [
        "".join(node.itertext())
        for node in root.findall(".//main:si", namespace)
    ]


def extract_sheet_row(row: ElementTree.Element, shared_strings: list[str], namespace: dict[str, str]) -> list[str]:
    values: list[str] = []
    expected_index = 0
    for cell in row.findall("main:c", namespace):
        cell_ref = cell.attrib.get("r", "")
        column_letters = "".join(character for character in cell_ref if character.isalpha())
        current_index = column_letters_to_index(column_letters) if column_letters else expected_index
        while expected_index < current_index:
            values.append("")
            expected_index += 1
        values.append(extract_cell_value(cell, shared_strings, namespace))
        expected_index += 1
    return values


def extract_cell_value(cell: ElementTree.Element, shared_strings: list[str], namespace: dict[str, str]) -> str:
    value_node = cell.find("main:v", namespace)
    inline_node = cell.find("main:is", namespace)
    cell_type = cell.attrib.get("t")
    if inline_node is not None:
        return "".join(inline_node.itertext())
    if value_node is None or value_node.text is None:
        return ""
    value = value_node.text
    if cell_type == "s":
        return shared_strings[int(value)]
    return value


def column_letters_to_index(letters: str) -> int:
    result = 0
    for character in letters.upper():
        result = result * 26 + (ord(character) - ord("A") + 1)
    return max(result - 1, 0)


def lower_key_map(row: dict[str, object]) -> dict[str, object]:
    return {str(key).strip().lower(): value for key, value in row.items() if key is not None}


def first_present(row: dict[str, object], *keys: str) -> object:
    for key in keys:
        if key in row and row[key] not in {None, ""}:
            return row[key]
    return ""


def normalize_whitespace(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_email(value: object) -> str:
    email = normalize_whitespace(value).lower()
    return email


def normalize_url(value: object) -> str:
    return normalize_whitespace(value)


def normalize_domain(value: object) -> str:
    text = normalize_whitespace(value).lower()
    if not text:
        return ""
    text = re.sub(r"^https?://", "", text)
    text = re.sub(r"^www\.", "", text)
    return text.split("/")[0]


def infer_domain_from_email(email: str) -> str:
    return email.split("@", 1)[1] if email and "@" in email else ""


def split_full_name(full_name: str) -> tuple[str, str]:
    parts = full_name.split(" ")
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def canonicalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def safe_float(value: object) -> float:
    try:
        return float(str(value or "").strip() or 0)
    except ValueError:
        return 0.0


def fill_missing_fields(existing: dict[str, object], incoming: dict[str, object]) -> dict[str, object]:
    updates: dict[str, object] = {}
    for key, incoming_value in incoming.items():
        if incoming_value in {None, "", 0}:
            continue
        existing_value = existing.get(key)
        if existing_value in {None, "", 0}:
            updates[key] = incoming_value
    return updates
