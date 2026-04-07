"""Lead and company repositories."""

from __future__ import annotations

from backend.app.core.logging import utc_now
from backend.app.domain.models import LeadCreate
from backend.app.repositories.base import SqliteRepository


class CompanyRepository(SqliteRepository):
    """Persist company records."""

    def create(
        self,
        *,
        source: str,
        name: str,
        domain: str | None = None,
        external_source_id: str | None = None,
        website: str = "",
        phone: str = "",
        vertical: str = "",
        employee_count_band: str = "",
        revenue_band: str = "",
        city: str = "",
        state: str = "",
        country: str = "",
        notes: str = "",
        created_at: str | None = None,
    ) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO companies (
                source,
                external_source_id,
                name,
                domain,
                website,
                phone,
                vertical,
                employee_count_band,
                revenue_band,
                city,
                state,
                country,
                notes,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source,
                external_source_id,
                name,
                domain,
                website,
                phone,
                vertical,
                employee_count_band,
                revenue_band,
                city,
                state,
                country,
                notes,
                timestamp,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)


class LeadRepository(SqliteRepository):
    """Persist lead records."""

    def create(self, payload: LeadCreate, created_at: str | None = None) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO leads (
                source,
                external_source_id,
                first_name,
                last_name,
                full_name,
                email,
                phone,
                title,
                linkedin_url,
                company_id,
                company_name_snapshot,
                company_domain_snapshot,
                city,
                state,
                country,
                enrichment_status,
                verification_status,
                suppression_status,
                fit_score,
                personalization_json,
                notes,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.source,
                payload.external_source_id,
                payload.first_name,
                payload.last_name,
                payload.full_name,
                payload.email,
                payload.phone,
                payload.title,
                payload.linkedin_url,
                payload.company_id,
                payload.company_name_snapshot,
                payload.company_domain_snapshot,
                payload.city,
                payload.state,
                payload.country,
                payload.enrichment_status,
                payload.verification_status,
                payload.suppression_status,
                payload.fit_score,
                payload.personalization_json,
                payload.notes,
                timestamp,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def get(self, lead_id: int) -> dict[str, object] | None:
        return self.fetch_one("SELECT * FROM leads WHERE id = ?", (lead_id,))
