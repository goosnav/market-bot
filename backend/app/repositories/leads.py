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

    def get(self, company_id: int) -> dict[str, object] | None:
        return self.fetch_one("SELECT * FROM companies WHERE id = ?", (company_id,))

    def find_by_domain(self, domain: str) -> dict[str, object] | None:
        return self.fetch_one("SELECT * FROM companies WHERE domain = ? COLLATE NOCASE", (domain,))

    def find_by_name(self, name: str) -> dict[str, object] | None:
        return self.fetch_one("SELECT * FROM companies WHERE name = ? COLLATE NOCASE", (name,))

    def update_fields(self, company_id: int, fields: dict[str, object], updated_at: str | None = None) -> None:
        if not fields:
            return
        timestamp = updated_at or utc_now()
        assignments = ", ".join(f"{column} = ?" for column in fields)
        parameters = tuple(fields.values()) + (timestamp, company_id)
        self.execute(
            f"UPDATE companies SET {assignments}, updated_at = ? WHERE id = ?",
            parameters,
        )

    def count_all(self) -> int:
        row = self.execute("SELECT COUNT(*) AS total FROM companies").fetchone()
        return int(row["total"])


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

    def find_by_email(self, email: str) -> dict[str, object] | None:
        return self.fetch_one("SELECT * FROM leads WHERE email = ? COLLATE NOCASE", (email,))

    def find_by_source_external_id(self, source: str, external_source_id: str) -> dict[str, object] | None:
        return self.fetch_one(
            "SELECT * FROM leads WHERE source = ? AND external_source_id = ?",
            (source, external_source_id),
        )

    def find_exact_name_company_match(self, full_name: str, company_name_snapshot: str) -> dict[str, object] | None:
        return self.fetch_one(
            """
            SELECT *
            FROM leads
            WHERE full_name = ? COLLATE NOCASE
              AND company_name_snapshot = ? COLLATE NOCASE
            """,
            (full_name, company_name_snapshot),
        )

    def list_company_candidates(self, company_name_snapshot: str, company_domain_snapshot: str) -> list[dict[str, object]]:
        conditions: list[str] = []
        parameters: list[object] = []
        if company_name_snapshot:
            conditions.append("company_name_snapshot = ? COLLATE NOCASE")
            parameters.append(company_name_snapshot)
        if company_domain_snapshot:
            conditions.append("company_domain_snapshot = ? COLLATE NOCASE")
            parameters.append(company_domain_snapshot)
        if not conditions:
            return []
        where_clause = " OR ".join(conditions)
        return self.fetch_all(f"SELECT * FROM leads WHERE {where_clause}", tuple(parameters))

    def update_fields(self, lead_id: int, fields: dict[str, object], updated_at: str | None = None) -> None:
        if not fields:
            return
        timestamp = updated_at or utc_now()
        assignments = ", ".join(f"{column} = ?" for column in fields)
        parameters = tuple(fields.values()) + (timestamp, lead_id)
        self.execute(
            f"UPDATE leads SET {assignments}, updated_at = ? WHERE id = ?",
            parameters,
        )

    def list_filtered(self, filters: dict[str, object], limit: int = 100) -> list[dict[str, object]]:
        joins: list[str] = ["LEFT JOIN companies ON companies.id = leads.company_id"]
        conditions: list[str] = []
        parameters: list[object] = []

        if filters.get("list_id"):
            joins.append("INNER JOIN list_memberships ON list_memberships.lead_id = leads.id")
            conditions.append("list_memberships.list_id = ?")
            parameters.append(int(filters["list_id"]))
        if filters.get("source"):
            conditions.append("leads.source = ?")
            parameters.append(str(filters["source"]))
        if filters.get("suppression_status"):
            conditions.append("leads.suppression_status = ?")
            parameters.append(str(filters["suppression_status"]))
        if filters.get("enrichment_status"):
            conditions.append("leads.enrichment_status = ?")
            parameters.append(str(filters["enrichment_status"]))
        if filters.get("title"):
            conditions.append("leads.title = ? COLLATE NOCASE")
            parameters.append(str(filters["title"]))
        if filters.get("city"):
            conditions.append("leads.city = ? COLLATE NOCASE")
            parameters.append(str(filters["city"]))
        if filters.get("state_region"):
            conditions.append("leads.state = ? COLLATE NOCASE")
            parameters.append(str(filters["state_region"]))
        if filters.get("country"):
            conditions.append("leads.country = ? COLLATE NOCASE")
            parameters.append(str(filters["country"]))
        if filters.get("domain"):
            conditions.append("(companies.domain = ? COLLATE NOCASE OR leads.company_domain_snapshot = ? COLLATE NOCASE)")
            parameters.extend([str(filters["domain"]), str(filters["domain"])])
        if filters.get("tag_name"):
            joins.append("INNER JOIN entity_tags ON entity_tags.entity_id = leads.id AND entity_tags.entity_type = 'lead'")
            joins.append("INNER JOIN tags ON tags.id = entity_tags.tag_id")
            conditions.append("tags.name = ? COLLATE NOCASE")
            parameters.append(str(filters["tag_name"]))
        if "has_email" in filters:
            if bool(filters["has_email"]):
                conditions.append("leads.email IS NOT NULL AND leads.email != ''")
            else:
                conditions.append("(leads.email IS NULL OR leads.email = '')")
        if filters.get("query"):
            conditions.append("(leads.full_name LIKE ? OR companies.name LIKE ?)")
            query_value = f"%{filters['query']}%"
            parameters.extend([query_value, query_value])

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"""
            SELECT
                leads.*,
                companies.name AS company_name,
                companies.domain AS company_domain,
                companies.vertical AS company_vertical
            FROM leads
            {' '.join(dict.fromkeys(joins))}
            {where_clause}
            ORDER BY leads.updated_at DESC, leads.id DESC
            LIMIT ?
        """
        parameters.append(limit)
        return self.fetch_all(sql, tuple(parameters))

    def count_all(self) -> int:
        row = self.execute("SELECT COUNT(*) AS total FROM leads").fetchone()
        return int(row["total"])
