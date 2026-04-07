"""Apollo payload normalization for Sprint 3 lead imports."""

from __future__ import annotations


class ApolloLeadAdapter:
    """Convert Apollo-style person payloads into the internal import row shape."""

    @staticmethod
    def normalize_people_records(records: list[dict[str, object]]) -> list[dict[str, object]]:
        normalized_rows: list[dict[str, object]] = []
        for record in records:
            organization = record.get("organization") if isinstance(record.get("organization"), dict) else {}
            normalized_rows.append(
                {
                    "external_source_id": record.get("id", ""),
                    "first_name": record.get("first_name", ""),
                    "last_name": record.get("last_name", ""),
                    "full_name": record.get("name", ""),
                    "email": record.get("email", ""),
                    "phone": record.get("phone", ""),
                    "title": record.get("title", ""),
                    "linkedin_url": record.get("linkedin_url", ""),
                    "company_name": organization.get("name", record.get("company_name", "")),
                    "company_domain": organization.get("primary_domain", record.get("company_domain", "")),
                    "website": organization.get("website_url", ""),
                    "city": record.get("city", ""),
                    "state": record.get("state", ""),
                    "country": record.get("country", ""),
                }
            )
        return normalized_rows
