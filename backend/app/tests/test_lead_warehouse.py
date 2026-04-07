"""Sprint 3 lead warehouse tests."""

from __future__ import annotations

import io
import json
from textwrap import dedent
import zipfile

from backend.app.repositories import read_session, write_session
from backend.app.services import LeadWarehouseService
from backend.app.tests.support import DatabaseTestCase


class LeadWarehouseTests(DatabaseTestCase):
    def test_malformed_csv_import_is_reported_as_failed(self) -> None:
        malformed_csv = 'name,email,company\n"Jordan Smith,jordan@example.com,Acme\n'

        with write_session(self.database_path) as connection:
            summary = LeadWarehouseService(connection).import_csv_text(malformed_csv, actor="operator")

        self.assertEqual(summary["status"], "failed")
        self.assertIn("Malformed CSV import", summary["error"])

    def test_duplicate_csv_rows_merge_into_single_lead(self) -> None:
        csv_text = dedent(
            """
            first_name,last_name,email,company,domain,title
            Jordan,Smith,jordan@example.com,Acme,acme.example,Owner
            Jordan,Smith,jordan@example.com,Acme,acme.example,Owner
            """
        ).strip()

        with write_session(self.database_path) as connection:
            summary = LeadWarehouseService(connection).import_csv_text(csv_text, actor="operator", list_name="CSV Batch")

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["inserted"], 1)
        self.assertEqual(summary["merged"], 1)

        with read_session(self.database_path) as connection:
            lead_count = connection.execute("SELECT COUNT(*) AS total FROM leads").fetchone()["total"]
            membership_count = connection.execute("SELECT COUNT(*) AS total FROM list_memberships").fetchone()["total"]

        self.assertEqual(lead_count, 1)
        self.assertEqual(membership_count, 1)

    def test_apollo_results_are_normalized_and_inserted(self) -> None:
        apollo_payload = [
            {
                "id": "apollo-1",
                "first_name": "Taylor",
                "last_name": "Ames",
                "name": "Taylor Ames",
                "email": "taylor@factory.example",
                "title": "COO",
                "organization": {
                    "name": "Factory Co",
                    "primary_domain": "factory.example",
                    "website_url": "https://factory.example",
                },
            }
        ]

        with write_session(self.database_path) as connection:
            summary = LeadWarehouseService(connection).import_apollo_people(apollo_payload, actor="operator")

        self.assertEqual(summary["inserted"], 1)
        self.assertEqual(summary["status"], "completed")

        with read_session(self.database_path) as connection:
            lead = connection.execute("SELECT * FROM leads").fetchone()
            company = connection.execute("SELECT * FROM companies").fetchone()

        self.assertEqual(lead["source"], "apollo")
        self.assertEqual(lead["external_source_id"], "apollo-1")
        self.assertEqual(company["domain"], "factory.example")

    def test_house_list_and_apollo_list_merge_on_exact_email(self) -> None:
        csv_text = dedent(
            """
            full_name,email,company,domain
            Jordan Smith,jordan@acme.example,Acme,acme.example
            """
        ).strip()
        apollo_payload = [
            {
                "id": "apollo-2",
                "name": "Jordan Smith",
                "email": "jordan@acme.example",
                "title": "Founder",
                "organization": {
                    "name": "Acme",
                    "primary_domain": "acme.example",
                },
            }
        ]

        with write_session(self.database_path) as connection:
            service = LeadWarehouseService(connection)
            first_summary = service.import_csv_text(csv_text, actor="operator", list_name="House List")
            second_summary = service.import_apollo_people(apollo_payload, actor="operator", list_name="Apollo List")

        self.assertEqual(first_summary["inserted"], 1)
        self.assertEqual(second_summary["merged"], 1)

        with read_session(self.database_path) as connection:
            lead = connection.execute("SELECT * FROM leads").fetchone()
            list_membership_count = connection.execute("SELECT COUNT(*) AS total FROM list_memberships").fetchone()["total"]

        self.assertEqual(lead["title"], "Founder")
        self.assertEqual(list_membership_count, 2)

    def test_similar_name_same_company_is_flagged_for_manual_review(self) -> None:
        csv_text = dedent(
            """
            full_name,email,company,domain
            Jordan Smith,jordan@acme.example,Acme,acme.example
            Jordon Smyth,jordon@acme.example,Acme,acme.example
            """
        ).strip()

        with write_session(self.database_path) as connection:
            summary = LeadWarehouseService(connection).import_csv_text(csv_text, actor="operator")

        self.assertEqual(summary["inserted"], 1)
        self.assertEqual(summary["manual_review_required"], 1)

        with read_session(self.database_path) as connection:
            lead_count = connection.execute("SELECT COUNT(*) AS total FROM leads").fetchone()["total"]

        self.assertEqual(lead_count, 1)

    def test_xlsx_import_loads_first_sheet_rows(self) -> None:
        xlsx_bytes = build_simple_xlsx(
            [
                ["full_name", "email", "company", "domain"],
                ["Alex Rivera", "alex@metal.example", "Metal Works", "metal.example"],
            ]
        )

        with write_session(self.database_path) as connection:
            summary = LeadWarehouseService(connection).import_xlsx_bytes(xlsx_bytes, actor="operator")

        self.assertEqual(summary["inserted"], 1)
        with read_session(self.database_path) as connection:
            lead = connection.execute("SELECT * FROM leads").fetchone()
        self.assertEqual(lead["email"], "alex@metal.example")

    def test_saved_filter_correctness(self) -> None:
        csv_text = dedent(
            """
            full_name,email,company,domain,source
            Jordan Smith,jordan@acme.example,Acme,acme.example,csv
            Riley Stone,riley@forge.example,Forge,forge.example,csv
            """
        ).strip()

        with write_session(self.database_path) as connection:
            service = LeadWarehouseService(connection)
            service.import_csv_text(csv_text, actor="operator", list_name="Filter Seed", source="csv")
            service.assign_tag(1, "priority")
            saved_filter = service.save_filter(
                "Priority Leads",
                {
                    "tag_name": "priority",
                    "has_email": True,
                    "domain": "acme.example",
                },
            )
            results = service.list_leads({"saved_filter_id": saved_filter["id"]}, limit=10)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["email"], "jordan@acme.example")


def build_simple_xlsx(rows: list[list[str]]) -> bytes:
    shared_strings: list[str] = []
    for row in rows:
        for value in row:
            if value not in shared_strings:
                shared_strings.append(value)

    def shared_index(value: str) -> int:
        return shared_strings.index(value)

    workbook_xml = """<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Sheet1" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>"""
    workbook_rels_xml = """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1"
    Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"
    Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2"
    Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings"
    Target="sharedStrings.xml"/>
</Relationships>"""
    root_rels_xml = """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1"
    Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument"
    Target="xl/workbook.xml"/>
</Relationships>"""
    content_types_xml = """<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml"
    ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml"
    ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/sharedStrings.xml"
    ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>
</Types>"""
    shared_strings_xml = (
        """<?xml version="1.0" encoding="UTF-8"?>"""
        f"""<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="{len(shared_strings)}" uniqueCount="{len(shared_strings)}">"""
        + "".join(f"<si><t>{value}</t></si>" for value in shared_strings)
        + "</sst>"
    )

    sheet_rows = []
    for row_index, row in enumerate(rows, start=1):
        cells = []
        for column_index, value in enumerate(row):
            column_letter = chr(ord("A") + column_index)
            cells.append(f'<c r="{column_letter}{row_index}" t="s"><v>{shared_index(value)}</v></c>')
        sheet_rows.append(f'<row r="{row_index}">{"".join(cells)}</row>')
    worksheet_xml = (
        """<?xml version="1.0" encoding="UTF-8"?>"""
        """<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">"""
        f"<sheetData>{''.join(sheet_rows)}</sheetData>"
        "</worksheet>"
    )

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr("[Content_Types].xml", content_types_xml)
        workbook.writestr("_rels/.rels", root_rels_xml)
        workbook.writestr("xl/workbook.xml", workbook_xml)
        workbook.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        workbook.writestr("xl/sharedStrings.xml", shared_strings_xml)
        workbook.writestr("xl/worksheets/sheet1.xml", worksheet_xml)
    return buffer.getvalue()
