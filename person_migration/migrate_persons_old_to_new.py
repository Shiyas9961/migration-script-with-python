#!/usr/bin/env python3
import os
import sys
import json
import hashlib
import uuid
import shutil
import argparse
from html import escape as html_escape
from dataclasses import dataclass
from collections import defaultdict
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv


# ============================================================
# LOAD ENV
# ============================================================
load_dotenv()
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


# ============================================================
# CONFIG
# ============================================================
DEFAULT_BATCH_SIZE = 500

# Set True only if old and new media files are on filesystem
# and you want to physically copy images.
COPY_PHOTO_FILES = False
OLD_MEDIA_ROOT = ""
NEW_MEDIA_ROOT = ""

# Logging preview count per section
PREVIEW_LIMIT_PER_TABLE = 20

# Default HTML report generated after migration
DEFAULT_REPORT_FILE = "person_migration_report.html"

# Trade code generation for organisation_trade.code
TRADE_CODE_PREFIX = "T"
TRADE_CODE_WIDTH = 3
TRADE_CODE_COUNTER: Optional[int] = None

# Tables cleared by --roll-back cleanup mode.
ROLLBACK_CLEANUP_TABLES = [
    # Children first, then parents. TRUNCATE handles FK dependencies.
    "person_personaccesspass",
    "person_personphoto",
    "person_ic",
    "person_personcompany",
    "person_personcontract",
    "person_personprofile",
    "organisation_company",
    "organisation_contract",
    "organisation_trade",
    "organisation_tradegroup",
]


# ============================================================
# DB CONFIG
# ============================================================
@dataclass
class DBConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


OLD_DB = DBConfig(
    host=os.getenv("OLD_DB_HOST", "127.0.0.1"),
    port=int(os.getenv("OLD_DB_PORT", "5432")),
    dbname=os.getenv("OLD_DB_NAME", ""),
    user=os.getenv("OLD_DB_USER", ""),
    password=os.getenv("OLD_DB_PASSWORD", ""),
)

NEW_DB = DBConfig(
    host=os.getenv("NEW_DB_HOST", "127.0.0.1"),
    port=int(os.getenv("NEW_DB_PORT", "5432")),
    dbname=os.getenv("NEW_DB_NAME", ""),
    user=os.getenv("NEW_DB_USER", ""),
    password=os.getenv("NEW_DB_PASSWORD", ""),
)


# ============================================================
# TABLE NAMES
# VERIFY THESE IF YOUR DB USES DIFFERENT TABLE NAMES
# ============================================================
OLD_TABLES = {
    "applicant_profile": "applicant_applicantprofile",
    "applicant_gate_pass": "applicant_applicantgatepass",
    "applicant_photo": "applicant_applicantphoto",
    "company": "organisation_company",
    "contract": "organisation_contract",
    "trade_information": "organisation_tradeinformation",
    "trade_information_group": "organisation_tradeinformationgroup",
}

NEW_TABLES = {
    "person_profile": "person_personprofile",
    "person_company": "person_personcompany",
    "person_contract": "person_personcontract",
    "person_access_pass": "person_personaccesspass",
    "person_access_pass_type": "person_personaccesspasstype",
    "person_category": "person_personcategory",
    "person_photo": "person_personphoto",
    "ic": "person_ic",
    "ic_type": "person_ictype",
    "company": "organisation_company",
    "contract": "organisation_contract",
    "trade": "organisation_trade",
    "trade_group": "organisation_tradegroup",
}


# ============================================================
# MAPPING RULES
# ============================================================
CATEGORY_MAP = {
    "staff": "employee",
    "delivery_regular": "employee",
    "visitor": "visitor",
    "delivery_adhoc": "visitor",
}

PASS_TYPE_MAP = {
    "staff": "staff",
    "delivery_regular": "frequent-delivery",
    "visitor": "visitor",
    "delivery_adhoc": "delivery-adhoc",
}

PERSON_STATUS_MAP = {
    "pending": "active",
    "approved": "active",
    "active": "active",
    "rejected": "banned",
    "blocked": "banned",
    "expired": "archived",
    "deactivated": "archived",
    "archived": "archived",
}

PASS_STATUS_MAP = {
    "pending": "pending",
    "active": "active",
    "expired": "expired",
    "rejected": "rejected",
}


# ============================================================
# REPORT SHEETS
# ============================================================
REPORT_SHEETS = {
    "Profile": [
        "full_name",
        "profile_id",
        "status",
        "email",
        "designation",
        "related_trade",
        "identity_type",
        "identity_number",
        "old_pass_count",
        "new_pass_count",
        "pass_delta",
    ],
}


# ============================================================
# ARGUMENTS
# ============================================================
def parse_args():
    parser = argparse.ArgumentParser(description="Migrate old applicant data to new person data")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--dry-run", action="store_true", help="Run migration in dry-run mode")
    mode_group.add_argument(
        "--roll-back",
        "--rollback",
        dest="rollback",
        action="store_true",
        help="Empty the migration tables in the new DB and exit",
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help=f"Batch size (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--data-limit", type=int, default=None, help="Maximum number of applicant rows to migrate")
    parser.add_argument(
        "--report-file",
        type=str,
        default=DEFAULT_REPORT_FILE,
        help=f"HTML report file path (default: {DEFAULT_REPORT_FILE})",
    )

    args = parser.parse_args()
    if args.batch_size <= 0:
        parser.error("--batch-size must be greater than 0")
    if args.data_limit is not None and args.data_limit <= 0:
        parser.error("--data-limit must be greater than 0")
    return args


# ============================================================
# STATS + LOGGING
# ============================================================
class MigrationStats:
    def __init__(self):
        self.total = defaultdict(lambda: {"created": 0, "existing": 0, "skipped": 0, "failed": 0})
        self.batch = defaultdict(lambda: {"created": 0, "existing": 0, "skipped": 0, "failed": 0})
        self.batch_preview = defaultdict(list)
        self.report_rows = defaultdict(list)
        self.run_metadata = {}
        self.total_processed_profiles = 0
        self.total_processed_passes = 0
        self.batch_errors = []

    def inc(self, table: str, key: str):
        self.total[table][key] += 1
        self.batch[table][key] += 1

    def add_preview(self, table: str, line: str):
        if len(self.batch_preview[table]) < PREVIEW_LIMIT_PER_TABLE:
            self.batch_preview[table].append(line)

    def add_error(self, profile_id, full_name, error):
        self.batch_errors.append(f"- {profile_id} | {full_name} | {error}")

    def add_report_row(self, sheet_name: str, row: Dict[str, Any]):
        if sheet_name == "Profile":
            self.report_rows[sheet_name].append(row)

    def set_run_metadata(self, **kwargs):
        self.run_metadata.update(kwargs)

    def print_batch_errors(self):
        if not self.batch_errors:
            return
        print("\nErrors:")
        for line in self.batch_errors[:20]:
            print(line)

    def reset_batch(self):
        self.batch = defaultdict(lambda: {"created": 0, "existing": 0, "skipped": 0, "failed": 0})
        self.batch_preview = defaultdict(list)
        self.batch_errors = []

    def print_batch_summary(self, batch_no: int):
        print(f"\n{'='*90}")
        print(f"BATCH {batch_no} SUMMARY")
        print(f"{'='*90}")
        for table, values in self.batch.items():
            print(
                f"{table:<22} "
                f"created={values['created']:<5} "
                f"existing={values['existing']:<5} "
                f"skipped={values['skipped']:<5} "
                f"failed={values['failed']:<5}"
            )

    def print_batch_previews(self, batch_no: int):
        print(f"\n{'-'*90}")
        print(f"BATCH {batch_no} RECORD PREVIEW")
        print(f"{'-'*90}")
        for table, lines in self.batch_preview.items():
            if not lines:
                continue
            print(f"\n[{table}]")
            for line in lines:
                print(line)

    def print_final_summary(self):
        print(f"\n{'='*90}")
        print("FINAL SUMMARY")
        print(f"{'='*90}")
        for table, values in self.total.items():
            print(
                f"{table:<22} "
                f"created={values['created']:<5} "
                f"existing={values['existing']:<5} "
                f"skipped={values['skipped']:<5} "
                f"failed={values['failed']:<5}"
            )
        print(f"\nTotal processed profiles : {self.total_processed_profiles}")
        print(f"Total processed passes   : {self.total_processed_passes}")


# ============================================================
# HTML REPORT HELPERS
# ============================================================
def _format_html_value(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if hasattr(value, "isoformat") and not isinstance(value, (str, bytes)):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def _status_slug(value: Any) -> str:
    if value is None:
        return "unknown"
    slug = str(value).strip().lower().replace(" ", "-").replace("_", "-")
    return slug or "unknown"


def normalize_report_file_path(report_file: str) -> str:
    base, ext = os.path.splitext(report_file)
    if ext.lower() in {".html", ".htm"}:
        target = report_file
    else:
        target = f"{base or report_file}.html"
    return target if os.path.isabs(target) else os.path.join(SCRIPT_DIR, target)


def stable_pk(value: Any) -> Any:
    return value


def stable_relation_pk(*parts: Any, prefix: str) -> str:
    joined = "|".join("" if part is None else str(part) for part in parts)
    digest = hashlib.sha1(joined.encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:24]}"


def initialize_trade_code_counter(new_cur) -> int:
    row = fetchone(
        new_cur,
        f"""
        SELECT COALESCE(
            MAX(
                CASE
                    WHEN UPPER(code) ~ '^T[0-9]+$'
                    THEN CAST(SUBSTRING(UPPER(code) FROM 2) AS INTEGER)
                    ELSE 0
                END
            ),
            0
        ) AS max_code
        FROM {NEW_TABLES['trade']}
        """,
    )
    return int(row["max_code"] or 0)


def allocate_trade_code(new_cur) -> str:
    global TRADE_CODE_COUNTER
    if TRADE_CODE_COUNTER is None:
        TRADE_CODE_COUNTER = initialize_trade_code_counter(new_cur)
    TRADE_CODE_COUNTER += 1
    return f"{TRADE_CODE_PREFIX}{TRADE_CODE_COUNTER:0{TRADE_CODE_WIDTH}d}"


def source_timestamps(row: Optional[Dict[str, Any]]) -> Tuple[datetime, datetime]:
    now = datetime.now().astimezone()
    if not row:
        return now, now

    created_at = row.get("created")
    modified_at = row.get("modified")

    if created_at is None and modified_at is None:
        return now, now

    if created_at is None:
        created_at = modified_at
    if modified_at is None:
        modified_at = created_at

    return created_at, modified_at


def capture_created_profile_report(new_cur, stats: MigrationStats, person_id: str, old_pass_count: Optional[int] = None):
    row = fetchone(
        new_cur,
        f"""
        SELECT
            pp.profile_id,
            pp.full_name,
            pp.status,
            pp.email,
            pp.designation,
            pp.id_type AS identity_type,
            pp.id_number AS identity_number,
            pp.mobile_number AS phone_number,
            pp.work_permit_expiry,
            pp.is_deleted AS deleted,
            COALESCE(cat.name, cat.slug) AS category,
            COALESCE(t.name, t.code) AS related_trade,
            company.company_name AS company,
            contract.contract_name AS contract,
            COALESCE(pass_stats.new_pass_count, 0) AS new_pass_count
        FROM {NEW_TABLES['person_profile']} pp
        LEFT JOIN {NEW_TABLES['person_category']} cat
            ON cat.id = pp.category_id
        LEFT JOIN {NEW_TABLES['trade']} t
            ON t.id = pp.trade_id
        LEFT JOIN LATERAL (
            SELECT c.name AS company_name
            FROM {NEW_TABLES['person_company']} pc
            JOIN {NEW_TABLES['company']} c ON c.id = pc.company_id
            WHERE pc.person_id = pp.id AND pc.is_active = TRUE
            ORDER BY pc.created DESC
            LIMIT 1
        ) company ON TRUE
        LEFT JOIN LATERAL (
            SELECT ct.name AS contract_name
            FROM {NEW_TABLES['person_contract']} pc
            JOIN {NEW_TABLES['contract']} ct ON ct.id = pc.contract_id
            WHERE pc.person_id = pp.id AND pc.is_active = TRUE
            ORDER BY pc.created DESC
            LIMIT 1
        ) contract ON TRUE
        LEFT JOIN LATERAL (
            SELECT COUNT(*)::int AS new_pass_count
            FROM {NEW_TABLES['person_access_pass']} ap
            WHERE ap.person_id = pp.id
        ) pass_stats ON TRUE
        WHERE pp.id = %s
        LIMIT 1
        """,
        (person_id,),
    )
    if not row:
        return

    row = dict(row)
    old_pass_count_value = int(old_pass_count or 0)
    new_pass_count_value = int(row.get("new_pass_count") or 0)
    stats.add_report_row(
        "Profile",
        {
            "full_name": row.get("full_name"),
            "profile_id": row.get("profile_id"),
            "status": row.get("status"),
            "email": row.get("email"),
            "designation": row.get("designation"),
            "related_trade": row.get("related_trade"),
            "identity_type": row.get("identity_type"),
            "identity_number": row.get("identity_number"),
            "phone_number": row.get("phone_number"),
            "deleted": row.get("deleted"),
            "category": row.get("category"),
            "company": row.get("company"),
            "contract": row.get("contract"),
            "work_permit_expiry": row.get("work_permit_expiry"),
            "old_pass_count": old_pass_count_value,
            "new_pass_count": new_pass_count_value,
            "pass_delta": new_pass_count_value - old_pass_count_value,
        },
    )


def build_report_payload(stats: MigrationStats) -> Dict[str, Any]:
    rows = list(stats.report_rows.get("Profile", []))
    summary_keys = ["person_profile", "person_access_pass", "company", "contract", "trade"]
    summary = {key: dict(stats.total[key]) for key in summary_keys}
    return {
        "meta": dict(stats.run_metadata),
        "summary": summary,
        "row_count": len(rows),
        "rows": rows,
    }


class HtmlReportWriter:
    PAGE_SIZE = 25

    def __init__(self, output_path: str):
        self.output_path = output_path
        base, _ = os.path.splitext(output_path)
        self.data_path = f"{base}.data.json"
        self.data_js_path = f"{base}.data.js"

    def write(self, stats: MigrationStats):
        payload = build_report_payload(stats)
        payload_json = json.dumps(payload, default=str, ensure_ascii=False, separators=(",", ":"))

        with open(self.data_path, "w", encoding="utf-8") as handle:
            handle.write(payload_json)

        with open(self.data_js_path, "w", encoding="utf-8") as handle:
            handle.write(f"window.__REPORT_DATA__ = {payload_json};\n")

        html_doc = self._build_html(stats, payload)
        with open(self.output_path, "w", encoding="utf-8") as handle:
            handle.write(html_doc)

    def _count_block(self, stats: MigrationStats, key: str) -> Dict[str, int]:
        values = stats.total[key]
        return {
            "created": values["created"],
            "existing": values["existing"],
            "skipped": values["skipped"],
            "failed": values["failed"],
        }

    def _build_metric_card(self, label: str, created: int, detail: str) -> str:
        return f"""
        <article class="metric-card">
          <div class="metric-label">{html_escape(label)}</div>
          <div class="metric-value">{created:,}</div>
          <div class="metric-detail">{html_escape(detail)}</div>
        </article>
        """

    def _build_summary_cards(self, stats: MigrationStats) -> str:
        cards = [
            ("Persons Migrated", self._count_block(stats, "person_profile")),
            ("Access Passes", self._count_block(stats, "person_access_pass")),
            ("Companies", self._count_block(stats, "company")),
            ("Contracts", self._count_block(stats, "contract")),
            ("Trades", self._count_block(stats, "trade")),
        ]
        rendered = []
        for label, counts in cards:
            rendered.append(
                self._build_metric_card(
                    label,
                    counts["created"],
                    f"existing {counts['existing']} · skipped {counts['skipped']} · failed {counts['failed']}",
                )
            )
        return "".join(rendered)

    def _build_meta_pills(self, stats: MigrationStats) -> str:
        meta = stats.run_metadata
        if meta.get("dry_run"):
            mode_label = "Dry Run"
        elif meta.get("rollback"):
            mode_label = "Rollback"
        else:
            mode_label = "Live"
        items = [
            ("Mode", mode_label),
            ("Batch Size", meta.get("batch_size")),
            ("Data Limit", meta.get("data_limit") if meta.get("data_limit") is not None else "All"),
            ("Source Applicants", meta.get("source_total")),
            ("Processed Profiles", stats.total_processed_profiles),
            ("Processed Passes", stats.total_processed_passes),
            ("Report Rows", len(stats.report_rows.get("Profile", []))),
        ]
        pills = []
        for label, value in items:
            pills.append(
                f"""
                <div class="pill">
                  <span class="pill-label">{html_escape(label)}</span>
                  <span class="pill-value">{html_escape(_format_html_value(value))}</span>
                </div>
                """
            )
        return "".join(pills)

    def _build_html(self, stats: MigrationStats, payload: Dict[str, Any]) -> str:
        meta = stats.run_metadata
        generated_at = html_escape(_format_html_value(meta.get("generated_at")))
        report_file = html_escape(_format_html_value(meta.get("report_file")))
        summary_cards = self._build_summary_cards(stats)
        meta_pills = self._build_meta_pills(stats)
        row_count = payload.get("row_count", len(stats.report_rows.get("Profile", [])))
        data_file_name = json.dumps(os.path.basename(self.data_path))
        data_js_name = json.dumps(os.path.basename(self.data_js_path))

        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Person Migration Report</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f8fafc;
      --bg2: #eef2f7;
      --panel: rgba(255, 255, 255, 0.94);
      --panel-border: rgba(15, 23, 42, 0.08);
      --text: #0f172a;
      --muted: #64748b;
      --accent: #c2410c;
      --accent-2: #0284c7;
      --shadow: 0 18px 50px rgba(15, 23, 42, 0.08);
      --radius: 22px;
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      min-height: 100vh;
      font-family: "Inter", "Segoe UI", system-ui, -apple-system, sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(251, 191, 36, 0.18), transparent 34%),
        radial-gradient(circle at top right, rgba(56, 189, 248, 0.16), transparent 30%),
        linear-gradient(180deg, var(--bg), var(--bg2));
    }}

    .shell {{
      width: min(1440px, calc(100% - 32px));
      margin: 0 auto;
      padding: 28px 0 40px;
    }}

    .hero {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 20px;
      padding: 24px 28px;
      border: 1px solid var(--panel-border);
      border-radius: var(--radius);
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(248, 250, 252, 0.92));
      box-shadow: var(--shadow);
      backdrop-filter: blur(14px);
    }}

    .eyebrow {{
      margin: 0 0 8px;
      color: var(--accent);
      text-transform: uppercase;
      letter-spacing: 0.18em;
      font-size: 0.74rem;
      font-weight: 700;
    }}

    h1 {{
      margin: 0;
      font-size: clamp(2rem, 3vw, 3.4rem);
      line-height: 1.05;
      letter-spacing: -0.04em;
    }}

    .lead {{
      margin: 12px 0 0;
      max-width: 72ch;
      color: var(--muted);
      line-height: 1.6;
    }}

    .hero-aside {{
      display: grid;
      gap: 10px;
      align-content: start;
      justify-items: end;
      text-align: right;
    }}

    .hero-aside .report-path {{
      color: var(--muted);
      font-size: 0.92rem;
    }}

    .meta-pills {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin: 18px 0 0;
    }}

    .pill {{
      display: grid;
      gap: 3px;
      padding: 12px 14px;
      min-width: 160px;
      border-radius: 16px;
      background: rgba(248, 250, 252, 0.96);
      border: 1px solid rgba(148, 163, 184, 0.18);
    }}

    .pill-label {{
      color: var(--muted);
      font-size: 0.74rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}

    .pill-value {{
      font-size: 1rem;
      font-weight: 600;
      color: var(--text);
    }}

    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 14px;
      margin-top: 18px;
    }}

    .metric-card {{
      padding: 18px;
      border: 1px solid var(--panel-border);
      border-radius: 20px;
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(248, 250, 252, 0.94));
      box-shadow: var(--shadow);
    }}

    .metric-label {{
      color: var(--muted);
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}

    .metric-value {{
      margin-top: 8px;
      font-size: 2.1rem;
      font-weight: 800;
      letter-spacing: -0.04em;
    }}

    .metric-detail {{
      margin-top: 6px;
      color: var(--muted);
      font-size: 0.9rem;
      line-height: 1.45;
    }}

    .table-card {{
      margin-top: 18px;
      border: 1px solid var(--panel-border);
      border-radius: var(--radius);
      background: var(--panel);
      box-shadow: var(--shadow);
      overflow: hidden;
    }}

    .table-head {{
      display: flex;
      align-items: end;
      justify-content: space-between;
      gap: 18px;
      padding: 20px 24px 14px;
      border-bottom: 1px solid rgba(148, 163, 184, 0.16);
    }}

    .table-head h2 {{
      margin: 0;
      font-size: 1.2rem;
      letter-spacing: -0.02em;
    }}

    .table-head p {{
      margin: 8px 0 0;
      color: var(--muted);
    }}

    .table-actions {{
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }}

    .pagination-info {{
      color: var(--muted);
      font-size: 0.92rem;
      white-space: nowrap;
    }}

    .btn {{
      appearance: none;
      border: 1px solid rgba(148, 163, 184, 0.24);
      background: rgba(255, 255, 255, 0.95);
      color: var(--text);
      border-radius: 12px;
      padding: 10px 14px;
      font: inherit;
      font-weight: 600;
      cursor: pointer;
      transition: transform 120ms ease, border-color 120ms ease, background 120ms ease, opacity 120ms ease;
    }}

    .btn:hover:not(:disabled) {{
      transform: translateY(-1px);
      border-color: rgba(194, 65, 12, 0.35);
      background: rgba(248, 250, 252, 1);
    }}

    .btn:disabled {{
      opacity: 0.4;
      cursor: not-allowed;
    }}

    .table-wrap {{
      overflow: auto;
    }}

    table {{
      width: 100%;
      border-collapse: collapse;
    }}

    thead th {{
      position: sticky;
      top: 0;
      z-index: 1;
      text-align: left;
      font-size: 0.75rem;
      letter-spacing: 0.09em;
      text-transform: uppercase;
      color: #475569;
      background: rgba(248, 250, 252, 0.98);
      padding: 14px 18px;
      border-bottom: 1px solid rgba(148, 163, 184, 0.18);
      white-space: nowrap;
    }}

    tbody td {{
      padding: 14px 18px;
      border-bottom: 1px solid rgba(148, 163, 184, 0.12);
      color: #0f172a;
      vertical-align: top;
    }}

    tbody tr:nth-child(even) {{
      background: rgba(248, 250, 252, 0.82);
    }}

    tbody tr:hover {{
      background: rgba(251, 191, 36, 0.08);
    }}

    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 7px 11px;
      border-radius: 999px;
      font-size: 0.85rem;
      font-weight: 700;
      letter-spacing: 0.01em;
      border: 1px solid transparent;
      text-transform: capitalize;
    }}

    .badge::before {{
      content: "";
      width: 8px;
      height: 8px;
      border-radius: 999px;
      background: currentColor;
      opacity: 0.9;
    }}

    .badge.active {{
      color: #34d399;
      background: rgba(16, 185, 129, 0.1);
      border-color: rgba(16, 185, 129, 0.24);
    }}

    .badge.pending {{
      color: #fbbf24;
      background: rgba(251, 191, 36, 0.1);
      border-color: rgba(251, 191, 36, 0.22);
    }}

    .badge.inactive,
    .badge.rejected,
    .badge.expired,
    .badge.banned {{
      color: #f87171;
      background: rgba(248, 113, 113, 0.1);
      border-color: rgba(248, 113, 113, 0.22);
    }}

    .badge.archived {{
      color: #64748b;
      background: rgba(100, 116, 139, 0.1);
      border-color: rgba(100, 116, 139, 0.22);
    }}

    .badge.unknown {{
      color: #475569;
      background: rgba(148, 163, 184, 0.1);
      border-color: rgba(148, 163, 184, 0.2);
    }}

    .delta {{
      font-weight: 700;
    }}

    .delta.positive {{
      color: #059669;
    }}

    .delta.zero {{
      color: #64748b;
    }}

    .delta.negative {{
      color: #dc2626;
    }}

    .badge.employee {{
      color: #38bdf8;
      background: rgba(56, 189, 248, 0.1);
      border-color: rgba(56, 189, 248, 0.22);
    }}

    .badge.visitor {{
      color: #f59e0b;
      background: rgba(245, 158, 11, 0.1);
      border-color: rgba(245, 158, 11, 0.22);
    }}

    .empty-state {{
      display: grid;
      place-items: center;
      min-height: 220px;
      padding: 32px;
      color: var(--muted);
      text-align: center;
    }}

    .table-footer {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      padding: 16px 24px 22px;
      border-top: 1px solid rgba(148, 163, 184, 0.16);
    }}

    .table-footer .hint {{
      color: var(--muted);
      font-size: 0.92rem;
    }}

    @media (max-width: 1100px) {{
      .summary-grid {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}

      .hero {{
        grid-template-columns: 1fr;
      }}

      .hero-aside {{
        justify-items: start;
        text-align: left;
      }}
    }}

    @media (max-width: 720px) {{
      .shell {{
        width: min(100% - 20px, 100%);
        padding-top: 16px;
      }}

      .summary-grid {{
        grid-template-columns: 1fr;
      }}

      .table-head,
      .table-footer {{
        flex-direction: column;
        align-items: stretch;
      }}

      .table-actions {{
        justify-content: flex-start;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div>
        <p class="eyebrow">Migration Report</p>
        <h1>Person migration dashboard</h1>
        <p class="lead">
          HTML summary of the migration run. The counts below reflect the migration stats, while the table is built from the new DB values for created profiles.
        </p>
        <div class="meta-pills">
          {meta_pills}
        </div>
      </div>
      <div class="hero-aside">
        <div class="report-path">Report file: {report_file}</div>
        <div class="report-path">Generated at: {generated_at}</div>
      </div>
    </section>

    <section class="summary-grid">
      {summary_cards}
    </section>

    <section class="table-card">
      <div class="table-head">
        <div>
          <h2>Created profiles</h2>
          <p>{row_count} rows captured from the new database.</p>
        </div>
        <div class="table-actions">
          <div class="pagination-info" id="page-info"></div>
          <button class="btn" id="prev-page" type="button">Previous</button>
          <button class="btn" id="next-page" type="button">Next</button>
        </div>
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Count</th>
              <th>Profile ID</th>
              <th>Full Name</th>
              <th>Email</th>
              <th>Designation</th>
              <th>Related Trade</th>
              <th>Old Pass Count</th>
              <th>New Pass Count</th>
              <th>Pass Delta</th>
              <th>ID Type</th>
              <th>ID Number</th>
              <th>Phone Number</th>
              <th>Deleted</th>
              <th>Status</th>
              <th>Category</th>
              <th>Company</th>
              <th>Contract</th>
              <th>Work Permit Expiry</th>
            </tr>
          </thead>
          <tbody id="person-table-body"></tbody>
        </table>
        <div class="empty-state" id="empty-state">
          Loading report data...
        </div>
      </div>

      <div class="table-footer">
        <div class="hint">Pagination is client-side. Use the buttons above to move through the captured profiles.</div>
        <div class="hint" id="range-info"></div>
      </div>
    </section>
  </div>

  <script>
    const REPORT_DATA_URL = {data_file_name};
    const REPORT_FALLBACK_JS_URL = {data_js_name};
    const PAGE_SIZE = {self.PAGE_SIZE};
    const tbody = document.getElementById('person-table-body');
    const emptyState = document.getElementById('empty-state');
    const prevButton = document.getElementById('prev-page');
    const nextButton = document.getElementById('next-page');
    const pageInfo = document.getElementById('page-info');
    const rangeInfo = document.getElementById('range-info');
    const dataLoadLabel = 'No created profiles were captured for this run.';
    let currentPage = 1;
    let rows = [];

    function escapeHtml(value) {{
      return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }}

    function displayValue(value) {{
      const text = value === null || value === undefined || value === '' ? '—' : String(value);
      return escapeHtml(text);
    }}

    function statusSlug(value) {{
      const slug = String(value ?? '').trim().toLowerCase().replace(/[\\s_]+/g, '-').replace(/[^a-z0-9-]/g, '');
      return slug || 'unknown';
    }}

    function statusClass(value) {{
      const slug = statusSlug(value);
      return ['active', 'pending', 'inactive', 'rejected', 'expired', 'banned', 'archived'].includes(slug) ? slug : 'unknown';
    }}

    function categoryClass(value) {{
      const slug = statusSlug(value);
      return ['employee', 'visitor'].includes(slug) ? slug : 'unknown';
    }}

    function deletedClass(value) {{
      return value ? 'negative' : 'positive';
    }}

    function passDeltaClass(value) {{
      const num = Number(value);
      if (!Number.isFinite(num) || num === 0) {{
        return 'zero';
      }}
      return num > 0 ? 'positive' : 'negative';
    }}

    function renderRow(row, rowNumber) {{
      const status = row.status ?? '';
      const category = row.category ?? '';
      return `
        <tr>
          <td>${{displayValue(rowNumber)}}</td>
          <td>${{displayValue(row.profile_id)}}</td>
          <td>${{displayValue(row.full_name)}}</td>
          <td>${{displayValue(row.email)}}</td>
          <td>${{displayValue(row.designation)}}</td>
          <td>${{displayValue(row.related_trade)}}</td>
          <td>${{displayValue(row.old_pass_count)}}</td>
          <td>${{displayValue(row.new_pass_count)}}</td>
          <td><span class="delta ${{passDeltaClass(row.pass_delta)}}">${{displayValue(row.pass_delta)}}</span></td>
          <td>${{displayValue(row.identity_type)}}</td>
          <td>${{displayValue(row.identity_number)}}</td>
          <td>${{displayValue(row.phone_number)}}</td>
          <td><span class="delta ${{deletedClass(row.deleted)}}">${{displayValue(row.deleted ? 'Yes' : 'No')}}</span></td>
          <td><span class="badge ${{statusClass(status)}}">${{displayValue(status || 'unknown')}}</span></td>
          <td><span class="badge ${{categoryClass(category)}}">${{displayValue(category || 'unknown')}}</span></td>
          <td>${{displayValue(row.company)}}</td>
          <td>${{displayValue(row.contract)}}</td>
          <td>${{displayValue(row.work_permit_expiry)}}</td>
        </tr>
      `;
    }}

    function renderPage(page) {{
      const totalRows = rows.length;
      const totalPages = totalRows === 0 ? 1 : Math.ceil(totalRows / PAGE_SIZE);
      currentPage = Math.min(Math.max(page, 1), totalPages);
      const start = (currentPage - 1) * PAGE_SIZE;
      const pageRows = rows.slice(start, start + PAGE_SIZE);

      const hasRows = totalRows > 0;
      emptyState.hidden = hasRows;
      prevButton.disabled = !hasRows || currentPage === 1;
      nextButton.disabled = !hasRows || currentPage === totalPages;

      if (hasRows) {{
        const end = Math.min(start + PAGE_SIZE, totalRows);
        pageInfo.textContent = `Page ${{currentPage}} of ${{totalPages}}`;
        rangeInfo.textContent = `Showing ${{start + 1}}-${{end}} of ${{totalRows}} profiles`;
        tbody.innerHTML = pageRows.map((row, index) => renderRow(row, start + index + 1)).join('');
      }} else {{
        pageInfo.textContent = 'No rows';
        rangeInfo.textContent = '';
        tbody.innerHTML = '';
      }}
    }}

    function loadFallbackScript() {{
      return new Promise((resolve, reject) => {{
        if (window.__REPORT_DATA__) {{
          resolve(window.__REPORT_DATA__);
          return;
        }}

        const script = document.createElement('script');
        script.src = REPORT_FALLBACK_JS_URL;
        script.async = true;
        script.onload = () => {{
          if (window.__REPORT_DATA__) {{
            resolve(window.__REPORT_DATA__);
          }} else {{
            reject(new Error('Fallback data script did not populate report data'));
          }}
        }};
        script.onerror = () => reject(new Error('Unable to load fallback report data'));
        document.head.appendChild(script);
      }});
    }}

    async function loadReportData() {{
      try {{
        const response = await fetch(REPORT_DATA_URL, {{ cache: 'no-store' }});
        if (!response.ok) {{
          throw new Error(`Failed to load report data: ${{response.status}} ${{response.statusText}}`);
        }}
        return await response.json();
      }} catch (error) {{
        return loadFallbackScript();
      }}
    }}

    function initializeReport(data) {{
      rows = Array.isArray(data && data.rows) ? data.rows : [];
      emptyState.textContent = rows.length ? '' : dataLoadLabel;
      emptyState.hidden = rows.length > 0;
      renderPage(1);
    }}

    function handleLoadError(error) {{
      console.error(error);
      rows = [];
      emptyState.textContent = 'Unable to load report data. ' + (error && error.message ? error.message : '');
      emptyState.hidden = false;
      pageInfo.textContent = 'No rows';
      rangeInfo.textContent = '';
      tbody.innerHTML = '';
      prevButton.disabled = true;
      nextButton.disabled = true;
    }}

    loadReportData()
      .then(initializeReport)
      .catch(handleLoadError);

    prevButton.addEventListener('click', () => renderPage(currentPage - 1));
    nextButton.addEventListener('click', () => renderPage(currentPage + 1));
  </script>
</body>
</html>
"""


# DB HELPERS
# ============================================================
def connect_db(cfg: DBConfig):
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )


def fetchone(cur, query: str, params=None):
    cur.execute(query, params or ())
    return cur.fetchone()


def fetchall(cur, query: str, params=None):
    cur.execute(query, params or ())
    return cur.fetchall()


def normalize_str(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value if value else None


def normalize_upper(value: Optional[str]) -> Optional[str]:
    value = normalize_str(value)
    return value.upper() if value else None


def safe_json(data: Dict[str, Any]) -> str:
    return json.dumps(data, default=str)


def map_person_status(old_status: Optional[str]) -> str:
    if not old_status:
        return "active"
    return PERSON_STATUS_MAP.get(old_status.lower(), "active")


def map_pass_status(old_status: Optional[str]) -> str:
    if not old_status:
        return "pending"
    return PASS_STATUS_MAP.get(old_status.lower(), "pending")


def resolve_identity(applicant: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Returns:
        (id_type_slug, id_number, error)
    """
    nric_number = normalize_str(applicant.get("nric_number"))
    nric_alpha = normalize_upper(applicant.get("nric_alpha"))
    fin_number = normalize_str(applicant.get("fin_number"))
    fin_alpha = normalize_upper(applicant.get("fin_alpha"))
    passport = normalize_upper(applicant.get("passport"))

    has_nric = bool(nric_number and nric_alpha)
    has_fin = bool(fin_number and fin_alpha)
    has_passport = bool(passport)

    source_count = sum([has_nric, has_fin, has_passport])

    if source_count == 0:
        return None, None, "missing_identity"

    if source_count > 1:
        return None, None, "multiple_identity_sources"

    if has_nric:
        return "nric", f"{nric_number}{nric_alpha}", None
    if has_fin:
        return "fin", f"{fin_number}{fin_alpha}", None
    return "passport", passport, None

def resolve_new_trade_id(old_cur, new_cur, stats, applicant, dry_run):
    old_trade = get_old_trade_information(old_cur, applicant.get("trade_information_id"))
    if not old_trade:
        return None

    group_id = None
    old_group = get_old_trade_information_group(old_cur, old_trade.get("group_id"))
    if old_group:
        new_group = ensure_trade_group(new_cur, stats, old_group, dry_run)
        if new_group:
            group_id = new_group["id"]

    new_trade = ensure_trade(new_cur, stats, old_trade, group_id, dry_run)
    if not new_trade:
        return None

    return new_trade["id"]


def map_category_slug(applicant_type: str) -> Optional[str]:
    return CATEGORY_MAP.get(applicant_type)


def map_pass_type_slug(applicant_type: str) -> Optional[str]:
    return PASS_TYPE_MAP.get(applicant_type)


def ensure_file_copy(old_path: str) -> str:
    if not COPY_PHOTO_FILES:
        return old_path

    if not OLD_MEDIA_ROOT or not NEW_MEDIA_ROOT:
        raise ValueError("OLD_MEDIA_ROOT and NEW_MEDIA_ROOT must be set if COPY_PHOTO_FILES=True")

    src = os.path.join(OLD_MEDIA_ROOT, old_path)
    dst = os.path.join(NEW_MEDIA_ROOT, old_path)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copy2(src, dst)
    return old_path


def cleanup_migration_tables(new_cur) -> Dict[str, int]:
    counts = {}
    for table in ROLLBACK_CLEANUP_TABLES:
        row = fetchone(new_cur, f"SELECT COUNT(*) AS cnt FROM {table}")
        counts[table] = int(row["cnt"]) if row else 0

    new_cur.execute(
        f"""
        TRUNCATE TABLE {", ".join(ROLLBACK_CLEANUP_TABLES)}
        RESTART IDENTITY CASCADE
        """
    )
    return counts


# ============================================================
# MASTER DATA HELPERS
# ============================================================
def get_slug_record(cur, table: str, slug: str):
    return fetchone(
        cur,
        f"SELECT id, slug, name FROM {table} WHERE slug = %s LIMIT 1",
        (slug,),
    )


def ensure_master_data(new_cur, dry_run: bool):
    # only validate existence — DO NOT create anything

    for slug in ["employee", "visitor"]:
        rec = get_person_category(new_cur, slug)
        if not rec:
            raise Exception(f"Missing person category slug={slug}")

    for old_slug in ["staff", "delivery_regular", "visitor", "delivery_adhoc"]:
        mapped_slug = PASS_TYPE_MAP[old_slug]
        rec = get_pass_type(new_cur, mapped_slug)
        if not rec:
            raise Exception(
                f"Missing pass type slug={mapped_slug} (mapped from {old_slug})"
            )

    for slug in ["nric", "fin", "passport"]:
        rec = get_ic_type(new_cur, slug)
        if not rec:
            raise Exception(f"Missing ic type slug={slug}")


def seed_reference_data_from_old_db(old_cur, new_cur, stats: MigrationStats, dry_run: bool):
    seed_profile_id = "__lookup_seed__"
    seed_full_name = "lookup tables"

    old_companies = fetchall(
        old_cur,
        f"SELECT id, name, created, modified FROM {OLD_TABLES['company']} ORDER BY id"
    )
    for old_company in old_companies:
        ensure_company(new_cur, stats, dict(old_company), seed_profile_id, seed_full_name, dry_run)

    old_contracts = fetchall(
        old_cur,
        f"SELECT id, name, created, modified FROM {OLD_TABLES['contract']} ORDER BY id"
    )
    for old_contract in old_contracts:
        ensure_contract(new_cur, stats, dict(old_contract), seed_profile_id, seed_full_name, dry_run)

    old_trade_groups = fetchall(
        old_cur,
        f"SELECT id, name, created, modified FROM {OLD_TABLES['trade_information_group']} ORDER BY id"
    )
    seeded_group_ids = {}
    for old_group in old_trade_groups:
        new_group = ensure_trade_group(new_cur, stats, dict(old_group), dry_run)
        if new_group:
            seeded_group_ids[old_group["id"]] = new_group["id"]

    old_trades = fetchall(
        old_cur,
        f"""
        SELECT id, name, group_id, created, modified
        FROM {OLD_TABLES['trade_information']}
        ORDER BY id
        """
    )
    for old_trade in old_trades:
        group_id = seeded_group_ids.get(old_trade.get("group_id"))
        ensure_trade(new_cur, stats, dict(old_trade), group_id, dry_run)


# ============================================================
# LOOKUPS
# ============================================================
def get_ic_type(cur, slug):
    return fetchone(
        cur,
        f"""
        SELECT id, name, slug
        FROM {NEW_TABLES['ic_type']}
        WHERE slug = %s
        """,
        (slug,),
    )


def get_person_category(cur, slug):
    return fetchone(
        cur,
        f"""
        SELECT id, name, slug
        FROM {NEW_TABLES['person_category']}
        WHERE slug = %s
        """,
        (slug,),
    )


def get_pass_type(cur, slug):
    return fetchone(
        cur,
        f"""
        SELECT id, name, slug
        FROM {NEW_TABLES['person_access_pass_type']}
        WHERE slug = %s
        """,
        (slug,),
    )

def get_old_company(old_cur, company_id):
    if not company_id:
        return None
    return fetchone(
        old_cur,
        f"SELECT id, name, created, modified FROM {OLD_TABLES['company']} WHERE id = %s LIMIT 1",
        (company_id,),
    )


def get_old_contract(old_cur, contract_id):
    if not contract_id:
        return None
    return fetchone(
        old_cur,
        f"SELECT id, name, created, modified FROM {OLD_TABLES['contract']} WHERE id = %s LIMIT 1",
        (contract_id,),
    )


def find_company_by_source_id(new_cur, company_id):
    return fetchone(
        new_cur,
        f"SELECT id, name FROM {NEW_TABLES['company']} WHERE id = %s LIMIT 1",
        (company_id,),
    )


def find_contract_by_name(new_cur, contract_name: str):
    return fetchone(
        new_cur,
        f"""
        SELECT id, name
        FROM {NEW_TABLES['contract']}
        WHERE LOWER(name) = LOWER(%s)
        LIMIT 1
        """,
        (contract_name,),
    )

def get_old_trade_information(old_cur, trade_information_id):
    if not trade_information_id:
        return None
    return fetchone(
        old_cur,
        f"""
        SELECT id, name, group_id, created, modified
        FROM {OLD_TABLES['trade_information']}
        WHERE id = %s
        LIMIT 1
        """,
        (trade_information_id,),
    )


def get_old_trade_information_group(old_cur, group_id):
    if not group_id:
        return None
    return fetchone(
        old_cur,
        f"""
        SELECT id, name, created, modified
        FROM {OLD_TABLES['trade_information_group']}
        WHERE id = %s
        LIMIT 1
        """,
        (group_id,),
    )

def find_trade_group_by_name(new_cur, group_name: str):
    return fetchone(
        new_cur,
        f"""
        SELECT id, name
        FROM {NEW_TABLES['trade_group']}
        WHERE LOWER(name) = LOWER(%s)
        LIMIT 1
        """,
        (group_name,),
    )


def find_trade_by_name_and_group(new_cur, trade_name: str, group_id):
    if group_id:
        return fetchone(
            new_cur,
            f"""
            SELECT id, name, group_id, code
            FROM {NEW_TABLES['trade']}
            WHERE LOWER(name) = LOWER(%s) AND group_id = %s
            LIMIT 1
            """,
            (trade_name, group_id),
        )

        return fetchone(
            new_cur,
            f"""
            SELECT id, name, group_id, code
            FROM {NEW_TABLES['trade']}
            WHERE LOWER(name) = LOWER(%s)
            LIMIT 1
            """,
            (trade_name,),
    )


def find_person_by_profile_id(new_cur, profile_id: str):
    return fetchone(
        new_cur,
        f"""
        SELECT id, profile_id, full_name, work_permit_expiry
        FROM {NEW_TABLES['person_profile']}
        WHERE profile_id = %s
        LIMIT 1
        """,
        (profile_id,),
    )


def find_pass_by_pass_id(new_cur, pass_id: str):
    return fetchone(
        new_cur,
        f"""
        SELECT id, pass_id
        FROM {NEW_TABLES['person_access_pass']}
        WHERE pass_id = %s
        LIMIT 1
        """,
        (pass_id,),
    )


# ============================================================
# CREATE / ENSURE HELPERS
# ============================================================
def ensure_company(new_cur, stats, old_company, profile_id, full_name, dry_run):
    source_company_id = old_company.get("id") if old_company else None
    company_name = old_company.get("name") if old_company else None
    created_at, modified_at = source_timestamps(old_company)
    if not old_company or not company_name:
        stats.inc("company", "skipped")
        stats.add_report_row(
            "Company",
            {
                "Event": "company",
                "Result": "skipped",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Company ID": source_company_id,
                "Company Name": company_name,
                "Company ID": None,
                "Person-Company ID": None,
                "Message": "missing company name",
            },
        )
        return None

    company_name = company_name.strip()

    existing = find_company_by_source_id(new_cur, source_company_id)

    if existing:
        stats.inc("company", "existing")
        stats.add_preview("company", f"EXISTING | {company_name}")
        stats.add_report_row(
            "Company",
            {
                "Event": "company",
                "Result": "existing",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Company ID": source_company_id,
                "Company Name": company_name,
                "Company ID": existing["id"],
                "Person-Company ID": None,
                "Message": "matched existing company",
            },
        )
        return existing

    if dry_run:
        stats.inc("company", "created")
        stats.add_preview("company", f"CREATE   | {company_name}")
        fake = {
            "id": stable_pk(source_company_id),
            "name": company_name,
            "created": created_at,
            "modified": modified_at,
        }
        stats.add_report_row(
            "Company",
            {
                "Event": "company",
                "Result": "created",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Company ID": source_company_id,
                "Company Name": company_name,
                "Company ID": fake["id"],
                "Person-Company ID": None,
                "Message": "dry-run",
            },
        )
        return fake

    try:
        new_id = stable_pk(source_company_id)

        new_cur.execute(
            f"""
            INSERT INTO {NEW_TABLES['company']} (
                id,
                name,
                created,
                modified,
                current_sequence_number,
                "default"
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id, name, created, modified
            """,
            (
                new_id,
                company_name,
                created_at,
                modified_at,
                0,
                False,
            ),
        )

        created = new_cur.fetchone()
        stats.inc("company", "created")
        stats.add_preview("company", f"CREATE   | {company_name}")
        stats.add_report_row(
            "Company",
            {
                "Event": "company",
                "Result": "created",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Company ID": source_company_id,
                "Company Name": company_name,
                "Company ID": created["id"],
                "Person-Company ID": None,
                "Message": "created",
            },
        )

        return created

    except Exception as e:
        stats.inc("company", "failed")
        stats.add_preview("company", f"FAILED   | {company_name}")
        stats.add_report_row(
            "Company",
            {
                "Event": "company",
                "Result": "failed",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Company ID": source_company_id,
                "Company Name": company_name,
                "Company ID": None,
                "Person-Company ID": None,
                "Message": str(e),
            },
        )
        print("COMPANY ERROR:", company_name, e)
        raise


def ensure_contract(new_cur, stats: MigrationStats, old_contract: Dict[str, Any], profile_id, full_name, dry_run: bool):
    source_contract_id = old_contract.get("id") if old_contract else None
    contract_name = old_contract.get("name") if old_contract else None
    created_at, modified_at = source_timestamps(old_contract)
    if not old_contract or not contract_name:
        stats.inc("contract", "skipped")
        stats.add_report_row(
            "Contract",
            {
                "Event": "contract",
                "Result": "skipped",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Contract ID": source_contract_id,
                "Contract Name": contract_name,
                "Contract ID": None,
                "Person-Contract ID": None,
                "Message": "missing contract name",
            },
        )
        return None

    contract_name = contract_name.strip()
    existing = find_contract_by_name(new_cur, contract_name)
    if existing:
        stats.inc("contract", "existing")
        stats.add_preview("contract", f"EXISTING | {existing['name']}")
        stats.add_report_row(
            "Contract",
            {
                "Event": "contract",
                "Result": "existing",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Contract ID": source_contract_id,
                "Contract Name": existing["name"],
                "Contract ID": existing["id"],
                "Person-Contract ID": None,
                "Message": "matched existing contract",
            },
        )
        return existing

    if dry_run:
        fake = {
            "id": stable_pk(source_contract_id),
            "name": contract_name,
            "created": created_at,
            "modified": modified_at,
        }
        stats.inc("contract", "created")
        stats.add_preview("contract", f"CREATE   | {contract_name}")
        stats.add_report_row(
            "Contract",
            {
                "Event": "contract",
                "Result": "created",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Contract ID": source_contract_id,
                "Contract Name": contract_name,
                "Contract ID": fake["id"],
                "Person-Contract ID": None,
                "Message": "dry-run",
            },
        )
        return fake

    try:
        new_id = stable_pk(source_contract_id)
        new_cur.execute(
            f"""
            INSERT INTO {NEW_TABLES['contract']} (id, name, created, modified)
            VALUES (%s, %s, %s, %s)
            RETURNING id, name, created, modified
            """,
            (new_id, contract_name, created_at, modified_at),
        )
        created = new_cur.fetchone()
        stats.inc("contract", "created")
        stats.add_preview("contract", f"CREATE   | {created['name']}")
        stats.add_report_row(
            "Contract",
            {
                "Event": "contract",
                "Result": "created",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Contract ID": source_contract_id,
                "Contract Name": created["name"],
                "Contract ID": created["id"],
                "Person-Contract ID": None,
                "Message": "created",
            },
        )
        return created
    except Exception as e:
        stats.inc("contract", "failed")
        stats.add_preview("contract", f"FAILED   | {contract_name}")
        stats.add_report_row(
            "Contract",
            {
                "Event": "contract",
                "Result": "failed",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Contract ID": source_contract_id,
                "Contract Name": contract_name,
                "Contract ID": None,
                "Person-Contract ID": None,
                "Message": str(e),
            },
        )
        raise


def ensure_person_company(new_cur, stats: MigrationStats, person_id, company_id, modified_by_id, profile_id, full_name, company_name, dry_run: bool):
    if not person_id or not company_id:
        stats.inc("person_company", "skipped")
        stats.add_report_row(
            "Company",
            {
                "Event": "link",
                "Result": "skipped",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Company ID": company_id,
                "Company Name": company_name,
                "Company ID": company_id,
                "Person-Company ID": None,
                "Message": "missing person/company link",
            },
        )
        return None

    existing = fetchone(
        new_cur,
        f"""
        SELECT id FROM {NEW_TABLES['person_company']}
        WHERE person_id = %s AND company_id = %s AND is_active = TRUE
        LIMIT 1
        """,
        (person_id, company_id),
    )
    if existing:
        stats.inc("person_company", "existing")
        stats.add_preview("person_company", f"EXISTING | {profile_id} -> {company_name}")
        stats.add_report_row(
            "Company",
            {
                "Event": "link",
                "Result": "existing",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Company ID": company_id,
                "Company Name": company_name,
                "Company ID": company_id,
                "Person-Company ID": existing["id"],
                "Message": "existing person-company link",
            },
        )
        return existing

    if dry_run:
        fake = {
            "id": stable_relation_pk(person_id, company_id, prefix="person_company"),
        }
        stats.inc("person_company", "created")
        stats.add_preview("person_company", f"CREATE   | {profile_id} -> {company_name}")
        stats.add_report_row(
            "Company",
            {
                "Event": "link",
                "Result": "created",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Company ID": company_id,
                "Company Name": company_name,
                "Company ID": company_id,
                "Person-Company ID": fake["id"],
                "Message": "dry-run",
            },
        )
        return fake

    try:
        new_id = stable_relation_pk(person_id, company_id, prefix="person_company")
        new_cur.execute(
            f"""
            INSERT INTO {NEW_TABLES['person_company']} (
                id, person_id, company_id, modified_by_id, is_active, created, modified
            )
            VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
            RETURNING id
            """,
            (new_id, person_id, company_id, modified_by_id, True),
        )
        created = new_cur.fetchone()
        stats.inc("person_company", "created")
        stats.add_preview("person_company", f"CREATE   | {profile_id} -> {company_name}")
        stats.add_report_row(
            "Company",
            {
                "Event": "link",
                "Result": "created",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Company ID": company_id,
                "Company Name": company_name,
                "Company ID": company_id,
                "Person-Company ID": created["id"],
                "Message": "created",
            },
        )
        return created
    except Exception as e:
        stats.inc("person_company", "failed")
        stats.add_preview("person_company", f"FAILED   | {profile_id} -> {company_name}")
        stats.add_report_row(
            "Company",
            {
                "Event": "link",
                "Result": "failed",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Company ID": company_id,
                "Company Name": company_name,
                "Company ID": company_id,
                "Person-Company ID": None,
                "Message": str(e),
            },
        )
        raise


def ensure_person_contract(new_cur, stats: MigrationStats, person_id, contract_id, modified_by_id, profile_id, full_name, contract_name, dry_run: bool):
    if not person_id or not contract_id:
        stats.inc("person_contract", "skipped")
        stats.add_report_row(
            "Contract",
            {
                "Event": "link",
                "Result": "skipped",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Contract ID": contract_id,
                "Contract Name": contract_name,
                "Contract ID": contract_id,
                "Person-Contract ID": None,
                "Message": "missing person/contract link",
            },
        )
        return None

    existing = fetchone(
        new_cur,
        f"""
        SELECT id FROM {NEW_TABLES['person_contract']}
        WHERE person_id = %s AND contract_id = %s AND is_active = TRUE
        LIMIT 1
        """,
        (person_id, contract_id),
    )
    if existing:
        stats.inc("person_contract", "existing")
        stats.add_preview("person_contract", f"EXISTING | {profile_id} -> {contract_name}")
        stats.add_report_row(
            "Contract",
            {
                "Event": "link",
                "Result": "existing",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Contract ID": contract_id,
                "Contract Name": contract_name,
                "Contract ID": contract_id,
                "Person-Contract ID": existing["id"],
                "Message": "existing person-contract link",
            },
        )
        return existing

    if dry_run:
        fake = {
            "id": stable_relation_pk(person_id, contract_id, prefix="person_contract"),
        }
        stats.inc("person_contract", "created")
        stats.add_preview("person_contract", f"CREATE   | {profile_id} -> {contract_name}")
        stats.add_report_row(
            "Contract",
            {
                "Event": "link",
                "Result": "created",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Contract ID": contract_id,
                "Contract Name": contract_name,
                "Contract ID": contract_id,
                "Person-Contract ID": fake["id"],
                "Message": "dry-run",
            },
        )
        return fake

    try:
        new_id = stable_relation_pk(person_id, contract_id, prefix="person_contract")
        new_cur.execute(
            f"""
            INSERT INTO {NEW_TABLES['person_contract']} (
                id, person_id, contract_id, modified_by_id, is_active, created, modified
            )
            VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
            RETURNING id
            """,
            (new_id, person_id, contract_id, modified_by_id, True),
        )
        created = new_cur.fetchone()
        stats.inc("person_contract", "created")
        stats.add_preview("person_contract", f"CREATE   | {profile_id} -> {contract_name}")
        stats.add_report_row(
            "Contract",
            {
                "Event": "link",
                "Result": "created",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Contract ID": contract_id,
                "Contract Name": contract_name,
                "Contract ID": contract_id,
                "Person-Contract ID": created["id"],
                "Message": "created",
            },
        )
        return created
    except Exception as e:
        stats.inc("person_contract", "failed")
        stats.add_preview("person_contract", f"FAILED   | {profile_id} -> {contract_name}")
        stats.add_report_row(
            "Contract",
            {
                "Event": "link",
                "Result": "failed",
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Source Contract ID": contract_id,
                "Contract Name": contract_name,
                "Contract ID": contract_id,
                "Person-Contract ID": None,
                "Message": str(e),
            },
        )
        raise

def ensure_trade_group(new_cur, stats, old_group, dry_run):
    if not old_group or not old_group.get("name"):
        stats.inc("trade_group", "skipped")
        return None

    group_name = old_group["name"].strip()
    created_at, modified_at = source_timestamps(old_group)

    existing = find_trade_group_by_name(new_cur, group_name)
    if existing:
        stats.inc("trade_group", "existing")
        stats.add_preview("trade_group", f"EXISTING | {group_name}")
        return existing

    if dry_run:
        stats.inc("trade_group", "created")
        stats.add_preview("trade_group", f"CREATE   | {group_name}")
        return {
            "id": stable_pk(old_group.get("id")),
            "name": group_name,
            "created": created_at,
            "modified": modified_at,
        }

    try:
        new_id = stable_pk(old_group.get("id"))
        new_cur.execute(
            f"""
            INSERT INTO {NEW_TABLES['trade_group']} (
                id,
                name,
                created,
                modified
            )
            VALUES (%s, %s, %s, %s)
            RETURNING id, name, created, modified
            """,
            (
                new_id,
                group_name,
                created_at,
                modified_at,
            ),
        )
        created = new_cur.fetchone()
        stats.inc("trade_group", "created")
        stats.add_preview("trade_group", f"CREATE   | {group_name}")
        return created
    except Exception as e:
        stats.inc("trade_group", "failed")
        stats.add_preview("trade_group", f"FAILED   | {group_name}")
        print("TRADE GROUP ERROR:", group_name, e)
        raise

def ensure_trade(new_cur, stats, old_trade, group_id, dry_run):
    if not old_trade or not old_trade.get("name"):
        stats.inc("trade", "skipped")
        return None

    trade_name = old_trade["name"].strip()
    created_at, modified_at = source_timestamps(old_trade)

    existing = find_trade_by_name_and_group(new_cur, trade_name, group_id)
    if existing:
        existing_code = existing.get("code")
        if not existing_code or not str(existing_code).strip():
            assigned_code = allocate_trade_code(new_cur)
            if dry_run:
                existing["code"] = assigned_code
                stats.add_preview("trade", f"BACKFILL | {trade_name} | code={assigned_code}")
            else:
                new_cur.execute(
                    f"""
                    UPDATE {NEW_TABLES['trade']}
                    SET code = %s
                    WHERE id = %s
                      AND (code IS NULL OR BTRIM(code) = '')
                    """,
                    (assigned_code, existing["id"]),
                )
                existing["code"] = assigned_code
                stats.add_preview("trade", f"BACKFILL | {trade_name} | code={assigned_code}")
        stats.inc("trade", "existing")
        stats.add_preview("trade", f"EXISTING | {trade_name}")
        return existing

    trade_code = allocate_trade_code(new_cur)

    if dry_run:
        stats.inc("trade", "created")
        stats.add_preview("trade", f"CREATE   | {trade_name}")
        return {
            "id": stable_pk(old_trade.get("id")),
            "name": trade_name,
            "group_id": group_id,
            "code": trade_code,
            "created": created_at,
            "modified": modified_at,
        }

    try:
        new_id = stable_pk(old_trade.get("id"))
        new_cur.execute(
            f"""
            INSERT INTO {NEW_TABLES['trade']} (
                id,
                name,
                code,
                group_id,
                created,
                modified
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id, name, group_id, created, modified
            """,
            (
                new_id,
                trade_name,
                trade_code,
                group_id,
                created_at,
                modified_at,
            ),
        )
        created = new_cur.fetchone()
        stats.inc("trade", "created")
        stats.add_preview("trade", f"CREATE   | {trade_name}")
        return created
    except Exception as e:
        stats.inc("trade", "failed")
        stats.add_preview("trade", f"FAILED   | {trade_name}")
        print("TRADE ERROR:", trade_name, e)
        raise


def ensure_ic_record(new_cur, stats, person_id, ic_type_id, id_number, profile_id, full_name, dry_run):
    if not person_id or not ic_type_id or not id_number:
        stats.inc("ic", "skipped")
        return None

    existing = fetchone(
        new_cur,
        f"""
        SELECT id
        FROM {NEW_TABLES['ic']}
        WHERE person_id = %s AND ic_type_id = %s
        LIMIT 1
        """,
        (person_id, ic_type_id),
    )
    if existing:
        stats.inc("ic", "existing")
        stats.add_preview("ic", f"EXISTING | {profile_id:<18} | {id_number}")
        return existing

    if dry_run:
        stats.inc("ic", "created")
        stats.add_preview("ic", f"CREATE   | {profile_id:<18} | {id_number}")
        return {"id": stable_relation_pk(person_id, ic_type_id, prefix="ic")}

    try:
        new_id = stable_relation_pk(person_id, ic_type_id, prefix="ic")

        new_cur.execute(
            f"""
            INSERT INTO {NEW_TABLES['ic']} (
                id,
                ic_number,
                is_default,
                created_at,
                updated_at,
                ic_type_id,
                person_id,
                created_by_id,
                updated_by_id
            )
            VALUES (%s, %s, %s, NOW(), NOW(), %s, %s, %s, %s)
            RETURNING id
            """,
            (
                new_id,
                id_number,
                True,
                ic_type_id,
                person_id,
                None,
                None,
            ),
        )

        created = new_cur.fetchone()
        stats.inc("ic", "created")
        stats.add_preview("ic", f"CREATE   | {profile_id:<18} | {id_number}")
        return created

    except Exception as e:
        stats.inc("ic", "failed")
        stats.add_preview("ic", f"FAILED   | {profile_id:<18} | {id_number}")
        print(f"IC ERROR | {profile_id} | {full_name} | {e}")
        raise


def ensure_person_photo(new_cur, stats: MigrationStats, person_id, photo_id, avatar_path, profile_id, full_name, dry_run: bool):
    if not person_id or not avatar_path:
        stats.inc("person_photo", "skipped")
        return None

    existing = fetchone(
        new_cur,
        f"""
        SELECT id
        FROM {NEW_TABLES['person_photo']}
        WHERE person_id = %s AND avatar = %s
        LIMIT 1
        """,
        (person_id, avatar_path),
    )
    if existing:
        stats.inc("person_photo", "existing")
        stats.add_preview("person_photo", f"EXISTING | {profile_id} | {full_name} | {avatar_path}")
        return existing

    if dry_run:
        fake = {
            "id": stable_pk(photo_id),
        }
        stats.inc("person_photo", "created")
        stats.add_preview("person_photo", f"CREATE   | {profile_id} | {full_name} | {avatar_path}")
        return fake

    try:
        new_id = stable_pk(photo_id)
        new_cur.execute(
            f"""
            INSERT INTO {NEW_TABLES['person_photo']} (
                id, person_id, avatar, is_deleted, created, modified
            )
            VALUES (%s, %s, %s, %s, NOW(), NOW())
            RETURNING id
            """,
            (new_id, person_id, avatar_path, False),
        )
        created = new_cur.fetchone()
        stats.inc("person_photo", "created")
        stats.add_preview("person_photo", f"CREATE   | {profile_id} | {full_name} | {avatar_path}")
        return created
    except Exception:
        stats.inc("person_photo", "failed")
        stats.add_preview("person_photo", f"FAILED   | {profile_id} | {full_name} | {avatar_path}")
        raise


# ============================================================
# MIGRATION HELPERS
# ============================================================
def load_total_applicant_count(old_cur):
    row = fetchone(
        old_cur,
        f"SELECT COUNT(*) AS cnt FROM {OLD_TABLES['applicant_profile']}"
    )
    return row["cnt"]


def load_applicants_batch(old_cur, limit: int, offset: int):
    return fetchall(
        old_cur,
        f"""
        SELECT
            id,
            applicant_id,
            full_name,
            nric_number,
            nric_alpha,
            fin_number,
            fin_alpha,
            passport,
            mobile_number,
            work_permit_expiry,
            email,
            date_of_birth,
            gender,
            nationality,
            vehicle_number,
            company_id,
            contract_id,
            designation,
            trade_information_id,
            applicant_type,
            status,
            reject_note,
            previous_status,
            created_by_id,
            updated_by_id,
            created,
            modified,
            is_deleted
        FROM {OLD_TABLES['applicant_profile']}
        ORDER BY created ASC, applicant_id ASC
        LIMIT %s OFFSET %s
        """,
        (limit, offset),
    )


def load_applicant_photos(old_cur, applicant_pk):
    return fetchall(
        old_cur,
        f"""
        SELECT id, applicant_id, avatar, is_deleted, created, modified
        FROM {OLD_TABLES['applicant_photo']}
        WHERE applicant_id = %s AND is_deleted = FALSE
        ORDER BY created ASC
        """,
        (applicant_pk,),
    )


def load_applicant_gate_passes(old_cur, applicant_pk):
    return fetchall(
        old_cur,
        f"""
        SELECT
            id,
            pass_id,
            from_date_time,
            to_date_time,
            is_permanent,
            host_name,
            purpose_of_visit,
            po_number,
            do_number,
            qr_code,
            vehicle_number,
            applicant_id,
            contract_id,
            reject_note,
            status,
            created,
            modified
        FROM {OLD_TABLES['applicant_gate_pass']}
        WHERE applicant_id = %s
        ORDER BY created ASC, pass_id ASC
        """,
        (applicant_pk,),
    )


def create_or_get_person(old_cur, new_cur, stats: MigrationStats, applicant: Dict[str, Any], dry_run: bool):
    profile_id = applicant["applicant_id"]
    full_name = applicant["full_name"]
    work_permit_before = applicant.get("work_permit_expiry")

    existing = find_person_by_profile_id(new_cur, profile_id)
    if existing:
        existing = dict(existing)
        stats.inc("person_profile", "existing")
        stats.add_preview(
            "person_profile",
            f"EXISTING | {profile_id:<10} | {full_name:<30}"
        )
        existing["_created_now"] = False
        return existing

    category_slug = map_category_slug(applicant["applicant_type"])
    if not category_slug:
        stats.inc("person_profile", "skipped")
        stats.add_preview("person_profile", f"SKIPPED  | {profile_id:<10} | {full_name:<30} | reason=unknown applicant_type")
        return None

    category = get_person_category(new_cur, category_slug)

    if not category:
        raise Exception(f"Category not found slug={category_slug}")

    id_type_slug, id_number, identity_error = resolve_identity(applicant)
    if identity_error:
        stats.inc("person_profile", "skipped")
        stats.add_preview("person_profile", f"SKIPPED  | {profile_id:<10} | {full_name:<30} | reason={identity_error}")
        return None

    person_status = map_person_status(applicant.get("status"))
    note = applicant.get("reject_note") or None
    additional_info = {
        "old_applicant_id": str(applicant.get("id")),
        "old_applicant_type": applicant.get("applicant_type"),
        "old_previous_status": applicant.get("previous_status"),
        "old_trade_information_id": str(applicant.get("trade_information_id")) if applicant.get("trade_information_id") else None,
    }

    if dry_run:
        fake = {
            "id": stable_pk(applicant.get("id")),
            "profile_id": profile_id,
            "full_name": full_name,
            "status": person_status,
            "id_type": id_type_slug,
            "id_number": id_number,
            "work_permit_expiry": work_permit_before,
            "_resolved_id_type": id_type_slug,
            "_resolved_id_number": id_number,
            "_created_now": True,
        }
        stats.inc("person_profile", "created")
        stats.add_preview(
            "person_profile",
            f"CREATE | {profile_id:<18} | {full_name}"
        )
        return fake

    trade_id = resolve_new_trade_id(old_cur, new_cur, stats, applicant, dry_run)

    try:
        new_person_id = stable_pk(applicant.get("id"))
        new_cur.execute(
            f"""
            INSERT INTO {NEW_TABLES['person_profile']} (
                id,
                profile_id,
                full_name,
                id_number,
                id_type,
                mobile_number,
                work_permit_expiry,
                email,
                date_of_birth,
                gender,
                nationality,
                designation,
                trade_id,
                category_id,
                status,
                created_by_id,
                updated_by_id,
                created,
                modified,
                note,
                is_deleted,
                register_mode,
                additional_info
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
            )
            RETURNING id, profile_id, full_name, status, id_type, id_number
            """,
            (
                new_person_id,
                profile_id,
                full_name,
                id_number,
                id_type_slug,
                applicant.get("mobile_number"),
                work_permit_before,
                applicant.get("email"),
                applicant.get("date_of_birth"),
                applicant.get("gender"),
                applicant.get("nationality"),
                applicant.get("designation"),
                trade_id,
                category["id"],
                person_status,
                applicant.get("created_by_id"),
                applicant.get("updated_by_id"),
                applicant.get("created"),
                applicant.get("modified"),
                note,
                applicant.get("is_deleted", False),
                "direct",
                safe_json(additional_info),
            ),
        )
        created = dict(new_cur.fetchone())
        created["_resolved_id_type"] = id_type_slug
        created["_resolved_id_number"] = id_number
        created["_created_now"] = True

        stats.inc("person_profile", "created")
        stats.add_preview(
            "person_profile",
            f"CREATE   | {profile_id:<10} | {full_name:<30}")
        return created
    except Exception as e:
        stats.inc("person_profile", "failed")
        stats.add_preview("person_profile", f"FAILED   | {profile_id:<10} | {full_name:<30}")
        raise


def migrate_person_related(old_cur, new_cur, stats: MigrationStats, applicant: Dict[str, Any], person: Dict[str, Any], dry_run: bool):
    profile_id = applicant["applicant_id"]
    full_name = applicant["full_name"]

    # IC
    id_type_slug = person.get("_resolved_id_type")
    id_number = person.get("_resolved_id_number")
    if id_type_slug and id_number:
        ic_type = get_ic_type(new_cur, id_type_slug)

        if not ic_type:
            raise Exception(f"IC type not found for slug={id_type_slug}")

        ensure_ic_record(new_cur, stats, person["id"], ic_type["id"], id_number, profile_id, full_name, dry_run)

    # Company
    old_company = get_old_company(old_cur, applicant.get("company_id"))
    created_company = None
    if old_company:
        created_company = ensure_company(new_cur, stats, old_company, profile_id, full_name, dry_run)
        if created_company:
            ensure_person_company(
                new_cur,
                stats,
                person["id"],
                created_company["id"],
                applicant.get("updated_by_id"),
                profile_id,
                full_name,
                created_company["name"],
                dry_run,
            )

    # Contract
    old_contract = get_old_contract(old_cur, applicant.get("contract_id"))
    if old_contract:
        created_contract = ensure_contract(new_cur, stats, old_contract, profile_id, full_name, dry_run)
        if created_contract:
            ensure_person_contract(
                new_cur,
                stats,
                person["id"],
                created_contract["id"],
                applicant.get("updated_by_id"),
                profile_id,
                full_name,
                created_contract["name"],
                dry_run,
            )

def migrate_passes(old_cur, new_cur, stats: MigrationStats, applicant: Dict[str, Any], person: Dict[str, Any], dry_run: bool):
    profile_id = applicant["applicant_id"]
    full_name = applicant["full_name"]
    applicant_type = applicant["applicant_type"]
    gate_passes = load_applicant_gate_passes(old_cur, applicant["id"])
    person["_old_pass_count"] = len(gate_passes)

    pass_type_slug = map_pass_type_slug(applicant_type)
    if not pass_type_slug:
        stats.inc("person_access_pass", "skipped")
        stats.add_preview("person_access_pass", f"SKIPPED  | {profile_id} | {full_name} | reason=unknown pass type")
        stats.add_report_row(
            "Access Pass",
            {
                "Result": "skipped",
                "Old Pass PK": None,
                "Pass ID": None,
                "Profile ID": profile_id,
                "Full Name": full_name,
                "Pass Type": None,
                "Status": None,
                "From Date": None,
                "To Date": None,
                "Contract Name": None,
                "Contract ID": None,
                "Person Access Pass ID": None,
                "Message": "unknown pass type",
            },
        )
        return

    pass_type = get_pass_type(new_cur, pass_type_slug)
    if not pass_type:
        raise ValueError(f"Pass type not found: {pass_type_slug}")

    for gp in gate_passes:
        stats.total_processed_passes += 1
        old_pass_pk = gp.get("id")
        source_pass_id = normalize_str(gp.get("pass_id")) if gp.get("pass_id") is not None else None
        old_contract = get_old_contract(old_cur, gp.get("contract_id"))
        contract_id = old_contract["id"] if old_contract else None
        contract_name = old_contract["name"] if old_contract else None

        existing = None
        if source_pass_id:
            existing = find_pass_by_pass_id(new_cur, source_pass_id)
        if not existing:
            existing = fetchone(
                new_cur,
                f"""
                SELECT id
                FROM {NEW_TABLES['person_access_pass']}
                WHERE id = %s
                LIMIT 1
                """,
                (old_pass_pk,),
            )

        if existing:
            stats.inc("person_access_pass", "existing")
            stats.add_preview(
                "person_access_pass",
                f"EXISTING | {source_pass_id or old_pass_pk:<18} | {profile_id:<10} | {full_name:<30} | type={pass_type_slug}"
            )
            stats.add_report_row(
                "Access Pass",
                {
                    "Result": "existing",
                    "Old Pass PK": old_pass_pk,
                    "Pass ID": source_pass_id,
                    "Profile ID": profile_id,
                    "Full Name": full_name,
                    "Pass Type": pass_type_slug,
                    "Status": gp.get("status"),
                    "From Date": gp.get("from_date_time"),
                    "To Date": gp.get("to_date_time"),
                    "Contract Name": contract_name,
                    "Contract ID": contract_id,
                    "Person Access Pass ID": existing["id"],
                    "Message": "already exists",
                },
            )
            continue

        if old_contract:
            contract = ensure_contract(new_cur, stats, old_contract, profile_id, full_name, dry_run)
            contract_id = contract["id"]
            contract_name = contract["name"]

        if dry_run:
            stats.inc("person_access_pass", "created")
            stats.add_preview(
                "person_access_pass",
                f"CREATE   | {source_pass_id or old_pass_pk:<18} | {profile_id:<10} | {full_name:<30} | type={pass_type_slug}"
            )
            stats.add_report_row(
                "Access Pass",
                {
                    "Result": "created",
                    "Old Pass PK": old_pass_pk,
                    "Pass ID": source_pass_id,
                    "Profile ID": profile_id,
                    "Full Name": full_name,
                    "Pass Type": pass_type_slug,
                    "Status": map_pass_status(gp.get("status")),
                    "From Date": gp.get("from_date_time"),
                    "To Date": gp.get("to_date_time"),
                    "Contract Name": contract_name,
                    "Contract ID": contract_id,
                    "Person Access Pass ID": None,
                    "Message": "dry-run",
                },
            )
            continue

        try:
            new_id = stable_pk(old_pass_pk)
            new_cur.execute(
                f"""
                INSERT INTO {NEW_TABLES['person_access_pass']} (
                    id,
                    pass_id,
                    from_date_time,
                    to_date_time,
                    qr_code,
                    note,
                    modified_by_id,
                    status,
                    host_name,
                    purpose_of_visit,
                    vehicle_number,
                    person_id,
                    mode,
                    type_id,
                    contract_id,
                    created,
                    modified
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING id, pass_id
                """,
                (
                    new_id,
                    gp["pass_id"],
                    gp.get("from_date_time"),
                    gp.get("to_date_time"),
                    gp.get("qr_code"),
                    gp.get("reject_note"),
                    applicant.get("updated_by_id"),
                    map_pass_status(gp.get("status")),
                    gp.get("host_name"),
                    gp.get("purpose_of_visit"),
                    gp.get("vehicle_number"),
                    person["id"],
                    "regular",
                    pass_type["id"],
                    contract_id,
                    gp.get("created"),
                    gp.get("modified"),
                ),
            )
            new_cur.fetchone()
            stats.inc("person_access_pass", "created")
            stats.add_preview(
                "person_access_pass",
                f"CREATE   | {source_pass_id or old_pass_pk:<18} | {profile_id:<10} | {full_name:<30} | type={pass_type_slug}"
            )
            stats.add_report_row(
                "Access Pass",
                {
                    "Result": "created",
                    "Old Pass PK": old_pass_pk,
                    "Pass ID": source_pass_id,
                    "Profile ID": profile_id,
                    "Full Name": full_name,
                    "Pass Type": pass_type_slug,
                    "Status": map_pass_status(gp.get("status")),
                    "From Date": gp.get("from_date_time"),
                    "To Date": gp.get("to_date_time"),
                    "Contract Name": contract_name,
                    "Contract ID": contract_id,
                    "Person Access Pass ID": new_id,
                    "Message": "created",
                },
            )
        except Exception as e:
            stats.inc("person_access_pass", "failed")
            stats.add_preview(
                "person_access_pass",
                f"FAILED   | {source_pass_id or old_pass_pk:<18} | {profile_id:<10} | {full_name:<30} | type={pass_type_slug}"
            )
            stats.add_report_row(
                "Access Pass",
                {
                    "Result": "failed",
                    "Old Pass PK": old_pass_pk,
                    "Pass ID": source_pass_id,
                    "Profile ID": profile_id,
                    "Full Name": full_name,
                    "Pass Type": pass_type_slug,
                    "Status": map_pass_status(gp.get("status")),
                    "From Date": gp.get("from_date_time"),
                    "To Date": gp.get("to_date_time"),
                    "Contract Name": contract_name,
                    "Contract ID": contract_id,
                    "Person Access Pass ID": None,
                    "Message": str(e),
                },
            )
            raise


# ============================================================
# MAIN
# ============================================================
def main():
    args = parse_args()
    dry_run = args.dry_run
    cleanup_mode = args.rollback
    rollback_mode = cleanup_mode
    batch_size = args.batch_size or DEFAULT_BATCH_SIZE
    data_limit = args.data_limit
    report_file = normalize_report_file_path(args.report_file)
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")

    if cleanup_mode:
        new_conn = None
        try:
            print("\n" + "=" * 90)
            print("ROLLBACK CLEANUP MODE")
            print("Emptying the migration tables in the new DB only.")
            print("=" * 90)

            new_conn = connect_db(NEW_DB)
            new_conn.autocommit = False
            new_cur = new_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            before_counts = cleanup_migration_tables(new_cur)
            new_conn.commit()

            print("\nCleanup completed successfully.")
            print("Rows removed before cleanup:")
            for table, count in before_counts.items():
                print(f"{table:<28} {count}")
            return

        except Exception as exc:
            print(f"\nFATAL ERROR: {exc}")
            if new_conn:
                new_conn.rollback()
            sys.exit(1)

        finally:
            if new_conn:
                new_conn.close()

    old_conn = None
    new_conn = None
    stats = MigrationStats()
    stats.set_run_metadata(
        dry_run=dry_run,
        rollback=rollback_mode,
        batch_size=batch_size,
        data_limit=data_limit,
        report_file=report_file,
        generated_at=generated_at,
    )

    try:
        print("\n" + "=" * 90)
        if dry_run:
            print("DRY RUN MODE")
        elif rollback_mode:
            print("ROLLBACK MODE")
        else:
            print("LIVE MIGRATION MODE")
        print(f"BATCH SIZE: {batch_size}")
        if data_limit is not None:
            print(f"DATA LIMIT: {data_limit}")
        print(f"REPORT FILE: {report_file}")
        if rollback_mode:
            print("FINAL ACTION: all DB changes will be rolled back after processing")
        print("=" * 90)

        old_conn = connect_db(OLD_DB)
        new_conn = connect_db(NEW_DB)

        old_conn.autocommit = False
        new_conn.autocommit = False

        old_cur = old_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        new_cur = new_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        ensure_master_data(new_cur, dry_run)
        print("Seeding companies, contracts, trade groups, and trades from the old DB...")
        seed_reference_data_from_old_db(old_cur, new_cur, stats, dry_run)

        total_count = load_total_applicant_count(old_cur)
        effective_total = min(total_count, data_limit) if data_limit is not None else total_count
        stats.set_run_metadata(source_total=total_count, effective_total=effective_total)
        print(f"\nTotal old applicants found: {total_count}")
        if data_limit is not None:
            print(f"Applicants to process   : {effective_total}")

        offset = 0
        batch_no = 1

        while offset < effective_total:
            current_limit = min(batch_size, effective_total - offset)
            batch_rows = load_applicants_batch(old_cur, current_limit, offset)
            if not batch_rows:
                break

            print(f"\n{'='*90}")
            print(f"PROCESSING BATCH {batch_no} | offset={offset} | size={len(batch_rows)}")
            print(f"{'='*90}")

            batch_failed = False

            for applicant in batch_rows:
                stats.total_processed_profiles += 1
                profile_id = applicant["applicant_id"]
                full_name = applicant["full_name"]

                savepoint_name = f"sp_{uuid.uuid4().hex}"

                try:
                    new_cur.execute(f"SAVEPOINT {savepoint_name}")

                    person = create_or_get_person(old_cur, new_cur, stats, applicant, dry_run)
                    if not person:
                        new_cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                        continue

                    migrate_person_related(old_cur, new_cur, stats, applicant, person, dry_run)
                    migrate_passes(old_cur, new_cur, stats, applicant, person, dry_run)

                    if not dry_run and person.get("_created_now"):
                        try:
                            capture_created_profile_report(
                                new_cur,
                                stats,
                                person["id"],
                                person.get("_old_pass_count", 0),
                            )
                        except Exception as report_exc:
                            print(f"REPORT WARNING | {profile_id} | {full_name} | {report_exc}")

                    new_cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")

                except Exception as exc:
                    print(f"ERROR | {profile_id} | {full_name} | {exc}")
                    new_cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                    new_cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    stats.add_error(profile_id, full_name, str(exc))
                    continue

            # print preview lines first
            stats.print_batch_previews(batch_no)

            if dry_run:
                new_conn.rollback()
                print(f"\nBATCH {batch_no} rolled back because --dry-run is enabled.")
            elif rollback_mode:
                print(f"\nBATCH {batch_no} processed in rollback mode. Changes will be undone at the end.")
            else:
                new_conn.commit()
                print(f"\nBATCH {batch_no} committed successfully.")

            stats.reset_batch()
            offset += len(batch_rows)
            batch_no += 1

        stats.print_final_summary()
        report_path = os.path.abspath(report_file)
        report_writer = HtmlReportWriter(report_path)
        report_writer.write(stats)
        print(f"\nHTML report written to: {report_path}")
        print(f"Report data JSON written to: {report_writer.data_path}")
        print(f"Report fallback JS written to: {report_writer.data_js_path}")

        if rollback_mode:
            new_conn.rollback()
            print("All DB changes rolled back because --roll-back was enabled.")

    except Exception as exc:
        print(f"\nFATAL ERROR: {exc}")
        if new_conn:
            new_conn.rollback()
        sys.exit(1)

    finally:
        if old_conn:
            old_conn.close()
        if new_conn:
            new_conn.close()


if __name__ == "__main__":
    main()
