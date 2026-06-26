import os
import argparse
import json
import sqlite3
from itertools import islice
from datetime import datetime

import pymongo
import psycopg2
from dotenv import load_dotenv

# -----------------------------
# LOAD ENV
# -----------------------------
load_dotenv()
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# -----------------------------
# CONFIG FROM ENV
# -----------------------------
DEFAULT_BATCH_SIZE = 500
DEFAULT_REPORT_FILE = "event_migration_report.json"
DEFAULT_MISSING_PASS_DB_FILE = "event_missing_passes.sqlite3"
MISSING_PASS_REASON_NOT_FOUND = "pass_not_exist_in_db"
MISSING_PASS_REASON_PROFILE_MISMATCH = "pass_exist_but_profile_mismatch"
MISSING_PASS_REASON_NO_NUMBER = "missing_pass_number_in_event"

OLD_MONGO_URI = os.getenv("OLD_MONGO_URI")
OLD_MONGO_DB = os.getenv("OLD_MONGO_DB")
OLD_MONGO_COLLECTION = os.getenv("OLD_MONGO_COLLECTION")

NEW_MONGO_URI = os.getenv("NEW_MONGO_URI")
NEW_MONGO_DB = os.getenv("NEW_MONGO_DB")
NEW_MONGO_COLLECTION = os.getenv("NEW_MONGO_COLLECTION")

PG_CONN = psycopg2.connect(
    host=os.getenv("NEW_DB_HOST"),
    dbname=os.getenv("NEW_DB_NAME"),
    user=os.getenv("NEW_DB_USER"),
    password=os.getenv("NEW_DB_PASSWORD"),
    port=os.getenv("NEW_DB_PORT", 5432),
)

# -----------------------------
# CONNECTIONS
# -----------------------------
old_client = pymongo.MongoClient(OLD_MONGO_URI)
new_client = pymongo.MongoClient(NEW_MONGO_URI)

old_col = old_client[OLD_MONGO_DB][OLD_MONGO_COLLECTION]
new_col = new_client[NEW_MONGO_DB][NEW_MONGO_COLLECTION]

pg_cur = PG_CONN.cursor()

# -----------------------------
# HELPERS
# -----------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Migrate old event data to new event data")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--dry-run", action="store_true", help="Run migration without inserting any documents")
    mode_group.add_argument(
        "--roll-back",
        "--rollback",
        dest="rollback",
        action="store_true",
        help="Empty the new event collection and exit",
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help=f"Batch size (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--data-limit", type=int, default=None, help="Maximum number of old event rows to migrate")
    parser.add_argument(
        "--report-file",
        type=str,
        default=DEFAULT_REPORT_FILE,
        help=f"JSON report file path (default: {DEFAULT_REPORT_FILE})",
    )
    args = parser.parse_args()

    if args.batch_size <= 0:
        parser.error("--batch-size must be greater than 0")
    if args.data_limit is not None and args.data_limit <= 0:
        parser.error("--data-limit must be greater than 0")

    return args


def map_direction(val):
    return "Entry" if val == 1 else "Exit"

def map_event_name(name):
    if name and "successFace" in name:
        return "Authenticated via Face"
    return name


def normalize_report_file(path):
    if not path:
        return None
    root, ext = os.path.splitext(path)
    target = path if ext else f"{path}.json"
    return target if os.path.isabs(target) else os.path.join(SCRIPT_DIR, target)


def normalize_html_report_file(path):
    if not path:
        return None
    root, ext = os.path.splitext(path)
    target = path if ext.lower() in {".html", ".htm"} else f"{root or path}.html"
    return target if os.path.isabs(target) else os.path.join(SCRIPT_DIR, target)


def normalize_sqlite_report_file(path):
    if not path:
        return None
    root, ext = os.path.splitext(path)
    target = path if ext.lower() in {".sqlite", ".sqlite3", ".db"} else f"{root or path}.sqlite3"
    return target if os.path.isabs(target) else os.path.join(SCRIPT_DIR, target)


def json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def text_or_none(value):
    if value is None:
        return None
    return json_default(value)


def load_report_payload(report_file, fallback_payload):
    if not report_file:
        return fallback_payload

    try:
        with open(report_file, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Warning: failed to reload JSON report '{report_file}': {exc}")
        return fallback_payload


def write_report(report_file, payload):
    if not report_file:
        return

    try:
        directory = os.path.dirname(report_file)
        if directory:
            os.makedirs(directory, exist_ok=True)

        with open(report_file, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False, default=json_default)
    except OSError as exc:
        print(f"Warning: failed to write report file '{report_file}': {exc}")

    try:
        html_writer = HtmlReportWriter(report_file)
        html_writer.write(load_report_payload(report_file, payload))
        print(f"HTML report saved to {html_writer.output_path}")
        print(f"Report fallback JS saved to {html_writer.data_js_path}")
    except OSError as exc:
        print(f"Warning: failed to write HTML report for '{report_file}': {exc}")


def parse_event_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, dict) and "$date" in value:
        return parse_event_datetime(value["$date"])
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return value
    return value


def get_person_details(profile_id):
    pg_cur.execute("""
        SELECT 
            p.id,
            p.profile_id,
            p.status,
            p.email,
            cat.name,
            c.id,
            c.name,
            ct.id,
            ct.name
        FROM person_personprofile p
        LEFT JOIN person_personcategory cat ON cat.id = p.category_id
        LEFT JOIN person_personcompany pc ON pc.person_id = p.id AND pc.is_active = TRUE
        LEFT JOIN organisation_company c ON c.id = pc.company_id
        LEFT JOIN person_personcontract pct ON pct.person_id = p.id AND pct.is_active = TRUE
        LEFT JOIN organisation_contract ct ON ct.id = pct.contract_id
        WHERE p.profile_id = %s
        LIMIT 1
    """, (profile_id,))

    row = pg_cur.fetchone()
    if not row:
        return None

    return {
        "person_id": row[0],
        "profile_id": row[1],
        "profile_status": row[2],
        "email": row[3],
        "category": row[4],
        "company_id": row[5],
        "company": row[6],
        "contract_id": row[7],
        "contract": row[8],
    }


def get_person_access_pass(person_id, pass_number):
    if not person_id or not pass_number:
        return None

    normalized_pass_number = str(pass_number).strip()
    pg_cur.execute(
        """
        SELECT id, pass_id
        FROM person_personaccesspass
        WHERE person_id = %s AND pass_id = %s
        ORDER BY created ASC
        LIMIT 1
        """,
        (person_id, normalized_pass_number),
    )
    row = pg_cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "pass_id": row[1],
    }


def get_any_access_pass(pass_number):
    if not pass_number:
        return None

    normalized_pass_number = str(pass_number).strip()
    if not normalized_pass_number:
        return None

    pg_cur.execute(
        """
        SELECT ap.id, ap.pass_id, pp.profile_id, ap.person_id
        FROM person_personaccesspass ap
        JOIN person_personprofile pp ON pp.id = ap.person_id
        WHERE ap.pass_id = %s
        ORDER BY ap.created ASC
        LIMIT 1
        """,
        (normalized_pass_number,),
    )
    row = pg_cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "pass_id": row[1],
        "profile_id": row[2],
        "person_id": row[3],
    }


def normalize_missing_pass_number(value):
    if value is None:
        return "__NO_CARD_NO__"
    text = str(value).strip()
    return text if text else "__NO_CARD_NO__"


def open_missing_pass_store(db_path):
    if not db_path:
        return None

    directory = os.path.dirname(db_path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("DROP TABLE IF EXISTS missing_access_passes")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS missing_access_passes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id TEXT NOT NULL,
            pass_number TEXT NOT NULL,
            reason TEXT NOT NULL,
            event_time TEXT
        )
        """
    )
    conn.commit()
    return conn


def flush_missing_pass_records(conn, records):
    if not conn or not records:
        return 0

    params = [
        (
            record["profile_id"],
            record["pass_number"],
            record["reason"],
            record.get("event_time"),
        )
        for record in records
    ]

    conn.executemany(
        """
        INSERT INTO missing_access_passes (
            profile_id,
            pass_number,
            reason,
            event_time
        )
        VALUES (?, ?, ?, ?)
        """,
        params,
    )
    conn.commit()
    return len(params)


def iter_batches(cursor, batch_size):
    while True:
        batch = list(islice(cursor, batch_size))
        if not batch:
            break
        yield batch


def build_new_doc(doc, person, access_pass):
    event_time = parse_event_datetime(doc.get("eventTime"))
    receive_time = parse_event_datetime(doc.get("receiveTime")) or event_time
    pass_number = access_pass["pass_id"] if access_pass and access_pass.get("pass_id") else doc.get("cardNo")

    return {
        "event_type": 1,
        "event_time": event_time,
        "receive_time": receive_time,

        "person_id": person["person_id"],
        "profile_id": person["profile_id"],
        "full_name": doc.get("personName"),
        "pass_id": access_pass["id"] if access_pass else None,
        "pass_number": pass_number,

        "direction": map_direction(doc.get("inAndOutType")),
        "category": person["category"],
        "company": person["company"],
        "contract": person["contract"],
        "company_id": person["company_id"],
        "contract_id": person["contract_id"],
        "profile_status": person["profile_status"],

        "pic_uri": doc.get("picUri"),
        "email": person["email"],

        "event_id": doc.get("eventId"),
        "dev_name": doc.get("devName"),
        "dev_id": doc.get("devIndexCode"),
        "event_auth_mode": doc.get("eventType"),
        "reader_index_code": doc.get("readerDevIndexCode"),
        "ubio_person_id": doc.get("personId"),
        "dev_index_code": doc.get("devIndexCode"),
        "person_type": str(doc.get("personType")) if doc.get("personType") is not None else "0",
        "reader_name": doc.get("readerDevName"),

        "event_name": map_event_name(doc.get("eventName")),
        "is_images_uploaded": 1 if doc.get("picUri") else 0,
        "is_visit_updated": 0,
    }


def rollback_new_collection():
    result = new_col.delete_many({})
    print(f"Rollback completed ✅ | Removed: {result.deleted_count} docs from the new collection")


def new_counts():
    return {
        "read": 0,
        "ready_to_insert": 0,
        "inserted": 0,
        "skipped": 0,
        "failed": 0,
        "missing_access_pass": 0,
        "skip_missing_job_no": 0,
        "skip_person_not_found": 0,
        "person_lookup_errors": 0,
        "insert_errors": 0,
        "unexpected_errors": 0,
    }


class HtmlReportWriter:
    PAGE_SIZE = 20

    def __init__(self, report_json_path):
        self.report_json_path = report_json_path
        self.output_path = normalize_html_report_file(report_json_path)
        base, _ = os.path.splitext(report_json_path)
        self.data_js_path = f"{base}.data.js"

    def write(self, payload):
        payload_json = json.dumps(payload, default=json_default, ensure_ascii=False, separators=(",", ":"))

        directory = os.path.dirname(self.output_path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        with open(self.data_js_path, "w", encoding="utf-8") as handle:
            handle.write(f"window.__EVENT_REPORT__ = {payload_json};\n")

        with open(self.output_path, "w", encoding="utf-8") as handle:
            handle.write(self._build_html())

    def _build_html(self):
        report_json_name = json.dumps(os.path.basename(self.report_json_path))
        report_html_name = json.dumps(os.path.basename(self.output_path))
        data_js_name = json.dumps(os.path.basename(self.data_js_path))

        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Event Migration Report</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f6f7fb;
      --bg2: #eef3fb;
      --panel: rgba(255, 255, 255, 0.94);
      --panel-border: rgba(15, 23, 42, 0.08);
      --text: #0f172a;
      --muted: #64748b;
      --accent: #ea580c;
      --accent-2: #0ea5e9;
      --good: #16a34a;
      --warn: #d97706;
      --bad: #dc2626;
      --info: #2563eb;
      --shadow: 0 18px 52px rgba(15, 23, 42, 0.08);
      --radius: 22px;
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      min-height: 100vh;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(251, 146, 60, 0.16), transparent 32%),
        radial-gradient(circle at top right, rgba(14, 165, 233, 0.14), transparent 28%),
        linear-gradient(180deg, var(--bg), var(--bg2));
      font-family: "Segoe UI", system-ui, -apple-system, sans-serif;
    }}

    .shell {{
      width: min(1500px, calc(100% - 32px));
      margin: 0 auto;
      padding: 28px 0 40px;
    }}

    .hero {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 20px;
      padding: 26px 28px;
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
      font-weight: 800;
    }}

    h1 {{
      margin: 0;
      font-size: clamp(2rem, 3vw, 3.4rem);
      line-height: 1.02;
      letter-spacing: -0.05em;
    }}

    .lead {{
      margin: 12px 0 0;
      max-width: 76ch;
      color: var(--muted);
      line-height: 1.6;
    }}

    .meta-pills {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 18px;
    }}

    .pill {{
      display: grid;
      gap: 3px;
      min-width: 160px;
      padding: 12px 14px;
      border-radius: 16px;
      background: rgba(248, 250, 252, 0.98);
      border: 1px solid rgba(148, 163, 184, 0.18);
    }}

    .pill-label {{
      color: var(--muted);
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}

    .pill-value {{
      font-size: 0.98rem;
      font-weight: 700;
      color: var(--text);
    }}

    .hero-aside {{
      display: grid;
      gap: 10px;
      align-content: start;
      justify-items: end;
      text-align: right;
    }}

    .report-path {{
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.45;
    }}

    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 14px;
      margin-top: 18px;
    }}

    .metric-card {{
      padding: 18px;
      border-radius: 18px;
      border: 1px solid var(--panel-border);
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(248, 250, 252, 0.92));
      box-shadow: var(--shadow);
      position: relative;
      overflow: hidden;
    }}

    .metric-card::before {{
      content: "";
      position: absolute;
      inset: 0 auto 0 0;
      width: 4px;
      background: var(--tone, var(--accent));
    }}

    .metric-label {{
      color: var(--muted);
      font-size: 0.76rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}

    .metric-value {{
      margin-top: 8px;
      font-size: 2.1rem;
      font-weight: 800;
      letter-spacing: -0.05em;
    }}

    .metric-detail {{
      margin-top: 6px;
      color: var(--muted);
      font-size: 0.9rem;
      line-height: 1.45;
    }}

    .tone-read {{ --tone: var(--accent-2); }}
    .tone-ready {{ --tone: #7c3aed; }}
    .tone-inserted {{ --tone: var(--good); }}
    .tone-skipped {{ --tone: var(--warn); }}
    .tone-failed {{ --tone: var(--bad); }}
    .tone-missing {{ --tone: #0284c7; }}

    .progress-card,
    .reason-card,
    .table-card {{
      margin-top: 18px;
      border: 1px solid var(--panel-border);
      border-radius: var(--radius);
      background: var(--panel);
      box-shadow: var(--shadow);
      overflow: hidden;
    }}

    .section-head {{
      display: flex;
      align-items: end;
      justify-content: space-between;
      gap: 16px;
      padding: 18px 24px 14px;
      border-bottom: 1px solid rgba(148, 163, 184, 0.16);
    }}

    .section-head h2 {{
      margin: 0;
      font-size: 1.15rem;
      letter-spacing: -0.02em;
    }}

    .section-head p {{
      margin: 8px 0 0;
      color: var(--muted);
    }}

    .progress-body {{
      padding: 18px 24px 22px;
    }}

    .progress-meta {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      color: var(--muted);
      font-size: 0.93rem;
      margin-bottom: 10px;
      flex-wrap: wrap;
    }}

    .progress-track {{
      height: 14px;
      border-radius: 999px;
      background: rgba(148, 163, 184, 0.16);
      overflow: hidden;
    }}

    .progress-fill {{
      height: 100%;
      width: 0%;
      border-radius: inherit;
      background: linear-gradient(90deg, var(--accent), var(--accent-2));
      transition: width 220ms ease;
    }}

    .progress-note {{
      margin-top: 10px;
      color: var(--muted);
      font-size: 0.9rem;
    }}

    .reason-grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 14px;
      padding: 18px 24px 22px;
    }}

    .reason-item {{
      padding: 16px;
      border-radius: 18px;
      background: rgba(248, 250, 252, 0.94);
      border: 1px solid rgba(148, 163, 184, 0.16);
    }}

    .reason-item .reason-label {{
      color: var(--muted);
      font-size: 0.76rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}

    .reason-item .reason-value {{
      margin-top: 6px;
      font-size: 1.8rem;
      font-weight: 800;
      letter-spacing: -0.05em;
    }}

    .reason-item .reason-detail {{
      margin-top: 6px;
      color: var(--muted);
      font-size: 0.9rem;
      line-height: 1.45;
    }}

    .reason-item.tone-warn {{ border-color: rgba(217, 119, 6, 0.2); }}
    .reason-item.tone-info {{ border-color: rgba(37, 99, 235, 0.2); }}
    .reason-item.tone-bad {{ border-color: rgba(220, 38, 38, 0.2); }}
    .reason-item.tone-neutral {{ border-color: rgba(100, 116, 139, 0.2); }}

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
      font-size: 1.15rem;
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
      background: rgba(255, 255, 255, 0.96);
      color: var(--text);
      border-radius: 12px;
      padding: 10px 14px;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
      transition: transform 120ms ease, border-color 120ms ease, background 120ms ease, opacity 120ms ease;
    }}

    .btn:hover:not(:disabled) {{
      transform: translateY(-1px);
      border-color: rgba(234, 88, 12, 0.35);
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
      font-size: 0.74rem;
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
      background: rgba(14, 165, 233, 0.07);
    }}

    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 7px 11px;
      border-radius: 999px;
      font-size: 0.84rem;
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

    .badge.good {{
      color: var(--good);
      background: rgba(22, 163, 74, 0.1);
      border-color: rgba(22, 163, 74, 0.22);
    }}

    .badge.warn {{
      color: var(--warn);
      background: rgba(217, 119, 6, 0.1);
      border-color: rgba(217, 119, 6, 0.22);
    }}

    .badge.bad {{
      color: var(--bad);
      background: rgba(220, 38, 38, 0.1);
      border-color: rgba(220, 38, 38, 0.22);
    }}

    .badge.info {{
      color: var(--info);
      background: rgba(37, 99, 235, 0.1);
      border-color: rgba(37, 99, 235, 0.22);
    }}

    .num.good {{ color: var(--good); font-weight: 800; }}
    .num.warn {{ color: var(--warn); font-weight: 800; }}
    .num.bad {{ color: var(--bad); font-weight: 800; }}
    .num.info {{ color: var(--info); font-weight: 800; }}
    .num.neutral {{ color: var(--text); font-weight: 700; }}

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

    @media (max-width: 1280px) {{
      .summary-grid {{
        grid-template-columns: repeat(3, minmax(0, 1fr));
      }}

      .reason-grid {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}
    }}

    @media (max-width: 860px) {{
      .hero {{
        grid-template-columns: 1fr;
      }}

      .hero-aside {{
        justify-items: start;
        text-align: left;
      }}

      .summary-grid,
      .reason-grid {{
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
        <h1>Event migration dashboard</h1>
        <p class="lead">
          A visual summary of the event migration. The dashboard reads its values from the JSON report so the page stays lightweight even when the migration has millions of source rows.
        </p>
        <div class="meta-pills" id="meta-pills"></div>
      </div>
      <div class="hero-aside">
        <div class="report-path" id="report-json-path"></div>
        <div class="report-path" id="report-html-path"></div>
        <div class="report-path" id="report-data-js-path"></div>
        <div class="report-path" id="generated-at"></div>
      </div>
    </section>

    <section class="summary-grid" id="summary-grid"></section>

    <section class="progress-card">
      <div class="section-head">
        <div>
          <h2>Migration progress</h2>
          <p>Inserted rows compared with the effective input size.</p>
        </div>
        <div class="pagination-info" id="progress-text"></div>
      </div>
      <div class="progress-body">
        <div class="progress-meta">
          <span id="progress-meta-left"></span>
          <span id="progress-meta-right"></span>
        </div>
        <div class="progress-track"><div class="progress-fill" id="progress-fill"></div></div>
        <div class="progress-note" id="progress-note"></div>
      </div>
    </section>

    <section class="reason-card">
      <div class="section-head">
        <div>
          <h2>Skip reasons</h2>
          <p>These counts show where rows were dropped before insertion.</p>
        </div>
      </div>
      <div class="reason-grid" id="reason-grid"></div>
    </section>

    <section class="table-card">
      <div class="table-head">
        <div>
          <h2>Batch breakdown</h2>
          <p id="batch-caption"></p>
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
              <th>Batch</th>
              <th>Read</th>
              <th>Ready</th>
              <th>Inserted</th>
              <th>Skipped</th>
              <th>Failed</th>
              <th>Missing Pass</th>
              <th>Missing JobNo</th>
              <th>Person Not Found</th>
              <th>Insert Errors</th>
              <th>Unexpected Errors</th>
            </tr>
          </thead>
          <tbody id="batch-table-body"></tbody>
        </table>
        <div class="empty-state" id="empty-state">Loading report data...</div>
      </div>

      <div class="table-footer">
        <div class="hint">Pagination is client-side. Each row represents one Mongo batch from the migration run.</div>
        <div class="hint" id="range-info"></div>
      </div>
    </section>
  </div>

  <script>
    const REPORT_DATA_URL = {report_json_name};
    const REPORT_HTML_NAME = {report_html_name};
    const REPORT_FALLBACK_JS_URL = {data_js_name};
    const PAGE_SIZE = {self.PAGE_SIZE};

    const metaPills = document.getElementById('meta-pills');
    const reportJsonPath = document.getElementById('report-json-path');
    const reportHtmlPath = document.getElementById('report-html-path');
    const reportDataJsPath = document.getElementById('report-data-js-path');
    const generatedAt = document.getElementById('generated-at');
    const summaryGrid = document.getElementById('summary-grid');
    const reasonGrid = document.getElementById('reason-grid');
    const progressFill = document.getElementById('progress-fill');
    const progressText = document.getElementById('progress-text');
    const progressMetaLeft = document.getElementById('progress-meta-left');
    const progressMetaRight = document.getElementById('progress-meta-right');
    const progressNote = document.getElementById('progress-note');
    const batchCaption = document.getElementById('batch-caption');
    const tbody = document.getElementById('batch-table-body');
    const emptyState = document.getElementById('empty-state');
    const prevButton = document.getElementById('prev-page');
    const nextButton = document.getElementById('next-page');
    const pageInfo = document.getElementById('page-info');
    const rangeInfo = document.getElementById('range-info');

    let batches = [];
    let currentPage = 1;

    function escapeHtml(value) {{
      return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }}

    function formatNumber(value) {{
      const num = Number(value ?? 0);
      return new Intl.NumberFormat().format(Number.isFinite(num) ? num : 0);
    }}

    function formatPercent(value) {{
      const num = Number(value ?? 0);
      return `${{Number.isFinite(num) ? num : 0}}%`;
    }}

    function textOrDash(value) {{
      return value === null || value === undefined || value === '' ? '—' : String(value);
    }}

    function cardToneForMetric(key) {{
      const map = {{
        read: 'tone-read',
        ready_to_insert: 'tone-ready',
        inserted: 'tone-inserted',
        skipped: 'tone-skipped',
        failed: 'tone-failed',
        missing_access_pass: 'tone-missing',
      }};
      return map[key] || 'tone-read';
    }}

    function metricCard(label, value, detail, toneKey) {{
      return `
        <article class="metric-card ${{cardToneForMetric(toneKey)}}">
          <div class="metric-label">${{escapeHtml(label)}}</div>
          <div class="metric-value">${{formatNumber(value)}}</div>
          <div class="metric-detail">${{escapeHtml(detail)}}</div>
        </article>
      `;
    }}

    function reasonCard(label, value, detail, tone) {{
      return `
        <article class="reason-item tone-${{tone}}">
          <div class="reason-label">${{escapeHtml(label)}}</div>
          <div class="reason-value">${{formatNumber(value)}}</div>
          <div class="reason-detail">${{escapeHtml(detail)}}</div>
        </article>
      `;
    }}

    function renderSummary(summary, meta) {{
      summaryGrid.innerHTML = [
        metricCard('Read', summary.read, 'Rows scanned from the old collection', 'read'),
        metricCard('Ready To Insert', summary.ready_to_insert, 'Rows that passed person and pass resolution', 'ready_to_insert'),
        metricCard('Inserted', summary.inserted, 'Rows written to the new collection', 'inserted'),
        metricCard('Skipped', summary.skipped, 'Rows skipped before write', 'skipped'),
        metricCard('Failed', summary.failed, 'Rows that hit an error', 'failed'),
        metricCard('Missing Pass', summary.missing_access_pass, 'Person matched, but no pass was found', 'missing_access_pass'),
      ].join('');

      const effectiveTotal = Number(meta.effective_total ?? summary.effective_total ?? 0);
      const inserted = Number(summary.inserted ?? 0);
      const percent = effectiveTotal > 0 ? Math.min(100, Math.round((inserted / effectiveTotal) * 100)) : 0;
      progressFill.style.width = `${{percent}}%`;
      progressText.textContent = `${{formatNumber(inserted)}} / ${{formatNumber(effectiveTotal)}} inserted`;
      progressMetaLeft.textContent = `Completion: ${{formatPercent(percent)}}`;
      progressMetaRight.textContent = `Ready rows: ${{formatNumber(summary.ready_to_insert)}}`;
      progressNote.textContent = meta.dry_run ? 'Dry-run mode did not write to Mongo.' : 'Live migration data loaded from the JSON report.';
    }}

    function renderReasons(summary) {{
      reasonGrid.innerHTML = [
        reasonCard('Missing jobNo', summary.skip_missing_job_no, 'Old event row did not include a job number.', 'warn'),
        reasonCard('Person not found', summary.skip_person_not_found, 'No matching profile was found in the new DB.', 'info'),
        reasonCard('Missing access pass', summary.missing_access_pass, 'The person existed, but no matching pass was found.', 'warn'),
        reasonCard('Person lookup errors', summary.person_lookup_errors, 'PostgreSQL profile lookup failed.', 'bad'),
        reasonCard('Insert errors', summary.insert_errors, 'Mongo bulk insert returned write errors.', 'bad'),
        reasonCard('Unexpected errors', summary.unexpected_errors, 'Unhandled processing errors.', 'bad'),
      ].join('');
    }}

    function renderMeta(meta, batches) {{
      const mode = meta.dry_run ? 'Dry Run' : (meta.rollback ? 'Rollback' : 'Live');
      const pills = [
        ['Mode', mode],
        ['Batch Size', meta.batch_size],
        ['Data Limit', meta.data_limit === null || meta.data_limit === undefined ? 'All' : meta.data_limit],
        ['Source Total', meta.source_total],
        ['Effective Total', meta.effective_total],
        ['Batches', batches.length],
      ];

      metaPills.innerHTML = pills.map(([label, value]) => `
        <div class="pill">
          <span class="pill-label">${{escapeHtml(label)}}</span>
          <span class="pill-value">${{escapeHtml(textOrDash(value))}}</span>
        </div>
      `).join('');

      reportJsonPath.textContent = `JSON source: ${{REPORT_DATA_URL}}`;
      reportHtmlPath.textContent = `HTML dashboard: ${{REPORT_HTML_NAME}}`;
      reportDataJsPath.textContent = `Fallback JS: ${{REPORT_FALLBACK_JS_URL}}`;
      generatedAt.textContent = `Generated at: ${{textOrDash(meta.generated_at)}}`;

      batchCaption.textContent = `${{formatNumber(batches.length)}} batch row(s) loaded from the report JSON`;
    }}

    function renderBatchRow(batch) {{
      return `
        <tr>
          <td><span class="badge info">Batch ${{formatNumber(batch.batch_no)}}</span></td>
          <td class="num neutral">${{formatNumber(batch.read)}}</td>
          <td class="num info">${{formatNumber(batch.ready_to_insert)}}</td>
          <td class="num good">${{formatNumber(batch.inserted)}}</td>
          <td class="num warn">${{formatNumber(batch.skipped)}}</td>
          <td class="num bad">${{formatNumber(batch.failed)}}</td>
          <td class="num warn">${{formatNumber(batch.missing_access_pass)}}</td>
          <td class="num warn">${{formatNumber(batch.skip_missing_job_no)}}</td>
          <td class="num info">${{formatNumber(batch.skip_person_not_found)}}</td>
          <td class="num bad">${{formatNumber(batch.insert_errors)}}</td>
          <td class="num bad">${{formatNumber(batch.unexpected_errors)}}</td>
        </tr>
      `;
    }}

    function renderPage(page) {{
      const totalRows = batches.length;
      const totalPages = totalRows === 0 ? 1 : Math.ceil(totalRows / PAGE_SIZE);
      currentPage = Math.min(Math.max(page, 1), totalPages);
      const start = (currentPage - 1) * PAGE_SIZE;
      const pageRows = batches.slice(start, start + PAGE_SIZE);

      const hasRows = totalRows > 0;
      emptyState.hidden = hasRows;
      prevButton.disabled = !hasRows || currentPage === 1;
      nextButton.disabled = !hasRows || currentPage === totalPages;

      if (hasRows) {{
        const end = Math.min(start + PAGE_SIZE, totalRows);
        pageInfo.textContent = `Page ${{currentPage}} of ${{totalPages}}`;
        rangeInfo.textContent = `Showing ${{start + 1}}-${{end}} of ${{totalRows}} batch rows`;
        tbody.innerHTML = pageRows.map(renderBatchRow).join('');
      }} else {{
        pageInfo.textContent = 'No rows';
        rangeInfo.textContent = '';
        tbody.innerHTML = '';
      }}
    }}

    function loadFallbackScript() {{
      return new Promise((resolve, reject) => {{
        if (window.__EVENT_REPORT__) {{
          resolve(window.__EVENT_REPORT__);
          return;
        }}

        const script = document.createElement('script');
        script.src = REPORT_FALLBACK_JS_URL;
        script.async = true;
        script.onload = () => {{
          if (window.__EVENT_REPORT__) {{
            resolve(window.__EVENT_REPORT__);
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
      const meta = data && data.meta ? data.meta : {{}};
      const summary = data && data.summary ? data.summary : {{}};
      batches = Array.isArray(data && data.batches) ? data.batches : [];
      renderMeta(meta, batches);
      renderSummary(summary, meta);
      renderReasons(summary);
      renderPage(1);
    }}

    function handleLoadError(error) {{
      console.error(error);
      emptyState.textContent = 'Unable to load report data. ' + (error && error.message ? error.message : '');
      emptyState.hidden = false;
      pageInfo.textContent = 'No rows';
      rangeInfo.textContent = '';
      tbody.innerHTML = '';
      prevButton.disabled = true;
      nextButton.disabled = true;
      progressText.textContent = '0 / 0 inserted';
      progressMetaLeft.textContent = '';
      progressMetaRight.textContent = '';
      progressNote.textContent = '';
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


def migrate_events(batch_size, data_limit, dry_run, report_file):
    source_total = old_col.count_documents({})
    report_html_file = normalize_html_report_file(report_file) if report_file else None
    report_js_file = f"{os.path.splitext(report_file)[0]}.data.js" if report_file else None
    missing_pass_db_file = normalize_sqlite_report_file(report_file) if report_file else None
    cursor = old_col.find({}).sort("_id", pymongo.ASCENDING)
    if data_limit is not None:
        cursor = cursor.limit(data_limit)
    cursor = cursor.batch_size(batch_size)

    total = new_counts()
    report = {
        "meta": {
            "dry_run": dry_run,
            "rollback": False,
            "batch_size": batch_size,
            "data_limit": data_limit,
            "report_file": report_file,
            "html_report_file": report_html_file,
            "report_data_file": report_js_file,
            "missing_pass_db_file": missing_pass_db_file,
            "generated_at": datetime.now().astimezone().isoformat(),
            "source_total": source_total,
            "effective_total": min(source_total, data_limit) if data_limit is not None else source_total,
        },
        "summary": new_counts(),
        "batches": [],
    }
    report["summary"]["source_total"] = source_total
    report["summary"]["effective_total"] = min(source_total, data_limit) if data_limit is not None else source_total

    person_cache = {}
    pass_cache = {}
    ownership_cache = {}
    missing_pass_conn = open_missing_pass_store(missing_pass_db_file)

    try:
        for batch_no, batch in enumerate(iter_batches(cursor, batch_size), start=1):
            batch_stats = new_counts()
            batch_docs = []
            batch_missing_pass_records = []

            for doc in batch:
                batch_stats["read"] += 1
                total["read"] += 1

                profile_id = doc.get("jobNo")
                if not profile_id:
                    batch_stats["skipped"] += 1
                    batch_stats["skip_missing_job_no"] += 1
                    total["skipped"] += 1
                    total["skip_missing_job_no"] += 1
                    continue

                profile_key = str(profile_id)
                try:
                    if profile_key in person_cache:
                        person = person_cache[profile_key]
                    else:
                        person = get_person_details(profile_key)
                        person_cache[profile_key] = person
                except Exception as exc:
                    batch_stats["failed"] += 1
                    batch_stats["person_lookup_errors"] += 1
                    total["failed"] += 1
                    total["person_lookup_errors"] += 1
                    print(f"Failed to load person details for jobNo={profile_id}: {exc}")
                    continue

                if not person:
                    batch_stats["skipped"] += 1
                    batch_stats["skip_person_not_found"] += 1
                    total["skipped"] += 1
                    total["skip_person_not_found"] += 1
                    continue

                card_no = doc.get("cardNo")
                pass_cache_key = (person["person_id"], str(card_no).strip() if card_no is not None else None)
                try:
                    if pass_cache_key in pass_cache:
                        access_pass = pass_cache[pass_cache_key]
                    else:
                        access_pass = get_person_access_pass(person["person_id"], card_no)
                        pass_cache[pass_cache_key] = access_pass
                except Exception as exc:
                    batch_stats["failed"] += 1
                    batch_stats["unexpected_errors"] += 1
                    total["failed"] += 1
                    total["unexpected_errors"] += 1
                    print(f"Failed to load access pass for jobNo={profile_id} cardNo={card_no}: {exc}")
                    continue

                if access_pass is None:
                    batch_stats["missing_access_pass"] += 1
                    batch_stats["skipped"] += 1
                    total["missing_access_pass"] += 1
                    total["skipped"] += 1
                    event_time_text = text_or_none(parse_event_datetime(doc.get("eventTime")))
                    missing_pass_number = normalize_missing_pass_number(card_no)
                    matched_profile_id = None
                    if card_no is None or not str(card_no).strip():
                        reason = MISSING_PASS_REASON_NO_NUMBER
                    else:
                        ownership_key = str(card_no).strip()
                        if ownership_key in ownership_cache:
                            owned_pass = ownership_cache[ownership_key]
                        else:
                            owned_pass = get_any_access_pass(card_no)
                            ownership_cache[ownership_key] = owned_pass

                        if owned_pass is None:
                            reason = MISSING_PASS_REASON_NOT_FOUND
                            matched_profile_id = None
                        else:
                            matched_profile_id = owned_pass.get("profile_id")
                            if matched_profile_id and str(matched_profile_id) != str(person["profile_id"]):
                                reason = MISSING_PASS_REASON_PROFILE_MISMATCH
                            else:
                                reason = MISSING_PASS_REASON_NOT_FOUND

                    print(
                        f"MISSING PASS | jobNo={person['profile_id']} | "
                        f"pass_number={missing_pass_number} | event_time={event_time_text} | reason={reason}"
                        f"{'' if not matched_profile_id else f' | matched_jobNo={matched_profile_id}'}"
                    )
                    batch_missing_pass_records.append(
                        {
                            "profile_id": person["profile_id"],
                            "pass_number": missing_pass_number,
                            "reason": reason,
                            "event_time": event_time_text,
                        }
                    )
                    continue

                batch_docs.append(build_new_doc(doc, person, access_pass))
                batch_stats["ready_to_insert"] += 1
                total["ready_to_insert"] += 1

            if batch_missing_pass_records:
                try:
                    flush_missing_pass_records(missing_pass_conn, batch_missing_pass_records)
                except Exception as exc:
                    print(f"Failed to store missing pass debug records for batch {batch_no}: {exc}")

            if not dry_run and batch_docs:
                try:
                    result = new_col.insert_many(batch_docs, ordered=False)
                    batch_stats["inserted"] = len(result.inserted_ids)
                    total["inserted"] += batch_stats["inserted"]
                except pymongo.errors.BulkWriteError as exc:
                    details = exc.details or {}
                    inserted_count = details.get("nInserted", 0)
                    write_errors = details.get("writeErrors", [])
                    error_count = len(write_errors)
                    batch_stats["inserted"] = inserted_count
                    batch_stats["failed"] += error_count
                    batch_stats["insert_errors"] += error_count
                    total["inserted"] += inserted_count
                    total["failed"] += error_count
                    total["insert_errors"] += error_count
                    print(
                        f"Batch {batch_no} bulk insert had {error_count} errors; "
                        f"inserted={inserted_count}"
                    )
                except Exception as exc:
                    batch_stats["failed"] += len(batch_docs)
                    batch_stats["unexpected_errors"] += len(batch_docs)
                    total["failed"] += len(batch_docs)
                    total["unexpected_errors"] += len(batch_docs)
                    print(f"Batch {batch_no} bulk insert failed: {exc}")

            mode_label = "DRY-RUN" if dry_run else "LIVE"
            print(
                f"BATCH {batch_no} [{mode_label}] | "
                f"read={batch_stats['read']} ready={batch_stats['ready_to_insert']} "
                f"inserted={batch_stats['inserted']} skipped={batch_stats['skipped']} "
                f"failed={batch_stats['failed']} missing_pass={batch_stats['missing_access_pass']}"
            )

            report["batches"].append(
                {
                    "batch_no": batch_no,
                    **batch_stats,
                }
            )
    finally:
        if missing_pass_conn is not None:
            missing_pass_conn.close()

    report["summary"].update(total)

    print(
        f"Migration completed ✅ | "
        f"read={total['read']} ready={total['ready_to_insert']} inserted={total['inserted']} "
        f"skipped={total['skipped']} failed={total['failed']} missing_pass={total['missing_access_pass']}"
    )
    write_report(report_file, report)
    print(f"Report saved to {report_file}")
    return report


def main():
    args = parse_args()
    report_file = normalize_report_file(args.report_file)

    print("=" * 80)
    print("EVENT MIGRATION")
    print("=" * 80)
    if args.rollback:
        print("Mode: ROLLBACK")
        rollback_new_collection()
        return

    print(f"Mode: {'DRY-RUN' if args.dry_run else 'LIVE'}")
    print(f"Batch size: {args.batch_size}")
    if args.data_limit is not None:
        print(f"Data limit: {args.data_limit}")
    if report_file:
        print(f"Report file: {report_file}")
        print(f"Missing pass DB: {normalize_sqlite_report_file(report_file)}")

    migrate_events(
        batch_size=args.batch_size,
        data_limit=args.data_limit,
        dry_run=args.dry_run,
        report_file=report_file,
    )


if __name__ == "__main__":
    main()
