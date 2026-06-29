#!/usr/bin/env python3
import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from html import escape as html_escape
from pathlib import Path
from string import Template
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv


load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_BATCH_SIZE = 500
DEFAULT_REPORT_FILE = "admin_log_migration_report.json"

OLD_TABLES = {
    "admin_log": "django_admin_log",
    "content_type": "django_content_type",
    "user": "accounts_user",
}

NEW_TABLES = {
    "admin_log": "django_admin_log",
    "content_type": "django_content_type",
    "user": "accounts_user",
}

ACTION_LABELS = {
    1: "ADDITION",
    2: "CHANGE",
    3: "DELETION",
}


@dataclass
class DBConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


@dataclass
class MigrationStats:
    total: Dict[str, Counter] = field(default_factory=lambda: defaultdict(Counter))
    batch: Dict[str, Counter] = field(default_factory=lambda: defaultdict(Counter))
    report_rows: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    run_metadata: Dict[str, Any] = field(default_factory=dict)

    def inc(self, section: str, key: str, amount: int = 1):
        self.total[section][key] += amount
        self.batch[section][key] += amount

    def add_error(self, message: str):
        self.errors.append(message)

    def add_report_row(self, row: Dict[str, Any]):
        self.report_rows.append(row)

    def reset_batch(self):
        self.batch = defaultdict(Counter)

    def set_run_metadata(self, **kwargs):
        self.run_metadata.update(kwargs)


def env_bool(name: str, default: bool = True) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def env_first(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value
    return default


def normalize_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return str(value)


def connect_db(cfg: DBConfig):
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )


def fetchone(conn, sql: str, params: Optional[Tuple[Any, ...]] = None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params or ())
        return cur.fetchone()


def fetchall(conn, sql: str, params: Optional[Tuple[Any, ...]] = None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def execute(conn, sql: str, params: Optional[Tuple[Any, ...]] = None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())


def normalize_report_file_path(report_file: str) -> str:
    target = Path(report_file).expanduser()
    if target.suffix.lower() != ".json":
        target = target.with_suffix(".json")
    if not target.is_absolute():
        target = SCRIPT_DIR / target
    return str(target)


def build_sidecar_paths(report_file: str) -> Tuple[Path, Path, Path]:
    json_path = Path(normalize_report_file_path(report_file))
    base = json_path.with_suffix("")
    html_path = Path(f"{base}.html")
    data_json_path = Path(f"{base}.data.json")
    data_js_path = Path(f"{base}.data.js")
    return html_path, data_json_path, data_js_path


def action_label(action_flag: Any) -> str:
    try:
        return ACTION_LABELS.get(int(action_flag), f"UNKNOWN({action_flag})")
    except Exception:
        return f"UNKNOWN({action_flag})"


def parse_args():
    parser = argparse.ArgumentParser(description="Migrate old admin logs into the new DB")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--dry-run", action="store_true", help="Run without inserting any rows")
    mode_group.add_argument(
        "--roll-back",
        "--rollback",
        dest="rollback",
        action="store_true",
        help="Run the migration in a transaction and roll it back at the end",
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help=f"Batch size (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--data-limit", type=int, default=None, help="Maximum number of old log rows to migrate")
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


def load_total_old_logs(old_conn) -> int:
    row = fetchone(old_conn, f"SELECT COUNT(*) AS cnt FROM {OLD_TABLES['admin_log']}")
    return int(row["cnt"])


def load_old_logs_batch(old_conn, limit: int, offset: int):
    return fetchall(
        old_conn,
        f"""
        SELECT
            al.id AS old_log_id,
            al.action_time,
            al.object_id,
            al.object_repr,
            al.action_flag,
            al.change_message,
            al.content_type_id AS old_content_type_id,
            ct.app_label,
            ct.model,
            al.user_id AS old_user_id,
            u.username AS old_username
        FROM {OLD_TABLES['admin_log']} al
        LEFT JOIN {OLD_TABLES['content_type']} ct ON ct.id = al.content_type_id
        LEFT JOIN {OLD_TABLES['user']} u ON u.id = al.user_id
        ORDER BY al.action_time ASC, al.id ASC
        LIMIT %s OFFSET %s
        """,
        (limit, offset),
    )


def load_new_content_types(new_conn) -> Dict[Tuple[str, str], int]:
    rows = fetchall(
        new_conn,
        f"SELECT id, app_label, model FROM {NEW_TABLES['content_type']}",
    )
    mapping: Dict[Tuple[str, str], int] = {}
    for row in rows:
        key = (normalize_text(row["app_label"]) or "", normalize_text(row["model"]) or "")
        mapping[key] = row["id"]
    return mapping


def load_new_user_maps(new_conn) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    rows = fetchall(
        new_conn,
        f"SELECT id, username FROM {NEW_TABLES['user']}",
    )
    by_id: Dict[str, Any] = {}
    by_username: Dict[str, Any] = {}
    for row in rows:
        user_id = row["id"]
        user_key = str(user_id)
        by_id[user_key] = user_id
        username = normalize_text(row["username"])
        if username:
            by_username[username] = user_id
    return by_id, by_username


def load_existing_signatures(new_conn) -> set:
    rows = fetchall(
        new_conn,
        f"""
        SELECT action_time, object_id, object_repr, action_flag, change_message, content_type_id, user_id
        FROM {NEW_TABLES['admin_log']}
        """,
    )
    signatures = set()
    for row in rows:
        signatures.add(
            (
                row["action_time"],
                normalize_text(row["object_id"]),
                normalize_text(row["object_repr"]) or "",
                int(row["action_flag"]),
                normalize_text(row["change_message"]) or "",
                row["content_type_id"],
                str(row["user_id"]),
            )
        )
    return signatures


def resolve_new_user_id(
    old_user_id: Any,
    old_username: Optional[str],
    user_by_id: Dict[str, Any],
    user_by_username: Dict[str, Any],
):
    if old_user_id is not None:
        existing = user_by_id.get(str(old_user_id))
        if existing is not None:
            return existing
    username = normalize_text(old_username)
    if username:
        return user_by_username.get(username)
    return None


def resolve_new_content_type_id(
    app_label: Optional[str],
    model: Optional[str],
    content_type_map: Dict[Tuple[str, str], int],
):
    label = normalize_text(app_label) or ""
    model_name = normalize_text(model) or ""
    if not label or not model_name:
        return None
    return content_type_map.get((label, model_name))


def build_signature(
    action_time: Any,
    object_id: Any,
    object_repr: Any,
    action_flag: Any,
    change_message: Any,
    content_type_id: Any,
    user_id: Any,
):
    return (
        action_time,
        normalize_text(object_id),
        normalize_text(object_repr) or "",
        int(action_flag),
        normalize_text(change_message) or "",
        content_type_id,
        str(user_id),
    )


def add_report_row(
    stats: MigrationStats,
    *,
    result: str,
    reason: Optional[str],
    message: str,
    old_log_id: Any,
    new_log_id: Any,
    action_time: Any,
    username: Optional[str],
    user_id: Optional[Any],
    content_type_label: Optional[str],
    object_id: Optional[str],
    object_repr: Optional[str],
    action_flag: Any,
    change_message: Optional[str],
):
    stats.add_report_row(
        {
            "result": result,
            "reason": reason,
            "message": message,
            "old_log_id": str(old_log_id) if old_log_id is not None else None,
            "new_log_id": str(new_log_id) if new_log_id is not None else None,
            "action_time": action_time,
            "username": normalize_text(username),
            "user_id": str(user_id) if user_id is not None else None,
            "content_type": content_type_label,
            "object_id": object_id,
            "object_repr": object_repr,
            "action_flag": int(action_flag) if action_flag is not None else None,
            "action_label": action_label(action_flag),
            "change_message": change_message,
        }
    )


def process_admin_log_row(
    new_conn,
    stats: MigrationStats,
    row: Dict[str, Any],
    *,
    dry_run: bool,
    content_type_map: Dict[Tuple[str, str], int],
    user_by_id: Dict[str, Any],
    user_by_username: Dict[str, Any],
    existing_signatures: set,
):
    old_log_id = row["old_log_id"]
    action_time = row["action_time"]
    object_id = normalize_text(row.get("object_id"))
    object_repr = normalize_text(row.get("object_repr")) or ""
    action_flag = row.get("action_flag")
    change_message = normalize_text(row.get("change_message")) or ""
    old_content_type_id = row.get("old_content_type_id")
    app_label = normalize_text(row.get("app_label"))
    model = normalize_text(row.get("model"))
    old_user_id = row.get("old_user_id")
    old_username = row.get("old_username")
    content_type_label = f"{app_label}.{model}" if app_label and model else None

    new_content_type_id = resolve_new_content_type_id(app_label, model, content_type_map)
    if old_content_type_id is not None and new_content_type_id is None:
        stats.inc("admin_log", "missing_content_type")

    new_user_id = resolve_new_user_id(old_user_id, old_username, user_by_id, user_by_username)
    if new_user_id is None:
        stats.inc("admin_log", "missing_user")
        stats.inc("admin_log", "skipped")
        add_report_row(
            stats,
            result="skipped",
            reason="missing_user",
            message="user not found in new DB",
            old_log_id=old_log_id,
            new_log_id=None,
            action_time=action_time,
            username=old_username,
            user_id=old_user_id,
            content_type_label=content_type_label,
            object_id=object_id,
            object_repr=object_repr,
            action_flag=action_flag,
            change_message=change_message,
        )
        return

    signature = build_signature(
        action_time,
        object_id,
        object_repr,
        action_flag,
        change_message,
        new_content_type_id,
        new_user_id,
    )
    if signature in existing_signatures:
        stats.inc("admin_log", "existing")
        add_report_row(
            stats,
            result="existing",
            reason="already_exists",
            message="identical admin log already exists in new DB",
            old_log_id=old_log_id,
            new_log_id=None,
            action_time=action_time,
            username=old_username,
            user_id=new_user_id,
            content_type_label=content_type_label,
            object_id=object_id,
            object_repr=object_repr,
            action_flag=action_flag,
            change_message=change_message,
        )
        return

    if dry_run:
        stats.inc("admin_log", "created")
        existing_signatures.add(signature)
        add_report_row(
            stats,
            result="created",
            reason="dry_run",
            message="dry-run",
            old_log_id=old_log_id,
            new_log_id=None,
            action_time=action_time,
            username=old_username,
            user_id=new_user_id,
            content_type_label=content_type_label,
            object_id=object_id,
            object_repr=object_repr,
            action_flag=action_flag,
            change_message=change_message,
        )
        return

    try:
        new_cur = new_conn.cursor()
        new_cur.execute(
            f"""
            INSERT INTO {NEW_TABLES['admin_log']} (
                action_time,
                object_id,
                object_repr,
                action_flag,
                change_message,
                content_type_id,
                user_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                action_time,
                object_id,
                object_repr,
                action_flag,
                change_message,
                new_content_type_id,
                new_user_id,
            ),
        )
        new_log_id = new_cur.fetchone()[0]
        new_cur.close()
        existing_signatures.add(signature)
        stats.inc("admin_log", "created")
        add_report_row(
            stats,
            result="created",
            reason="inserted",
            message="inserted into new DB",
            old_log_id=old_log_id,
            new_log_id=new_log_id,
            action_time=action_time,
            username=old_username,
            user_id=new_user_id,
            content_type_label=content_type_label,
            object_id=object_id,
            object_repr=object_repr,
            action_flag=action_flag,
            change_message=change_message,
        )
    except Exception as exc:
        stats.inc("admin_log", "failed")
        stats.add_error(f"{old_log_id} | {old_username or old_user_id or '-'} | {exc}")
        add_report_row(
            stats,
            result="failed",
            reason="unexpected_error",
            message=str(exc),
            old_log_id=old_log_id,
            new_log_id=None,
            action_time=action_time,
            username=old_username,
            user_id=old_user_id,
            content_type_label=content_type_label,
            object_id=object_id,
            object_repr=object_repr,
            action_flag=action_flag,
            change_message=change_message,
        )


HTML_TEMPLATE = Template(
    """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>$title</title>
  <style>
    :root {
      --bg: #f7f8fb;
      --panel: #ffffff;
      --text: #182230;
      --muted: #607086;
      --border: #dbe3ee;
      --shadow: 0 16px 40px rgba(22, 31, 46, 0.08);
      --accent: #2b6cb0;
      --good: #188038;
      --warn: #b7791f;
      --bad: #c53030;
      --chip: #eef3f9;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: linear-gradient(180deg, #f8fbff 0%, #f4f7fb 100%);
      color: var(--text);
    }
    .wrap {
      max-width: 1800px;
      margin: 0 auto;
      padding: 28px;
    }
    .hero {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 22px;
      padding: 24px;
      box-shadow: var(--shadow);
      margin-bottom: 18px;
    }
    h1 {
      margin: 0 0 8px;
      font-size: 30px;
      line-height: 1.1;
    }
    .sub {
      color: var(--muted);
      margin: 0;
      font-size: 14px;
    }
    .pills {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 18px;
    }
    .pill {
      background: var(--chip);
      border: 1px solid var(--border);
      color: var(--text);
      border-radius: 999px;
      padding: 8px 12px;
      font-size: 13px;
    }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
      margin: 18px 0;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 18px;
      box-shadow: var(--shadow);
    }
    .card .label {
      color: var(--muted);
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      margin-bottom: 10px;
    }
    .card .value {
      font-size: 30px;
      font-weight: 800;
    }
    .card .hint {
      color: var(--muted);
      margin-top: 6px;
      font-size: 13px;
    }
    .section {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 22px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .section-head {
      padding: 18px 20px 12px;
      border-bottom: 1px solid var(--border);
    }
    .section-head h2 {
      margin: 0;
      font-size: 20px;
    }
    .section-head .hint {
      color: var(--muted);
      margin-top: 6px;
      font-size: 13px;
    }
    .reason-wrap {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      padding: 0 20px 16px;
    }
    .reason-chip {
      background: #eef4ff;
      border: 1px solid #d7e4ff;
      color: #224c9f;
      border-radius: 999px;
      padding: 7px 11px;
      font-size: 13px;
    }
    .table-shell {
      padding: 16px 20px 20px;
      overflow-x: auto;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 1400px;
    }
    th, td {
      border-bottom: 1px solid #e6edf5;
      text-align: left;
      padding: 10px 12px;
      vertical-align: top;
      font-size: 13px;
    }
    th {
      position: sticky;
      top: 0;
      background: #f8fbff;
      z-index: 1;
      color: #415269;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.03em;
    }
    tbody tr:hover {
      background: #f8fbff;
    }
    .mono {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 12px;
      word-break: break-word;
    }
    .muted {
      color: var(--muted);
      font-size: 12px;
      margin-top: 4px;
      word-break: break-word;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      border: 1px solid transparent;
      white-space: nowrap;
    }
    .badge.created { background: #e8f6ee; color: #176f39; border-color: #ccebd8; }
    .badge.existing { background: #edf2f7; color: #4a5568; border-color: #d8e0ea; }
    .badge.skipped { background: #fff4e5; color: #9a6114; border-color: #ffe1b5; }
    .badge.failed { background: #fdecec; color: #b42318; border-color: #f7c7c7; }
    .badge.addition { background: #e8f6ee; color: #176f39; border-color: #ccebd8; }
    .badge.change { background: #eef4ff; color: #2251a6; border-color: #d7e4ff; }
    .badge.deletion { background: #fff1f3; color: #b42318; border-color: #ffd2da; }
    .controls {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 14px;
    }
    .page-info {
      color: var(--muted);
      font-size: 13px;
    }
    .pager {
      display: flex;
      gap: 8px;
      align-items: center;
    }
    button {
      appearance: none;
      border: 1px solid var(--border);
      background: white;
      color: var(--text);
      border-radius: 10px;
      padding: 8px 12px;
      cursor: pointer;
      font: inherit;
    }
    button:disabled {
      opacity: 0.45;
      cursor: not-allowed;
    }
    .empty {
      padding: 28px 0;
      color: var(--muted);
      text-align: center;
    }
    @media (max-width: 1200px) {
      .summary-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 760px) {
      .wrap { padding: 14px; }
      .summary-grid { grid-template-columns: 1fr; }
      h1 { font-size: 24px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1>$title</h1>
      <p class="sub">Migrates old Django admin log history into the new database and preserves links to the migrated records where the primary keys were kept stable.</p>
      <div class="pills">
        <span class="pill">Mode: <span id="mode-pill">-</span></span>
        <span class="pill">Generated: <span id="generated-pill">-</span></span>
        <span class="pill">Batch Size: <span id="batch-pill">-</span></span>
        <span class="pill">Data Limit: <span id="limit-pill">-</span></span>
      </div>
    </div>

    <section class="summary-grid" id="summary-grid"></section>

    <section class="section">
      <div class="section-head">
        <h2>Admin Log Rows</h2>
        <div class="hint">Paginated view of the migrated history. User and content-type IDs are shown for debugging.</div>
      </div>
      <div id="reasons" class="reason-wrap"></div>
      <div class="table-shell">
        <div class="controls">
          <div class="page-info" id="row-count-hint"></div>
          <div class="pager">
            <button id="prev-btn" type="button">Previous</button>
            <button id="next-btn" type="button">Next</button>
          </div>
        </div>
        <table>
          <thead>
            <tr>
              <th>#</th>
              <th>Result</th>
              <th>Reason</th>
              <th>Old ID</th>
              <th>New ID</th>
              <th>Action Time</th>
              <th>User</th>
              <th>Content Type</th>
              <th>Object ID</th>
              <th>Object Repr</th>
              <th>Action</th>
              <th>Change Message</th>
            </tr>
          </thead>
          <tbody id="rows-body"></tbody>
        </table>
        <div class="empty" id="empty-state" style="display:none;">No rows to display.</div>
        <div class="page-info" id="page-info">Page 1</div>
      </div>
    </section>
  </div>

  <script src="$data_js_name"></script>
  <script>
    (function () {
      const payload = window.__ADMIN_LOG_MIGRATION_DATA__ || {};
      const summary = payload.summary || {};
      const reasons = payload.reason_counts || {};
      const rows = Array.isArray(payload.rows) ? payload.rows : [];
      const pageSize = 100;
      let currentPage = 1;

      function esc(value) {
        return String(value === null || value === undefined || value === '' ? '-' : value)
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#39;');
      }

      function badge(text, cls) {
        return '<span class="badge ' + cls + '">' + esc(text) + '</span>';
      }

      function fmtUser(row) {
        const name = esc(row.username || '-');
        const id = row.user_id ? '<div class="muted mono">' + esc(row.user_id) + '</div>' : '';
        return name + id;
      }

      function fmtContentType(row) {
        const label = esc(row.content_type || '-');
        return label;
      }

      function renderSummaryCards() {
        const cards = [
          ['Read', summary.read || 0, 'Old log rows scanned'],
          ['Inserted', summary.inserted || 0, 'Rows written into new DB'],
          ['Existing', summary.existing || 0, 'Rows already present'],
          ['Skipped', summary.skipped || 0, 'Rows not migrated'],
          ['Failed', summary.failed || 0, 'Rows that hit an error'],
          ['Missing Content Types', summary.missing_content_type || 0, 'Rows inserted with null content_type_id'],
          ['Missing Users', summary.missing_user || 0, 'Rows skipped because the user was not found'],
        ];
        document.getElementById('summary-grid').innerHTML = cards.map(function (card) {
          return '<div class="card"><div class="label">' + esc(card[0]) + '</div><div class="value">' + esc(card[1]) + '</div><div class="hint">' + esc(card[2]) + '</div></div>';
        }).join('');
      }

      function renderReasons() {
        const entries = Object.entries(reasons);
        const target = document.getElementById('reasons');
        if (!entries.length) {
          target.innerHTML = '';
          return;
        }
        target.innerHTML = entries.map(function (entry) {
          return '<span class="reason-chip">' + esc(entry[0]) + ': ' + esc(entry[1]) + '</span>';
        }).join('');
      }

      function renderMode() {
        const meta = payload.run_metadata || {};
        document.getElementById('mode-pill').textContent = meta.mode || '-';
        document.getElementById('generated-pill').textContent = meta.generated_at || '-';
        document.getElementById('batch-pill').textContent = meta.batch_size || '-';
        document.getElementById('limit-pill').textContent = meta.data_limit === null || meta.data_limit === undefined ? 'all' : meta.data_limit;
      }

      function renderTable() {
        const start = (currentPage - 1) * pageSize;
        const pageRows = rows.slice(start, start + pageSize);
        const tbody = document.getElementById('rows-body');
        const emptyState = document.getElementById('empty-state');
        const rowCountHint = document.getElementById('row-count-hint');
        const pageInfo = document.getElementById('page-info');
        const prevBtn = document.getElementById('prev-btn');
        const nextBtn = document.getElementById('next-btn');

        rowCountHint.textContent = rows.length ? ('Showing ' + (start + 1) + '-' + Math.min(start + pageRows.length, rows.length) + ' of ' + rows.length + ' rows') : 'No rows';
        pageInfo.textContent = rows.length ? ('Page ' + currentPage + ' of ' + Math.max(1, Math.ceil(rows.length / pageSize))) : 'Page 1';
        prevBtn.disabled = currentPage <= 1;
        nextBtn.disabled = start + pageRows.length >= rows.length;

        if (!pageRows.length) {
          tbody.innerHTML = '';
          emptyState.style.display = 'block';
          return;
        }

        emptyState.style.display = 'none';
        tbody.innerHTML = pageRows.map(function (row, idx) {
          const no = start + idx + 1;
          return '<tr>' +
            '<td class="mono">' + no + '</td>' +
            '<td>' + badge(row.result || '-', row.result || 'existing') + '</td>' +
            '<td class="mono">' + esc(row.reason || '-') + '</td>' +
            '<td class="mono">' + esc(row.old_log_id || '-') + '</td>' +
            '<td class="mono">' + esc(row.new_log_id || '-') + '</td>' +
            '<td class="mono">' + esc(row.action_time || '-') + '</td>' +
            '<td>' + fmtUser(row) + '</td>' +
            '<td class="mono">' + fmtContentType(row) + '</td>' +
            '<td class="mono">' + esc(row.object_id || '-') + '</td>' +
            '<td class="mono">' + esc(row.object_repr || '-') + '</td>' +
            '<td>' + badge(row.action_label || '-', (row.action_label || '').toLowerCase()) + '</td>' +
            '<td class="mono">' + esc(row.change_message || '-') + '</td>' +
          '</tr>';
        }).join('');
      }

      document.getElementById('prev-btn').addEventListener('click', function () {
        if (currentPage > 1) {
          currentPage -= 1;
          renderTable();
        }
      });
      document.getElementById('next-btn').addEventListener('click', function () {
        if (currentPage * pageSize < rows.length) {
          currentPage += 1;
          renderTable();
        }
      });

      renderMode();
      renderSummaryCards();
      renderReasons();
      renderTable();
    })();
  </script>
</body>
</html>
"""
)


def build_report_payload(stats: MigrationStats, args, total_old_rows: int, processed_rows: int) -> Dict[str, Any]:
    result_counts = Counter((row.get("result") or "unknown") for row in stats.report_rows)
    reason_counts = Counter(row.get("reason") for row in stats.report_rows if row.get("reason"))
    summary = {
        "read": stats.total["admin_log"].get("read", 0),
        "inserted": stats.total["admin_log"].get("created", 0),
        "existing": stats.total["admin_log"].get("existing", 0),
        "skipped": stats.total["admin_log"].get("skipped", 0),
        "failed": stats.total["admin_log"].get("failed", 0),
        "missing_content_type": stats.total["admin_log"].get("missing_content_type", 0),
        "missing_user": stats.total["admin_log"].get("missing_user", 0),
    }
    return {
        "run_metadata": {
            **stats.run_metadata,
            "mode": "rollback" if args.rollback else ("dry-run" if args.dry_run else "live"),
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "batch_size": args.batch_size,
            "data_limit": args.data_limit,
            "total_old_rows": total_old_rows,
            "processed_rows": processed_rows,
        },
        "summary": summary,
        "result_counts": dict(result_counts),
        "reason_counts": dict(reason_counts),
        "rows": stats.report_rows,
        "errors": stats.errors,
    }


def write_report_files(report_file: str, payload: Dict[str, Any]):
    json_path = Path(normalize_report_file_path(report_file))
    html_path, data_json_path, data_js_path = build_sidecar_paths(report_file)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.parent.mkdir(parents=True, exist_ok=True)

    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False, default=json_default)

    with data_json_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False, default=json_default)

    with data_js_path.open("w", encoding="utf-8") as handle:
        handle.write("window.__ADMIN_LOG_MIGRATION_DATA__ = ")
        json.dump(payload, handle, ensure_ascii=False, default=json_default)
        handle.write(";\n")

    html_content = HTML_TEMPLATE.substitute(
        title=html_escape("Admin Log Migration Report"),
        data_js_name=html_escape(data_js_path.name),
    )
    with html_path.open("w", encoding="utf-8") as handle:
        handle.write(html_content)

    print(f"Report JSON saved to {json_path}")
    print(f"Report data JSON saved to {data_json_path}")
    print(f"Report fallback JS saved to {data_js_path}")
    print(f"HTML report saved to {html_path}")


def print_batch_summary(batch_no: int, stats: MigrationStats):
    values = stats.batch["admin_log"]
    print("\n" + "=" * 90)
    print(f"Batch {batch_no} summary: inserted={values['created']} existing={values['existing']} skipped={values['skipped']} failed={values['failed']}")
    if values["missing_content_type"] or values["missing_user"]:
        print(
            f"  missing_content_type={values['missing_content_type']} missing_user={values['missing_user']}"
        )
    if stats.errors:
        print("  Errors:")
        for err in stats.errors[:10]:
            print(f"  - {err}")


def print_final_summary(stats: MigrationStats):
    values = stats.total["admin_log"]
    print("\n" + "=" * 90)
    print("FINAL SUMMARY")
    print("=" * 90)
    print(f"read={values['read']} inserted={values['created']} existing={values['existing']} skipped={values['skipped']} failed={values['failed']}")
    print(f"missing_content_type={values['missing_content_type']} missing_user={values['missing_user']}")
    if stats.errors:
        print("\nErrors:")
        for err in stats.errors[:20]:
            print(f"- {err}")


def main():
    args = parse_args()

    old_db = DBConfig(
        host=env_first("OLD_DB_HOST", default="127.0.0.1"),
        port=int(env_first("OLD_DB_PORT", default="5432")),
        dbname=env_first("OLD_DB_NAME"),
        user=env_first("OLD_DB_USER"),
        password=env_first("OLD_DB_PASSWORD"),
    )
    new_db = DBConfig(
        host=env_first("NEW_DB_HOST", default="127.0.0.1"),
        port=int(env_first("NEW_DB_PORT", default="5432")),
        dbname=env_first("NEW_DB_NAME"),
        user=env_first("NEW_DB_USER"),
        password=env_first("NEW_DB_PASSWORD"),
    )

    if not old_db.dbname or not new_db.dbname:
        raise SystemExit("Missing OLD_DB_* or NEW_DB_* environment variables")

    old_conn = connect_db(old_db)
    new_conn = connect_db(new_db)

    stats = MigrationStats()
    stats.set_run_metadata(
        old_db=old_db.dbname,
        new_db=new_db.dbname,
        old_db_host=old_db.host,
        new_db_host=new_db.host,
    )

    try:
        total_old_rows = load_total_old_logs(old_conn)
        target_total = total_old_rows if args.data_limit is None else min(total_old_rows, args.data_limit)
        if target_total == 0:
            print("No admin log rows found in the old DB.")
            payload = build_report_payload(stats, args, total_old_rows, 0)
            write_report_files(args.report_file, payload)
            return

        print("=" * 90)
        print("ADMIN LOG MIGRATION")
        print("=" * 90)
        if args.dry_run:
            print("DRY RUN MODE")
        elif args.rollback:
            print("ROLLBACK MODE")
        print(f"Total old rows: {total_old_rows}")
        print(f"Rows to process: {target_total}")
        print(f"Batch size: {args.batch_size}")

        content_type_map = load_new_content_types(new_conn)
        user_by_id, user_by_username = load_new_user_maps(new_conn)
        existing_signatures = load_existing_signatures(new_conn)

        offset = 0
        batch_no = 1
        processed_rows = 0

        while offset < target_total:
            remaining = target_total - offset
            batch_limit = min(args.batch_size, remaining)
            batch_rows = load_old_logs_batch(old_conn, batch_limit, offset)
            if not batch_rows:
                break

            stats.inc("admin_log", "read", len(batch_rows))
            print(f"\n--- Batch {batch_no} ({len(batch_rows)} rows) ---")

            for row in batch_rows:
                process_admin_log_row(
                    new_conn,
                    stats,
                    row,
                    dry_run=args.dry_run,
                    content_type_map=content_type_map,
                    user_by_id=user_by_id,
                    user_by_username=user_by_username,
                    existing_signatures=existing_signatures,
                )

            processed_rows += len(batch_rows)
            print_batch_summary(batch_no, stats)
            stats.reset_batch()
            offset += len(batch_rows)
            batch_no += 1

        print_final_summary(stats)

        if args.dry_run or args.rollback:
            new_conn.rollback()
        else:
            new_conn.commit()

        payload = build_report_payload(stats, args, total_old_rows, processed_rows)
        write_report_files(args.report_file, payload)

    except Exception as exc:
        try:
            new_conn.rollback()
        except Exception:
            pass
        print(f"FATAL ERROR: {exc}")
        raise
    finally:
        try:
            old_conn.close()
        except Exception:
            pass
        try:
            new_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
