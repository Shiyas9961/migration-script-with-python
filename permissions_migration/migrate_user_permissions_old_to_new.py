#!/usr/bin/env python3
import argparse
import json
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv


load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_BATCH_SIZE = 500
DEFAULT_FIXTURE_FILE = "user_permissions_fixture.json"
DEFAULT_REPORT_FILE = "user_permissions_report.json"
DEFAULT_ROLLBACK_FILE = "user_permissions_rollback.json"

OLD_TABLES = {
    "user": "accounts_user",
    "permission": "auth_permission",
    "content_type": "django_content_type",
    "user_permissions": "accounts_user_user_permissions",
}

NEW_TABLES = {
    "user": "accounts_user",
    "permission": "auth_permission",
    "content_type": "django_content_type",
    "user_permissions": "accounts_user_user_permissions",
}

# Old-to-new Django permission remaps for renamed app/model pairs.
# Keep this limited to clear one-to-one replacements so we do not grant
# permissions on a wrong target model when an old table was actually removed.
PERMISSION_MODEL_REMAP = {
    ("applicant", "applicantprofile"): ("person", "personprofile"),
    ("applicant", "applicantgatepass"): ("person", "personaccesspass"),
    ("applicant", "applicantphoto"): ("person", "personphoto"),
    ("applicant", "applicanttraining"): ("training", "slotbooking"),
    ("organisation", "tradeinformation"): ("organisation", "trade"),
    ("organisation", "tradeinformationgroup"): ("organisation", "tradegroup"),
    ("system", "applicantbulkmigrate"): ("person", "bulkprofilesupload"),
    ("system", "overridepass"): ("access_pass", "overridepass"),
    ("system", "resource"): ("system", "module"),
    ("system", "resourceaction"): ("system", "modulepermission"),
    ("training", "trainingschedule"): ("training", "trainingslot"),
    ("applicant", "historicalapplicantgatepass"): ("person", "personaccesspasshistory"),
}
DEFAULT_PERMISSION_CODENAME_PREFIXES = ("add_", "change_", "delete_", "view_")


@dataclass
class DBConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


@dataclass
class Stats:
    total: Dict[str, Counter]
    errors: List[str]

    def __init__(self):
        self.total = defaultdict(Counter)
        self.errors = []

    def inc(self, key: str, amount: int = 1):
        self.total["permissions"][key] += amount

    def add_error(self, message: str):
        self.errors.append(message)


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


def fetchone(cur, query: str, params: Optional[Tuple[Any, ...]] = None):
    cur.execute(query, params or ())
    return cur.fetchone()


def fetchall(cur, query: str, params: Optional[Tuple[Any, ...]] = None):
    cur.execute(query, params or ())
    return cur.fetchall()


def normalize_output_path(value: str, default_name: str) -> Path:
    target = Path(value or default_name).expanduser()
    if target.suffix.lower() != ".json":
        target = target.with_suffix(".json")
    if not target.is_absolute():
        target = SCRIPT_DIR / target
    return target


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export old user permissions into a normalized fixture and apply them to the new DB"
    )
    parser.add_argument("--dry-run", action="store_true", help="Export the fixture without writing to the new DB")
    parser.add_argument("--from-fixture", type=str, default=None, help="Apply permissions from an existing fixture JSON file")
    parser.add_argument(
        "--roll-back",
        action="store_true",
        help="Delete only the permission links created by the last live migration run",
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help=f"Batch size (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--data-limit", type=int, default=None, help="Maximum number of old permission rows to export")
    parser.add_argument(
        "--fixture-file",
        type=str,
        default=DEFAULT_FIXTURE_FILE,
        help=f"Fixture file path (default: {DEFAULT_FIXTURE_FILE})",
    )
    parser.add_argument(
        "--report-file",
        type=str,
        default=DEFAULT_REPORT_FILE,
        help=f"Report file path (default: {DEFAULT_REPORT_FILE})",
    )
    parser.add_argument(
        "--rollback-file",
        type=str,
        default=DEFAULT_ROLLBACK_FILE,
        help=f"Rollback journal file path (default: {DEFAULT_ROLLBACK_FILE})",
    )
    args = parser.parse_args()
    if args.batch_size <= 0:
        parser.error("--batch-size must be greater than 0")
    if args.data_limit is not None and args.data_limit <= 0:
        parser.error("--data-limit must be greater than 0")
    if args.roll_back and args.dry_run:
        parser.error("--roll-back cannot be combined with --dry-run")
    if args.roll_back and args.from_fixture:
        parser.error("--roll-back cannot be combined with --from-fixture")
    return args


def load_old_batch(old_cur, limit: int, offset: int):
    return fetchall(
        old_cur,
        f"""
        SELECT
            up.user_id,
            up.permission_id AS old_permission_id,
            p.codename,
            p.name AS permission_name,
            ct.app_label,
            ct.model,
            u.username
        FROM {OLD_TABLES['user_permissions']} up
        JOIN {OLD_TABLES['permission']} p ON p.id = up.permission_id
        JOIN {OLD_TABLES['content_type']} ct ON ct.id = p.content_type_id
        LEFT JOIN {OLD_TABLES['user']} u ON u.id = up.user_id
        ORDER BY up.user_id, ct.app_label, ct.model, p.codename
        LIMIT %s OFFSET %s
        """,
        (limit, offset),
    )


def load_old_total(old_cur) -> int:
    row = fetchone(old_cur, f"SELECT COUNT(*) AS cnt FROM {OLD_TABLES['user_permissions']}")
    return int(row["cnt"])


def load_new_user_ids(new_cur) -> set:
    rows = fetchall(new_cur, f"SELECT id FROM {NEW_TABLES['user']}")
    return {str(row["id"]) for row in rows}


def remap_permission_codename(codename: Optional[str], old_model: str, new_model: str) -> Optional[str]:
    if codename is None:
        return None
    if old_model == new_model:
        return codename
    for prefix in DEFAULT_PERMISSION_CODENAME_PREFIXES:
        if codename == f"{prefix}{old_model}":
            return f"{prefix}{new_model}"
    if codename.endswith(f"_{old_model}"):
        return codename[: -len(old_model)] + new_model
    if codename.startswith(f"{old_model}_"):
        return new_model + codename[len(old_model):]
    return codename


def resolve_permission_identity(
    app_label: Optional[str],
    model: Optional[str],
    codename: Optional[str],
) -> Tuple[Optional[str], Optional[str], Optional[str], bool]:
    source_app_label = normalize_text(app_label)
    source_model = normalize_text(model)
    source_codename = normalize_text(codename)
    if not source_app_label or not source_model or not source_codename:
        return source_app_label, source_model, source_codename, False
    mapped = PERMISSION_MODEL_REMAP.get((source_app_label, source_model))
    if not mapped:
        return source_app_label, source_model, source_codename, False
    target_app_label, target_model = mapped
    target_codename = remap_permission_codename(source_codename, source_model, target_model)
    return target_app_label, target_model, target_codename, True


def load_new_permission_map(new_cur) -> Dict[Tuple[str, str, str], int]:
    rows = fetchall(
        new_cur,
        f"""
        SELECT p.id, p.codename, p.name AS permission_name, ct.app_label, ct.model
        FROM {NEW_TABLES['permission']} p
        JOIN {NEW_TABLES['content_type']} ct ON ct.id = p.content_type_id
        """,
    )
    mapping: Dict[Tuple[str, str, str], int] = {}
    for row in rows:
        key = (str(row["app_label"]), str(row["model"]), str(row["codename"]))
        mapping[key] = row["id"]
    return mapping


def load_existing_pairs(new_cur) -> set:
    rows = fetchall(
        new_cur,
        f"""
        SELECT user_id, permission_id
        FROM {NEW_TABLES['user_permissions']}
        """,
    )
    return {(str(row["user_id"]), row["permission_id"]) for row in rows}


def build_rollback_record(
    *,
    user_id: str,
    permission_id: int,
    username: Optional[str],
    source_permission: Tuple[Optional[str], Optional[str], Optional[str]],
    resolved_permission: Tuple[Optional[str], Optional[str], Optional[str]],
    old_permission_id: Any = None,
) -> Dict[str, Any]:
    return {
        "user_id": str(user_id),
        "permission_id": int(permission_id),
        "username": normalize_text(username),
        "source_permission": {
            "app_label": source_permission[0],
            "model": source_permission[1],
            "codename": source_permission[2],
        },
        "resolved_permission": {
            "app_label": resolved_permission[0],
            "model": resolved_permission[1],
            "codename": resolved_permission[2],
        },
        "source": {
            "old_permission_id": old_permission_id,
        },
    }


def load_rollback_journal(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"Rollback journal not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if isinstance(payload, dict):
        records = payload.get("records", [])
    else:
        records = payload
    if not isinstance(records, list):
        raise ValueError("Rollback journal must contain a list of records or an object with a 'records' list")
    return records


def rollback_inserted_permissions(new_cur, records: List[Dict[str, Any]], stats: Stats) -> int:
    deleted_rows = 0
    for record in records:
        user_id = normalize_text(record.get("user_id"))
        permission_id = record.get("permission_id")
        if not user_id or permission_id is None:
            continue
        new_cur.execute(
            f"DELETE FROM {NEW_TABLES['user_permissions']} WHERE user_id = %s AND permission_id = %s",
            (user_id, permission_id),
        )
        deleted_rows += int(new_cur.rowcount or 0)
    stats.inc("rolled_back", deleted_rows)
    return deleted_rows


def make_fixture_record(
    row: Dict[str, Any],
    resolved_permission: Tuple[Optional[str], Optional[str], Optional[str]],
    remapped: bool,
) -> Dict[str, Any]:
    source_permission = {
        "app_label": normalize_text(row.get("app_label")),
        "model": normalize_text(row.get("model")),
        "codename": normalize_text(row.get("codename")),
        "name": normalize_text(row.get("permission_name")),
    }
    target_permission = {
        "app_label": resolved_permission[0],
        "model": resolved_permission[1],
        "codename": resolved_permission[2],
        "name": normalize_text(row.get("permission_name")),
    }
    return {
        "user_id": str(row["user_id"]),
        "username": normalize_text(row.get("username")),
        "permission": target_permission,
        "source_permission": source_permission,
        "remapped": remapped,
        "source": {
            "old_permission_id": row.get("old_permission_id"),
        },
    }


def extract_permission_identity(record: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    for key in ("permission", "resolved_permission", "source_permission"):
        perm = record.get(key)
        if not isinstance(perm, dict):
            continue
        app_label = normalize_text(perm.get("app_label"))
        model = normalize_text(perm.get("model"))
        codename = normalize_text(perm.get("codename"))
        if app_label and model and codename:
            return app_label, model, codename
    return None, None, None


def load_fixture_file(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if isinstance(payload, dict):
        records = payload.get("records", [])
    else:
        records = payload
    if not isinstance(records, list):
        raise ValueError("Fixture file must contain a list of records or an object with a 'records' list")
    return records


def write_json(path: Path, payload: Dict[str, Any]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False, default=json_default)


def build_report(
    stats: Stats,
    *,
    mode: str,
    total_source_rows: int,
    exported_rows: int,
    applied_rows: int,
    rolled_back_rows: int,
    fixture_file: Path,
    rollback_file: Path,
    report_file: Path,
) -> Dict[str, Any]:
    summary = {
        "read": stats.total["permissions"].get("read", 0),
        "exported": stats.total["permissions"].get("exported", 0),
        "applied": stats.total["permissions"].get("applied", 0),
        "rolled_back": stats.total["permissions"].get("rolled_back", 0),
        "existing": stats.total["permissions"].get("existing", 0),
        "skipped": stats.total["permissions"].get("skipped", 0),
        "failed": stats.total["permissions"].get("failed", 0),
        "missing_user": stats.total["permissions"].get("missing_user", 0),
        "missing_permission": stats.total["permissions"].get("missing_permission", 0),
        "remapped": stats.total["permissions"].get("remapped", 0),
    }
    return {
        "run_metadata": {
            "mode": mode,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "total_source_rows": total_source_rows,
            "exported_rows": exported_rows,
            "applied_rows": applied_rows,
            "rolled_back_rows": rolled_back_rows,
            "fixture_file": str(fixture_file),
            "rollback_file": str(rollback_file),
            "report_file": str(report_file),
        },
        "summary": summary,
        "reason_counts": dict(stats.total["reasons"]),
        "errors": stats.errors,
    }


def export_and_apply_from_db(args, old_cur, new_cur, fixture_file: Path, stats: Stats):
    total_source_rows = load_old_total(old_cur)
    target_total = total_source_rows if args.data_limit is None else min(total_source_rows, args.data_limit)
    user_ids = load_new_user_ids(new_cur)
    permission_map = load_new_permission_map(new_cur)
    existing_pairs = load_existing_pairs(new_cur)

    print("=" * 90)
    print("USER PERMISSIONS FIxture MIGRATION")
    print("=" * 90)
    print("DRY RUN MODE" if args.dry_run else "LIVE MODE")
    print(f"Batch size: {args.batch_size}")
    print(f"Total old rows: {total_source_rows}")
    print(f"Rows to process: {target_total}")

    exported_records: List[Dict[str, Any]] = []
    rollback_records: List[Dict[str, Any]] = []
    offset = 0
    batch_no = 1
    applied_rows = 0

    while offset < target_total:
        batch_limit = min(args.batch_size, target_total - offset)
        batch = load_old_batch(old_cur, batch_limit, offset)
        if not batch:
            break

        print()
        print(f"--- Batch {batch_no} ({len(batch)} rows) ---")
        batch_created = 0
        batch_existing = 0
        batch_skipped = 0
        batch_failed = 0
        batch_remapped = 0

        for row in batch:
            stats.inc("read")
            user_id = str(row["user_id"])
            username = normalize_text(row.get("username"))
            if user_id not in user_ids:
                batch_skipped += 1
                stats.inc("skipped")
                stats.inc("missing_user")
                stats.total["reasons"]["missing_user"] += 1
                continue

            source_app_label = str(row["app_label"])
            source_model = str(row["model"])
            source_codename = str(row["codename"])
            resolved_app_label, resolved_model, resolved_codename, remapped = resolve_permission_identity(
                source_app_label,
                source_model,
                source_codename,
            )
            if remapped:
                batch_remapped += 1
                stats.inc("remapped")

            permission_id = permission_map.get((resolved_app_label, resolved_model, resolved_codename))
            if permission_id is None:
                batch_skipped += 1
                stats.inc("skipped")
                stats.inc("missing_permission")
                stats.total["reasons"]["missing_permission"] += 1
                continue

            exported_records.append(
                make_fixture_record(
                    row,
                    (resolved_app_label, resolved_model, resolved_codename),
                    remapped,
                )
            )
            stats.inc("exported")

            pair = (user_id, permission_id)
            if pair in existing_pairs:
                batch_existing += 1
                stats.inc("existing")
                continue

            if args.dry_run:
                batch_created += 1
                stats.inc("applied")
                applied_rows += 1
                continue

            try:
                new_cur.execute(
                    f"""
                    INSERT INTO {NEW_TABLES['user_permissions']} (user_id, permission_id)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id, permission_id) DO NOTHING
                    """,
                    (row["user_id"], permission_id),
                )
                if (new_cur.rowcount or 0) > 0:
                    existing_pairs.add(pair)
                    batch_created += 1
                    stats.inc("applied")
                    applied_rows += 1
                    rollback_records.append(
                        build_rollback_record(
                            user_id=user_id,
                            permission_id=permission_id,
                            username=username,
                            source_permission=(source_app_label, source_model, source_codename),
                            resolved_permission=(resolved_app_label, resolved_model, resolved_codename),
                            old_permission_id=row.get("old_permission_id"),
                        )
                    )
                else:
                    existing_pairs.add(pair)
                    batch_existing += 1
                    stats.inc("existing")
            except Exception as exc:
                batch_failed += 1
                stats.inc("failed")
                stats.add_error(
                    f"{user_id} | {username or '-'} | {source_app_label}.{source_model}.{source_codename} -> {resolved_app_label}.{resolved_model}.{resolved_codename} | {exc}"
                )

        print(
            f"Batch {batch_no} summary: exported={batch_created + batch_existing} created={batch_created} existing={batch_existing} remapped={batch_remapped} skipped={batch_skipped} failed={batch_failed}"
        )
        offset += len(batch)
        batch_no += 1

    fixture_payload = {
        "table": NEW_TABLES["user_permissions"],
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "records": exported_records,
    }
    write_json(fixture_file, fixture_payload)
    print(f"Fixture saved to {fixture_file}")
    return total_source_rows, len(exported_records), applied_rows, rollback_records


def apply_from_fixture(args, new_cur, fixture_path: Path, stats: Stats):
    user_ids = load_new_user_ids(new_cur)
    permission_map = load_new_permission_map(new_cur)
    existing_pairs = load_existing_pairs(new_cur)
    records = load_fixture_file(fixture_path)

    print("=" * 90)
    print("APPLY USER PERMISSIONS FROM FIXTURE")
    print("=" * 90)
    print("DRY RUN MODE" if args.dry_run else "LIVE MODE")
    print(f"Fixture: {fixture_path}")
    print(f"Records: {len(records)}")

    applied_rows = 0
    exported_rows = 0
    rollback_records: List[Dict[str, Any]] = []

    for row in records:
        exported_rows += 1
        user_id = str(row.get("user_id"))
        if user_id not in user_ids:
            stats.inc("skipped")
            stats.inc("missing_user")
            stats.total["reasons"]["missing_user"] += 1
            continue

        source_app_label, source_model, source_codename = extract_permission_identity(row)
        if not source_app_label or not source_model or not source_codename:
            stats.inc("skipped")
            stats.inc("missing_permission")
            stats.total["reasons"]["missing_permission"] += 1
            continue

        resolved_app_label, resolved_model, resolved_codename, remapped = resolve_permission_identity(
            source_app_label,
            source_model,
            source_codename,
        )
        if remapped:
            stats.inc("remapped")

        permission_id = permission_map.get((resolved_app_label, resolved_model, resolved_codename))
        if permission_id is None:
            stats.inc("skipped")
            stats.inc("missing_permission")
            stats.total["reasons"]["missing_permission"] += 1
            continue

        pair = (user_id, permission_id)
        if pair in existing_pairs:
            stats.inc("existing")
            continue

        if args.dry_run:
            stats.inc("applied")
            applied_rows += 1
            continue

        try:
            new_cur.execute(
                f"""
                INSERT INTO {NEW_TABLES['user_permissions']} (user_id, permission_id)
                VALUES (%s, %s)
                ON CONFLICT (user_id, permission_id) DO NOTHING
                """,
                (row["user_id"], permission_id),
            )
            if (new_cur.rowcount or 0) > 0:
                existing_pairs.add(pair)
                stats.inc("applied")
                applied_rows += 1
                rollback_records.append(
                    build_rollback_record(
                        user_id=user_id,
                        permission_id=permission_id,
                        username=normalize_text(row.get("username")),
                        source_permission=(source_app_label, source_model, source_codename),
                        resolved_permission=(resolved_app_label, resolved_model, resolved_codename),
                        old_permission_id=(row.get("source") or {}).get("old_permission_id"),
                    )
                )
            else:
                existing_pairs.add(pair)
                stats.inc("existing")
        except Exception as exc:
            stats.inc("failed")
            stats.add_error(
                f"{user_id} | {source_app_label}.{source_model}.{source_codename} -> {resolved_app_label}.{resolved_model}.{resolved_codename} | {exc}"
            )

    return exported_rows, applied_rows, rollback_records


def main():
    args = parse_args()
    fixture_file = normalize_output_path(args.fixture_file, DEFAULT_FIXTURE_FILE)
    report_file = normalize_output_path(args.report_file, DEFAULT_REPORT_FILE)
    rollback_file = normalize_output_path(args.rollback_file, DEFAULT_ROLLBACK_FILE)
    stats = Stats()

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

    if not new_db.dbname:
        raise SystemExit("Missing NEW_DB_* environment variables")

    old_conn = None
    new_conn = None
    total_source_rows = 0
    exported_rows = 0
    applied_rows = 0
    rolled_back_rows = 0

    try:
        new_conn = connect_db(new_db)
        new_conn.autocommit = False
        new_cur = new_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        if args.roll_back:
            records = load_rollback_journal(rollback_file)
            print("=" * 90)
            print("ROLLBACK USER PERMISSIONS")
            print("=" * 90)
            print(f"Rollback journal: {rollback_file}")
            print(f"Records: {len(records)}")
            rolled_back_rows = rollback_inserted_permissions(new_cur, records, stats)
            total_source_rows = len(records)
            new_conn.commit()
            report_payload = build_report(
                stats,
                mode="rollback",
                total_source_rows=total_source_rows,
                exported_rows=0,
                applied_rows=0,
                rolled_back_rows=rolled_back_rows,
                fixture_file=rollback_file,
                rollback_file=rollback_file,
                report_file=report_file,
            )
            write_json(report_file, report_payload)
            print(f"Report saved to {report_file}")
            print(
                f"Summary: read={report_payload['summary']['read']} exported={report_payload['summary']['exported']} applied={report_payload['summary']['applied']} rolled_back={report_payload['summary']['rolled_back']} existing={report_payload['summary']['existing']} skipped={report_payload['summary']['skipped']} failed={report_payload['summary']['failed']} remapped={report_payload['summary']['remapped']}"
            )
            if stats.errors:
                print()
                print("Errors:")
                for err in stats.errors[:20]:
                    print(f"- {err}")
            return

        if args.from_fixture:
            fixture_path = normalize_output_path(args.from_fixture, DEFAULT_FIXTURE_FILE)
            print(f"Loading fixture from {fixture_path}")
            exported_rows, applied_rows, rollback_records = apply_from_fixture(args, new_cur, fixture_path, stats)
            total_source_rows = len(load_fixture_file(fixture_path))
            if args.dry_run:
                new_conn.rollback()
            else:
                new_conn.commit()
                rollback_payload = {
                    "table": NEW_TABLES["user_permissions"],
                    "generated_at": datetime.now().isoformat(timespec="seconds"),
                    "source_mode": "fixture",
                    "source_file": str(fixture_path),
                    "records": rollback_records,
                }
                write_json(rollback_file, rollback_payload)
                print(f"Rollback journal saved to {rollback_file}")
        else:
            if not old_db.dbname:
                raise SystemExit("Missing OLD_DB_* environment variables")
            old_conn = connect_db(old_db)
            old_conn.autocommit = False
            old_cur = old_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            total_source_rows, exported_rows, applied_rows, rollback_records = export_and_apply_from_db(
                args,
                old_cur,
                new_cur,
                fixture_file,
                stats,
            )
            if args.dry_run:
                new_conn.rollback()
            else:
                new_conn.commit()
                rollback_payload = {
                    "table": NEW_TABLES["user_permissions"],
                    "generated_at": datetime.now().isoformat(timespec="seconds"),
                    "source_mode": "live",
                    "source_file": str(fixture_file),
                    "records": rollback_records,
                }
                write_json(rollback_file, rollback_payload)
                print(f"Rollback journal saved to {rollback_file}")

        report_payload = build_report(
            stats,
            mode="dry-run" if args.dry_run else ("fixture" if args.from_fixture else "live"),
            total_source_rows=total_source_rows,
            exported_rows=exported_rows,
            applied_rows=applied_rows,
            rolled_back_rows=0,
            fixture_file=fixture_file if not args.from_fixture else normalize_output_path(args.from_fixture, DEFAULT_FIXTURE_FILE),
            rollback_file=rollback_file,
            report_file=report_file,
        )
        write_json(report_file, report_payload)
        print(f"Report saved to {report_file}")
        print(
            f"Summary: read={report_payload['summary']['read']} exported={report_payload['summary']['exported']} applied={report_payload['summary']['applied']} rolled_back={report_payload['summary']['rolled_back']} existing={report_payload['summary']['existing']} skipped={report_payload['summary']['skipped']} failed={report_payload['summary']['failed']} remapped={report_payload['summary']['remapped']}"
        )
        if stats.errors:
            print()
            print("Errors:")
            for err in stats.errors[:20]:
                print(f"- {err}")

    except Exception as exc:
        if new_conn:
            new_conn.rollback()
        print(f"FATAL ERROR: {exc}")
        raise
    finally:
        if old_conn:
            old_conn.close()
        if new_conn:
            new_conn.close()


if __name__ == "__main__":

    main()
