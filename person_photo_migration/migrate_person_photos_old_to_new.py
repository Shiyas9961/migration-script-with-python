#!/usr/bin/env python3
import argparse
import json
import mimetypes
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
DEFAULT_REPORT_FILE = "person_photo_migration_report.html"
DEFAULT_STORAGE_PREFIX = "media/person/photos"
ROLLBACK_CLEANUP_TABLES = [
    "person_personphoto",
]

OLD_TABLES = {
    "applicant_profile": "applicant_applicantprofile",
    "applicant_photo": "applicant_applicantphoto",
}

NEW_TABLES = {
    "person_profile": "person_personprofile",
    "person_photo": "person_personphoto",
}


@dataclass
class DBConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


@dataclass
class StorageConfig:
    bucket: str
    region: str
    endpoint_url: Optional[str]
    public_base_url: Optional[str]
    prefix: str
    access_key_id: Optional[str]
    secret_access_key: Optional[str]
    session_token: Optional[str]
    verify_ssl: bool = True


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

    def add_error(self, photo_id: Optional[str], profile_id: Optional[str], full_name: Optional[str], message: str):
        self.errors.append(f"{photo_id or '-'} | {profile_id or '-'} | {full_name or '-'} | {message}")

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


def normalize_report_file_path(report_file: str) -> str:
    target = Path(report_file).expanduser()
    if target.suffix.lower() != ".html":
        target = target.with_suffix(".html")
    if not target.is_absolute():
        target = SCRIPT_DIR / target
    return str(target)


def build_sidecar_paths(report_file: str) -> Tuple[Path, Path, Path]:
    html_path = Path(normalize_report_file_path(report_file))
    base = html_path.with_suffix("")
    json_path = Path(f"{base}.data.json")
    js_path = Path(f"{base}.data.js")
    return html_path, json_path, js_path


def resolve_source_media_root(path: Optional[str]) -> Path:
    if not path:
        return SCRIPT_DIR
    root = Path(path).expanduser()
    if not root.is_absolute():
        root = SCRIPT_DIR / root
    return root


def normalize_storage_prefix(prefix: str) -> str:
    value = (prefix or "").strip().strip("/")
    if not value:
        return DEFAULT_STORAGE_PREFIX
    if not value.startswith("media/"):
        value = "media/" + value
    return value


def connect_db(cfg: DBConfig):
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )
    conn.autocommit = True
    return conn


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


def json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return str(value)


def stable_pk(value: Any) -> Any:
    return value


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


def normalize_avatar_value(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def strip_media_prefix(value: Optional[str]) -> Optional[str]:
    raw = normalize_avatar_value(value)
    if not raw:
        return None
    if raw.startswith("http://") or raw.startswith("https://"):
        from urllib.parse import urlparse

        path = urlparse(raw).path.lstrip("/")
        if "/media/" in path:
            path = path.split("/media/", 1)[1]
        elif path.startswith("media/"):
            path = path[len("media/") :]
        return path.strip("/") or None
    raw = raw.strip("/")
    if raw.startswith("media/"):
        raw = raw[len("media/") :]
    return raw or None


def ensure_media_prefix(value: Optional[str]) -> Optional[str]:
    raw = strip_media_prefix(value)
    if not raw:
        return None
    return "media/" + raw


def build_object_key(prefix: str, person_id: str, old_avatar: str) -> str:
    filename = os.path.basename(old_avatar.rstrip("/"))
    return "/".join(part.strip("/") for part in [prefix, str(person_id), filename] if part)


def build_avatar_url(storage: StorageConfig, object_key: str) -> str:
    if not object_key:
        return object_key
    if str(object_key).startswith(("http://", "https://")):
        return str(object_key)
    if storage.public_base_url:
        base = storage.public_base_url.rstrip("/")
    elif storage.endpoint_url and storage.bucket:
        base = storage.endpoint_url.rstrip("/") + "/" + storage.bucket.strip("/")
    elif storage.bucket:
        base = f"https://s3.{storage.region}.amazonaws.com/{storage.bucket.strip('/')}"
    else:
        return ensure_media_prefix(object_key)
    return base + "/" + ensure_media_prefix(object_key).lstrip("/")


def resolve_avatar_preview(storage: StorageConfig, stored_value: Optional[str]) -> Optional[str]:
    return build_avatar_url(storage, stored_value)


def build_storage_client(storage: StorageConfig):
    try:
        import boto3
        from botocore.config import Config as BotocoreConfig
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for photo uploads. Install dependencies with `pip install -r requirements.txt`."
        ) from exc

    kwargs: Dict[str, Any] = {
        "region_name": storage.region or None,
        "config": BotocoreConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
        "verify": storage.verify_ssl,
    }
    if storage.endpoint_url:
        kwargs["endpoint_url"] = storage.endpoint_url
    if storage.access_key_id:
        kwargs["aws_access_key_id"] = storage.access_key_id
    if storage.secret_access_key:
        kwargs["aws_secret_access_key"] = storage.secret_access_key
    if storage.session_token:
        kwargs["aws_session_token"] = storage.session_token
    return boto3.client("s3", **kwargs)


def upload_file(storage_client, storage: StorageConfig, local_path: Path, object_key: str):
    content_type, _ = mimetypes.guess_type(str(local_path))
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type
    if extra_args:
        storage_client.upload_file(str(local_path), storage.bucket, object_key, ExtraArgs=extra_args)
    else:
        storage_client.upload_file(str(local_path), storage.bucket, object_key)


def cleanup_storage_objects(storage_client, storage: StorageConfig) -> int:
    if not storage_client or not storage.bucket:
        return 0

    prefix = storage.prefix.strip("/")
    if not prefix:
        print("Storage prefix is empty; skipping uploaded photo cleanup.")
        return 0

    target_prefix = prefix.rstrip("/") + "/"
    deleted = 0
    pending: List[Dict[str, str]] = []
    paginator = storage_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=storage.bucket, Prefix=target_prefix):
        for item in page.get("Contents", []):
            pending.append({"Key": item["Key"]})
            if len(pending) == 1000:
                storage_client.delete_objects(
                    Bucket=storage.bucket,
                    Delete={"Objects": pending, "Quiet": True},
                )
                deleted += len(pending)
                pending.clear()

    if pending:
        storage_client.delete_objects(
            Bucket=storage.bucket,
            Delete={"Objects": pending, "Quiet": True},
        )
        deleted += len(pending)

    return deleted


def load_total_photo_count(old_conn) -> int:
    row = fetchone(
        old_conn,
        f"SELECT COUNT(*) AS cnt FROM {OLD_TABLES['applicant_photo']}"
    )
    return int(row["cnt"]) if row else 0


def load_photo_batch(old_conn, limit: int, offset: int):
    return fetchall(
        old_conn,
        f"""
        SELECT
            photo.id AS photo_id,
            photo.applicant_id AS applicant_pk,
            app.applicant_id AS profile_id,
            app.full_name AS full_name,
            photo.avatar AS old_avatar,
            photo.is_deleted AS is_deleted,
            photo.created AS created,
            photo.modified AS modified
        FROM {OLD_TABLES['applicant_photo']} photo
        LEFT JOIN {OLD_TABLES['applicant_profile']} app ON app.id = photo.applicant_id
        ORDER BY photo.created ASC, photo.id ASC
        LIMIT %s OFFSET %s
        """,
        (limit, offset),
    )


def find_person_by_profile_id(new_conn, profile_id: str, cache: Dict[str, Dict[str, Any]]):
    if profile_id in cache:
        return cache[profile_id]
    row = fetchone(
        new_conn,
        f"""
        SELECT id, profile_id, full_name
        FROM {NEW_TABLES['person_profile']}
        WHERE profile_id = %s
        LIMIT 1
        """,
        (profile_id,),
    )
    if row:
        cache[profile_id] = dict(row)
    return row


def find_photo_by_id(new_conn, photo_id: str):
    return fetchone(
        new_conn,
        f"""
        SELECT id, avatar, person_id
        FROM {NEW_TABLES['person_photo']}
        WHERE id = %s
        LIMIT 1
        """,
        (photo_id,),
    )


def add_photo_report_row(
    stats: MigrationStats,
    *,
    result: str,
    reason: Optional[str],
    photo_id: Optional[str],
    applicant_pk: Optional[str],
    profile_id: Optional[str],
    person_id: Optional[str],
    full_name: Optional[str],
    old_avatar: Optional[str],
    source_path: Optional[str],
    storage_key: Optional[str],
    avatar_url: Optional[str],
    is_deleted: Optional[bool],
    message: str,
):
    stats.add_report_row(
        {
            "result": result,
            "reason": reason,
            "photo_id": photo_id,
            "applicant_pk": applicant_pk,
            "profile_id": profile_id,
            "person_id": person_id,
            "full_name": full_name,
            "old_avatar": old_avatar,
            "source_path": source_path,
            "storage_key": storage_key,
            "avatar_url": avatar_url,
            "is_deleted": is_deleted,
            "message": message,
        }
    )


def migrate_photo_row(
    old_conn,
    new_conn,
    storage_client,
    storage: StorageConfig,
    source_media_root: Path,
    stats: MigrationStats,
    row: Dict[str, Any],
    dry_run: bool,
    person_cache: Dict[str, Dict[str, Any]],
):
    stats.inc("photo", "read")

    photo_id = row.get("photo_id")
    applicant_pk = row.get("applicant_pk")
    profile_id = row.get("profile_id")
    full_name = row.get("full_name")
    old_avatar = normalize_avatar_value(row.get("old_avatar"))
    is_deleted = bool(row.get("is_deleted"))
    created_at, modified_at = source_timestamps(row)

    if not photo_id:
        stats.inc("photo", "skipped")
        add_photo_report_row(
            stats,
            result="skipped",
            reason="missing_photo_id",
            photo_id=None,
            applicant_pk=str(applicant_pk) if applicant_pk else None,
            profile_id=profile_id,
            person_id=None,
            full_name=full_name,
            old_avatar=old_avatar,
            source_path=None,
            storage_key=None,
            avatar_url=None,
            is_deleted=is_deleted,
            message="photo primary key is missing",
        )
        return

    if not profile_id:
        stats.inc("photo", "skipped")
        add_photo_report_row(
            stats,
            result="skipped",
            reason="missing_profile_id",
            photo_id=str(photo_id) if photo_id else None,
            applicant_pk=str(applicant_pk) if applicant_pk else None,
            profile_id=None,
            person_id=None,
            full_name=full_name,
            old_avatar=old_avatar,
            source_path=None,
            storage_key=None,
            avatar_url=None,
            is_deleted=is_deleted,
            message="applicant profile missing in old DB",
        )
        return

    person = find_person_by_profile_id(new_conn, profile_id, person_cache)
    if not person:
        stats.inc("photo", "missing_person")
        stats.inc("photo", "skipped")
        add_photo_report_row(
            stats,
            result="skipped",
            reason="missing_person",
            photo_id=str(photo_id) if photo_id else None,
            applicant_pk=str(applicant_pk) if applicant_pk else None,
            profile_id=profile_id,
            person_id=None,
            full_name=full_name,
            old_avatar=old_avatar,
            source_path=None,
            storage_key=None,
            avatar_url=None,
            is_deleted=is_deleted,
            message="person profile not found in new DB",
        )
        return

    if not old_avatar:
        stats.inc("photo", "empty_avatar")
        stats.inc("photo", "skipped")
        add_photo_report_row(
            stats,
            result="skipped",
            reason="empty_avatar",
            photo_id=str(photo_id) if photo_id else None,
            applicant_pk=str(applicant_pk) if applicant_pk else None,
            profile_id=profile_id,
            person_id=str(person["id"]),
            full_name=full_name,
            old_avatar=old_avatar,
            source_path=None,
            storage_key=None,
            avatar_url=None,
            is_deleted=is_deleted,
            message="avatar path is empty",
        )
        return

    source_path = source_media_root / old_avatar.lstrip("/")
    if not source_path.exists():
        stats.inc("photo", "missing_source_file")
        stats.inc("photo", "skipped")
        add_photo_report_row(
            stats,
            result="skipped",
            reason="missing_source_file",
            photo_id=str(photo_id) if photo_id else None,
            applicant_pk=str(applicant_pk) if applicant_pk else None,
            profile_id=profile_id,
            person_id=str(person["id"]),
            full_name=full_name,
            old_avatar=old_avatar,
            source_path=str(source_path),
            storage_key=None,
            avatar_url=None,
            is_deleted=is_deleted,
            message="local photo file not found",
        )
        return

    object_key = build_object_key(storage.prefix, str(person["id"]), old_avatar)
    model_avatar = strip_media_prefix(object_key)
    avatar_url = build_avatar_url(storage, object_key)

    existing = find_photo_by_id(new_conn, photo_id)
    if existing:
        existing_avatar = normalize_avatar_value(existing.get("avatar"))
        normalized_avatar = strip_media_prefix(existing_avatar)
        avatar_preview = build_avatar_url(storage, normalized_avatar or existing_avatar or object_key)
        if normalized_avatar and existing_avatar != normalized_avatar:
            if dry_run:
                stats.inc("photo", "updated")
                add_photo_report_row(
                    stats,
                    result="updated",
                    reason="normalized_media_prefix",
                    photo_id=str(photo_id),
                    applicant_pk=str(applicant_pk) if applicant_pk else None,
                    profile_id=profile_id,
                    person_id=str(person["id"]),
                    full_name=full_name,
                    old_avatar=old_avatar,
                    source_path=str(source_path),
                    storage_key=normalized_avatar,
                    avatar_url=avatar_preview,
                    is_deleted=is_deleted,
                    message="would rewrite avatar without media prefix",
                )
                return

            execute(
                new_conn,
                f"UPDATE {NEW_TABLES['person_photo']} SET avatar = %s WHERE id = %s",
                (normalized_avatar, photo_id),
            )
            stats.inc("photo", "updated")
            add_photo_report_row(
                stats,
                result="updated",
                reason="normalized_media_prefix",
                photo_id=str(photo_id),
                applicant_pk=str(applicant_pk) if applicant_pk else None,
                profile_id=profile_id,
                person_id=str(person["id"]),
                full_name=full_name,
                old_avatar=old_avatar,
                source_path=str(source_path),
                storage_key=normalized_avatar,
                avatar_url=avatar_preview,
                is_deleted=is_deleted,
                message="avatar rewritten without media prefix",
            )
            return
        stats.inc("photo", "existing")
        add_photo_report_row(
            stats,
            result="existing",
            reason=None,
            photo_id=str(photo_id),
            applicant_pk=str(applicant_pk) if applicant_pk else None,
            profile_id=profile_id,
            person_id=str(person["id"]),
            full_name=full_name,
            old_avatar=old_avatar,
            source_path=str(source_path),
            storage_key=normalized_avatar or object_key,
            avatar_url=avatar_preview,
            is_deleted=is_deleted,
            message="already exists",
        )
        return

    if dry_run:
        stats.inc("photo", "created")
        add_photo_report_row(
            stats,
            result="created",
            reason=None,
            photo_id=str(photo_id),
            applicant_pk=str(applicant_pk) if applicant_pk else None,
            profile_id=profile_id,
            person_id=str(person["id"]),
            full_name=full_name,
            old_avatar=old_avatar,
            source_path=str(source_path),
            storage_key=model_avatar or object_key,
            avatar_url=avatar_url,
            is_deleted=is_deleted,
            message="dry-run",
        )
        return

    uploaded = False
    try:
        upload_file(storage_client, storage, source_path, object_key)
        uploaded = True

        created = fetchone(
            new_conn,
            f"""
            INSERT INTO {NEW_TABLES['person_photo']} (
                id,
                avatar,
                is_deleted,
                person_id,
                created,
                modified
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id, avatar
            """,
            (
                stable_pk(photo_id),
                model_avatar,
                is_deleted,
                person["id"],
                created_at,
                modified_at,
            ),
        )
        stats.inc("photo", "created")
        add_photo_report_row(
            stats,
            result="created",
            reason=None,
            photo_id=str(photo_id),
            applicant_pk=str(applicant_pk) if applicant_pk else None,
            profile_id=profile_id,
            person_id=str(person["id"]),
            full_name=full_name,
            old_avatar=old_avatar,
            source_path=str(source_path),
            storage_key=model_avatar or object_key,
            avatar_url=resolve_avatar_preview(storage, created.get("avatar") if created else model_avatar or object_key) or avatar_url,
            is_deleted=is_deleted,
            message="created",
        )
    except Exception as exc:
        if uploaded:
            try:
                storage_client.delete_object(Bucket=storage.bucket, Key=object_key)
            except Exception:
                pass
        stats.inc("photo", "failed")
        stats.add_error(str(photo_id) if photo_id else None, profile_id, full_name, str(exc))
        add_photo_report_row(
            stats,
            result="failed",
            reason="upload_or_insert_error",
            photo_id=str(photo_id) if photo_id else None,
            applicant_pk=str(applicant_pk) if applicant_pk else None,
            profile_id=profile_id,
            person_id=str(person["id"]),
            full_name=full_name,
            old_avatar=old_avatar,
            source_path=str(source_path),
            storage_key=model_avatar or object_key,
            avatar_url=avatar_url,
            is_deleted=is_deleted,
            message=str(exc),
        )
        print(f"PHOTO ERROR | {profile_id} | {full_name} | {exc}")


HTML_TEMPLATE = Template(
    """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>$title</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5f7fb;
      --panel: #ffffff;
      --text: #102033;
      --muted: #5d6b82;
      --border: #d9e1ef;
      --accent: #2b5fd9;
      --good: #15803d;
      --warn: #b45309;
      --bad: #b91c1c;
      --info: #1d4ed8;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: linear-gradient(180deg, #f8fbff 0%, #eef3fb 100%);
      color: var(--text);
    }
    .page {
      max-width: 1600px;
      margin: 0 auto;
      padding: 24px;
    }
    .hero {
      background: linear-gradient(135deg, #ffffff 0%, #f7faff 100%);
      border: 1px solid var(--border);
      border-radius: 20px;
      box-shadow: 0 10px 30px rgba(16, 32, 51, 0.06);
      padding: 24px;
      margin-bottom: 20px;
    }
    .title-row {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
      flex-wrap: wrap;
    }
    .title {
      font-size: 28px;
      font-weight: 800;
      letter-spacing: -0.03em;
      margin: 0;
    }
    .subtitle {
      color: var(--muted);
      margin-top: 8px;
      line-height: 1.5;
    }
    .pill-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 16px;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 999px;
      padding: 8px 12px;
      font-size: 13px;
      font-weight: 700;
      border: 1px solid var(--border);
      background: #fff;
      color: var(--text);
    }
    .pill.good { color: var(--good); }
    .pill.warn { color: var(--warn); }
    .pill.bad { color: var(--bad); }
    .pill.info { color: var(--info); }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 16px 18px;
      box-shadow: 0 8px 24px rgba(16, 32, 51, 0.04);
    }
    .card-label {
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.1em;
      margin-bottom: 10px;
    }
    .card-value {
      font-size: 30px;
      font-weight: 800;
      letter-spacing: -0.04em;
    }
    .card-foot {
      margin-top: 8px;
      font-size: 13px;
      color: var(--muted);
      line-height: 1.45;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 20px;
      box-shadow: 0 10px 30px rgba(16, 32, 51, 0.05);
      overflow: hidden;
    }
    .panel-header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 16px;
      padding: 18px 20px;
      border-bottom: 1px solid var(--border);
      flex-wrap: wrap;
    }
    .panel-header h2 {
      margin: 0;
      font-size: 20px;
    }
    .panel-header p {
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 14px;
    }
    .table-wrap {
      overflow-x: auto;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 1400px;
    }
    th, td {
      text-align: left;
      padding: 12px 14px;
      border-bottom: 1px solid var(--border);
      vertical-align: top;
      font-size: 13px;
    }
    th {
      position: sticky;
      top: 0;
      background: #f9fbff;
      z-index: 1;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      white-space: nowrap;
    }
    td.mono {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 12px;
      word-break: break-word;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      padding: 5px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      border: 1px solid transparent;
      white-space: nowrap;
    }
    .badge.created { background: rgba(21, 128, 61, 0.1); color: var(--good); border-color: rgba(21, 128, 61, 0.2); }
    .badge.existing { background: rgba(29, 78, 216, 0.1); color: var(--info); border-color: rgba(29, 78, 216, 0.2); }
    .badge.skipped { background: rgba(180, 83, 9, 0.1); color: var(--warn); border-color: rgba(180, 83, 9, 0.2); }
    .badge.failed { background: rgba(185, 28, 28, 0.1); color: var(--bad); border-color: rgba(185, 28, 28, 0.2); }
    .badge.dry-run { background: rgba(100, 116, 139, 0.14); color: #475569; border-color: rgba(100, 116, 139, 0.18); }
    .badge.deleted-yes { background: rgba(185, 28, 28, 0.1); color: var(--bad); border-color: rgba(185, 28, 28, 0.2); }
    .badge.deleted-no { background: rgba(21, 128, 61, 0.1); color: var(--good); border-color: rgba(21, 128, 61, 0.2); }
    .footer {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
      padding: 14px 20px;
      border-top: 1px solid var(--border);
      color: var(--muted);
      font-size: 13px;
    }
    .pager {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .pager button {
      border: 1px solid var(--border);
      background: #fff;
      color: var(--text);
      border-radius: 10px;
      padding: 8px 12px;
      cursor: pointer;
      font-weight: 600;
    }
    .pager button:disabled {
      opacity: 0.45;
      cursor: not-allowed;
    }
    .empty {
      padding: 24px;
      color: var(--muted);
    }
    @media (max-width: 768px) {
      .page { padding: 14px; }
      .hero { padding: 18px; }
      .title { font-size: 22px; }
      .card-value { font-size: 24px; }
    }
  </style>
</head>
<body>
  <div class="page">
    <div class="hero">
      <div class="title-row">
        <div>
          <h1 class="title">Person Photo Migration Report</h1>
          <div class="subtitle" id="meta-subtitle">Loading report data...</div>
          <div class="pill-row" id="meta-pills"></div>
        </div>
      </div>
    </div>

    <div class="summary-grid" id="summary-grid"></div>

    <div class="panel">
      <div class="panel-header">
        <div>
          <h2>Photo Rows</h2>
          <p>Paginated view of migrated, skipped, existing, and failed photo rows.</p>
        </div>
        <div class="pager">
          <button id="prev-btn" type="button">Prev</button>
          <span id="page-label">Page 1 / 1</span>
          <button id="next-btn" type="button">Next</button>
        </div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Count</th>
              <th>Result</th>
              <th>Profile ID</th>
              <th>Full Name</th>
              <th>Photo ID</th>
              <th>Deleted</th>
              <th>Source Avatar</th>
              <th>Local Path</th>
              <th>Storage Key</th>
              <th>Avatar URL</th>
              <th>Message</th>
            </tr>
          </thead>
          <tbody id="rows-body"></tbody>
        </table>
      </div>
      <div class="footer">
        <div id="rows-summary">Loading rows...</div>
        <div id="rows-source"></div>
      </div>
    </div>
  </div>

  <script src="$data_js"></script>
  <script>
    (function () {
      function valueOrDash(value) {
        return value === null || value === undefined || value === '' ? '-' : String(value);
      }

      function badge(text, className) {
        var span = document.createElement('span');
        span.className = 'badge ' + className;
        span.textContent = text;
        return span;
      }

      function setText(id, value) {
        var el = document.getElementById(id);
        if (el) {
          el.textContent = value;
        }
      }

      var report = window.PHOTO_MIGRATION_REPORT;
      if (!report) {
        setText('meta-subtitle', 'Report data is missing.');
        return;
      }

      var summary = report.summary || {};
      var rows = report.rows || [];
      var meta = report.run || {};
      var pageSize = 50;
      var currentPage = 1;

      setText('meta-subtitle',
        'Generated at ' + valueOrDash(report.generated_at) +
        ' | Mode: ' + valueOrDash(meta.mode) +
        ' | Batch size: ' + valueOrDash(meta.batch_size) +
        ' | Data limit: ' + valueOrDash(meta.data_limit));

      var pills = document.getElementById('meta-pills');
      var pillItems = [
        ['Source root: ' + valueOrDash(meta.source_media_root), 'info'],
        ['Bucket: ' + valueOrDash(meta.bucket), 'info'],
        ['Prefix: ' + valueOrDash(meta.prefix), 'info'],
        ['Endpoint: ' + valueOrDash(meta.endpoint_url), 'info'],
      ];
      pillItems.forEach(function (item) {
        var span = document.createElement('span');
        span.className = 'pill ' + item[1];
        span.textContent = item[0];
        pills.appendChild(span);
      });

      var summaryGrid = document.getElementById('summary-grid');
      var cards = [
        ['Processed', summary.processed_rows, 'Rows read from the old DB within the current run'],
        ['Created', summary.created, 'Inserted into the new DB and uploaded to storage'],
        ['Existing', summary.existing, 'Rows already present in the new DB'],
        ['Skipped', summary.skipped, 'Rows skipped for a recoverable reason'],
        ['Failed', summary.failed, 'Rows that failed during upload or insert'],
        ['Missing Person', summary.missing_person, 'Old photo rows with no matching person profile'],
        ['Missing File', summary.missing_source_file, 'Source file not found on disk'],
        ['Deleted Source', summary.deleted_source, 'Old photo rows marked as deleted'],
      ];
      cards.forEach(function (item) {
        var card = document.createElement('div');
        card.className = 'card';
        var label = document.createElement('div');
        label.className = 'card-label';
        label.textContent = item[0];
        var value = document.createElement('div');
        value.className = 'card-value';
        value.textContent = valueOrDash(item[1]);
        var foot = document.createElement('div');
        foot.className = 'card-foot';
        foot.textContent = item[2];
        card.appendChild(label);
        card.appendChild(value);
        card.appendChild(foot);
        summaryGrid.appendChild(card);
      });

      function renderPage() {
        var tbody = document.getElementById('rows-body');
        tbody.innerHTML = '';

        var totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
        if (currentPage > totalPages) {
          currentPage = totalPages;
        }
        if (currentPage < 1) {
          currentPage = 1;
        }

        var start = (currentPage - 1) * pageSize;
        var end = Math.min(start + pageSize, rows.length);
        var pageRows = rows.slice(start, end);

        pageRows.forEach(function (row, index) {
          var tr = document.createElement('tr');

          function addCell(value, className) {
            var td = document.createElement('td');
            if (className) {
              td.className = className;
            }
            if (value && typeof value === 'object' && value.nodeType === 1) {
              td.appendChild(value);
            } else {
              td.textContent = valueOrDash(value);
            }
            tr.appendChild(td);
          }

          addCell(start + index + 1, 'mono');
          addCell(badge(row.result, row.result || 'dry-run'));
          addCell(row.profile_id, 'mono');
          addCell(row.full_name);
          addCell(row.photo_id, 'mono');

          var deletedBadge = row.is_deleted ? badge('Yes', 'deleted-yes') : badge('No', 'deleted-no');
          addCell(deletedBadge);
          addCell(row.old_avatar, 'mono');
          addCell(row.source_path, 'mono');
          addCell(row.storage_key, 'mono');
          addCell(row.avatar_url, 'mono');
          addCell(row.message);

          tbody.appendChild(tr);
        });

        if (!pageRows.length) {
          var trEmpty = document.createElement('tr');
          var tdEmpty = document.createElement('td');
          tdEmpty.colSpan = 11;
          tdEmpty.className = 'empty';
          tdEmpty.textContent = 'No rows to display.';
          trEmpty.appendChild(tdEmpty);
          tbody.appendChild(trEmpty);
        }

        setText('page-label', 'Page ' + currentPage + ' / ' + totalPages);
        setText('rows-summary', 'Showing ' + (rows.length ? (start + 1) + ' - ' + end : '0') + ' of ' + rows.length + ' rows');
        setText('rows-source', 'Report rows are loaded from ' + valueOrDash(report.report_data_file || (report.run && report.run.report_data_file)));

        document.getElementById('prev-btn').disabled = currentPage <= 1;
        document.getElementById('next-btn').disabled = currentPage >= totalPages;
      }

      document.getElementById('prev-btn').addEventListener('click', function () {
        if (currentPage > 1) {
          currentPage -= 1;
          renderPage();
        }
      });

      document.getElementById('next-btn').addEventListener('click', function () {
        if (currentPage < Math.ceil(rows.length / pageSize)) {
          currentPage += 1;
          renderPage();
        }
      });

      renderPage();
    })();
  </script>
</body>
</html>
"""
)


def summarize_rows(stats: MigrationStats) -> Dict[str, Any]:
    rows = stats.report_rows
    result_counts = Counter((row.get("result") or "unknown") for row in rows)
    reason_counts = Counter(row.get("reason") for row in rows if row.get("reason"))
    summary = {
        "processed_rows": stats.total["photo"].get("read", 0),
        "created": result_counts.get("created", 0),
        "existing": result_counts.get("existing", 0),
        "skipped": result_counts.get("skipped", 0),
        "failed": result_counts.get("failed", 0),
        "missing_person": stats.total["photo"].get("missing_person", 0),
        "missing_source_file": stats.total["photo"].get("missing_source_file", 0),
        "empty_avatar": stats.total["photo"].get("empty_avatar", 0),
        "deleted_source": sum(1 for row in rows if row.get("is_deleted")),
    }
    return {
        "summary": summary,
        "result_counts": dict(result_counts),
        "reason_counts": dict(reason_counts),
    }


def write_report_files(
    report_file: str,
    report_payload: Dict[str, Any],
):
    html_path, json_path, js_path = build_sidecar_paths(report_file)
    html_path.parent.mkdir(parents=True, exist_ok=True)

    payload_json = json.dumps(report_payload, ensure_ascii=False, indent=2, default=json_default)
    json_path.write_text(payload_json, encoding="utf-8")
    js_path.write_text(
        "window.PHOTO_MIGRATION_REPORT = " + json.dumps(report_payload, ensure_ascii=False, default=json_default) + ";\n",
        encoding="utf-8",
    )
    html_path.write_text(
        HTML_TEMPLATE.substitute(
            title=html_escape("Person Photo Migration Report"),
            data_js=js_path.name,
        ),
        encoding="utf-8",
    )


def cleanup_tables(new_conn):
    execute(
        new_conn,
        "TRUNCATE TABLE " + ", ".join(ROLLBACK_CLEANUP_TABLES) + " RESTART IDENTITY CASCADE",
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Migrate applicant photos to person photos")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--dry-run", action="store_true", help="Run migration without writing to storage or DB")
    mode_group.add_argument(
        "--roll-back",
        "--rollback",
        dest="rollback",
        action="store_true",
        help="Empty the person photo table and exit",
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help=f"Batch size (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--data-limit", type=int, default=None, help="Limit how many photo rows are processed")
    parser.add_argument("--report-file", default=DEFAULT_REPORT_FILE, help="HTML report file path")
    parser.add_argument("--source-media-root", default=env_first("SOURCE_MEDIA_ROOT", default=str(SCRIPT_DIR)), help="Root folder that contains applicant/photos")

    parser.add_argument("--storage-bucket", default=env_first("STORAGE_BUCKET", "AWS_BUCKET_NAME", "MINIO_BUCKET", default=""), help="S3/MinIO bucket name")
    parser.add_argument("--storage-region", default=env_first("STORAGE_REGION", "AWS_DEFAULT_REGION", default="ap-southeast-1"), help="Storage region")
    parser.add_argument("--storage-endpoint-url", default=env_first("STORAGE_ENDPOINT_URL", "AWS_ENDPOINT_URL", "MINIO_ENDPOINT_URL", default=""), help="Custom S3 endpoint URL for MinIO or private S3")
    parser.add_argument("--public-base-url", default=env_first("STORAGE_PUBLIC_BASE_URL", "PUBLIC_BASE_URL", default=""), help="Public URL base used in the avatar field")
    parser.add_argument(
        "--storage-prefix",
        default=env_first("PHOTO_STORAGE_PREFIX", "STORAGE_PREFIX", default=DEFAULT_STORAGE_PREFIX),
        help="Object key prefix inside the bucket",
    )
    parser.add_argument("--storage-access-key", default=env_first("STORAGE_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID", "MINIO_ACCESS_KEY", default=""), help="Storage access key")
    parser.add_argument("--storage-secret-key", default=env_first("STORAGE_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY", "MINIO_SECRET_KEY", default=""), help="Storage secret key")
    parser.add_argument("--storage-session-token", default=env_first("STORAGE_SESSION_TOKEN", "AWS_SESSION_TOKEN", default=""), help="Storage session token")
    parser.add_argument("--storage-verify-ssl", default=env_first("STORAGE_VERIFY_SSL", default="true"), help="Verify TLS certificate for storage uploads")
    return parser.parse_args()


def build_db_config(prefix: str) -> DBConfig:
    return DBConfig(
        host=os.getenv(f"{prefix}_HOST", "127.0.0.1"),
        port=int(os.getenv(f"{prefix}_PORT", "5432")),
        dbname=os.getenv(f"{prefix}_NAME", ""),
        user=os.getenv(f"{prefix}_USER", ""),
        password=os.getenv(f"{prefix}_PASSWORD", ""),
    )


def require_db_config(cfg: DBConfig, label: str):
    if not cfg.dbname or not cfg.user:
        raise SystemExit(f"{label} database configuration is incomplete. Set {label}_DB_NAME and {label}_DB_USER in .env.")


def main():
    args = parse_args()
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be greater than 0")
    if args.data_limit is not None and args.data_limit <= 0:
        raise SystemExit("--data-limit must be greater than 0 when provided")

    old_db = build_db_config("OLD_DB")
    new_db = build_db_config("NEW_DB")
    require_db_config(old_db, "OLD_DB")
    require_db_config(new_db, "NEW_DB")

    html_path, json_path, js_path = build_sidecar_paths(args.report_file)
    source_media_root = resolve_source_media_root(args.source_media_root)
    stats = MigrationStats()
    storage = StorageConfig(
        bucket=args.storage_bucket.strip(),
        region=args.storage_region.strip() if args.storage_region else "ap-southeast-1",
        endpoint_url=args.storage_endpoint_url.strip() or None,
        public_base_url=args.public_base_url.strip() or None,
        prefix=normalize_storage_prefix(args.storage_prefix),
        access_key_id=args.storage_access_key.strip() or None,
        secret_access_key=args.storage_secret_key.strip() or None,
        session_token=args.storage_session_token.strip() or None,
        verify_ssl=env_bool("STORAGE_VERIFY_SSL", str(args.storage_verify_ssl).strip().lower() not in {"0", "false", "no", "off"}),
    )

    stats.set_run_metadata(
        mode="dry-run" if args.dry_run else ("rollback" if args.rollback else "live"),
        batch_size=args.batch_size,
        data_limit=args.data_limit,
        source_media_root=str(source_media_root),
        storage_bucket=args.storage_bucket or None,
        storage_region=args.storage_region or None,
        endpoint_url=args.storage_endpoint_url or None,
        public_base_url=args.public_base_url or None,
        storage_prefix=args.storage_prefix,
        html_report_file=str(html_path),
        json_report_file=str(json_path),
        report_data_file=str(js_path),
        generated_at=datetime.now().astimezone().isoformat(timespec="seconds"),
    )

    old_conn = connect_db(old_db)
    new_conn = connect_db(new_db)

    try:
        if args.rollback:
            print("=" * 90)
            print("ROLLBACK MODE")
            print("=" * 90)
            if storage.bucket:
                storage_client = build_storage_client(storage)
                deleted_objects = cleanup_storage_objects(storage_client, storage)
                print(f"Removed {deleted_objects} uploaded photo objects from storage.")
            else:
                print("Storage bucket is not set; skipping uploaded photo cleanup.")
            cleanup_tables(new_conn)
            print("Cleared person_personphoto and exited before migration.")
            return

        if not args.dry_run:
            if not args.storage_bucket:
                raise SystemExit("STORAGE_BUCKET (or AWS_BUCKET_NAME / MINIO_BUCKET) is required for a live photo migration.")

        storage_client = None
        if not args.dry_run:
            storage_client = build_storage_client(storage)

        total_available = load_total_photo_count(old_conn)
        stats.set_run_metadata(total_available_rows=total_available)
        print("=" * 90)
        print("PERSON PHOTO MIGRATION")
        print("=" * 90)
        print(f"Source root: {source_media_root}")
        print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
        print(f"Batch size: {args.batch_size}")
        if args.data_limit is not None:
            print(f"Data limit: {args.data_limit}")
        print(f"Available photo rows: {total_available}")
        if args.storage_bucket:
            print(f"Storage bucket: {storage.bucket}")
            print(f"Storage prefix: {storage.prefix}")
            if storage.endpoint_url:
                print(f"Storage endpoint: {storage.endpoint_url}")

        person_cache: Dict[str, Dict[str, Any]] = {}
        processed = 0
        offset = 0
        batch_no = 0

        while True:
            if args.data_limit is not None and processed >= args.data_limit:
                break

            current_limit = args.batch_size
            if args.data_limit is not None:
                remaining = args.data_limit - processed
                current_limit = min(current_limit, remaining)

            batch_rows = load_photo_batch(old_conn, current_limit, offset)
            if not batch_rows:
                break

            batch_no += 1
            stats.reset_batch()
            print(f"\n--- Batch {batch_no} ({len(batch_rows)} rows) ---")
            for row in batch_rows:
                try:
                    migrate_photo_row(
                        old_conn,
                        new_conn,
                        storage_client,
                        storage,
                        source_media_root,
                        stats,
                        dict(row),
                        args.dry_run,
                        person_cache,
                    )
                except Exception as exc:
                    stats.inc("photo", "failed")
                    photo_id = row.get("photo_id")
                    profile_id = row.get("profile_id")
                    full_name = row.get("full_name")
                    stats.add_error(str(photo_id) if photo_id else None, profile_id, full_name, str(exc))
                    add_photo_report_row(
                        stats,
                        result="failed",
                        reason="unexpected_error",
                        photo_id=str(photo_id) if photo_id else None,
                        applicant_pk=str(row.get("applicant_pk")) if row.get("applicant_pk") else None,
                        profile_id=profile_id,
                        person_id=None,
                        full_name=full_name,
                        old_avatar=normalize_avatar_value(row.get("old_avatar")),
                        source_path=None,
                        storage_key=None,
                        avatar_url=None,
                        is_deleted=bool(row.get("is_deleted")),
                        message=str(exc),
                    )
                    print(f"ERROR | {profile_id or '-'} | {full_name or '-'} | {exc}")

                processed += 1
                if args.data_limit is not None and processed >= args.data_limit:
                    break

            print(
                f"Batch {batch_no} summary: "
                f"created={stats.batch['photo'].get('created', 0)} "
                f"existing={stats.batch['photo'].get('existing', 0)} "
                f"skipped={stats.batch['photo'].get('skipped', 0)} "
                f"failed={stats.batch['photo'].get('failed', 0)}"
            )
            offset += len(batch_rows)

        summary_bundle = summarize_rows(stats)
        report_payload = {
            "generated_at": stats.run_metadata["generated_at"],
            "script": "migrate_person_photos_old_to_new.py",
        "run": stats.run_metadata,
        "report_data_file": str(js_path),
        "summary": summary_bundle["summary"],
            "result_counts": summary_bundle["result_counts"],
            "reason_counts": summary_bundle["reason_counts"],
            "rows": stats.report_rows,
            "errors": stats.errors,
        }

        write_report_files(args.report_file, report_payload)

        print("\nFinal summary:")
        for key, value in summary_bundle["summary"].items():
            print(f"  {key}: {value}")
        print(f"\nReport written to: {html_path}")
        print(f"JSON written to: {json_path}")
        print(f"JS written to: {js_path}")

    finally:
        old_conn.close()
        new_conn.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)
