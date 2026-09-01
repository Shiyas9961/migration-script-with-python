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
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv


load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_BATCH_SIZE = 500
DEFAULT_REPORT_FILE = "pass_qr_migration_report.html"
DEFAULT_STORAGE_PREFIX = "media/access_pass/qr_code"
SOURCE_QR_ROOT = "applicant/qr_code"
TARGET_QR_ROOT = "access_pass/qr_code"


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
    uploaded_keys: List[str] = field(default_factory=list)

    def inc(self, section: str, key: str, amount: int = 1):
        self.total[section][key] += amount
        self.batch[section][key] += amount

    def add_error(
        self,
        pass_pk: Optional[str],
        profile_id: Optional[str],
        full_name: Optional[str],
        message: str,
    ):
        self.errors.append(f"{pass_pk or '-'} | {profile_id or '-'} | {full_name or '-'} | {message}")

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


def normalize_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def connect_db(cfg: DBConfig, autocommit: bool = False):
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )
    conn.autocommit = autocommit
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


def normalize_qr_reference(value: Any) -> Optional[str]:
    raw = normalize_str(value)
    if not raw:
        return None
    if raw.startswith("http://") or raw.startswith("https://"):
        path = urlparse(raw).path.lstrip("/")
        if "/media/" in path:
            path = path.split("/media/", 1)[1]
        return path.strip("/") or None
    raw = raw.strip("/")
    if raw.startswith("media/"):
        raw = raw[len("media/") :]
    return raw or None


def strip_media_prefix(value: Optional[str]) -> Optional[str]:
    raw = normalize_qr_reference(value)
    if not raw:
        return None
    if raw.startswith("media/"):
        raw = raw[len("media/") :]
    return raw or None


def ensure_media_prefix(value: Optional[str]) -> Optional[str]:
    raw = strip_media_prefix(value)
    if not raw:
        return None
    return "media/" + raw


def is_raw_target_key(value: Any) -> bool:
    raw = strip_media_prefix(value)
    return bool(raw and raw.startswith(TARGET_QR_ROOT + "/"))


def build_object_key(prefix: str, source_reference: Any, fallback_name: Optional[str] = None) -> str:
    source_ref = normalize_qr_reference(source_reference)
    filename = Path(source_ref or (fallback_name or "")).name
    if not filename:
        filename = fallback_name or "qr_code.jpg"
    prefix = normalize_storage_prefix(prefix)
    return "/".join(part.strip("/") for part in [prefix, filename] if part)


def build_storage_url(storage: StorageConfig, object_key: str) -> str:
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


def build_storage_client(storage: StorageConfig):
    try:
        import boto3
        from botocore.config import Config as BotocoreConfig
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for QR uploads. Install dependencies with `pip install -r requirements.txt`."
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


def storage_object_exists(storage_client, storage: StorageConfig, object_key: str) -> bool:
    try:
        storage_client.head_object(Bucket=storage.bucket, Key=object_key)
        return True
    except Exception as exc:
        response = getattr(exc, "response", None) or {}
        error = response.get("Error", {})
        code = str(error.get("Code", "")).lower()
        if code in {"404", "notfound", "nosuchkey", "no_such_key"}:
            return False
        return False


def cached_storage_object_exists(storage_client, storage: StorageConfig, object_key: str, cache: Dict[str, bool]) -> bool:
    if object_key not in cache:
        cache[object_key] = storage_object_exists(storage_client, storage, object_key)
    return cache[object_key]


def delete_uploaded_objects(storage_client, storage: StorageConfig, keys: List[str]) -> int:
    if not storage_client or not storage.bucket or not keys:
        return 0
    unique_keys = list(dict.fromkeys(key for key in keys if key))
    deleted = 0
    for idx in range(0, len(unique_keys), 1000):
        chunk = unique_keys[idx : idx + 1000]
        storage_client.delete_objects(
            Bucket=storage.bucket,
            Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
        )
        deleted += len(chunk)
    return deleted


def load_total_qr_count(new_conn) -> int:
    row = fetchone(
        new_conn,
        """
        SELECT COUNT(*) AS cnt
        FROM person_personaccesspass
        WHERE qr_code IS NOT NULL AND BTRIM(qr_code) <> ''
        """,
    )
    return int(row["cnt"]) if row else 0


def load_pass_batch(new_conn, limit: int, offset: int):
    return fetchall(
        new_conn,
        """
        SELECT
            ap.id AS pass_pk,
            ap.pass_id,
            ap.qr_code,
            ap.person_id,
            pp.profile_id,
            pp.full_name,
            ap.created,
            ap.modified
        FROM person_personaccesspass ap
        LEFT JOIN person_personprofile pp ON pp.id = ap.person_id
        WHERE ap.qr_code IS NOT NULL AND BTRIM(ap.qr_code) <> ''
        ORDER BY ap.created ASC, ap.id ASC
        LIMIT %s OFFSET %s
        """,
        (limit, offset),
    )


def build_source_file_index(source_media_root: Path) -> Dict[str, Optional[Path]]:
    """Index source files once so missing rows do not rescan the media tree."""
    index: Dict[str, Optional[Path]] = {}
    search_roots = [source_media_root / "applicant", source_media_root]
    seen_roots = set()
    for search_root in search_roots:
        resolved_root = search_root.resolve()
        if resolved_root in seen_roots or not search_root.exists():
            continue
        seen_roots.add(resolved_root)
        for path in search_root.rglob("*"):
            if path.is_file():
                index.setdefault(path.name, path)
    return index


def resolve_source_path(
    source_media_root: Path,
    qr_value: Any,
    basename_cache: Dict[str, Optional[Path]],
) -> Tuple[Optional[Path], Optional[str]]:
    normalized = normalize_qr_reference(qr_value)
    if not normalized:
        return None, None

    # Support both source-root layouts: applicant/override/qr_code and override/qr_code.
    candidates = [source_media_root / normalized]
    if not normalized.startswith("applicant/"):
        candidates.append(source_media_root / "applicant" / normalized)

    for candidate in candidates:
        if candidate.is_file():
            return candidate, normalized

    basename = Path(normalized).name
    if not basename:
        return None, normalized

    if basename in basename_cache:
        return basename_cache[basename], normalized

    return basename_cache.get(basename), normalized


def add_report_row(
    stats: MigrationStats,
    *,
    result: str,
    reason: Optional[str],
    message: str,
    pass_pk: Optional[str],
    pass_id: Optional[str],
    profile_id: Optional[str],
    full_name: Optional[str],
    source_qr: Optional[str],
    source_path: Optional[str],
    stored_key: Optional[str],
    storage_url: Optional[str],
    person_id: Optional[str],
):
    stats.add_report_row(
        {
            "Count": len(stats.report_rows) + 1,
            "Person Access Pass ID": pass_pk,
            "Pass ID": pass_id,
            "Profile ID": profile_id,
            "Full Name": full_name,
            "Person ID": person_id,
            "Source QR": source_qr,
            "Source Path": source_path,
            "Stored QR": stored_key,
            "Storage URL": storage_url,
            "Result": result,
            "Reason": reason,
            "Message": message,
        }
    )


def find_current_pass_row(new_conn, pass_pk: Any):
    return fetchone(
        new_conn,
        """
        SELECT id, qr_code
        FROM person_personaccesspass
        WHERE id = %s
        LIMIT 1
        """,
        (pass_pk,),
    )


def migrate_pass_qr_row(
    new_conn,
    storage_client,
    storage: StorageConfig,
    source_media_root: Path,
    stats: MigrationStats,
    row: Dict[str, Any],
    dry_run: bool,
    basename_cache: Dict[str, Optional[Path]],
    storage_exists_cache: Dict[str, bool],
    savepoint_name: str,
):
    pass_pk = row.get("pass_pk")
    pass_id = normalize_str(row.get("pass_id"))
    profile_id = normalize_str(row.get("profile_id"))
    full_name = normalize_str(row.get("full_name"))
    person_id = str(row.get("person_id")) if row.get("person_id") is not None else None
    raw_qr = normalize_str(row.get("qr_code"))

    execute(new_conn, f"SAVEPOINT {savepoint_name}")
    uploaded = False
    object_key = None

    try:
        if not raw_qr:
            stats.inc("qr", "missing_qr_code")
            stats.inc("qr", "skipped")
            add_report_row(
                stats,
                result="skipped",
                reason="missing_qr_code",
                message="qr_code is empty",
                pass_pk=str(pass_pk) if pass_pk is not None else None,
                pass_id=pass_id,
                profile_id=profile_id,
                full_name=full_name,
                source_qr=raw_qr,
                source_path=None,
                stored_key=None,
                storage_url=None,
                person_id=person_id,
            )
            execute(new_conn, f"RELEASE SAVEPOINT {savepoint_name}")
            return

        existing_model_key = strip_media_prefix(raw_qr)
        if existing_model_key and existing_model_key.startswith(TARGET_QR_ROOT + "/"):
            storage_url = build_storage_url(storage, existing_model_key)
            if raw_qr != existing_model_key:
                if dry_run:
                    stats.inc("qr", "updated")
                    add_report_row(
                        stats,
                        result="updated",
                        reason="normalized_media_prefix",
                        message="would rewrite qr_code without media prefix",
                        pass_pk=str(pass_pk) if pass_pk is not None else None,
                        pass_id=pass_id,
                        profile_id=profile_id,
                        full_name=full_name,
                        source_qr=raw_qr,
                        source_path=None,
                        stored_key=existing_model_key,
                        storage_url=storage_url,
                        person_id=person_id,
                    )
                    execute(new_conn, f"RELEASE SAVEPOINT {savepoint_name}")
                    return

                execute(
                    new_conn,
                    "UPDATE person_personaccesspass SET qr_code = %s WHERE id = %s",
                    (existing_model_key, pass_pk),
                )
                stats.inc("qr", "updated")
                add_report_row(
                    stats,
                    result="updated",
                    reason="normalized_media_prefix",
                    message="qr_code rewritten without media prefix",
                    pass_pk=str(pass_pk) if pass_pk is not None else None,
                    pass_id=pass_id,
                    profile_id=profile_id,
                    full_name=full_name,
                    source_qr=raw_qr,
                    source_path=None,
                    stored_key=existing_model_key,
                    storage_url=storage_url,
                    person_id=person_id,
                )
                execute(new_conn, f"RELEASE SAVEPOINT {savepoint_name}")
                return

            stats.inc("qr", "existing")
            add_report_row(
                stats,
                result="existing",
                reason="already_target_key",
                message="already stored as relative key",
                pass_pk=str(pass_pk) if pass_pk is not None else None,
                pass_id=pass_id,
                profile_id=profile_id,
                full_name=full_name,
                source_qr=raw_qr,
                source_path=None,
                stored_key=existing_model_key,
                storage_url=storage_url,
                person_id=person_id,
            )
            execute(new_conn, f"RELEASE SAVEPOINT {savepoint_name}")
            return

        source_path, normalized_source = resolve_source_path(source_media_root, raw_qr, basename_cache)
        fallback_name = pass_id or (str(pass_pk) if pass_pk is not None else "qr_code.jpg")
        object_key = build_object_key(storage.prefix, normalized_source or raw_qr, fallback_name)
        model_qr_key = strip_media_prefix(object_key)
        storage_url = build_storage_url(storage, object_key)

        # Reuse an object already present in storage on reruns. This also
        # handles rows whose local source file is still available.
        if storage_client and cached_storage_object_exists(storage_client, storage, object_key, storage_exists_cache):
            source_path = None

        if not source_path:
            if storage_client and cached_storage_object_exists(storage_client, storage, object_key, storage_exists_cache):
                if dry_run:
                    stats.inc("qr", "updated")
                    add_report_row(
                        stats,
                        result="updated",
                        reason="storage_object_exists",
                        message="would rewrite qr_code to relative key",
                        pass_pk=str(pass_pk) if pass_pk is not None else None,
                        pass_id=pass_id,
                        profile_id=profile_id,
                        full_name=full_name,
                        source_qr=raw_qr,
                        source_path=None,
                        stored_key=model_qr_key,
                        storage_url=storage_url,
                        person_id=person_id,
                    )
                    execute(new_conn, f"RELEASE SAVEPOINT {savepoint_name}")
                    return

                execute(
                    new_conn,
                    "UPDATE person_personaccesspass SET qr_code = %s WHERE id = %s",
                    (model_qr_key, pass_pk),
                )
                stats.inc("qr", "updated")
                add_report_row(
                    stats,
                    result="updated",
                    reason="storage_object_exists",
                    message="qr_code rewritten to relative key",
                    pass_pk=str(pass_pk) if pass_pk is not None else None,
                    pass_id=pass_id,
                    profile_id=profile_id,
                    full_name=full_name,
                    source_qr=raw_qr,
                    source_path=None,
                    stored_key=model_qr_key,
                    storage_url=storage_url,
                    person_id=person_id,
                )
                execute(new_conn, f"RELEASE SAVEPOINT {savepoint_name}")
                return

            stats.inc("qr", "missing_source_file")
            stats.inc("qr", "skipped")
            add_report_row(
                stats,
                result="skipped",
                reason="missing_source_file",
                message="local qr file not found",
                pass_pk=str(pass_pk) if pass_pk is not None else None,
                pass_id=pass_id,
                profile_id=profile_id,
                full_name=full_name,
                source_qr=raw_qr,
                source_path=None,
                stored_key=model_qr_key,
                storage_url=storage_url,
                person_id=person_id,
            )
            execute(new_conn, f"RELEASE SAVEPOINT {savepoint_name}")
            return

        if dry_run:
            stats.inc("qr", "updated")
            add_report_row(
                stats,
                result="updated",
                reason="dry_run",
                message="dry-run",
                pass_pk=str(pass_pk) if pass_pk is not None else None,
                pass_id=pass_id,
                profile_id=profile_id,
                full_name=full_name,
                source_qr=raw_qr,
                source_path=str(source_path),
                stored_key=model_qr_key,
                storage_url=storage_url,
                person_id=person_id,
            )
            execute(new_conn, f"RELEASE SAVEPOINT {savepoint_name}")
            return

        upload_file(storage_client, storage, source_path, object_key)
        uploaded = True

        execute(
            new_conn,
            "UPDATE person_personaccesspass SET qr_code = %s WHERE id = %s",
            (model_qr_key, pass_pk),
        )
        stats.uploaded_keys.append(object_key)
        stats.inc("qr", "updated")
        add_report_row(
            stats,
            result="updated",
            reason="uploaded",
            message="uploaded and updated qr_code",
            pass_pk=str(pass_pk) if pass_pk is not None else None,
            pass_id=pass_id,
            profile_id=profile_id,
            full_name=full_name,
            source_qr=raw_qr,
            source_path=str(source_path),
            stored_key=model_qr_key,
            storage_url=storage_url,
            person_id=person_id,
        )
        execute(new_conn, f"RELEASE SAVEPOINT {savepoint_name}")
    except Exception as exc:
        try:
            execute(new_conn, f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        except Exception:
            pass
        try:
            execute(new_conn, f"RELEASE SAVEPOINT {savepoint_name}")
        except Exception:
            pass
        if uploaded and object_key and storage_client:
            try:
                storage_client.delete_object(Bucket=storage.bucket, Key=object_key)
            except Exception:
                pass
        stats.inc("qr", "failed")
        stats.add_error(str(pass_pk) if pass_pk is not None else None, profile_id, full_name, str(exc))
        add_report_row(
            stats,
            result="failed",
            reason="unexpected_error",
            message=str(exc),
            pass_pk=str(pass_pk) if pass_pk is not None else None,
            pass_id=pass_id,
            profile_id=profile_id,
            full_name=full_name,
            source_qr=raw_qr,
            source_path=str(source_path) if "source_path" in locals() and source_path else None,
            stored_key=strip_media_prefix(object_key) if object_key else None,
            storage_url=build_storage_url(storage, object_key) if object_key else None,
            person_id=person_id,
        )
        print(f"QR ERROR | {profile_id or '-'} | {pass_id or pass_pk or '-'} | {full_name or '-'} | {exc}")


def summarize_rows(stats: MigrationStats) -> Dict[str, Any]:
    rows = stats.report_rows
    result_counts = Counter((row.get("Result") or "unknown") for row in rows)
    reason_counts = Counter(row.get("Reason") for row in rows if row.get("Reason"))
    summary = {
        "processed_rows": stats.total["qr"].get("read", 0),
        "updated": result_counts.get("updated", 0),
        "existing": result_counts.get("existing", 0),
        "skipped": result_counts.get("skipped", 0),
        "failed": result_counts.get("failed", 0),
        "missing_qr_code": stats.total["qr"].get("missing_qr_code", 0),
        "missing_source_file": stats.total["qr"].get("missing_source_file", 0),
    }
    return {
        "summary": summary,
        "result_counts": dict(result_counts),
        "reason_counts": dict(reason_counts),
    }


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
    .hero h1 {
      margin: 0 0 8px;
      font-size: clamp(26px, 3vw, 40px);
      letter-spacing: -0.03em;
    }
    .hero p {
      margin: 0;
      color: var(--muted);
      line-height: 1.5;
    }
    .meta {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 16px;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      background: #f3f6fc;
      border: 1px solid var(--border);
      color: #20324b;
      font-size: 13px;
      font-weight: 600;
    }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 16px;
      margin-bottom: 22px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 8px 24px rgba(16, 32, 51, 0.05);
    }
    .card .label {
      color: var(--muted);
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .card .value {
      font-size: 34px;
      font-weight: 800;
      margin-top: 10px;
      line-height: 1;
    }
    .card .sub {
      margin-top: 12px;
      color: var(--muted);
      font-size: 13px;
    }
    .table-shell {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 20px;
      overflow: hidden;
      box-shadow: 0 10px 30px rgba(16, 32, 51, 0.06);
    }
    .table-topbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      padding: 18px 20px;
      border-bottom: 1px solid var(--border);
      flex-wrap: wrap;
    }
    .table-topbar h2 {
      margin: 0;
      font-size: 20px;
    }
    .table-topbar .hint {
      color: var(--muted);
      font-size: 13px;
    }
    .table-wrap {
      overflow: auto;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 1450px;
    }
    thead th {
      position: sticky;
      top: 0;
      background: #f8fafc;
      z-index: 1;
      text-align: left;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: #51627a;
      padding: 14px 16px;
      border-bottom: 1px solid var(--border);
      white-space: nowrap;
    }
    tbody td {
      padding: 14px 16px;
      border-bottom: 1px solid #edf1f7;
      vertical-align: top;
      font-size: 14px;
      color: #1d2b3f;
    }
    tbody tr:hover {
      background: #fbfdff;
    }
    .muted {
      color: var(--muted);
    }
    .code {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
      font-size: 12px;
      word-break: break-all;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 5px 10px;
      font-size: 12px;
      font-weight: 700;
      border: 1px solid transparent;
      white-space: nowrap;
    }
    .badge-updated { background: rgba(21, 128, 61, 0.1); color: var(--good); border-color: rgba(21, 128, 61, 0.2); }
    .badge-existing { background: rgba(29, 78, 216, 0.1); color: var(--info); border-color: rgba(29, 78, 216, 0.2); }
    .badge-skipped { background: rgba(180, 83, 9, 0.1); color: var(--warn); border-color: rgba(180, 83, 9, 0.2); }
    .badge-failed { background: rgba(185, 28, 28, 0.1); color: var(--bad); border-color: rgba(185, 28, 28, 0.2); }
    .pagination {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 16px 20px;
      border-top: 1px solid var(--border);
      flex-wrap: wrap;
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
      padding: 8px 12px;
      border-radius: 10px;
      cursor: pointer;
      font-weight: 600;
    }
    .pager button:disabled {
      opacity: 0.45;
      cursor: not-allowed;
    }
    .page-info {
      color: var(--muted);
      font-size: 13px;
    }
    @media (max-width: 1200px) {
      .summary-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 720px) {
      .page { padding: 16px; }
      .summary-grid { grid-template-columns: 1fr; }
      .hero, .card, .table-shell { border-radius: 16px; }
    }
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1>Pass QR Migration Report</h1>
      <p>Uploads QR files from the local <span class="code">applicant/qr_code</span> tree and stores the relative storage key in <span class="code">person_personaccesspass.qr_code</span>.</p>
      <div class="meta">
        <span class="pill">Mode: <span id="mode-pill">-</span></span>
        <span class="pill">Generated: <span id="generated-pill">-</span></span>
        <span class="pill">Storage Prefix: <span id="prefix-pill">-</span></span>
      </div>
    </section>

    <section class="summary-grid" id="summary-grid"></section>

    <section class="table-shell">
      <div class="table-topbar">
        <div>
          <h2>Updated QR Rows</h2>
          <div class="hint">Paginated view built from the JSON report payload.</div>
        </div>
        <div class="hint" id="row-count-hint"></div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Count</th>
              <th>Profile ID</th>
              <th>Full Name</th>
              <th>Pass ID</th>
              <th>Person Access Pass ID</th>
              <th>Source QR</th>
              <th>Source Path</th>
              <th>Stored QR</th>
              <th>Result</th>
              <th>Reason</th>
            </tr>
          </thead>
          <tbody id="rows-body"></tbody>
        </table>
      </div>
      <div class="pagination">
        <div class="page-info" id="page-info">Page 1</div>
        <div class="pager">
          <button type="button" id="prev-btn">Previous</button>
          <button type="button" id="next-btn">Next</button>
        </div>
      </div>
    </section>
  </div>

  <script src="$data_js"></script>
  <script>
    (function () {
      const report = window.PASS_QR_MIGRATION_REPORT || {};
      const rows = report.rows || [];
      const summary = report.summary || {};
      const pageSize = 50;
      let currentPage = 1;

      const resultClassMap = {
        updated: 'badge-updated',
        existing: 'badge-existing',
        skipped: 'badge-skipped',
        failed: 'badge-failed',
      };

      function escapeHtml(value) {
        if (value === null || value === undefined) return '';
        return String(value)
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#39;');
      }

      function badge(result) {
        const cls = resultClassMap[result] || 'badge-existing';
        return `<span class="badge $${cls}">$${escapeHtml(result || '-')}</span>`;
      }

      function renderSummary() {
        document.getElementById('mode-pill').textContent = report.run?.mode || '-';
        document.getElementById('generated-pill').textContent = report.generated_at || '-';
        document.getElementById('prefix-pill').textContent = report.run?.storage_prefix || '-';

        const items = [
          { label: 'Processed', value: summary.processed_rows || 0, sub: 'Rows read from new DB' },
          { label: 'Updated', value: summary.updated || 0, sub: 'Rows rewritten to storage keys' },
          { label: 'Existing', value: summary.existing || 0, sub: 'Already stored as relative keys' },
          { label: 'Skipped', value: summary.skipped || 0, sub: 'Missing QR or missing source file' },
          { label: 'Failed', value: summary.failed || 0, sub: 'Rows that hit an unexpected error' },
        ];

        const html = items.map((item) => `
          <article class="card">
            <div class="label">$${escapeHtml(item.label)}</div>
            <div class="value">$${escapeHtml(item.value)}</div>
            <div class="sub">$${escapeHtml(item.sub)}</div>
          </article>
        `).join('');
        document.getElementById('summary-grid').innerHTML = html;
      }

      function renderTable() {
        const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
        if (currentPage > totalPages) currentPage = totalPages;
        if (currentPage < 1) currentPage = 1;

        const start = (currentPage - 1) * pageSize;
        const slice = rows.slice(start, start + pageSize);
        const tbody = document.getElementById('rows-body');
        tbody.innerHTML = slice.map((row, index) => `
          <tr>
            <td>$${escapeHtml(start + index + 1)}</td>
            <td>$${escapeHtml(row['Profile ID'])}</td>
            <td>$${escapeHtml(row['Full Name'])}</td>
            <td class="code">$${escapeHtml(row['Pass ID'])}</td>
            <td class="code">$${escapeHtml(row['Person Access Pass ID'])}</td>
            <td class="code">$${escapeHtml(row['Source QR'])}</td>
            <td class="code">$${escapeHtml(row['Source Path'])}</td>
            <td class="code">$${escapeHtml(row['Stored QR'])}</td>
            <td>$${badge(row['Result'])}</td>
            <td class="muted">$${escapeHtml(row['Reason'] || '')}</td>
          </tr>
        `).join('');

        document.getElementById('page-info').textContent = `Page $${currentPage} of $${totalPages}`;
        document.getElementById('row-count-hint').textContent = `$${rows.length} rows`;
        document.getElementById('prev-btn').disabled = currentPage <= 1;
        document.getElementById('next-btn').disabled = currentPage >= totalPages;
      }

      document.getElementById('prev-btn').addEventListener('click', function () {
        if (currentPage > 1) {
          currentPage -= 1;
          renderTable();
        }
      });

      document.getElementById('next-btn').addEventListener('click', function () {
        const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
        if (currentPage < totalPages) {
          currentPage += 1;
          renderTable();
        }
      });

      renderSummary();
      renderTable();
    })();
  </script>
</body>
</html>
"""
)


def write_report_files(report_file: str, report_payload: Dict[str, Any]):
    html_path, json_path, js_path = build_sidecar_paths(report_file)
    html_path.parent.mkdir(parents=True, exist_ok=True)

    payload_json = json.dumps(report_payload, ensure_ascii=False, indent=2, default=json_default)
    json_path.write_text(payload_json, encoding="utf-8")
    js_path.write_text(
        "window.PASS_QR_MIGRATION_REPORT = " + json.dumps(report_payload, ensure_ascii=False, default=json_default) + ";\n",
        encoding="utf-8",
    )
    html_path.write_text(
        HTML_TEMPLATE.substitute(
            title=html_escape("Pass QR Migration Report"),
            data_js=js_path.name,
        ),
        encoding="utf-8",
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Upload access pass QR files to storage and rewrite qr_code keys in the new DB")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--dry-run", action="store_true", help="Run without uploading files or updating the DB")
    mode_group.add_argument(
        "--roll-back",
        "--rollback",
        dest="rollback",
        action="store_true",
        help="Run the migration and roll back DB changes and uploaded objects at the end",
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help=f"Batch size (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--data-limit", type=int, default=None, help="Limit how many rows are processed")
    parser.add_argument("--report-file", default=DEFAULT_REPORT_FILE, help="HTML report file path")
    parser.add_argument("--source-media-root", default=env_first("SOURCE_MEDIA_ROOT", default=str(SCRIPT_DIR)), help="Root folder that contains applicant/qr_code")

    parser.add_argument("--storage-bucket", default=env_first("STORAGE_BUCKET", "AWS_BUCKET_NAME", "MINIO_BUCKET", default=""), help="S3/MinIO bucket name")
    parser.add_argument("--storage-region", default=env_first("STORAGE_REGION", "AWS_DEFAULT_REGION", default="ap-southeast-1"), help="Storage region")
    parser.add_argument("--storage-endpoint-url", default=env_first("STORAGE_ENDPOINT_URL", "AWS_ENDPOINT_URL", "MINIO_ENDPOINT_URL", default=""), help="Custom S3 endpoint URL for MinIO or private S3")
    parser.add_argument("--storage-public-base-url", default=env_first("STORAGE_PUBLIC_BASE_URL", "PUBLIC_BASE_URL", default=""), help="Public URL base used in report previews")
    parser.add_argument(
        "--storage-prefix",
        default=env_first("QR_STORAGE_PREFIX", "STORAGE_PREFIX", default=DEFAULT_STORAGE_PREFIX),
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

    new_db = build_db_config("NEW_DB")
    require_db_config(new_db, "NEW_DB")

    html_path, json_path, js_path = build_sidecar_paths(args.report_file)
    source_media_root = resolve_source_media_root(args.source_media_root)
    stats = MigrationStats()
    storage = StorageConfig(
        bucket=args.storage_bucket.strip(),
        region=args.storage_region.strip() if args.storage_region else "ap-southeast-1",
        endpoint_url=args.storage_endpoint_url.strip() or None,
        public_base_url=args.storage_public_base_url.strip() or None,
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
        storage_bucket=storage.bucket or None,
        storage_region=storage.region or None,
        endpoint_url=storage.endpoint_url or None,
        public_base_url=storage.public_base_url or None,
        storage_prefix=storage.prefix,
        html_report_file=str(html_path),
        json_report_file=str(json_path),
        report_data_file=str(js_path),
        generated_at=datetime.now().astimezone().isoformat(timespec="seconds"),
    )

    new_conn = connect_db(new_db, autocommit=False)
    storage_client = None
    if not args.dry_run:
        if not storage.bucket:
            raise SystemExit("STORAGE_BUCKET (or AWS_BUCKET_NAME / MINIO_BUCKET) is required for a live QR migration.")
        storage_client = build_storage_client(storage)

    try:
        total_available = load_total_qr_count(new_conn)
        stats.set_run_metadata(total_available_rows=total_available)

        print("=" * 90)
        print("PASS QR MIGRATION")
        print("=" * 90)
        print(f"Source root: {source_media_root}")
        print(f"Mode: {'DRY RUN' if args.dry_run else ('ROLLBACK' if args.rollback else 'LIVE')}")
        print(f"Batch size: {args.batch_size}")
        if args.data_limit is not None:
            print(f"Data limit: {args.data_limit}")
        print(f"Available QR rows: {total_available}")
        if storage.bucket:
            print(f"Storage bucket: {storage.bucket}")
            print(f"Storage prefix: {storage.prefix}")
            if storage.endpoint_url:
                print(f"Storage endpoint: {storage.endpoint_url}")

        processed = 0
        offset = 0
        batch_no = 0
        basename_cache = build_source_file_index(source_media_root)
        storage_exists_cache: Dict[str, bool] = {}

        while True:
            if args.data_limit is not None and processed >= args.data_limit:
                break

            current_limit = args.batch_size
            if args.data_limit is not None:
                current_limit = min(current_limit, args.data_limit - processed)

            batch_rows = load_pass_batch(new_conn, current_limit, offset)
            if not batch_rows:
                break

            batch_no += 1
            stats.reset_batch()
            print(f"\n--- Batch {batch_no} ({len(batch_rows)} rows) ---")
            for idx, row in enumerate(batch_rows, start=1):
                stats.inc("qr", "read")
                savepoint_name = f"qr_sp_{batch_no}_{idx}"
                migrate_pass_qr_row(
                    new_conn,
                    storage_client,
                    storage,
                    source_media_root,
                    stats,
                    dict(row),
                    args.dry_run,
                    basename_cache,
                    storage_exists_cache,
                    savepoint_name,
                )
                processed += 1
                if args.data_limit is not None and processed >= args.data_limit:
                    break

            print(
                f"Batch {batch_no} summary: "
                f"updated={stats.batch['qr'].get('updated', 0)} "
                f"existing={stats.batch['qr'].get('existing', 0)} "
                f"skipped={stats.batch['qr'].get('skipped', 0)} "
                f"failed={stats.batch['qr'].get('failed', 0)}"
            )
            offset += len(batch_rows)

        summary_bundle = summarize_rows(stats)
        report_payload = {
            "generated_at": stats.run_metadata["generated_at"],
            "script": "migrate_pass_qr_old_to_new.py",
            "run": stats.run_metadata,
            "report_data_file": str(js_path),
            "summary": summary_bundle["summary"],
            "result_counts": summary_bundle["result_counts"],
            "reason_counts": summary_bundle["reason_counts"],
            "rows": stats.report_rows,
            "errors": stats.errors,
        }

        if args.rollback:
            new_conn.rollback()
            deleted_objects = delete_uploaded_objects(storage_client, storage, stats.uploaded_keys) if storage_client else 0
            print(f"Rollback mode removed {deleted_objects} uploaded objects from storage.")
        elif not args.dry_run:
            new_conn.commit()

        write_report_files(args.report_file, report_payload)

        print("\nFinal summary:")
        for key, value in summary_bundle["summary"].items():
            print(f"  {key}: {value}")
        print(f"\nReport written to: {html_path}")
        print(f"JSON written to: {json_path}")
        print(f"JS written to: {js_path}")

    except Exception as exc:
        try:
            new_conn.rollback()
        except Exception:
            pass
        if storage_client and stats.uploaded_keys:
            try:
                deleted_objects = delete_uploaded_objects(storage_client, storage, stats.uploaded_keys)
                print(f"Cleanup removed {deleted_objects} uploaded objects after error.")
            except Exception:
                pass
        raise
    finally:
        new_conn.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)
