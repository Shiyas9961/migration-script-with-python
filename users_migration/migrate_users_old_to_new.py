#!/usr/bin/env python3
import os
import sys
import uuid
import argparse
from dataclasses import dataclass
from collections import defaultdict

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv


load_dotenv()


# ============================================================
# CONFIG
# ============================================================
DEFAULT_BATCH_SIZE = 200

OLD_TABLES = {
    "user": "accounts_user",
    "permission": "auth_permission",
    "content_type": "django_content_type",
    "user_permissions": "accounts_user_user_permissions",
}

NEW_TABLES = {
    "user": "accounts_user",
    "role": "accounts_role",
    "permission": "auth_permission",
    "content_type": "django_content_type",
    "user_permissions": "accounts_user_user_permissions",
}

DEFAULT_NEW_ROLE_SLUG = "guard"


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
# ARGUMENTS
# ============================================================
def parse_args():
    parser = argparse.ArgumentParser(description="Migrate users from old DB to new DB")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry-run mode")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help=f"Batch size (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--role-slug", type=str, default=DEFAULT_NEW_ROLE_SLUG, help="New DB role slug to assign to all users")
    return parser.parse_args()


# ============================================================
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


def fetchone(cur, query, params=None):
    cur.execute(query, params or ())
    return cur.fetchone()


def fetchall(cur, query, params=None):
    cur.execute(query, params or ())
    return cur.fetchall()


def normalize_email(value):
    return value.strip().lower() if value else value


# ============================================================
# STATS
# ============================================================
class Stats:
    def __init__(self):
        self.total = defaultdict(lambda: {"created": 0, "updated": 0, "existing": 0, "skipped": 0, "failed": 0})
        self.batch = defaultdict(lambda: {"created": 0, "updated": 0, "existing": 0, "skipped": 0, "failed": 0})
        self.errors = []

    def inc(self, table, key):
        self.total[table][key] += 1
        self.batch[table][key] += 1

    def add_error(self, msg):
        self.errors.append(msg)

    def print_batch_summary(self, batch_no):
        print("\n" + "=" * 80)
        print(f"BATCH {batch_no} SUMMARY")
        print("=" * 80)
        for table, values in self.batch.items():
            print(
                f"{table:<20} "
                f"created={values['created']:<4} "
                f"updated={values['updated']:<4} "
                f"existing={values['existing']:<4} "
                f"skipped={values['skipped']:<4} "
                f"failed={values['failed']:<4}"
            )
        if self.errors:
            print("\nErrors:")
            for err in self.errors[:20]:
                print(f"- {err}")

    def print_final_summary(self):
        print("\n" + "=" * 80)
        print("FINAL SUMMARY")
        print("=" * 80)
        for table, values in self.total.items():
            print(
                f"{table:<20} "
                f"created={values['created']:<4} "
                f"updated={values['updated']:<4} "
                f"existing={values['existing']:<4} "
                f"skipped={values['skipped']:<4} "
                f"failed={values['failed']:<4}"
            )

    def reset_batch(self):
        self.batch = defaultdict(lambda: {"created": 0, "updated": 0, "existing": 0, "skipped": 0, "failed": 0})
        self.errors = []


# ============================================================
# LOOKUPS
# ============================================================
def get_new_role_id(new_cur, slug):
    row = fetchone(
        new_cur,
        f"SELECT id, name, slug FROM {NEW_TABLES['role']} WHERE slug = %s LIMIT 1",
        (slug,),
    )
    if not row:
        raise ValueError(f"Role with slug '{slug}' not found in new DB")
    return row["id"]


def find_new_user_by_id(new_cur, user_id):
    return fetchone(
        new_cur,
        f"""
        SELECT id, username, email, role_id
        FROM {NEW_TABLES['user']}
        WHERE id = %s
        LIMIT 1
        """,
        (user_id,),
    )


def find_new_user_by_username(new_cur, username):
    if not username:
        return None
    return fetchone(
        new_cur,
        f"""
        SELECT id, username, email, role_id
        FROM {NEW_TABLES['user']}
        WHERE username = %s
        LIMIT 1
        """,
        (username,),
    )


def find_new_user_by_email(new_cur, email):
    if not email:
        return None
    return fetchone(
        new_cur,
        f"""
        SELECT id, username, email, role_id
        FROM {NEW_TABLES['user']}
        WHERE LOWER(email) = LOWER(%s)
        LIMIT 1
        """,
        (email,),
    )


def load_new_permission_map(new_cur):
    rows = fetchall(
        new_cur,
        f"""
        SELECT p.id, p.codename, ct.app_label, ct.model
        FROM {NEW_TABLES['permission']} p
        JOIN {NEW_TABLES['content_type']} ct ON ct.id = p.content_type_id
        """,
    )
    mapping = {}
    for row in rows:
        key = (row["app_label"], row["model"], row["codename"])
        mapping[key] = row["id"]
    return mapping


def load_existing_user_permission_pairs(new_cur):
    rows = fetchall(
        new_cur,
        f"""
        SELECT user_id, permission_id
        FROM {NEW_TABLES['user_permissions']}
        """,
    )
    return {(str(row["user_id"]), row["permission_id"]) for row in rows}


def load_old_user_permissions(old_cur):
    return fetchall(
        old_cur,
        f"""
        SELECT
            up.user_id,
            up.permission_id AS old_permission_id,
            p.codename,
            p.name,
            ct.app_label,
            ct.model,
            u.username
        FROM {OLD_TABLES['user_permissions']} up
        JOIN {OLD_TABLES['permission']} p ON p.id = up.permission_id
        JOIN {OLD_TABLES['content_type']} ct ON ct.id = p.content_type_id
        LEFT JOIN {OLD_TABLES['user']} u ON u.id = up.user_id
        ORDER BY up.user_id, ct.app_label, ct.model, p.codename
        """,
    )


# ============================================================
# OLD USER LOADING
# ============================================================
def load_total_old_users(old_cur):
    row = fetchone(old_cur, f"SELECT COUNT(*) AS cnt FROM {OLD_TABLES['user']}")
    return row["cnt"]


def load_old_users_batch(old_cur, limit, offset):
    return fetchall(
        old_cur,
        f"""
        SELECT
            id,
            role_id,
            contracts_id,
            date_joined,
            email,
            first_name,
            full_name,
            is_active,
            is_staff,
            is_superuser,
            last_login,
            last_name,
            password,
            phone_number,
            username
        FROM {OLD_TABLES['user']}
        ORDER BY date_joined ASC, username ASC
        LIMIT %s OFFSET %s
        """,
        (limit, offset),
    )


# ============================================================
# MIGRATION
# ============================================================
def upsert_user(new_cur, stats, old_user, new_role_id, dry_run):
    user_id = old_user["id"]
    username = old_user.get("username")
    email = normalize_email(old_user.get("email"))
    full_name = old_user.get("full_name") or ""
    first_name = old_user.get("first_name") or ""
    last_name = old_user.get("last_name") or ""
    phone_number = old_user.get("phone_number")
    password = old_user.get("password")
    date_joined = old_user.get("date_joined")
    last_login = old_user.get("last_login")
    is_active = old_user.get("is_active", True)
    is_staff = old_user.get("is_staff", False)
    is_superuser = old_user.get("is_superuser", False)
    contracts_id = old_user.get("contracts_id")

    if not username:
        existing = find_new_user_by_id(new_cur, user_id)
        if existing:
            print(f"EXISTING | {'-':<20} | {email or '-'}")
            stats.inc("accounts_user", "existing")
            return "existing"
        stats.inc("accounts_user", "skipped")
        return "skipped"

    existing = find_new_user_by_id(new_cur, user_id)
    if not existing:
        existing = find_new_user_by_username(new_cur, username)
    if not existing and email:
        existing = find_new_user_by_email(new_cur, email)

    # same user already there
    if existing and str(existing["id"]) == str(user_id):
        if dry_run:
            print(f"EXISTING | {username:<20} | {email or '-'}")
            stats.inc("accounts_user", "existing")
            return "existing"

        new_cur.execute(
            f"""
            UPDATE {NEW_TABLES['user']}
            SET
                role_id = %s,
                contracts_id = %s,
                date_joined = %s,
                email = %s,
                first_name = %s,
                full_name = %s,
                is_active = %s,
                is_staff = %s,
                is_superuser = %s,
                last_login = %s,
                last_name = %s,
                password = %s,
                phone_number = %s,
                username = %s,
                avatar = %s,
                updated_by_id = NULL,
                user_tz = %s
            WHERE id = %s
            """,
            (
                new_role_id,
                contracts_id,
                date_joined,
                email,
                first_name,
                full_name,
                is_active,
                is_staff,
                is_superuser,
                last_login,
                last_name,
                password,
                phone_number,
                username,
                "",
                "UTC",
                user_id,
            ),
        )
        print(f"UPDATED  | {username:<20} | {email or '-'}")
        stats.inc("accounts_user", "updated")
        return "updated"

    # username/email already used by different id
    if existing and str(existing["id"]) != str(user_id):
        stats.inc("accounts_user", "failed")
        stats.add_error(
            f"username/email conflict | old_id={user_id} | username={username} | email={email} | existing_id={existing['id']}"
        )
        return "failed"

    if dry_run:
        print(f"CREATE   | {username:<20} | {email or '-'}")
        stats.inc("accounts_user", "created")
        return "created"

    new_cur.execute(
        f"""
        INSERT INTO {NEW_TABLES['user']} (
            id,
            role_id,
            avatar,
            contracts_id,
            date_joined,
            email,
            first_name,
            full_name,
            is_active,
            is_staff,
            is_superuser,
            last_login,
            last_name,
            password,
            phone_number,
            username,
            created_by_id,
            updated_by_id,
            user_tz
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            user_id,
            new_role_id,
            "",                  # avatar
            contracts_id,
            date_joined,
            email,
            first_name,
            full_name,
            is_active,
            is_staff,
            is_superuser,
            last_login,
            last_name,
            password,
            phone_number,
            username,
            None,
            None,
            "UTC",               # user_tz
        ),
    )
    print(f"CREATE   | {username:<20} | {email or '-'}")
    stats.inc("accounts_user", "created")
    return "created"


def migrate_user_permissions(old_cur, new_cur, stats, eligible_user_ids, dry_run):
    old_rows = load_old_user_permissions(old_cur)
    print("\n" + "=" * 80)
    print("MIGRATING USER PERMISSIONS")
    print("=" * 80)

    if not old_rows:
        print("No user permissions found in old DB.")
        return

    permission_map = load_new_permission_map(new_cur)
    existing_pairs = load_existing_user_permission_pairs(new_cur)
    eligible_ids = {str(user_id) for user_id in eligible_user_ids}

    processed = 0
    created = 0
    existing = 0
    skipped = 0
    failed = 0

    for row in old_rows:
        processed += 1
        user_id = row["user_id"]
        user_key = str(user_id)
        username = row.get("username")
        app_label = row.get("app_label")
        model = row.get("model")
        codename = row.get("codename")

        if user_key not in eligible_ids:
            skipped += 1
            stats.inc("user_permissions", "skipped")
            continue

        permission_id = permission_map.get((app_label, model, codename))
        if permission_id is None:
            skipped += 1
            stats.inc("user_permissions", "skipped")
            continue

        pair = (user_key, permission_id)
        if pair in existing_pairs:
            existing += 1
            stats.inc("user_permissions", "existing")
            continue

        if dry_run:
            created += 1
            stats.inc("user_permissions", "created")
            existing_pairs.add(pair)
            continue

        try:
            new_cur.execute(
                f"""
                INSERT INTO {NEW_TABLES['user_permissions']} (user_id, permission_id)
                VALUES (%s, %s)
                ON CONFLICT (user_id, permission_id) DO NOTHING
                RETURNING id
                """,
                (user_id, permission_id),
            )
            inserted = new_cur.fetchone()
            if inserted:
                created += 1
                stats.inc("user_permissions", "created")
                existing_pairs.add(pair)
            else:
                existing += 1
                stats.inc("user_permissions", "existing")
                existing_pairs.add(pair)
        except Exception as exc:
            failed += 1
            stats.inc("user_permissions", "failed")
            stats.add_error(f"{username or user_key} | {app_label}.{model}.{codename} | {exc}")

    print(
        f"User permissions summary: processed={processed} created={created} existing={existing} skipped={skipped} failed={failed}"
    )


# ============================================================
# MAIN
# ============================================================
def main():
    args = parse_args()
    dry_run = args.dry_run
    batch_size = args.batch_size
    role_slug = args.role_slug

    print("\n" + "=" * 80)
    print("DRY RUN MODE" if dry_run else "LIVE MIGRATION MODE")
    print(f"BATCH SIZE: {batch_size}")
    print(f"TARGET ROLE: {role_slug}")
    print("=" * 80)

    stats = Stats()
    old_conn = None
    new_conn = None

    try:
        old_conn = connect_db(OLD_DB)
        new_conn = connect_db(NEW_DB)

        old_conn.autocommit = False
        new_conn.autocommit = False

        old_cur = old_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        new_cur = new_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        new_role_id = get_new_role_id(new_cur, role_slug)
        total_users = load_total_old_users(old_cur)

        print(f"\nTotal old users found: {total_users}")

        offset = 0
        batch_no = 1
        eligible_user_ids = set()

        while True:
            users = load_old_users_batch(old_cur, batch_size, offset)
            if not users:
                break

            print("\n" + "=" * 80)
            print(f"PROCESSING BATCH {batch_no} | offset={offset} | size={len(users)}")
            print("=" * 80)

            batch_failed = False
            batch_user_statuses = {}

            for old_user in users:
                username = old_user.get("username")
                savepoint_name = f"sp_{uuid.uuid4().hex}"

                try:
                    new_cur.execute(f"SAVEPOINT {savepoint_name}")
                    status = upsert_user(new_cur, stats, old_user, new_role_id, dry_run)
                    new_cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    if status:
                        batch_user_statuses[old_user["id"]] = status
                except Exception as e:
                    batch_failed = True
                    new_cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                    new_cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    stats.inc("accounts_user", "failed")
                    stats.add_error(f"{username or '-'} | {e}")

            stats.print_batch_summary(batch_no)
            if dry_run:
                eligible_user_ids.update(
                    user_id
                    for user_id, status in batch_user_statuses.items()
                    if status in {"existing", "updated", "created"}
                )
            elif batch_failed:
                eligible_user_ids.update(
                    user_id
                    for user_id, status in batch_user_statuses.items()
                    if status in {"existing", "updated"}
                )
            else:
                eligible_user_ids.update(
                    user_id
                    for user_id, status in batch_user_statuses.items()
                    if status in {"existing", "updated", "created"}
                )

            if dry_run:
                new_conn.rollback()
                print(f"\nBATCH {batch_no} rolled back because --dry-run is enabled.")
            else:
                if batch_failed:
                    new_conn.rollback()
                    print(f"\nBATCH {batch_no} rolled back because one or more rows failed.")
                else:
                    new_conn.commit()
                    print(f"\nBATCH {batch_no} committed successfully.")

            stats.reset_batch()
            offset += batch_size
            batch_no += 1

        migrate_user_permissions(old_cur, new_cur, stats, eligible_user_ids, dry_run)

        if dry_run:
            new_conn.rollback()
        else:
            new_conn.commit()

        stats.print_final_summary()

    except Exception as e:
        if new_conn:
            new_conn.rollback()
        print(f"\nFATAL ERROR: {e}")
        sys.exit(1)

    finally:
        if old_conn:
            old_conn.close()
        if new_conn:
            new_conn.close()


if __name__ == "__main__":
    main()