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
    "role": "accounts_role",
}

NEW_TABLES = {
    "user": "accounts_user",
    "role": "accounts_role",
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


def load_new_roles(new_cur):
    rows = fetchall(
        new_cur,
        f"SELECT id, slug FROM {NEW_TABLES['role']} WHERE slug IS NOT NULL",
    )
    return {str(row["slug"]).strip().lower(): row["id"] for row in rows}


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
            old_user.id,
            old_user.role_id,
            old_role.slug AS old_role_slug,
            old_user.contracts_id,
            old_user.date_joined,
            old_user.email,
            old_user.first_name,
            old_user.full_name,
            old_user.is_active,
            old_user.is_staff,
            old_user.is_superuser,
            old_user.last_login,
            old_user.last_name,
            old_user.password,
            old_user.phone_number,
            old_user.username
        FROM {OLD_TABLES['user']} old_user
        LEFT JOIN {OLD_TABLES['role']} old_role ON old_role.id = old_user.role_id
        ORDER BY old_user.date_joined ASC, old_user.username ASC
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

    # Same user already exists: reconcile only the role. Do not overwrite
    # fields that may have been changed in the new system.
    if existing and str(existing["id"]) == str(user_id):
        role_needs_update = str(existing.get("role_id")) != str(new_role_id)
        if not role_needs_update:
            print(f"EXISTING | {username:<20} | {email or chr(45)}")
            stats.inc("accounts_user", "existing")
            return "existing"

        if dry_run:
            print(f"ROLE UPDATE | {username:<16} | {email or chr(45)} | role_id={new_role_id}")
            stats.inc("accounts_user", "updated")
            return "updated"

        new_cur.execute(
            f"UPDATE {NEW_TABLES['user']} SET role_id = %s WHERE id = %s",
            (new_role_id, user_id),
        )
        print(f"ROLE UPDATED | {username:<14} | {email or chr(45)}")
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

        fallback_role_id = get_new_role_id(new_cur, role_slug)
        role_ids_by_slug = load_new_roles(new_cur)
        total_users = load_total_old_users(old_cur)

        print(f"\nTotal old users found: {total_users}")

        offset = 0
        batch_no = 1

        while True:
            users = load_old_users_batch(old_cur, batch_size, offset)
            if not users:
                break

            print("\n" + "=" * 80)
            print(f"PROCESSING BATCH {batch_no} | offset={offset} | size={len(users)}")
            print("=" * 80)

            batch_failed = False

            for old_user in users:
                username = old_user.get("username")
                savepoint_name = f"sp_{uuid.uuid4().hex}"

                try:
                    new_cur.execute(f"SAVEPOINT {savepoint_name}")
                    old_role_slug = old_user.get("old_role_slug")
                    normalized_old_role_slug = str(old_role_slug).strip().lower() if old_role_slug else ""
                    resolved_role_id = role_ids_by_slug.get(normalized_old_role_slug, fallback_role_id)
                    upsert_user(new_cur, stats, old_user, resolved_role_id, dry_run)
                    new_cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                except Exception as e:
                    batch_failed = True
                    new_cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                    new_cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    stats.inc("accounts_user", "failed")
                    stats.add_error(f"{username or '-'} | {e}")

            stats.print_batch_summary(batch_no)

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