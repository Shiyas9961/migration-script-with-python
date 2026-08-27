# Migration Runbook

This repository migrates data from the old TWRP database into the new Enterprise database.
Follow this workflow in order. Do not skip the validation steps.

## 1. Clone the repository and prepare the Python environment

```bash
git clone <repo-url>
cd Twrp_to_Enterprise
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

If you use a local `.env` file, copy the example and edit it for your environment:

```bash
cp .env.example .env
```

Make sure the database and storage settings point to the correct target environment before running anything.

## 2. Apply Django migrations first

Before restoring data, create the schema in the new database:

```bash
python manage.py migrate
```

This must be the first database operation on the fresh target.

## 3. Restore the required dump files into the new database

After the schema is ready, restore the seed dumps in this order:

```bash
pg_restore -h localhost -U shiyas -d twrp_prod --no-owner --no-privileges --clean --if-exists system_001.dump
pg_restore -h localhost -U shiyas -d twrp_prod --no-owner --no-privileges --clean --if-exists roles_002.dump
pg_restore -h localhost -U shiyas -d twrp_prod --no-owner --no-privileges --clean --if-exists types_003.dump
pg_restore -h localhost -U shiyas -d twrp_prod --no-owner --no-privileges --clean --if-exists dbmail_data_004.dump
pg_restore -h localhost -U shiyas -d twrp_prod --no-owner --no-privileges --clean --if-exists dashboard_dump_005.dump
```

If the dumps are on another machine, copy them to the host where `pg_restore` will run before starting.

## 4. Clear default data that should not remain in the target DB

Run these only on a fresh target database.

```sql
TRUNCATE TABLE person_personcategory CASCADE;
TRUNCATE TABLE person_personaccesspasstype CASCADE;

TRUNCATE TABLE accounts_user RESTART IDENTITY CASCADE;
TRUNCATE TABLE organisation_trade RESTART IDENTITY CASCADE;
TRUNCATE TABLE organisation_tradegroup RESTART IDENTITY CASCADE;
DELETE FROM person_personphoto;
```

## 5. Verify the target tables are empty

After cleanup, verify the rows are gone before moving ahead.

```sql
SELECT count(*) FROM person_personcategory;
SELECT count(*) FROM person_personaccesspasstype;
SELECT count(*) FROM accounts_user;
SELECT count(*) FROM organisation_trade;
SELECT count(*) FROM organisation_tradegroup;
SELECT count(*) FROM person_personphoto;
```

Every query above should return `0`.

## 6. Safety checks before running migration scripts

Confirm these items before starting the scripts:

- `OLD_DB_*` points to the old database.
- `NEW_DB_*` points to the new database.
- Storage settings are correct for MinIO or AWS S3.
- The script host can reach the database hosts.
- A fresh backup exists for the target database.

If you run the scripts inside Docker, use the container name or reachable service host instead of `127.0.0.1` when required.

## 7. Migration workflow

Run the migration scripts in this order:

1. Users
2. Permissions
3. Persons
4. Person photo
5. Pass QR
6. Admin log
7. Events

### 7.1 Users

Start with a dry run, then run the live migration.

```bash
python users_migration/migrate_users_old_to_new.py --dry-run
python users_migration/migrate_users_old_to_new.py
```

### 7.2 Permissions

The permissions script writes a rollback journal for safe undo.

```bash
python permissions_migration/migrate_user_permissions_old_to_new.py --dry-run
python permissions_migration/migrate_user_permissions_old_to_new.py
```

Rollback for the last live permission run:

```bash
python permissions_migration/migrate_user_permissions_old_to_new.py --roll-back
```

### 7.3 Persons

```bash
python person_migration/migrate_persons_old_to_new.py --dry-run
python person_migration/migrate_persons_old_to_new.py
```

### 7.4 Person photo

```bash
python person_photo_migration/migrate_person_photos_old_to_new.py --dry-run
python person_photo_migration/migrate_person_photos_old_to_new.py
```

### 7.5 Pass QR

```bash
python pass_qr_migration/migrate_pass_qr_old_to_new.py --dry-run
python pass_qr_migration/migrate_pass_qr_old_to_new.py
```

### 7.6 Admin log

```bash
python admin_log_migration/migrate_admin_logs_old_to_new.py --dry-run
python admin_log_migration/migrate_admin_logs_old_to_new.py
```

### 7.7 Events

```bash
python events_migration/migrate_events_old_to_new.py --dry-run
python events_migration/migrate_events_old_to_new.py
```

## 8. Recommended safe execution pattern

For large tables, use a small limit first and validate the output before a full run.

```bash
python <script>.py --dry-run --data-limit 500
python <script>.py --data-limit 500
```

Once the sample looks correct, remove `--data-limit` and run the full migration.

## 9. Validation after each step

After every migration script:

- Check the generated report JSON or HTML.
- Confirm the applied count matches expectations.
- Confirm skipped and failed rows are understood.
- Stop immediately if the report shows unexpected failures.

## 10. Rollback guidance

- Use the script-provided rollback feature when available.
- Keep the generated rollback journal files.
- If a script does not support rollback, restore the database backup before retrying.

## 11. Suggested final order

A clean end-to-end run should look like this:

1. `python manage.py migrate`
2. Restore the dump files
3. Clear the default seed data
4. Run users migration
5. Run permissions migration
6. Run persons migration
7. Run person photo migration
8. Run pass QR migration
9. Run admin log migration
10. Run events migration
11. Review the final reports

