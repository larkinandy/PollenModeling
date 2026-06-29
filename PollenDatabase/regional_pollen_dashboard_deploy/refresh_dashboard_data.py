"""Synchronize the dashboard database from pollen and rebuild Parquet files."""

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import psycopg2
from psycopg2 import sql


ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent

TABLES = [
    ("city", ["city_id"]),
    ("site", ["site_id"]),
    ("sensor", ["sensor_id"]),
    ("site_sensor_join", ["site_id", "sensor_id"]),
    ("category", ["category_id"]),
    ("hourly_flow", ["sensor_id", "moment"]),
    ("hourly_metrics", ["sensor_id", "category_id", "moment"]),
]


def load_env_file(path):
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def connection_kwargs(database):
    return {
        "dbname": database,
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PW"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
    }


def table_columns(cursor, table):
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table,),
    )
    return [row[0] for row in cursor.fetchall()]


def sync_table(source, target, table, primary_key):
    with source.cursor() as source_cursor, target.cursor() as target_cursor:
        source_columns = table_columns(source_cursor, table)
        target_columns = table_columns(target_cursor, table)
        if set(source_columns) != set(target_columns):
            raise RuntimeError(
                "%s schema mismatch:\nsource=%s\ntarget=%s"
                % (table, source_columns, target_columns)
            )

        # Physical column order can differ between otherwise compatible databases.
        source_columns = target_columns
        columns_sql = sql.SQL(", ").join(map(sql.Identifier, source_columns))
        staging_table = "sync_%s" % table
        target_cursor.execute(
            sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP").format(
                sql.Identifier(staging_table), sql.Identifier(table)
            )
        )

        with tempfile.TemporaryFile(mode="w+b") as transfer:
            source_copy = sql.SQL("COPY (SELECT {} FROM {}) TO STDOUT WITH (FORMAT BINARY)").format(
                columns_sql, sql.Identifier(table)
            )
            source_cursor.copy_expert(source_copy.as_string(source), transfer)
            transfer.seek(0)
            target_copy = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT BINARY)").format(
                sql.Identifier(staging_table), columns_sql
            )
            target_cursor.copy_expert(target_copy.as_string(target), transfer)

        update_columns = [column for column in source_columns if column not in primary_key]
        conflict_sql = sql.SQL(", ").join(map(sql.Identifier, primary_key))
        assignments = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(column), sql.Identifier(column))
            for column in update_columns
        )
        target_cursor.execute(
            sql.SQL(
                "INSERT INTO {} ({}) SELECT {} FROM {} "
                "ON CONFLICT ({}) DO UPDATE SET {}"
            ).format(
                sql.Identifier(table),
                columns_sql,
                columns_sql,
                sql.Identifier(staging_table),
                conflict_sql,
                assignments,
            )
        )
        target_cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(staging_table)))
        return target_cursor.fetchone()[0]


def reset_category_sequence(target):
    with target.cursor() as cursor:
        cursor.execute(
            """
            SELECT setval(
                pg_get_serial_sequence('category', 'category_id'),
                COALESCE((SELECT MAX(category_id) FROM category), 1),
                true
            )
            """
        )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Copy pollen records to pollen_dashboard and rebuild deploy Parquet files."
    )
    parser.add_argument("--source-db", default="pollen")
    parser.add_argument("--target-db", default="pollen_dashboard")
    parser.add_argument(
        "--skip-parquet",
        action="store_true",
        help="Only synchronize PostgreSQL; do not rebuild deployment files.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.source_db == args.target_db:
        raise ValueError("Source and target databases must be different.")

    load_env_file(REPO_ROOT / ".env")
    source = psycopg2.connect(**connection_kwargs(args.source_db))
    target = psycopg2.connect(**connection_kwargs(args.target_db))
    try:
        for table, primary_key in TABLES:
            row_count = sync_table(source, target, table, primary_key)
            print("Synchronized %s: %i source rows" % (table, row_count), flush=True)
        reset_category_sequence(target)
        target.commit()
    except Exception:
        target.rollback()
        raise
    finally:
        source.close()
        target.close()

    if not args.skip_parquet:
        env = os.environ.copy()
        env["DASHBOARD_DB_NAME"] = args.target_db
        subprocess.run(
            [sys.executable, str(ROOT / "export_dashboard_data.py")],
            cwd=REPO_ROOT,
            env=env,
            check=True,
        )


if __name__ == "__main__":
    main()
