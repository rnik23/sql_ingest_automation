"""
Lean Fabric notebook logic (can run as a .py notebook cell script) for:
1) Reading SQL files from a workspace folder (Queries/)
2) Extracting source tables using sqlglot
3) Upserting source table metadata to a Bronze Delta table

Expected runtime:
- Microsoft Fabric Notebook attached to a Lakehouse
- sqlglot installed in notebook environment
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Iterable

import sqlglot
from sqlglot import exp

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable


# ---------- Config (keep simple + editable) ----------
SOURCE_SQL_FOLDER = "Files/Queries"         # Fabric Lakehouse path for raw SQL files
BRONZE_DB = "bronze"
BRONZE_TABLE = "ingest_source_table_catalog"


def list_sql_files(base_path: str) -> list[str]:
    """Return full paths for .sql files under a directory."""
    files = []
    for item in mssparkutils.fs.ls(base_path):  # type: ignore[name-defined]
        if item.isFile and item.path.lower().endswith(".sql"):
            files.append(item.path)
    return files


def read_sql_file(path: str) -> str:
    """Read SQL text file from Fabric Files path."""
    return mssparkutils.fs.head(path, 10_000_000)  # type: ignore[name-defined]


def extract_source_tables(sql_text: str) -> set[str]:
    """Parse SQL and return fully qualified source table names."""
    parsed = sqlglot.parse_one(sql_text, read="tsql")

    tables = set()
    for node in parsed.find_all(exp.Table):
        # Ignore CTE aliases: sqlglot Table nodes with db/catalog None can still be base tables.
        # Here we keep all table references and de-dupe.
        table_name = ".".join(part for part in [node.catalog, node.db, node.name] if part)
        if table_name:
            tables.add(table_name.lower())

    return tables


def build_rows(sql_file_path: str, source_tables: Iterable[str]) -> list[dict]:
    ts = datetime.utcnow().isoformat()
    query_name = os.path.basename(sql_file_path)
    return [
        {
            "query_name": query_name,
            "query_path": sql_file_path,
            "source_table": table,
            "first_seen_utc": ts,
            "last_seen_utc": ts,
            "is_active": True,
        }
        for table in source_tables
    ]


def upsert_catalog(spark: SparkSession, rows: list[dict], db: str, table: str) -> None:
    """Upsert rows into Bronze Delta catalog table."""
    full_name = f"{db}.{table}"
    df = spark.createDataFrame(rows)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    if not spark.catalog.tableExists(full_name):
        (
            df.write.format("delta")
            .mode("overwrite")
            .saveAsTable(full_name)
        )
        return

    delta_tbl = DeltaTable.forName(spark, full_name)

    (
        delta_tbl.alias("t")
        .merge(
            df.alias("s"),
            "t.query_path = s.query_path AND t.source_table = s.source_table",
        )
        .whenMatchedUpdate(
            set={
                "last_seen_utc": "s.last_seen_utc",
                "is_active": "true",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def mark_missing_as_inactive(spark: SparkSession, active_pairs_df, db: str, table: str) -> None:
    """Optional hygiene: deactivate rows not seen in current run."""
    full_name = f"{db}.{table}"
    if not spark.catalog.tableExists(full_name):
        return

    current = spark.table(full_name)
    missing = (
        current.alias("c")
        .join(
            active_pairs_df.alias("a"),
            on=["query_path", "source_table"],
            how="left_anti",
        )
        .select("query_path", "source_table")
    )

    if missing.count() == 0:
        return

    delta_tbl = DeltaTable.forName(spark, full_name)
    delta_tbl.alias("t").merge(
        missing.alias("m"),
        "t.query_path = m.query_path AND t.source_table = m.source_table",
    ).whenMatchedUpdate(
        set={
            "is_active": "false",
            "last_seen_utc": F.current_timestamp().cast("string"),
        }
    ).execute()


# ---------- Main ----------
spark = SparkSession.builder.getOrCreate()

sql_files = list_sql_files(SOURCE_SQL_FOLDER)
all_rows = []
for sql_file in sql_files:
    sql_text = read_sql_file(sql_file)
    tables = extract_source_tables(sql_text)
    all_rows.extend(build_rows(sql_file, tables))

if all_rows:
    upsert_catalog(spark, all_rows, BRONZE_DB, BRONZE_TABLE)

    active_pairs_df = spark.createDataFrame(all_rows).select("query_path", "source_table").dropDuplicates()
    mark_missing_as_inactive(spark, active_pairs_df, BRONZE_DB, BRONZE_TABLE)

    display(spark.table(f"{BRONZE_DB}.{BRONZE_TABLE}"))
else:
    print("No SQL files found. Nothing to process.")
