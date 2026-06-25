#!/usr/bin/env python3
"""Evaluate Neo4j JDBC SQL2Cypher on the curated Neo4j ETL benchmark graph."""

from __future__ import annotations

import csv
import json
import math
import os
import re
import sqlite3
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from neo4j import GraphDatabase


BASE_DIR = Path("/Users/leamonzea/Desktop/Rel2KG/baselines/neo4j_etl")
CONFIG_PATH = BASE_DIR / "neo4j_etl_config.json"
WORKLOAD_PATH = Path("/Users/leamonzea/Documents/rel2kg/sql_to_cypher_eval/query_details.jsonl")
OUT_DIR = BASE_DIR / "sql2cypher_jdbc_eval"
SUMMARY_JSON = OUT_DIR / "ea_vs_summary.json"
SUMMARY_CSV = OUT_DIR / "ea_vs_summary.csv"
DETAIL_JSONL = OUT_DIR / "query_details.jsonl"
JDBC_JAR = BASE_DIR / "neo4j-jdbc-full-bundle-6.13.1.jar"
JAVA_CLASS_DIR = BASE_DIR
JAVA_BIN = (
    Path.home()
    / "Library/Application Support/neo4j-desktop/Application/Cache/runtime/"
    / "zulu21.48.17-ca-jre21.0.10-macosx_aarch64/zulu-21.jre/Contents/Home/bin/java"
)
SELECTED_DBS = ["store_1", "college_3", "hospital_1", "tracking_software_problems"]


def qsql(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def rel_type(*parts: str) -> str:
    value = "_".join(str(part) for part in parts if part)
    value = re.sub(r"[^0-9A-Za-z_]", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value.upper() or "RELATED_TO"


def cypher_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def qprop(name: str) -> str:
    if re.fullmatch(r"[A-Za-z_][0-9A-Za-z_]*", name):
        return name
    return "`" + name.replace("`", "``") + "`"


def load_config() -> dict[str, Any]:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def sqlite_path(database_root: Path, db_id: str) -> Path:
    direct = database_root / db_id / f"{db_id}.sqlite"
    if direct.exists():
        return direct
    candidates = sorted((database_root / db_id).glob("*.sqlite"))
    if not candidates:
        raise FileNotFoundError(f"No SQLite file found for {db_id}")
    return candidates[0]


def connect_sqlite(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.text_factory = lambda b: b.decode("utf-8", errors="replace")
    conn.row_factory = sqlite3.Row
    return conn


def sqlite_tables(conn: sqlite3.Connection) -> list[str]:
    return [
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
    ]


def table_fks(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    grouped: dict[int, dict[str, Any]] = {}
    for row in conn.execute(f"PRAGMA foreign_key_list({qsql(table)})").fetchall():
        item = grouped.setdefault(
            row["id"],
            {"from_table": table, "to_table": row["table"], "from_cols": [], "to_cols": []},
        )
        item["from_cols"].append(row["from"])
        item["to_cols"].append(row["to"])
    return [grouped[key] for key in sorted(grouped)]


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [row["name"] for row in conn.execute(f"PRAGMA table_info({qsql(table)})").fetchall()]


def load_schema(database_root: Path, db_id: str) -> dict[str, Any]:
    conn = connect_sqlite(sqlite_path(database_root, db_id))
    try:
        tables = sqlite_tables(conn)
        fks = {table: table_fks(conn, table) for table in tables}
        columns = {table: table_columns(conn, table) for table in tables}
    finally:
        conn.close()
    relationship_tables = {table for table in tables if len(fks[table]) == 2}
    return {
        "tables": tables,
        "fks": fks,
        "columns": columns,
        "relationship_tables": relationship_tables,
        "table_lookup": {table.lower(): table for table in tables},
        "column_lookup": {
            table: {column.lower(): column for column in table_cols}
            for table, table_cols in columns.items()
        },
    }


def build_mappings(schema: dict[str, Any], db_id: str) -> tuple[str, str]:
    tables = schema["tables"]
    fks = schema["fks"]
    relationship_tables = schema["relationship_tables"]

    table_mappings = []
    for table in tables:
        target = rel_type(table) if table in relationship_tables else f"{db_id}.{table}"
        table_mappings.append(f"{table}:{target}")

    join_mappings = []
    for table in tables:
        for fk in fks[table]:
            typ = rel_type(table) if table in relationship_tables else rel_type(table, "TO", fk["to_table"])
            for col in fk["from_cols"]:
                join_mappings.append(f"{table}.{col}:{typ}")
    return ";".join(table_mappings), ";".join(join_mappings)


def load_workload() -> list[dict[str, Any]]:
    records = []
    seen = set()
    with WORKLOAD_PATH.open("r", encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if row.get("db_id") not in SELECTED_DBS:
                continue
            key = (row["db_id"], row["sql"], row.get("split_index", -1))
            if key in seen:
                continue
            seen.add(key)
            records.append(
                {
                    "split": row.get("split"),
                    "split_index": row.get("split_index"),
                    "db_id": row["db_id"],
                    "question": row.get("question"),
                    "sql": row["sql"],
                }
            )
    return records


def execute_sql(database_root: Path, db_id: str, sql: str) -> list[tuple[Any, ...]]:
    conn = sqlite3.connect(sqlite_path(database_root, db_id))
    conn.text_factory = lambda b: b.decode("utf-8", errors="replace")
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def translate_sql(jdbc_url: str, user: str, password: str, table_mapping: str, join_mapping: str, sql: str) -> str:
    cp = f"{JAVA_CLASS_DIR}:{JDBC_JAR}"
    proc = subprocess.run(
        [
            str(JAVA_BIN),
            "-cp",
            cp,
            "Neo4jSql2CypherCli",
            jdbc_url,
            user,
            password,
            table_mapping,
            join_mapping,
        ],
        input=sql,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    cypher = proc.stdout.strip()
    if not cypher:
        raise ValueError("empty Cypher")
    return cypher


def normalize_sql_for_jdbc(sql: str) -> str:
    """Convert Spider/SQLite double-quoted values to string literals."""

    def repl(match: re.Match[str]) -> str:
        return cypher_string(match.group(1).replace('""', '"'))

    return re.sub(r'"([^"]*)"', repl, sql)


def parse_alias_tables(cypher: str, schema: dict[str, Any], db_id: str) -> tuple[dict[str, str], dict[str, str]]:
    node_alias: dict[str, str] = {}
    rel_alias: dict[str, str] = {}
    table_lookup = schema["table_lookup"]
    rel_type_to_table = {rel_type(table): table for table in schema["relationship_tables"]}

    for match in re.finditer(r"\(([A-Za-z_][0-9A-Za-z_]*)\s*:\s*`?([^`)]+)`?\)", cypher):
        alias, label = match.groups()
        if label.startswith(f"{db_id}."):
            table = table_lookup.get(label.split(".", 1)[1].lower())
        else:
            table = table_lookup.get(label.lower())
        if table:
            node_alias[alias] = table

    for match in re.finditer(r"\[([A-Za-z_][0-9A-Za-z_]*)\s*:\s*`?([^`\]]+)`?\]", cypher):
        alias, typ = match.groups()
        table = rel_type_to_table.get(typ)
        if table:
            rel_alias[alias] = table
    return node_alias, rel_alias


def patch_table_labels(cypher: str, schema: dict[str, Any], db_id: str) -> str:
    table_lookup = schema["table_lookup"]
    relationship_types = {rel_type(table) for table in schema["relationship_tables"]}

    def repl(match: re.Match[str]) -> str:
        label = match.group(1)
        if label in relationship_types:
            return match.group(0)
        table = table_lookup.get(label.lower())
        if not table:
            return match.group(0)
        return f":`{db_id}.{table}`"

    return re.sub(r":`?([A-Za-z_][0-9A-Za-z_]*)`?", repl, cypher)


def patch_property_case(cypher: str, schema: dict[str, Any], db_id: str) -> str:
    node_alias, rel_alias = parse_alias_tables(cypher, schema, db_id)
    alias_table = {**node_alias, **rel_alias}
    column_lookup = schema["column_lookup"]

    def repl(match: re.Match[str]) -> str:
        alias, prop = match.groups()
        table = alias_table.get(alias)
        if not table:
            return match.group(0)
        actual = column_lookup.get(table, {}).get(prop.lower())
        if not actual:
            return match.group(0)
        return f"{alias}.{qprop(actual)}"

    return re.sub(r"\b([A-Za-z_][0-9A-Za-z_]*)\.([A-Za-z_][0-9A-Za-z_]*)\b", repl, cypher)


def adapt_cypher(cypher: str, schema: dict[str, Any], db_id: str) -> str:
    adapted = patch_table_labels(cypher, schema, db_id)
    adapted = patch_property_case(adapted, schema, db_id)
    return adapted


def norm_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value):
            return None
        return round(value, 10)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def normalize_rows(rows: Any) -> list[tuple[Any, ...]]:
    normalized = []
    for row in rows:
        values = row.values() if isinstance(row, dict) else row
        normalized.append(tuple(norm_value(value) for value in values))
    return sorted(normalized, key=lambda item: repr(item))


def evaluate() -> dict[str, Any]:
    config = load_config()
    database_root = Path(config["database_root"])
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    schemas = {db_id: load_schema(database_root, db_id) for db_id in SELECTED_DBS}
    mappings = {db_id: build_mappings(schemas[db_id], db_id) for db_id in SELECTED_DBS}
    records = load_workload()
    jdbc_url = config["neo4j_uri"].replace("bolt://", "jdbc:neo4j://")
    if "?" not in jdbc_url:
        jdbc_url += f"?database={config.get('target_database', 'neo4j')}"
    driver = GraphDatabase.driver(
        config["neo4j_uri"],
        auth=(config.get("neo4j_user", "neo4j"), config["neo4j_password"]),
    )
    raw_summary = {db_id: Counter() for db_id in SELECTED_DBS}
    adapted_summary = {db_id: Counter() for db_id in SELECTED_DBS}

    with DETAIL_JSONL.open("w", encoding="utf-8") as detail_out:
        for ordinal, record in enumerate(records):
            db_id = record["db_id"]
            raw_stats = raw_summary[db_id]
            adapted_stats = adapted_summary[db_id]
            raw_stats["total"] += 1
            adapted_stats["total"] += 1
            detail = {**record, "ordinal": ordinal}
            try:
                sql_rows = normalize_rows(execute_sql(database_root, db_id, record["sql"]))
                raw_stats["sql_executed"] += 1
                adapted_stats["sql_executed"] += 1
            except Exception as exc:
                raw_stats["sql_error"] += 1
                adapted_stats["sql_error"] += 1
                detail.update({"status": "sql_error", "failure_type": type(exc).__name__, "translated_cypher": None})
                detail_out.write(json.dumps(detail, ensure_ascii=False) + "\n")
                continue
            try:
                table_mapping, join_mapping = mappings[db_id]
                normalized_sql = normalize_sql_for_jdbc(record["sql"])
                cypher = translate_sql(
                    jdbc_url,
                    config.get("neo4j_user", "neo4j"),
                    config["neo4j_password"],
                    table_mapping,
                    join_mapping,
                    normalized_sql,
                )
                adapted_cypher = adapt_cypher(cypher, schemas[db_id], db_id)
                raw_stats["translated"] += 1
                adapted_stats["translated"] += 1
            except Exception as exc:
                raw_stats["translation_error"] += 1
                adapted_stats["translation_error"] += 1
                detail.update(
                    {
                        "status": "translation_error",
                        "failure_type": type(exc).__name__,
                        "failure_reason": str(exc)[:1000],
                        "translated_cypher": None,
                        "normalized_sql": normalized_sql if "normalized_sql" in locals() else None,
                    }
                )
                detail_out.write(json.dumps(detail, ensure_ascii=False) + "\n")
                continue
            raw_rows: list[tuple[Any, ...]] = []
            adapted_rows: list[tuple[Any, ...]] = []
            raw_status = "not_run"
            adapted_status = "not_run"
            raw_failure = None
            adapted_failure = None
            try:
                with driver.session(database=config.get("target_database", "neo4j")) as session:
                    cypher_rows = normalize_rows(session.run(cypher).data())
                raw_stats["cypher_executed"] += 1
                raw_rows = cypher_rows
                raw_match = sql_rows == cypher_rows
                raw_stats["correct" if raw_match else "incorrect"] += 1
                raw_status = "correct" if raw_match else "incorrect"
            except Exception as exc:
                raw_stats["cypher_error"] += 1
                raw_status = "cypher_error"
                raw_failure = f"{type(exc).__name__}: {str(exc)[:1000]}"
            try:
                with driver.session(database=config.get("target_database", "neo4j")) as session:
                    cypher_rows = normalize_rows(session.run(adapted_cypher).data())
                adapted_stats["cypher_executed"] += 1
                adapted_rows = cypher_rows
                adapted_match = sql_rows == cypher_rows
                adapted_stats["correct" if adapted_match else "incorrect"] += 1
                adapted_status = "correct" if adapted_match else "incorrect"
            except Exception as exc:
                adapted_stats["cypher_error"] += 1
                adapted_status = "cypher_error"
                adapted_failure = f"{type(exc).__name__}: {str(exc)[:1000]}"
            detail.update(
                {
                    "status": adapted_status,
                    "raw_status": raw_status,
                    "adapted_status": adapted_status,
                    "normalized_sql": normalized_sql,
                    "translated_cypher": cypher,
                    "adapted_cypher": adapted_cypher,
                    "raw_failure_reason": raw_failure,
                    "adapted_failure_reason": adapted_failure,
                    "sql_result_sample": repr(sql_rows[:5]),
                    "raw_cypher_result_sample": repr(raw_rows[:5]),
                    "adapted_cypher_result_sample": repr(adapted_rows[:5]),
                    "sql_result_size": len(sql_rows),
                    "raw_cypher_result_size": len(raw_rows),
                    "adapted_cypher_result_size": len(adapted_rows),
                }
            )
            detail_out.write(json.dumps(detail, ensure_ascii=False) + "\n")
    driver.close()

    def build_rows(summary: dict[str, Counter]) -> tuple[list[dict[str, Any]], Counter, float, float]:
        rows = []
        totals = Counter()
        for db_id in SELECTED_DBS:
            stats = summary[db_id]
            totals.update(stats)
            valid_n = stats["cypher_executed"]
            failure_n = stats["translation_error"] + stats["cypher_error"]
            ea = stats["correct"] / valid_n if valid_n else 0.0
            vs = ea * (valid_n / (valid_n + failure_n)) if valid_n + failure_n else 0.0
            rows.append(
                {
                    "db_id": db_id,
                    **{key: stats[key] for key in [
                        "total", "sql_executed", "translated", "cypher_executed",
                        "correct", "incorrect", "sql_error", "translation_error", "cypher_error"
                    ]},
                    "valid_n": valid_n,
                    "failure_n": failure_n,
                    "execution_accuracy": ea,
                    "valid_score": vs,
                }
            )
        valid_total = totals["cypher_executed"]
        failure_total = totals["translation_error"] + totals["cypher_error"]
        overall_ea = totals["correct"] / valid_total if valid_total else 0.0
        overall_vs = overall_ea * (valid_total / (valid_total + failure_total)) if valid_total + failure_total else 0.0
        return rows, totals, overall_ea, overall_vs

    def build_overall(totals: Counter, ea: float, vs: float) -> dict[str, Any]:
        valid_total = totals["cypher_executed"]
        failure_total = totals["translation_error"] + totals["cypher_error"]
        return {
            **{key: totals[key] for key in [
                "total", "sql_executed", "translated", "cypher_executed",
                "correct", "incorrect", "sql_error", "translation_error", "cypher_error"
            ]},
            "valid_n": valid_total,
            "failure_n": failure_total,
            "execution_accuracy": ea,
            "valid_score": vs,
        }

    raw_rows, raw_totals, raw_ea, raw_vs = build_rows(raw_summary)
    adapted_rows, adapted_totals, adapted_ea, adapted_vs = build_rows(adapted_summary)
    output = {
        "method": "Neo4j JDBC SQL2Cypher",
        "translator": "org.neo4j:neo4j-jdbc-full-bundle:6.13.1 via Connection.nativeSQL",
        "official_docs": "https://neo4j.com/docs/jdbc-manual/current/sql2cypher/",
        "graph": "curated neo4j-etl-benchmarking graph",
        "adapter_scope": (
            "The adapted run preserves the official translated query structure and only normalizes "
            "Spider/SQLite double-quoted string literals, table/column case variants, and Neo4j ETL "
            "label/property names."
        ),
        "target_database": config.get("target_database", "neo4j"),
        "selected_databases": SELECTED_DBS,
        "raw_official": {
            "overall": build_overall(raw_totals, raw_ea, raw_vs),
            "per_database": raw_rows,
        },
        "official_plus_adapter": {
            "overall": build_overall(adapted_totals, adapted_ea, adapted_vs),
            "per_database": adapted_rows,
        },
    }
    SUMMARY_JSON.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    with SUMMARY_CSV.open("w", newline="", encoding="utf-8") as handle:
        csv_rows = []
        for method, method_rows in [("raw_official", raw_rows), ("official_plus_adapter", adapted_rows)]:
            for row in method_rows:
                csv_rows.append({"method": method, **row})
        writer = csv.DictWriter(handle, fieldnames=list(csv_rows[0].keys()))
        writer.writeheader()
        writer.writerows(csv_rows)
    return output


if __name__ == "__main__":
    result = evaluate()
    print(json.dumps(result["raw_official"]["overall"], indent=2, ensure_ascii=False))
    print(json.dumps(result["official_plus_adapter"]["overall"], indent=2, ensure_ascii=False))
