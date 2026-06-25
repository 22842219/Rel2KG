#!/usr/bin/env python3
"""Run a true Neo4j-ETL-style baseline from raw Spider SQLite files.

This importer does not read Rel2KG Cypher. It reads SQLite tables and FK
metadata, then applies the Neo4j ETL-style mapping:

* ordinary table rows -> nodes
* ordinary foreign keys -> relationships
* exactly-two-FK linking tables -> relationships with row properties
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from neo4j import GraphDatabase


DEFAULT_CONFIG = Path(__file__).with_name("neo4j_etl_config.json")


def qsql(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def qcy(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def rel_type(*parts: str) -> str:
    value = "_".join(str(part) for part in parts if part)
    value = re.sub(r"[^0-9A-Za-z_]", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value.upper() or "RELATED_TO"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


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


def sqlite_path(database_root: Path, db_id: str) -> Path:
    direct = database_root / db_id / f"{db_id}.sqlite"
    if direct.exists():
        return direct
    candidates = sorted((database_root / db_id).glob("*.sqlite"))
    if not candidates:
        raise FileNotFoundError(f"No SQLite file found for {db_id} under {database_root}")
    return candidates[0]


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


def table_rows(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    rows = [dict(row) for row in conn.execute(f"SELECT rowid AS __rowid, * FROM {qsql(table)}").fetchall()]
    return [row for row in rows if any(v is not None and v != "" for k, v in row.items() if k != "__rowid")]


def value_key(row: dict[str, Any], cols: list[str]) -> tuple[Any, ...] | None:
    values = []
    for col in cols:
        value = row.get(col)
        if value is None or value == "":
            return None
        values.append(value)
    return tuple(values)


def clear_database(driver, database: str) -> None:
    with driver.session(database=database) as session:
        while True:
            deleted = session.run(
                "MATCH (n) WITH n LIMIT 10000 DETACH DELETE n RETURN count(n) AS c"
            ).single()["c"]
            if deleted == 0:
                break
            print(f"Deleted {deleted} nodes from target database")


def create_indexes(driver, database: str) -> None:
    with driver.session(database=database) as session:
        session.run(
            "CREATE INDEX neo4j_etl_lookup_idx IF NOT EXISTS "
            "FOR (n:Neo4jETLNode) ON (n._etl_db, n._etl_table, n._etl_row_id)"
        ).consume()
        session.run("CALL db.awaitIndexes()").consume()


def build_model(database_root: Path, db_id: str) -> dict[str, Any]:
    conn = connect_sqlite(sqlite_path(database_root, db_id))
    tables = sqlite_tables(conn)
    fk_by_table: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for table in tables:
        fk_by_table[table].extend(table_fks(conn, table))
    rel_tables = {table for table in tables if len(fk_by_table[table]) == 2}

    table_data: dict[str, list[dict[str, Any]]] = {}
    lookup: dict[tuple[str, tuple[str, ...]], dict[tuple[Any, ...], int]] = defaultdict(dict)
    for table in tables:
        rows = table_rows(conn, table)
        for idx, row in enumerate(rows):
            row["_etl_row_id"] = idx
        table_data[table] = rows
        columns = sorted({col for row in rows for col in row if col not in {"__rowid", "_etl_row_id"}})
        for row in rows:
            for col in columns:
                key = value_key(row, [col])
                if key is not None and key not in lookup[(table, (col,))]:
                    lookup[(table, (col,))][key] = row["_etl_row_id"]
            for fk in fk_by_table.values():
                for item in fk:
                    if item["to_table"] == table:
                        key = value_key(row, item["to_cols"])
                        if key is not None and key not in lookup[(table, tuple(item["to_cols"]))]:
                            lookup[(table, tuple(item["to_cols"]))][key] = row["_etl_row_id"]
    conn.close()

    node_tables = [table for table in tables if table not in rel_tables]
    node_count = sum(len(table_data[table]) for table in node_tables)
    relationships: list[dict[str, Any]] = []
    skipped_fk_rows = 0
    skipped_link_rows = 0

    for table in node_tables:
        for row in table_data[table]:
            for fk in fk_by_table.get(table, []):
                key = value_key(row, fk["from_cols"])
                if key is None:
                    skipped_fk_rows += 1
                    continue
                target_id = lookup.get((fk["to_table"], tuple(fk["to_cols"])), {}).get(key)
                if target_id is None:
                    skipped_fk_rows += 1
                    continue
                relationships.append(
                    {
                        "kind": "fk",
                        "type": rel_type(table, "TO", fk["to_table"]),
                        "from_table": table,
                        "from_id": row["_etl_row_id"],
                        "to_table": fk["to_table"],
                        "to_id": target_id,
                        "props": {},
                    }
                )

    for table in sorted(rel_tables):
        fks = fk_by_table.get(table, [])
        if len(fks) != 2:
            continue
        left, right = fks
        for row in table_data[table]:
            left_key = value_key(row, left["from_cols"])
            right_key = value_key(row, right["from_cols"])
            left_id = lookup.get((left["to_table"], tuple(left["to_cols"])), {}).get(left_key)
            right_id = lookup.get((right["to_table"], tuple(right["to_cols"])), {}).get(right_key)
            if left_id is None or right_id is None:
                skipped_link_rows += 1
                continue
            props = {k: v for k, v in row.items() if k not in {"__rowid", "_etl_row_id"}}
            relationships.append(
                {
                    "kind": "link_table",
                    "type": rel_type(table),
                    "from_table": left["to_table"],
                    "from_id": left_id,
                    "to_table": right["to_table"],
                    "to_id": right_id,
                    "props": props | {"_etl_db": db_id, "_etl_table": table},
                }
            )

    return {
        "db_id": db_id,
        "tables": tables,
        "node_tables": node_tables,
        "relationship_tables": sorted(rel_tables),
        "table_data": table_data,
        "relationships": relationships,
        "expected_nodes": node_count,
        "skipped_fk_rows": skipped_fk_rows,
        "skipped_link_rows": skipped_link_rows,
        "raw_rows": sum(len(rows) for rows in table_data.values()),
    }


def create_nodes(driver, database: str, model: dict[str, Any], batch_size: int) -> None:
    db_id = model["db_id"]
    with driver.session(database=database) as session:
        for table in model["node_tables"]:
            rows = []
            for row in model["table_data"][table]:
                props = {k: v for k, v in row.items() if k not in {"__rowid", "_etl_row_id"}}
                props.update({"_etl_db": db_id, "_etl_table": table, "_etl_row_id": row["_etl_row_id"]})
                rows.append({"props": props})
            query = (
                f"UNWIND $rows AS row "
                f"CREATE (n:Neo4jETLNode:{qcy(db_id + '.' + table)}) "
                "SET n = row.props"
            )
            for start in range(0, len(rows), batch_size):
                session.run(query, rows=rows[start : start + batch_size]).consume()


def create_relationships(driver, database: str, model: dict[str, Any], batch_size: int) -> None:
    db_id = model["db_id"]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for rel in model["relationships"]:
        grouped[rel["type"]].append(rel)
    with driver.session(database=database) as session:
        for typ, rows in grouped.items():
            query = (
                "UNWIND $rows AS row "
                "MATCH (a:Neo4jETLNode {_etl_db: $db_id, _etl_table: row.from_table, _etl_row_id: row.from_id}) "
                "MATCH (b:Neo4jETLNode {_etl_db: $db_id, _etl_table: row.to_table, _etl_row_id: row.to_id}) "
                f"CREATE (a)-[r:{qcy(typ)}]->(b) "
                "SET r = row.props"
            )
            for start in range(0, len(rows), batch_size):
                session.run(query, rows=rows[start : start + batch_size], db_id=db_id).consume()


def collect_stats(driver, database: str, model: dict[str, Any]) -> dict[str, Any]:
    db_id = model["db_id"]
    with driver.session(database=database) as session:
        row = session.run(
            "MATCH (n:Neo4jETLNode {_etl_db: $db_id}) "
            "OPTIONAL MATCH (n)-[r]->(m:Neo4jETLNode {_etl_db: $db_id}) "
            "WITH count(DISTINCT n) AS nodes, count(r) AS relationships "
            "MATCH (x:Neo4jETLNode {_etl_db: $db_id}) "
            "UNWIND labels(x) AS label "
            "WITH nodes, relationships, collect(DISTINCT label) AS labels "
            "OPTIONAL MATCH (:Neo4jETLNode {_etl_db: $db_id})-[rel]->(:Neo4jETLNode {_etl_db: $db_id}) "
            "RETURN nodes, relationships, size(labels) AS labels, count(DISTINCT type(rel)) AS relationship_types",
            db_id=db_id,
        ).single()
    return {
        "db_id": db_id,
        "raw_tables": len(model["tables"]),
        "raw_rows": model["raw_rows"],
        "node_tables": len(model["node_tables"]),
        "relationship_tables": len(model["relationship_tables"]),
        "nodes": row["nodes"],
        "relationships": row["relationships"],
        "labels": row["labels"],
        "relationship_types": row["relationship_types"],
        "skipped_fk_rows": model["skipped_fk_rows"],
        "skipped_link_rows": model["skipped_link_rows"],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    args = parser.parse_args()
    config = load_json(Path(args.config))
    database = config.get("target_database", "neo4j")
    batch_size = int(config.get("batch_size", 5000))
    selected = config["selected_databases"]
    database_root = Path(config["database_root"])

    driver = GraphDatabase.driver(
        config["neo4j_uri"],
        auth=(config.get("neo4j_user", "neo4j"), config["neo4j_password"]),
    )
    started = time.perf_counter()
    clear_database(driver, database)
    create_indexes(driver, database)
    per_db = []
    try:
        for db_id in selected:
            model = build_model(database_root, db_id)
            create_nodes(driver, database, model, batch_size)
            create_relationships(driver, database, model, batch_size)
            per_db.append(collect_stats(driver, database, model))
    finally:
        driver.close()

    output = {
        "baseline": "Neo4j ETL",
        "implementation": "sqlite_schema_to_neo4j_etl_baseline",
        "input_source": str(database_root),
        "target_dbms_id": config.get("dbms_id"),
        "target_dbms_path": config.get("dbms_path"),
        "target_database": database,
        "selected_databases": selected,
        "summary": {
            "database_count": len(per_db),
            "total_nodes": sum(row["nodes"] for row in per_db),
            "total_relationships": sum(row["relationships"] for row in per_db),
            "total_elapsed_seconds": round(time.perf_counter() - started, 4),
        },
        "per_database": per_db,
        "note": "This run does not read Rel2KG Cypher. It reads SQLite tables and FK metadata; exactly-two-FK linking tables are mapped to relationships.",
    }
    output_path = Path(config["output_json"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(output, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
