import argparse
import json
import os
import re
import sqlite3
import time
import uuid
from collections import defaultdict

from neo4j import GraphDatabase


MAIN_DBS = [
    "world_1",
    "store_1",
    "college_2",
    "hospital_1",
    "tracking_software_problems",
    "sakila_1",
]


def qname(name):
    return "`" + str(name).replace("`", "``") + "`"


def rel_type(name):
    value = re.sub(r"[^0-9A-Za-z_]", "_", str(name))
    value = re.sub(r"_+", "_", value).strip("_")
    return value.upper() or "RELATED_TO"


def connect_sqlite(path):
    conn = sqlite3.connect(path)
    conn.text_factory = lambda b: b.decode("utf-8", errors="replace")
    conn.row_factory = sqlite3.Row
    return conn


def sqlite_tables(conn):
    return [
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    ]


def table_rows(conn, table):
    return [dict(row) for row in conn.execute(f"SELECT * FROM {qname(table)}").fetchall()]


def table_columns(conn, table):
    return [dict(row) for row in conn.execute(f"PRAGMA table_info({qname(table)})").fetchall()]


def table_fks(conn, table):
    grouped = defaultdict(lambda: {"from_cols": [], "to_table": None, "to_cols": []})
    for row in conn.execute(f"PRAGMA foreign_key_list({qname(table)})").fetchall():
        grouped[row["id"]]["from_cols"].append(row["from"])
        grouped[row["id"]]["to_table"] = row["table"]
        grouped[row["id"]]["to_cols"].append(row["to"])
    return list(grouped.values())


def value_key(row, cols):
    values = []
    for col in cols:
        value = row.get(col)
        if value is None:
            return None
        if isinstance(value, str):
            value = value.strip()
            if value == "":
                return None
        values.append(value)
    return tuple(values)


def sqlite_path(database_root, db_id):
    direct = os.path.join(database_root, db_id, f"{db_id}.sqlite")
    if os.path.exists(direct):
        return direct
    folder = os.path.join(database_root, db_id)
    candidates = [os.path.join(folder, p) for p in os.listdir(folder) if p.endswith(".sqlite")]
    if not candidates:
        raise FileNotFoundError(f"No SQLite file found for {db_id}")
    return candidates[0]


def clear_run(driver, database, run_id, db_id=None):
    with driver.session(database=database) as session:
        while True:
            if db_id is None:
                deleted = session.run(
                    "MATCH (n {_etl_run_id: $run_id}) "
                    "WITH n LIMIT 10000 DETACH DELETE n RETURN count(n) AS c",
                    run_id=run_id,
                ).single()["c"]
            else:
                deleted = session.run(
                    "MATCH (n {_etl_run_id: $run_id, _etl_db: $db_id}) "
                    "WITH n LIMIT 10000 DETACH DELETE n RETURN count(n) AS c",
                    run_id=run_id,
                    db_id=db_id,
                ).single()["c"]
            if deleted == 0:
                break


def ensure_indexes(driver, database):
    with driver.session(database=database) as session:
        session.run(
            "CREATE INDEX etl_run_id_idx IF NOT EXISTS FOR (n:ETLTemp) ON (n._etl_run_id)"
        ).consume()
        session.run(
            "CREATE INDEX etl_node_lookup_idx IF NOT EXISTS FOR (n:ETLTemp) "
            "ON (n._etl_run_id, n._etl_table, n._etl_row_id)"
        ).consume()
        session.run("CALL db.awaitIndexes()").consume()


def create_nodes(session, db_id, table, rows, batch_size, run_id):
    label = qname(f"{db_id}.{table}")
    query = (
        f"UNWIND $rows AS row "
        f"CREATE (n:ETLTemp:{label}) "
        f"SET n = row.props, n._etl_db = $db_id, n._etl_table = $table, "
        f"n._etl_row_id = row.row_id, n._etl_run_id = $run_id"
    )
    for start in range(0, len(rows), batch_size):
        payload = [
            {"row_id": row["_etl_row_id"], "props": {k: v for k, v in row.items() if k != "_etl_row_id"}}
            for row in rows[start : start + batch_size]
        ]
        session.run(query, rows=payload, db_id=db_id, table=table, run_id=run_id).consume()


def create_relationships(session, db_id, rel_rows, batch_size, run_id):
    grouped = defaultdict(list)
    for row in rel_rows:
        grouped[row["type"]].append(row)
    for typ, rows in grouped.items():
        query = (
            "UNWIND $rows AS row "
            "MATCH (a {_etl_db: $db_id, _etl_run_id: $run_id, _etl_table: row.from_table, _etl_row_id: row.from_id}) "
            "MATCH (b {_etl_db: $db_id, _etl_run_id: $run_id, _etl_table: row.to_table, _etl_row_id: row.to_id}) "
            f"CREATE (a)-[:{qname(typ)}]->(b)"
        )
        for start in range(0, len(rows), batch_size):
            session.run(query, rows=rows[start : start + batch_size], db_id=db_id, run_id=run_id).consume()


def expected_model(sqlite_file, db_id):
    conn = connect_sqlite(sqlite_file)
    tables = sqlite_tables(conn)
    table_data = {}
    lookup = defaultdict(dict)
    total_rows = 0
    expected_relationships = 0

    for table in tables:
        rows = table_rows(conn, table)
        cols = table_columns(conn, table)
        pk_cols = [c["name"] for c in sorted(cols, key=lambda c: c["pk"]) if c["pk"] > 0]
        for i, row in enumerate(rows):
            row["_etl_row_id"] = i
        table_data[table] = {"rows": rows, "pk_cols": pk_cols, "fks": table_fks(conn, table)}
        total_rows += len(rows)
        for row in rows:
            for col in row:
                if col != "_etl_row_id":
                    key = value_key(row, [col])
                    if key is not None:
                        lookup[(table, (col,))][key] = row["_etl_row_id"]
            if pk_cols:
                key = value_key(row, pk_cols)
                if key is not None:
                    lookup[(table, tuple(pk_cols))][key] = row["_etl_row_id"]

    rel_rows = []
    for table, info in table_data.items():
        for row in info["rows"]:
            for fk in info["fks"]:
                key = value_key(row, fk["from_cols"])
                if key is None:
                    continue
                target_id = lookup.get((fk["to_table"], tuple(fk["to_cols"])), {}).get(key)
                if target_id is None:
                    continue
                expected_relationships += 1
                rel_rows.append(
                    {
                        "from_table": table,
                        "from_id": row["_etl_row_id"],
                        "to_table": fk["to_table"],
                        "to_id": target_id,
                        "type": rel_type(f"{table}_{'_'.join(fk['from_cols'])}_TO_{fk['to_table']}"),
                    }
                )
    conn.close()
    return {
        "db_id": db_id,
        "tables": tables,
        "table_data": table_data,
        "rel_rows": rel_rows,
        "expected_nodes": total_rows,
        "expected_relationships": expected_relationships,
    }


def run_db(driver, database, model, batch_size, run_id, keep_loaded_data):
    db_id = model["db_id"]
    start = time.perf_counter()
    clear_run(driver, database, run_id, db_id=db_id)
    with driver.session(database=database) as session:
        for table in model["tables"]:
            create_nodes(session, db_id, table, model["table_data"][table]["rows"], batch_size, run_id)
        create_relationships(session, db_id, model["rel_rows"], batch_size, run_id)
        node_count = session.run(
            "MATCH (n {_etl_run_id: $run_id, _etl_db: $db_id}) RETURN count(n) AS c",
            run_id=run_id,
            db_id=db_id,
        ).single()["c"]
        relationship_count = session.run(
            "MATCH (n {_etl_run_id: $run_id, _etl_db: $db_id})-[r]->"
            "(m {_etl_run_id: $run_id}) RETURN count(r) AS c",
            run_id=run_id,
            db_id=db_id,
        ).single()["c"]
        label_count = session.run(
            "MATCH (n {_etl_run_id: $run_id, _etl_db: $db_id}) "
            "UNWIND labels(n) AS label RETURN count(DISTINCT label) AS c",
            run_id=run_id,
            db_id=db_id,
        ).single()["c"]
        rel_type_count = session.run(
            "MATCH (n {_etl_run_id: $run_id, _etl_db: $db_id})-[r]->(m {_etl_run_id: $run_id}) "
            "RETURN count(DISTINCT type(r)) AS c",
            run_id=run_id,
            db_id=db_id,
        ).single()["c"]
    if not keep_loaded_data:
        clear_run(driver, database, run_id, db_id=db_id)
    elapsed = time.perf_counter() - start
    return {
        "db_id": db_id,
        "status": "ok",
        "table_count": len(model["tables"]),
        "expected_nodes": model["expected_nodes"],
        "loaded_nodes": node_count,
        "node_completion_ratio": node_count / model["expected_nodes"] if model["expected_nodes"] else 0,
        "expected_relationships": model["expected_relationships"],
        "loaded_relationships": relationship_count,
        "relationship_completion_ratio": relationship_count / model["expected_relationships"]
        if model["expected_relationships"]
        else 0,
        "label_count": label_count,
        "relationship_type_count": rel_type_count,
        "elapsed_seconds": round(elapsed, 4),
        "nodes_per_second": round(node_count / elapsed, 2) if elapsed else 0,
        "relationships_per_second": round(relationship_count / elapsed, 2) if elapsed else 0,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-root", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--neo4j-uri", default="neo4j://127.0.0.1:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", required=True)
    parser.add_argument("--target-database", default="neo4j")
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--keep-loaded-data", action="store_true")
    args = parser.parse_args()

    driver = GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password))
    official_tool_available = False
    results = []
    started = time.perf_counter()
    run_id = f"neo4j-etl-main-{uuid.uuid4()}"
    try:
        ensure_indexes(driver, args.target_database)
        for db_id in MAIN_DBS:
            model = expected_model(sqlite_path(args.database_root, db_id), db_id)
            results.append(
                run_db(
                    driver,
                    args.target_database,
                    model,
                    args.batch_size,
                    run_id,
                    args.keep_loaded_data,
                )
            )
    finally:
        if not args.keep_loaded_data:
            clear_run(driver, args.target_database, run_id)
        driver.close()

    total_elapsed = time.perf_counter() - started
    total_nodes = sum(r["loaded_nodes"] for r in results)
    total_relationships = sum(r["loaded_relationships"] for r in results)
    output = {
        "baseline": "neo4j_etl",
        "implementation": "sqlite_to_neo4j_etl_baseline",
        "official_neo4j_etl_tool_available": official_tool_available,
        "note": "This experiment uses a reproducible Neo4j ETL-style import: SQLite table rows become nodes and foreign keys become relationships.",
        "sets": {"main": MAIN_DBS, "smoke": [], "stress": []},
        "result_storage": "JSON only. Neo4j is used as a transient counting target; loaded nodes are tagged with _etl_run_id and deleted after counting unless --keep-loaded-data is set.",
        "target_database": args.target_database,
        "run_id": run_id,
        "loaded_data_kept": args.keep_loaded_data,
        "batch_size": args.batch_size,
        "summary": {
            "database_count": len(results),
            "total_loaded_nodes": total_nodes,
            "total_loaded_relationships": total_relationships,
            "total_elapsed_seconds": round(total_elapsed, 4),
            "avg_node_completion_ratio": sum(r["node_completion_ratio"] for r in results) / len(results),
            "avg_relationship_completion_ratio": sum(r["relationship_completion_ratio"] for r in results) / len(results),
        },
        "per_database": results,
    }
    with open(args.output_json, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
