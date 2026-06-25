import argparse
import json
import os
import re
import sqlite3
import time
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


def rel_type(from_table, to_table):
    value = re.sub(r"[^0-9A-Za-z_]", "_", f"{to_table}_{from_table}")
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "RELATED_TO"


def sqlite_path(database_root, db_id):
    direct = os.path.join(database_root, db_id, f"{db_id}.sqlite")
    if os.path.exists(direct):
        return direct
    folder = os.path.join(database_root, db_id)
    candidates = [os.path.join(folder, p) for p in os.listdir(folder) if p.endswith(".sqlite")]
    if not candidates:
        raise FileNotFoundError(f"No SQLite file found for {db_id}")
    return candidates[0]


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


def table_columns(conn, table):
    return [dict(row) for row in conn.execute(f"PRAGMA table_info({qname(table)})").fetchall()]


def table_rows(conn, table):
    return [dict(row) for row in conn.execute(f"SELECT * FROM {qname(table)}").fetchall()]


def table_fks(conn, table):
    grouped = defaultdict(lambda: {"from_cols": [], "to_table": None, "to_cols": []})
    for row in conn.execute(f"PRAGMA foreign_key_list({qname(table)})").fetchall():
        grouped[row["id"]]["from_cols"].append(row["from"])
        grouped[row["id"]]["to_table"] = row["table"]
        grouped[row["id"]]["to_cols"].append(row["to"])
    return list(grouped.values())


def row_key(row, cols):
    values = []
    for col in cols:
        value = row.get(col)
        if value is None:
            return None
        values.append(value)
    return tuple(values)


def load_model(database_root, db_id):
    conn = connect_sqlite(sqlite_path(database_root, db_id))
    tables = sqlite_tables(conn)
    model = {"db_id": db_id, "tables": {}, "relationships": []}
    for table in tables:
        columns = table_columns(conn, table)
        rows = table_rows(conn, table)
        for index, row in enumerate(rows):
            row["_sql2cypher_row_id"] = index
        model["tables"][table] = {
            "columns": [col["name"] for col in columns],
            "pk_cols": [col["name"] for col in sorted(columns, key=lambda c: c["pk"]) if col["pk"] > 0],
            "rows": rows,
            "fks": table_fks(conn, table),
        }

    lookup = defaultdict(list)
    for table, info in model["tables"].items():
        for row in info["rows"]:
            for col in info["columns"]:
                key = row_key(row, [col])
                if key is not None:
                    lookup[(table, (col,), key)].append(row["_sql2cypher_row_id"])
            if info["pk_cols"]:
                key = row_key(row, info["pk_cols"])
                if key is not None:
                    lookup[(table, tuple(info["pk_cols"]), key)].append(row["_sql2cypher_row_id"])

    for table, info in model["tables"].items():
        for row in info["rows"]:
            for fk in info["fks"]:
                key = row_key(row, fk["from_cols"])
                if key is None:
                    continue
                targets = lookup.get((fk["to_table"], tuple(fk["to_cols"]), key), [])
                for target_id in targets:
                    model["relationships"].append(
                        {
                            "from_table": table,
                            "from_id": row["_sql2cypher_row_id"],
                            "to_table": fk["to_table"],
                            "to_id": target_id,
                            "type": rel_type(table, fk["to_table"]),
                        }
                    )
    conn.close()
    model["expected_nodes"] = sum(len(info["rows"]) for info in model["tables"].values())
    model["expected_relationships"] = len(model["relationships"])
    return model


def ensure_database_or_alias(driver, database):
    with driver.session(database="system") as session:
        alias = session.run(
            "SHOW ALIASES FOR DATABASE YIELD name,database,location "
            "WHERE name = $name RETURN name,database,location",
            name=database,
        ).single()
        if alias:
            return {"kind": "alias", "target": alias["database"]}
        row = session.run(
            "SHOW DATABASES YIELD name,currentStatus,statusMessage "
            "WHERE name = $name RETURN currentStatus,statusMessage",
            name=database,
        ).single()
        if row and row["currentStatus"] == "online":
            return {"kind": "database", "target": database}
        if row:
            session.run(f"DROP DATABASE {qname(database)} IF EXISTS").consume()
        try:
            session.run(f"CREATE DATABASE {qname(database)} IF NOT EXISTS").consume()
            deadline = time.time() + 60
            while time.time() < deadline:
                row = session.run(
                    "SHOW DATABASES YIELD name,currentStatus,statusMessage "
                    "WHERE name = $name RETURN currentStatus,statusMessage",
                    name=database,
                ).single()
                if row and row["currentStatus"] == "online":
                    return {"kind": "database", "target": database}
                time.sleep(1)
        except Exception:
            pass
        session.run(f"DROP DATABASE {qname(database)} IF EXISTS").consume()
        session.run(f"CREATE ALIAS {qname(database)} IF NOT EXISTS FOR DATABASE neo4j").consume()
        return {"kind": "alias", "target": "neo4j"}


def clear_graph(driver, database):
    with driver.session(database=database) as session:
        while True:
            deleted = session.run(
                "MATCH (n:UNSWSQL2CypherNode) WITH n LIMIT 10000 DETACH DELETE n RETURN count(n) AS c"
            ).single()["c"]
            if deleted == 0:
                break


def ensure_indexes(driver, database):
    with driver.session(database=database) as session:
        session.run(
            "CREATE INDEX unsw_sql2cypher_lookup_idx IF NOT EXISTS FOR (n:UNSWSQL2CypherNode) "
            "ON (n._sql2cypher_db, n._sql2cypher_table, n._sql2cypher_row_id)"
        ).consume()
        session.run("CALL db.awaitIndexes()").consume()


def create_nodes(session, db_id, table, rows, batch_size):
    label = qname(table)
    query = (
        f"UNWIND $rows AS row "
        f"CREATE (n:UNSWSQL2CypherNode:{label}) "
        "SET n = row.props, n._sql2cypher_db = $db_id, "
        "n._sql2cypher_table = $table, n._sql2cypher_row_id = row.row_id"
    )
    for start in range(0, len(rows), batch_size):
        payload = [
            {
                "row_id": row["_sql2cypher_row_id"],
                "props": {k: v for k, v in row.items() if k != "_sql2cypher_row_id"},
            }
            for row in rows[start : start + batch_size]
        ]
        session.run(query, rows=payload, db_id=db_id, table=table).consume()


def create_relationships(session, db_id, rels, batch_size):
    grouped = defaultdict(list)
    for rel in rels:
        grouped[rel["type"]].append(rel)
    for typ, rows in grouped.items():
        query = (
            "UNWIND $rows AS row "
            "MATCH (src:UNSWSQL2CypherNode {_sql2cypher_db: $db_id, _sql2cypher_table: row.to_table, _sql2cypher_row_id: row.to_id}) "
            "MATCH (dst:UNSWSQL2CypherNode {_sql2cypher_db: $db_id, _sql2cypher_table: row.from_table, _sql2cypher_row_id: row.from_id}) "
            f"CREATE (src)-[:{qname(typ)}]->(dst)"
        )
        for start in range(0, len(rows), batch_size):
            session.run(query, rows=rows[start : start + batch_size], db_id=db_id).consume()


def run_model(driver, database, model, batch_size):
    db_id = model["db_id"]
    start = time.perf_counter()
    with driver.session(database=database) as session:
        for table, info in model["tables"].items():
            create_nodes(session, db_id, table, info["rows"], batch_size)
        create_relationships(session, db_id, model["relationships"], batch_size)
        nodes = session.run(
            "MATCH (n:UNSWSQL2CypherNode {_sql2cypher_db: $db_id}) RETURN count(n) AS c",
            db_id=db_id,
        ).single()["c"]
        rels = session.run(
            "MATCH (n:UNSWSQL2CypherNode {_sql2cypher_db: $db_id})-[r]->(:UNSWSQL2CypherNode) RETURN count(r) AS c",
            db_id=db_id,
        ).single()["c"]
        labels = session.run(
            "MATCH (n:UNSWSQL2CypherNode {_sql2cypher_db: $db_id}) UNWIND labels(n) AS label "
            "RETURN count(DISTINCT label) AS c",
            db_id=db_id,
        ).single()["c"]
        rel_types = session.run(
            "MATCH (n:UNSWSQL2CypherNode {_sql2cypher_db: $db_id})-[r]->(:UNSWSQL2CypherNode) "
            "RETURN count(DISTINCT type(r)) AS c",
            db_id=db_id,
        ).single()["c"]
    elapsed = time.perf_counter() - start
    return {
        "db_id": db_id,
        "status": "ok",
        "table_count": len(model["tables"]),
        "expected_nodes": model["expected_nodes"],
        "loaded_nodes": nodes,
        "node_completion_ratio": nodes / model["expected_nodes"] if model["expected_nodes"] else 0,
        "expected_relationships": model["expected_relationships"],
        "loaded_relationships": rels,
        "relationship_completion_ratio": rels / model["expected_relationships"]
        if model["expected_relationships"]
        else 0,
        "label_count": labels,
        "relationship_type_count": rel_types,
        "elapsed_seconds": round(elapsed, 4),
        "nodes_per_second": round(nodes / elapsed, 2) if elapsed else 0,
        "relationships_per_second": round(rels / elapsed, 2) if elapsed else 0,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-root", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--neo4j-uri", default="neo4j://127.0.0.1:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", required=True)
    parser.add_argument("--target-database", default="sql2cypher")
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--restart", action="store_true")
    parser.add_argument(
        "--db-ids",
        nargs="+",
        default=MAIN_DBS,
        help="Database ids to load. Defaults to the original main benchmark list.",
    )
    args = parser.parse_args()

    driver = GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password))
    started = time.perf_counter()
    try:
        target = ensure_database_or_alias(driver, args.target_database)
        if args.restart:
            clear_graph(driver, args.target_database)
        ensure_indexes(driver, args.target_database)
        results = []
        for db_id in args.db_ids:
            model = load_model(args.database_root, db_id)
            results.append(run_model(driver, args.target_database, model, args.batch_size))
    finally:
        driver.close()

    output = {
        "baseline": "unsw_sql2cypher_graph",
        "source_repository": "https://github.com/UNSW-database/SQL2Cypher",
        "source_checkout": "baselines/UNSW_SQL2Cypher",
        "implementation": "sqlite_adapter_for_unsw_table_node_fk_edge_mapping",
        "target_database": args.target_database,
        "target_resolution": target,
        "sets": {"main": args.db_ids, "smoke": [], "stress": []},
        "batch_size": args.batch_size,
        "summary": {
            "database_count": len(results),
            "total_loaded_nodes": sum(r["loaded_nodes"] for r in results),
            "total_loaded_relationships": sum(r["loaded_relationships"] for r in results),
            "total_elapsed_seconds": round(time.perf_counter() - started, 4),
            "avg_node_completion_ratio": sum(r["node_completion_ratio"] for r in results) / len(results),
            "avg_relationship_completion_ratio": sum(r["relationship_completion_ratio"] for r in results)
            / len(results),
        },
        "per_database": results,
    }
    with open(args.output_json, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
