import argparse
import contextlib
import csv
import io
import json
import math
import os
import sqlite3
import sys
import time
import traceback
from collections import Counter, defaultdict


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REL_DB2KG = os.path.join(REPO_ROOT, "rel_db2kg")
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
if REL_DB2KG not in sys.path:
    sys.path.insert(0, REL_DB2KG)

from rel_db2kg.py_compat import patch_legacy_collections

patch_legacy_collections()

from datasets import load_dataset
from dotenv import load_dotenv
from moz_sql_parser import parse
from py2neo import Graph

from rel_db2kg.rel2kg_utils import Logger
from rel_db2kg.schema2graph import DBengine, RelDBDataset
from rel_db2kg.sql2cypher import Formatter


SMOKE_DBS = ["department_management", "concert_singer", "car_1"]
MAIN_DBS = [
    "world_1",
    "store_1",
    "college_2",
    "hospital_1",
    "tracking_software_problems",
    "sakila_1",
]
SELECTED_DBS = SMOKE_DBS + MAIN_DBS


def load_queries(selected_dbs, max_queries_per_db=None):
    records = []
    per_db = Counter()
    for split in ("train", "validation"):
        ds = load_dataset("xlangai/spider", split=split)
        for idx, row in enumerate(ds):
            db_id = row["db_id"]
            if db_id not in selected_dbs:
                continue
            if max_queries_per_db and per_db[db_id] >= max_queries_per_db:
                continue
            per_db[db_id] += 1
            records.append(
                {
                    "split": split,
                    "split_index": idx,
                    "db_id": db_id,
                    "question": row["question"],
                    "query": row["query"],
                }
            )
    return records


def normalize_cell(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def normalize_result(rows):
    normalized = []
    for row in rows:
        if isinstance(row, dict):
            values = row.values()
        else:
            values = row
        normalized.append(tuple(normalize_cell(v) for v in values))
    return sorted(normalized, key=lambda x: repr(x))


def execute_sql(sqlite_path, query):
    conn = sqlite3.connect(sqlite_path)
    conn.text_factory = lambda b: b.decode("utf-8", errors="replace")
    try:
        cur = conn.cursor()
        cur.execute(query)
        return cur.fetchall()
    finally:
        conn.close()


def load_selected_dataset(database_root, selected_dbs):
    sqlite_paths = []
    for db_id in selected_dbs:
        db_dir = os.path.join(database_root, db_id)
        sqlite_path = os.path.join(db_dir, f"{db_id}.sqlite")
        if not os.path.exists(sqlite_path):
            candidates = [
                os.path.join(db_dir, p)
                for p in os.listdir(db_dir)
                if p.endswith(".sqlite")
            ]
            if candidates:
                sqlite_path = candidates[0]
        if not os.path.exists(sqlite_path):
            raise FileNotFoundError(f"SQLite database not found for {db_id}")
        sqlite_paths.append(sqlite_path)
    logger = Logger("/spider_selected_rel_schema2graph.log")
    with contextlib.redirect_stdout(io.StringIO()):
        return RelDBDataset(sqlite_paths, logger)


def graph_counts(graph, selected_dbs):
    counts = {}
    for db_id in selected_dbs:
        node_count = graph.run(
            "MATCH (n) WHERE any(label IN labels(n) WHERE label STARTS WITH $prefix) "
            "RETURN count(n) AS c",
            prefix=f"{db_id}.",
        ).evaluate()
        rel_count = graph.run(
            "MATCH ()-[r]->() "
            "WHERE type(r) STARTS WITH $prefix OR type(r) CONTAINS $contains "
            "RETURN count(r) AS c",
            prefix=f"{db_id}.",
            contains=f"{db_id}.",
        ).evaluate()
        counts[db_id] = {"neo4j_nodes": node_count or 0, "neo4j_relationships": rel_count or 0}
    return counts


def load_complexity_rows(path):
    rows = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows[row["db_id"]] = row
    return rows


def evaluate_sql2cypher(records, rel_dataset, graph, database_root, selected_dbs):
    logger = Logger("/spider_selected_sql2cypher.log")
    summary = {
        db_id: {
            "db_id": db_id,
            "total": 0,
            "sql_executed": 0,
            "parsed": 0,
            "translated": 0,
            "cypher_executed": 0,
            "correct": 0,
            "incorrect": 0,
            "sql_error": 0,
            "parse_error": 0,
            "translation_error": 0,
            "cypher_error": 0,
        }
        for db_id in selected_dbs
    }
    failures = []
    for record in records:
        db_id = record["db_id"]
        stats = summary[db_id]
        stats["total"] += 1
        sqlite_path = os.path.join(database_root, db_id, f"{db_id}.sqlite")
        start = time.perf_counter()
        try:
            sql_rows = execute_sql(sqlite_path, record["query"])
            sql_result = normalize_result(sql_rows)
            stats["sql_executed"] += 1
        except Exception as exc:
            stats["sql_error"] += 1
            failures.append({**record, "stage": "sql", "error": repr(exc)})
            continue
        try:
            parsed_sql = parse(record["query"])
            stats["parsed"] += 1
        except Exception as exc:
            stats["parse_error"] += 1
            failures.append({**record, "stage": "parse", "error": repr(exc)})
            continue
        try:
            formatter = Formatter(logger, db_id, rel_dataset.rel_dbs[db_id], graph)
            with contextlib.redirect_stdout(io.StringIO()):
                cypher = formatter.format(parsed_sql)
            if not cypher:
                raise ValueError("empty cypher")
            stats["translated"] += 1
        except Exception as exc:
            stats["translation_error"] += 1
            failures.append({**record, "stage": "translation", "error": repr(exc)})
            continue
        try:
            cypher_rows = graph.run(cypher).data()
            cypher_result = normalize_result(cypher_rows)
            stats["cypher_executed"] += 1
        except Exception as exc:
            stats["cypher_error"] += 1
            failures.append({**record, "stage": "cypher", "cypher": cypher, "error": repr(exc)})
            continue
        elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
        if cypher_result == sql_result:
            stats["correct"] += 1
        else:
            stats["incorrect"] += 1
            failures.append(
                {
                    **record,
                    "stage": "compare",
                    "cypher": cypher,
                    "sql_result": repr(sql_result[:5]),
                    "cypher_result": repr(cypher_result[:5]),
                    "elapsed_ms": elapsed_ms,
                }
            )
    for stats in summary.values():
        total = stats["total"]
        stats["parse_rate"] = stats["parsed"] / total if total else 0
        stats["translation_rate"] = stats["translated"] / total if total else 0
        stats["execution_rate"] = stats["cypher_executed"] / total if total else 0
        stats["accuracy_total"] = stats["correct"] / total if total else 0
        stats["accuracy_executed"] = (
            stats["correct"] / stats["cypher_executed"] if stats["cypher_executed"] else 0
        )
    return summary, failures


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-root", required=True)
    parser.add_argument("--complexity-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--neo4j-uri", default=os.environ.get("NEO4J_URI", "neo4j://127.0.0.1:7687"))
    parser.add_argument("--neo4j-user", default=os.environ.get("NEO4J_USER", "neo4j"))
    parser.add_argument("--neo4j-password", default=os.environ.get("NEO4J_PASSWORD"))
    parser.add_argument("--neo4j-database", default=os.environ.get("NEO4J_DATABASE", "rel2kg"))
    parser.add_argument("--max-queries-per-db", type=int, default=None)
    parser.add_argument("--include-sql2cypher", action="store_true")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    complexity = load_complexity_rows(args.complexity_csv)
    graph = Graph(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password), name=args.neo4j_database)
    counts = graph_counts(graph, SELECTED_DBS)
    records = []
    sql2cypher_summary = {}
    failures = []
    if args.include_sql2cypher:
        selected_dataset = load_selected_dataset(args.database_root, SELECTED_DBS)
        records = load_queries(SELECTED_DBS, args.max_queries_per_db)
        sql2cypher_summary, failures = evaluate_sql2cypher(
            records, selected_dataset, graph, args.database_root, SELECTED_DBS
        )

    r2g_rows = []
    for db_id in SELECTED_DBS:
        c = complexity[db_id]
        g = counts[db_id]
        expected_nodes = float(c["expected_graph_nodes"])
        expected_rels = float(c["expected_graph_relationships"])
        r2g_rows.append(
            {
                "set": "smoke" if db_id in SMOKE_DBS else "main",
                "db_id": db_id,
                "expected_graph_nodes": int(expected_nodes),
                "neo4j_nodes": g["neo4j_nodes"],
                "node_completion_ratio": g["neo4j_nodes"] / expected_nodes if expected_nodes else 0,
                "expected_graph_relationships": int(expected_rels),
                "neo4j_relationships": g["neo4j_relationships"],
                "relationship_completion_ratio": g["neo4j_relationships"] / expected_rels if expected_rels else 0,
            }
        )

    neo4j_etl = {
        "status": "tool_not_found",
        "detail": "No neo4j-etl executable was found in PATH or Neo4j Desktop application files.",
        "selected_databases": SELECTED_DBS,
    }

    overall = {
        "selected_databases": SELECTED_DBS,
        "sets": {
            "smoke": SMOKE_DBS,
            "main": MAIN_DBS,
            "stress": [],
        },
        "query_count": len(records),
        "r2g": {
            "validation_method": "Read-only Neo4j counts from the already built rel2kg graph; no benchmark result is written to Neo4j.",
            "per_database": r2g_rows,
            "node_completion_avg": sum(r["node_completion_ratio"] for r in r2g_rows) / len(r2g_rows),
            "relationship_completion_avg": sum(r["relationship_completion_ratio"] for r in r2g_rows)
            / len(r2g_rows),
        },
        "neo4j_etl": neo4j_etl,
    }
    if args.include_sql2cypher:
        sql_rows = []
        for db_id in SELECTED_DBS:
            row = dict(sql2cypher_summary[db_id])
            row["set"] = "smoke" if db_id in SMOKE_DBS else "main"
            sql_rows.append(row)
        overall["sql2cypher"] = {
            "output_method": "JSON file only; SQL gold results are read from SQLite and translated Cypher is executed read-only against rel2kg.",
            "per_database": sql_rows,
            "failures": failures,
            "total": sum(r["total"] for r in sql_rows),
            "correct": sum(r["correct"] for r in sql_rows),
            "cypher_executed": sum(r["cypher_executed"] for r in sql_rows),
            "accuracy_total": sum(r["correct"] for r in sql_rows) / sum(r["total"] for r in sql_rows),
            "accuracy_executed": sum(r["correct"] for r in sql_rows)
            / sum(r["cypher_executed"] for r in sql_rows)
            if sum(r["cypher_executed"] for r in sql_rows)
            else 0,
        }
    output_path = os.path.join(args.output_dir, "selected_baseline_results.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(overall, f, indent=2)
    print(f"wrote {output_path}")
    print(json.dumps(overall, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        raise
