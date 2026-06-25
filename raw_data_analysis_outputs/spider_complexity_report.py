import argparse
import csv
import math
import os
import re
import sqlite3
from collections import Counter, defaultdict
from statistics import mean


def load_spider_queries():
    from datasets import load_dataset

    records = []
    for split in ("train", "validation"):
        ds = load_dataset("xlangai/spider", split=split)
        for row in ds:
            records.append(
                {
                    "split": split,
                    "db_id": row["db_id"],
                    "query": row["query"],
                    "query_toks": row.get("query_toks") or row["query"].split(),
                    "query_toks_no_value": row.get("query_toks_no_value") or [],
                    "question_toks": row.get("question_toks") or [],
                }
            )
    return records


def sql_features(record):
    query = record["query"]
    toks = [str(t).lower() for t in record["query_toks"]]
    text = " ".join(toks)
    token_count = len(toks)
    select_count = toks.count("select")
    join_count = toks.count("join")
    where_count = toks.count("where")
    group_count = len(re.findall(r"\bgroup\s+by\b", text))
    order_count = len(re.findall(r"\border\s+by\b", text))
    having_count = toks.count("having")
    limit_count = toks.count("limit")
    setop_count = sum(toks.count(op) for op in ("intersect", "union", "except"))
    agg_count = sum(toks.count(fn) for fn in ("count", "sum", "avg", "min", "max"))
    condition_count = sum(toks.count(op) for op in ("=", ">", "<", ">=", "<=", "!=", "like", "in", "between"))
    bool_count = toks.count("and") + toks.count("or")
    nested_select_count = max(0, select_count - 1)
    table_refs = 0
    for idx, tok in enumerate(toks[:-1]):
        if tok in ("from", "join"):
            table_refs += 1
    score = (
        token_count
        + 8 * nested_select_count
        + 6 * setop_count
        + 4 * join_count
        + 3 * group_count
        + 3 * order_count
        + 4 * having_count
        + 2 * where_count
        + 1.5 * condition_count
        + agg_count
        + bool_count
        + limit_count
    )
    if score < 18 and join_count == 0 and nested_select_count == 0 and setop_count == 0:
        level = "easy"
    elif score < 35 and nested_select_count == 0 and setop_count == 0:
        level = "medium"
    elif score < 60 and setop_count <= 1:
        level = "hard"
    else:
        level = "extra"
    return {
        "sql_token_count": token_count,
        "sql_table_refs": table_refs,
        "sql_select_count": select_count,
        "sql_join_count": join_count,
        "sql_where_count": where_count,
        "sql_group_count": group_count,
        "sql_order_count": order_count,
        "sql_having_count": having_count,
        "sql_limit_count": limit_count,
        "sql_setop_count": setop_count,
        "sql_agg_count": agg_count,
        "sql_condition_count": condition_count,
        "sql_nested_select_count": nested_select_count,
        "sql_complexity_score": score,
        "sql_complexity_level": level,
    }


def sqlite_tables(conn):
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return [r[0] for r in rows]


def quoted(name):
    return '"' + name.replace('"', '""') + '"'


def is_compound_pk(conn, table, pk_cols):
    if len(pk_cols) <= 1:
        return False
    unique_indexes = conn.execute(f"PRAGMA index_list({quoted(table)})").fetchall()
    for index_row in unique_indexes:
        if not index_row[2]:
            continue
        index_name = index_row[1]
        cols = [r[2] for r in conn.execute(f"PRAGMA index_info({quoted(index_name)})").fetchall()]
        if cols == pk_cols:
            return True
    return False


def normalize_value(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip("'").strip('"').strip()
        return value if value else None
    return value


def lookup_key(db_id, table, cols, row):
    values = []
    for col in cols:
        if col not in row:
            return None
        value = normalize_value(row[col])
        if value is None:
            return None
        values.append(value)
    return (db_id, table, tuple(cols), tuple(values))


def db_stats(db_id, sqlite_path):
    conn = sqlite3.connect(sqlite_path)
    conn.text_factory = lambda b: b.decode("utf-8", errors="replace")
    conn.row_factory = sqlite3.Row
    tables = sqlite_tables(conn)
    table_info = {}
    row_total = 0
    nullable_values = 0
    total_values = 0
    empty_tables = 0
    pk_table_count = 0
    fk_count = 0
    fk_cols_total = 0
    column_total = 0
    distinct_type_count = set()
    max_table_rows = 0

    for table in tables:
        cols_info = conn.execute(f"PRAGMA table_info({quoted(table)})").fetchall()
        col_names = [r["name"] for r in cols_info]
        pk_cols = [r["name"] for r in sorted(cols_info, key=lambda r: r["pk"]) if r["pk"] > 0]
        fk_rows = conn.execute(f"PRAGMA foreign_key_list({quoted(table)})").fetchall()
        grouped_fks = defaultdict(lambda: {"from": [], "to_table": None, "to": []})
        for fk in fk_rows:
            grouped_fks[fk["id"]]["from"].append(fk["from"])
            grouped_fks[fk["id"]]["to_table"] = fk["table"]
            grouped_fks[fk["id"]]["to"].append(fk["to"])
        rows = [dict(r) for r in conn.execute(f"SELECT * FROM {quoted(table)}").fetchall()]
        row_count = len(rows)
        row_total += row_count
        max_table_rows = max(max_table_rows, row_count)
        if row_count == 0:
            empty_tables += 1
        if pk_cols:
            pk_table_count += 1
        for c in cols_info:
            column_total += 1
            distinct_type_count.add((c["type"] or "").lower())
        for row in rows:
            for col in col_names:
                total_values += 1
                if row.get(col) is None:
                    nullable_values += 1
        fk_count += len(grouped_fks)
        fk_cols_total += sum(len(v["from"]) for v in grouped_fks.values())
        table_info[table] = {
            "columns": col_names,
            "pk_cols": pk_cols,
            "fk_groups": list(grouped_fks.values()),
            "rows": rows,
            "row_count": row_count,
            "compound_pk": is_compound_pk(conn, table, pk_cols),
        }

    lookup_columns = defaultdict(set)
    for table, info in table_info.items():
        for col in info["columns"]:
            lookup_columns[table].add((col,))
        if info["pk_cols"]:
            lookup_columns[table].add(tuple(info["pk_cols"]))
        for fk in info["fk_groups"]:
            lookup_columns[table].add(tuple(fk["from"]))
            lookup_columns[fk["to_table"]].add(tuple(fk["to"]))

    node_index = defaultdict(list)
    row_node = {}
    expected_nodes = 0
    for table, info in table_info.items():
        node_table = (
            not info["fk_groups"]
            or len(info["fk_groups"]) != 2
            or (len(info["fk_groups"]) == 2 and bool(info["pk_cols"]) and not info["compound_pk"])
        )
        if not node_table:
            continue
        expected_nodes += info["row_count"]
        for idx, row in enumerate(info["rows"]):
            marker = (table, idx)
            row_node[marker] = True
            for cols in lookup_columns[table]:
                key = lookup_key(db_id, table, cols, row)
                if key:
                    node_index[key].append(marker)

    expected_direct_edges = 0
    expected_fk_edges = 0
    for table, info in table_info.items():
        if not info["fk_groups"]:
            continue
        if len(info["fk_groups"]) == 2 and (info["compound_pk"] or not info["pk_cols"]):
            for row in info["rows"]:
                matches = 0
                for fk in info["fk_groups"]:
                    lookup_row = {fk["to"][i]: row.get(fk["from"][i]) for i in range(len(fk["to"]))}
                    key = lookup_key(db_id, fk["to_table"], fk["to"], lookup_row)
                    if key and node_index.get(key):
                        matches += len(node_index[key])
                if matches == 2:
                    expected_direct_edges += 1
        if not (len(info["fk_groups"]) == 2 and info["compound_pk"]):
            for idx, row in enumerate(info["rows"]):
                if (table, idx) not in row_node:
                    continue
                for fk in info["fk_groups"]:
                    lookup_row = {fk["to"][i]: row.get(fk["from"][i]) for i in range(len(fk["to"]))}
                    key = lookup_key(db_id, fk["to_table"], fk["to"], lookup_row)
                    if key and node_index.get(key):
                        expected_fk_edges += len(node_index[key])

    conn.close()
    non_null_ratio = 1.0 if total_values == 0 else (total_values - nullable_values) / total_values
    non_empty_table_ratio = 1.0 if not tables else (len(tables) - empty_tables) / len(tables)
    pk_table_ratio = 1.0 if not tables else pk_table_count / len(tables)
    schema_complexity_score = (
        2.5 * len(tables)
        + column_total
        + 4 * fk_count
        + 0.5 * fk_cols_total
        + math.log10(row_total + 1) * 8
        + 0.02 * math.sqrt(max_table_rows)
    )
    completeness_score = 100 * (
        0.35 * non_null_ratio + 0.25 * non_empty_table_ratio + 0.25 * pk_table_ratio + 0.15
    )
    return {
        "db_id": db_id,
        "sqlite_exists": 1,
        "table_count": len(tables),
        "column_count": column_total,
        "primary_key_table_count": pk_table_count,
        "foreign_key_count": fk_count,
        "foreign_key_column_count": fk_cols_total,
        "row_count": row_total,
        "max_table_rows": max_table_rows,
        "empty_table_count": empty_tables,
        "non_null_ratio": non_null_ratio,
        "non_empty_table_ratio": non_empty_table_ratio,
        "pk_table_ratio": pk_table_ratio,
        "sqlite_type_count": len([t for t in distinct_type_count if t]),
        "schema_complexity_score": schema_complexity_score,
        "data_completeness_score": completeness_score,
        "expected_graph_nodes": expected_nodes,
        "expected_graph_relationships": expected_direct_edges + expected_fk_edges,
        "expected_direct_relationships": expected_direct_edges,
        "expected_fk_relationships": expected_fk_edges,
    }


def load_neo4j_counts(uri, user, password, database):
    if not (uri and user and password and database):
        return {}, {}
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(uri, auth=(user, password))
    node_counts = {}
    rel_counts = {}
    with driver.session(database=database) as session:
        for r in session.run(
            "MATCH (n) UNWIND labels(n) AS label "
            "WITH split(label, '.')[0] AS db_id, count(n) AS c "
            "RETURN db_id, sum(c) AS c"
        ):
            node_counts[r["db_id"]] = r["c"]
        for r in session.run(
            "MATCH ()-[rel]->() "
            "WITH type(rel) AS t "
            "WITH CASE WHEN t CONTAINS '_HAS_' THEN split(t, '_HAS_')[0] ELSE t END AS head "
            "WITH split(head, '.')[0] AS db_id, count(*) AS c "
            "RETURN db_id, sum(c) AS c"
        ):
            rel_counts[r["db_id"]] = r["c"]
    driver.close()
    return node_counts, rel_counts


def aggregate_queries(records):
    grouped = defaultdict(list)
    for record in records:
        features = sql_features(record)
        grouped[record["db_id"]].append((record, features))
    stats = {}
    for db_id, items in grouped.items():
        levels = Counter(f["sql_complexity_level"] for _, f in items)
        split_counts = Counter(r["split"] for r, _ in items)
        scores = [f["sql_complexity_score"] for _, f in items]
        tokens = [f["sql_token_count"] for _, f in items]
        stats[db_id] = {
            "query_count": len(items),
            "train_query_count": split_counts["train"],
            "validation_query_count": split_counts["validation"],
            "avg_sql_tokens": mean(tokens),
            "max_sql_tokens": max(tokens),
            "avg_sql_complexity_score": mean(scores),
            "max_sql_complexity_score": max(scores),
            "easy_query_count": levels["easy"],
            "medium_query_count": levels["medium"],
            "hard_query_count": levels["hard"],
            "extra_query_count": levels["extra"],
            "avg_join_count": mean(f["sql_join_count"] for _, f in items),
            "avg_nested_select_count": mean(f["sql_nested_select_count"] for _, f in items),
            "avg_setop_count": mean(f["sql_setop_count"] for _, f in items),
            "avg_condition_count": mean(f["sql_condition_count"] for _, f in items),
        }
    return stats


def write_markdown(rows, path):
    total = len(rows)
    total_queries = sum(r["query_count"] for r in rows)
    avg_schema = mean(r["schema_complexity_score"] for r in rows)
    avg_sql = mean(r["avg_sql_complexity_score"] for r in rows if r["query_count"])
    avg_complete = mean(r["data_completeness_score"] for r in rows)
    top_schema = sorted(rows, key=lambda r: r["schema_complexity_score"], reverse=True)[:10]
    top_sql = sorted(rows, key=lambda r: r["avg_sql_complexity_score"], reverse=True)[:10]
    low_complete = sorted(rows, key=lambda r: r["data_completeness_score"])[:10]
    lines = [
        "# Spider Complexity Report",
        "",
        "## Scope",
        "",
        f"- Databases: {total}",
        f"- SQL queries: {total_queries}",
        f"- Average schema complexity score: {avg_schema:.2f}",
        f"- Average SQL complexity score: {avg_sql:.2f}",
        f"- Average data completeness score: {avg_complete:.2f}",
        "",
        "## Metric Definitions",
        "",
        "- `schema_complexity_score`: weighted heuristic using table count, column count, foreign-key count, foreign-key columns, and row-volume scale.",
        "- `data_completeness_score`: weighted heuristic using non-null value ratio, non-empty table ratio, primary-key table ratio, and SQLite existence.",
        "- `avg_sql_complexity_score`: weighted heuristic using SQL token length, joins, nested selects, set operations, grouping, ordering, having, where, conditions, aggregations, and limits.",
        "- `expected_graph_nodes` / `expected_graph_relationships`: estimated from the current Rel2KG schema-to-graph rules, not from raw relational rows only.",
        "- `neo4j_*_completion_ratio`: observed Neo4j count divided by expected Rel2KG graph count when Neo4j counts are supplied.",
        "",
        "## Top Schema Complexity",
        "",
        "| rank | db_id | schema_score | tables | columns | rows | fks |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for i, r in enumerate(top_schema, 1):
        lines.append(
            f"| {i} | {r['db_id']} | {r['schema_complexity_score']:.2f} | {r['table_count']} | {r['column_count']} | {r['row_count']} | {r['foreign_key_count']} |"
        )
    lines.extend(
        [
            "",
            "## Top SQL Complexity",
            "",
            "| rank | db_id | avg_sql_score | max_sql_score | queries | easy | medium | hard | extra |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for i, r in enumerate(top_sql, 1):
        lines.append(
            f"| {i} | {r['db_id']} | {r['avg_sql_complexity_score']:.2f} | {r['max_sql_complexity_score']:.2f} | {r['query_count']} | {r['easy_query_count']} | {r['medium_query_count']} | {r['hard_query_count']} | {r['extra_query_count']} |"
        )
    lines.extend(
        [
            "",
            "## Lowest Data Completeness",
            "",
            "| rank | db_id | completeness | non_null | non_empty_tables | pk_table_ratio | empty_tables |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for i, r in enumerate(low_complete, 1):
        lines.append(
            f"| {i} | {r['db_id']} | {r['data_completeness_score']:.2f} | {r['non_null_ratio']:.3f} | {r['non_empty_table_ratio']:.3f} | {r['pk_table_ratio']:.3f} | {r['empty_table_count']} |"
        )
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-root", required=True)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--output-md", required=True)
    parser.add_argument("--neo4j-uri", default=None)
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default=None)
    parser.add_argument("--neo4j-database", default=None)
    args = parser.parse_args()

    query_stats = aggregate_queries(load_spider_queries())
    db_dirs = sorted(
        d for d in os.listdir(args.database_root) if os.path.isdir(os.path.join(args.database_root, d))
    )
    neo4j_node_counts, neo4j_rel_counts = load_neo4j_counts(
        args.neo4j_uri, args.neo4j_user, args.neo4j_password, args.neo4j_database
    )
    rows = []
    for db_id in db_dirs:
        sqlite_path = os.path.join(args.database_root, db_id, f"{db_id}.sqlite")
        if not os.path.exists(sqlite_path):
            candidates = [
                os.path.join(args.database_root, db_id, p)
                for p in os.listdir(os.path.join(args.database_root, db_id))
                if p.endswith(".sqlite")
            ]
            sqlite_path = candidates[0] if candidates else None
        if not sqlite_path:
            row = {"db_id": db_id, "sqlite_exists": 0}
        else:
            row = db_stats(db_id, sqlite_path)
        row.update(
            {
                "query_count": 0,
                "train_query_count": 0,
                "validation_query_count": 0,
                "avg_sql_tokens": 0,
                "max_sql_tokens": 0,
                "avg_sql_complexity_score": 0,
                "max_sql_complexity_score": 0,
                "easy_query_count": 0,
                "medium_query_count": 0,
                "hard_query_count": 0,
                "extra_query_count": 0,
                "avg_join_count": 0,
                "avg_nested_select_count": 0,
                "avg_setop_count": 0,
                "avg_condition_count": 0,
            }
        )
        row.update(query_stats.get(db_id, {}))
        row["neo4j_node_count"] = neo4j_node_counts.get(db_id, 0)
        row["neo4j_relationship_count"] = neo4j_rel_counts.get(db_id, 0)
        row["neo4j_node_completion_ratio"] = (
            row["neo4j_node_count"] / row["expected_graph_nodes"] if row.get("expected_graph_nodes") else 0
        )
        row["neo4j_relationship_completion_ratio"] = (
            row["neo4j_relationship_count"] / row["expected_graph_relationships"]
            if row.get("expected_graph_relationships")
            else 0
        )
        rows.append(row)

    fieldnames = list(rows[0].keys())
    with open(args.output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    write_markdown(rows, args.output_md)
    print(f"wrote {args.output_csv}")
    print(f"wrote {args.output_md}")


if __name__ == "__main__":
    main()
