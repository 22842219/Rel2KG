#!/usr/bin/env python3
"""Evaluate SQL2Cypher-Li with a lightweight fair adapter.

The adapter keeps the SQL2Cypher-Li graph model (one node per table row on the
hosted UNSWSQL2CypherNode graph) and only fixes benchmark compatibility issues:
alias binding, column qualification, aggregate expression rendering, predicates,
ordering, and limits. It does not use Rel2KG-specific schema repair or namespace
labels.
"""

from __future__ import annotations

import csv
import json
import math
import os
import re
import sqlite3
import sys
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


REPO_ROOT = Path("/Users/leamonzea/Desktop/Rel2KG")
WORKLOAD_PATH = Path("sql_to_cypher_eval/query_details.jsonl")
OUT_DIR = Path("sql2cypher_li_fair_adapter_eval")
SUMMARY_JSON = OUT_DIR / "ea_vs_summary.json"
SUMMARY_CSV = OUT_DIR / "ea_vs_summary.csv"
DETAIL_JSONL = OUT_DIR / "query_details.jsonl"
SELECTED_DBS = ["store_1", "college_3", "hospital_1", "tracking_software_problems"]
META_PREFIXES = ("_sql2cypher", "_source", "_rel2kg")

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from rel_db2kg.py_compat import patch_legacy_collections

patch_legacy_collections()

from moz_sql_parser import parse
from py2neo import Graph


def qlabel(label: str) -> str:
    return "`" + str(label).replace("`", "``") + "`"


def qstr(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def var_name(name: str) -> str:
    value = re.sub(r"[^0-9A-Za-z_]", "_", str(name))
    if not value or value[0].isdigit():
        value = "v_" + value
    return value


def load_workload() -> list[dict[str, Any]]:
    records = []
    selected = set(SELECTED_DBS)
    seen = set()
    with WORKLOAD_PATH.open("r", encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if row.get("db_id") not in selected:
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


def graph_conn() -> Graph:
    return Graph(
        os.environ.get("NEO4J_URI", "bolt://127.0.0.1:7687"),
        auth=(
            os.environ.get("NEO4J_USER", "neo4j"),
            os.environ.get("NEO4J_PASSWORD", "sql2cyphersql2cypher"),
        ),
        name=os.environ.get("NEO4J_DATABASE", "neo4j"),
    )


def clean_props(props: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in props.items()
        if not any(str(key).startswith(prefix) for prefix in META_PREFIXES)
    }


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def sqlite_type(values: list[Any]) -> str:
    non_null = [value for value in values if value is not None]
    if not non_null:
        return "TEXT"
    if all(isinstance(value, bool) or isinstance(value, int) for value in non_null):
        return "INTEGER"
    if all(isinstance(value, (bool, int, float)) for value in non_null):
        return "REAL"
    return "TEXT"


def reconstruct_sqlite_and_schema(graph: Graph, root: Path) -> tuple[dict[str, Path], dict[str, dict[str, set[str]]]]:
    paths: dict[str, Path] = {}
    schemas: dict[str, dict[str, set[str]]] = {}
    for db_id in SELECTED_DBS:
        rows = graph.run(
            """
            MATCH (n:UNSWSQL2CypherNode {_sql2cypher_db: $db_id})
            RETURN n._sql2cypher_table AS table, properties(n) AS props
            ORDER BY table
            """,
            db_id=db_id,
        ).data()
        by_table: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_table[row["table"]].append(clean_props(row["props"]))

        schemas[db_id] = {
            table: {col for item in table_rows for col in item}
            for table, table_rows in by_table.items()
        }
        db_dir = root / db_id
        db_dir.mkdir(parents=True, exist_ok=True)
        db_path = db_dir / f"{db_id}.sqlite"
        if db_path.exists():
            db_path.unlink()
        conn = sqlite3.connect(db_path)
        try:
            for table, table_rows in sorted(by_table.items()):
                columns = sorted(schemas[db_id][table])
                if not columns:
                    continue
                defs = [
                    f"{quote_ident(col)} {sqlite_type([item.get(col) for item in table_rows])}"
                    for col in columns
                ]
                conn.execute(f"CREATE TABLE {quote_ident(table)} ({', '.join(defs)})")
                placeholders = ", ".join(["?"] * len(columns))
                insert = (
                    f"INSERT INTO {quote_ident(table)} "
                    f"({', '.join(quote_ident(col) for col in columns)}) VALUES ({placeholders})"
                )
                conn.executemany(insert, [[item.get(col) for col in columns] for item in table_rows])
            conn.commit()
        finally:
            conn.close()
        paths[db_id] = db_path
    return paths, schemas


def from_aliases(from_clause: Any) -> tuple[list[dict[str, str]], list[Any]]:
    aliases: list[dict[str, str]] = []
    joins: list[Any] = []

    def add_table(item: Any) -> None:
        if isinstance(item, str):
            aliases.append({"table": item, "alias": item})
        elif isinstance(item, dict):
            if "join" in item:
                add_table(item["join"])
                if "on" in item:
                    joins.append(item["on"])
            elif "value" in item:
                aliases.append({"table": item["value"], "alias": item.get("name", item["value"])})

    if isinstance(from_clause, list):
        for part in from_clause:
            add_table(part)
    else:
        add_table(from_clause)
    return aliases, joins


class FairTranslator:
    def __init__(self, db_id: str, schema: dict[str, set[str]]):
        self.db_id = db_id
        self.schema = schema
        self.table_lookup = {table.lower(): table for table in schema}
        self.column_lookup = {
            table: {col.lower(): col for col in columns}
            for table, columns in schema.items()
        }
        self.alias_to_table: dict[str, str] = {}
        self.alias_to_var: dict[str, str] = {}
        self.select_aliases: dict[str, str] = {}

    def translate(self, parsed: dict[str, Any]) -> str:
        if "from" not in parsed:
            raise ValueError("missing FROM")
        aliases, join_conditions = from_aliases(parsed["from"])
        if not aliases:
            raise ValueError("empty FROM")
        for item in aliases:
            alias = str(item["alias"])
            self.alias_to_table[alias] = self.resolve_table(str(item["table"]))
            self.alias_to_var[alias] = var_name(alias)

        clauses = [
            "MATCH "
            + ", ".join(
                f"({self.alias_to_var[item['alias']]}:UNSWSQL2CypherNode:{qlabel(item['table'])} "
                f"{{_sql2cypher_db: {qstr(self.db_id)}}})"
                for item in ({**item, "table": self.resolve_table(str(item["table"]))} for item in aliases)
            )
        ]
        predicates = []
        for condition in join_conditions:
            predicates.append(self.expr(condition))
        if "where" in parsed:
            predicates.append(self.expr(parsed["where"]))
        predicates = [p for p in predicates if p]
        if predicates:
            clauses.append("WHERE " + " AND ".join(f"({p})" for p in predicates))

        select_key = "select_distinct" if "select_distinct" in parsed else "select"
        distinct = select_key == "select_distinct"
        select_items = self.select_items(parsed[select_key])
        has_agg = any(self.is_aggregate(item["expr"]) for item in select_items)
        group_items = self.group_items(parsed.get("groupby"))
        aux_aggs = self.auxiliary_aggregates(parsed)

        if has_agg or group_items:
            with_parts = []
            for item in group_items:
                with_parts.append(f"{self.expr(item)} AS {self.projection_alias(item)}")
            for item in select_items:
                if self.is_aggregate(item["expr"]):
                    with_parts.append(f"{self.expr(item['expr'])} AS {item['alias']}")
                elif item["alias"] not in [self.projection_alias(g) for g in group_items]:
                    with_parts.append(f"{self.expr(item['expr'])} AS {item['alias']}")
            for expr, alias in aux_aggs:
                with_parts.append(f"{self.expr(expr)} AS {alias}")
            if not with_parts:
                raise ValueError("empty aggregate projection")
            clauses.append("WITH " + ", ".join(dict.fromkeys(with_parts)))
            if "having" in parsed:
                clauses.append("WHERE " + self.expr_after_aggregation(parsed["having"]))
            return_parts = [item["alias"] for item in select_items]
        else:
            return_parts = [f"{self.expr(item['expr'])} AS {item['alias']}" for item in select_items]

        clauses.append("RETURN " + ("DISTINCT " if distinct else "") + ", ".join(return_parts))
        if "orderby" in parsed:
            clauses.append("ORDER BY " + self.order_by(parsed["orderby"]))
        if "limit" in parsed:
            clauses.append(f"LIMIT {int(parsed['limit'])}")
        return "\n".join(clauses)

    def select_items(self, data: Any) -> list[dict[str, Any]]:
        if isinstance(data, dict) and isinstance(data.get("value"), list):
            data = data["value"]
        raw_items = data if isinstance(data, list) else [data]
        items = []
        used: Counter[str] = Counter()
        for raw in raw_items:
            expr = raw.get("value") if isinstance(raw, dict) and "value" in raw else raw
            alias = raw.get("name") if isinstance(raw, dict) and raw.get("name") else self.projection_alias(expr)
            alias = var_name(alias)
            used[alias] += 1
            if used[alias] > 1:
                alias = f"{alias}_{used[alias]}"
            self.select_aliases[self.expr_key(expr)] = alias
            items.append({"expr": expr, "alias": alias})
        return items

    def group_items(self, data: Any) -> list[Any]:
        if not data:
            return []
        raw = data if isinstance(data, list) else [data]
        return [item.get("value") if isinstance(item, dict) and "value" in item else item for item in raw]

    def auxiliary_aggregates(self, parsed: dict[str, Any]) -> list[tuple[Any, str]]:
        found: dict[str, tuple[Any, str]] = {}

        def visit(expr: Any) -> None:
            if isinstance(expr, list):
                for item in expr:
                    visit(item)
            elif isinstance(expr, dict):
                if self.is_aggregate(expr):
                    key = self.expr_key(expr)
                    found.setdefault(key, (expr, self.projection_alias(expr)))
                for value in expr.values():
                    visit(value)

        if "having" in parsed:
            visit(parsed["having"])
        if "orderby" in parsed:
            visit(parsed["orderby"])
        return [(expr, alias) for expr, alias in found.values() if self.expr_key(expr) not in self.select_aliases]

    def order_by(self, data: Any) -> str:
        raw_items = data if isinstance(data, list) else [data]
        parts = []
        for raw in raw_items:
            expr = raw.get("value", raw) if isinstance(raw, dict) else raw
            sort = raw.get("sort", "asc").upper() if isinstance(raw, dict) else "ASC"
            key = self.expr_key(expr)
            rendered = self.select_aliases.get(key, self.expr(expr))
            parts.append(f"{rendered} {sort}" if sort else rendered)
        return ", ".join(parts)

    def projection_alias(self, expr: Any) -> str:
        if isinstance(expr, str):
            return expr.split(".")[-1]
        if isinstance(expr, dict) and len(expr) == 1:
            op, value = next(iter(expr.items()))
            if op in {"count", "sum", "avg", "min", "max"}:
                return op
            if op == "distinct":
                return self.projection_alias(value)
        return "expr"

    def expr_key(self, expr: Any) -> str:
        return json.dumps(expr, sort_keys=True, ensure_ascii=False)

    def is_aggregate(self, expr: Any) -> bool:
        return isinstance(expr, dict) and any(key in expr for key in ("count", "sum", "avg", "min", "max"))

    def resolve_table(self, table: str) -> str:
        return self.table_lookup.get(table.lower(), table)

    def resolve_column(self, table: str, column: str) -> str:
        return self.column_lookup.get(table, {}).get(column.lower(), column)

    def is_column_ref(self, value: str) -> bool:
        if value == "*":
            return True
        if "." in value:
            alias, col = value.split(".", 1)
            table = self.alias_to_table.get(alias)
            return bool(table and col.lower() in self.column_lookup.get(table, {}))
        return any(value.lower() in cols for cols in self.column_lookup.values())

    def field(self, value: str) -> str:
        if value == "*":
            return "*"
        if "." in value:
            alias, col = value.split(".", 1)
            table = self.alias_to_table.get(alias)
            resolved_col = self.resolve_column(table, col) if table else col
            return f"{self.alias_to_var.get(alias, var_name(alias))}.{resolved_col}"
        candidates = [
            alias
            for alias, table in self.alias_to_table.items()
            if value.lower() in self.column_lookup.get(table, {})
        ]
        alias = candidates[0] if len(candidates) == 1 else next(iter(self.alias_to_table))
        return f"{self.alias_to_var[alias]}.{self.resolve_column(self.alias_to_table[alias], value)}"

    def literal(self, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        return qstr(str(value))

    def expr(self, expr: Any) -> str:
        if isinstance(expr, str):
            return self.field(expr)
        if not isinstance(expr, dict):
            return self.literal(expr)
        if "literal" in expr:
            return self.literal(expr["literal"])
        if "value" in expr and len(expr) == 1:
            value = expr["value"]
            if isinstance(value, str) and not self.is_column_ref(value):
                return self.literal(value)
            return self.expr(value)
        if len(expr) != 1:
            raise ValueError(f"unsupported expression: {expr}")
        op, value = next(iter(expr.items()))
        if op in {"count", "sum", "avg", "min", "max"}:
            if isinstance(value, dict) and "distinct" in value:
                return f"{op}(DISTINCT {self.expr(value['distinct'])})"
            return f"{op}(*)" if value == "*" else f"{op}({self.expr(value)})"
        if op == "distinct":
            return "DISTINCT " + self.expr(value)
        if op in {"add", "sub", "mul", "div"}:
            symbol = {"add": "+", "sub": "-", "mul": "*", "div": "/"}[op]
            return f"({self.expr(value[0])} {symbol} {self.expr(value[1])})"
        if op in {"eq", "neq", "gt", "gte", "lt", "lte"}:
            symbol = {"eq": "=", "neq": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[op]
            left = self.expr(value[0])
            right_value = value[1]
            if isinstance(right_value, str) and not self.is_column_ref(right_value):
                right = self.literal(right_value)
            else:
                right = self.expr(right_value)
            return f"{left} {symbol} {right}"
        if op in {"and", "or"}:
            keyword = " AND " if op == "and" else " OR "
            return keyword.join(f"({self.expr(item)})" for item in value)
        if op == "in":
            left, right = value
            if isinstance(right, dict) and "literal" in right and isinstance(right["literal"], list):
                return f"{self.expr(left)} IN [{', '.join(self.literal(v) for v in right['literal'])}]"
            if isinstance(right, list):
                return f"{self.expr(left)} IN [{', '.join(self.expr(v) for v in right)}]"
            raise ValueError("subquery IN is not supported by fair adapter")
        if op == "nin":
            return f"NOT ({self.expr({'in': value})})"
        if op == "like":
            left, pattern = value
            raw = pattern.get("literal") if isinstance(pattern, dict) and "literal" in pattern else pattern
            regex = re.escape(str(raw)).replace("%", ".*").replace("_", ".")
            return f"{self.expr(left)} =~ {qstr('(?i)^' + regex + '$')}"
        if op == "between":
            left, low, high = value
            return f"({self.expr(left)} >= {self.expr(low)} AND {self.expr(left)} <= {self.expr(high)})"
        if op == "missing":
            return f"{self.expr(value)} IS NULL"
        if op == "exists":
            return f"{self.expr(value)} IS NOT NULL"
        raise ValueError(f"unsupported operator: {op}")

    def expr_after_aggregation(self, expr: Any) -> str:
        if isinstance(expr, dict) and self.is_aggregate(expr):
            return self.select_aliases.get(self.expr_key(expr), self.projection_alias(expr))
        if isinstance(expr, dict) and len(expr) == 1:
            op, value = next(iter(expr.items()))
            if op in {"eq", "neq", "gt", "gte", "lt", "lte"}:
                symbol = {"eq": "=", "neq": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[op]
                return f"{self.expr_after_aggregation(value[0])} {symbol} {self.expr_after_aggregation(value[1])}"
            if op in {"and", "or"}:
                keyword = " AND " if op == "and" else " OR "
                return keyword.join(f"({self.expr_after_aggregation(item)})" for item in value)
        return self.expr(expr)


def execute_sql(sqlite_paths: dict[str, Path], db_id: str, sql: str) -> list[tuple[Any, ...]]:
    conn = sqlite3.connect(sqlite_paths[db_id])
    conn.text_factory = lambda b: b.decode("utf-8", errors="replace")
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


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
    OUT_DIR.mkdir(exist_ok=True)
    graph = graph_conn()
    records = load_workload()
    sqlite_root = Path(tempfile.mkdtemp(prefix="sql2cypher_li_fair_sqlite_"))
    sqlite_paths, schemas = reconstruct_sqlite_and_schema(graph, sqlite_root)
    summary = {db_id: Counter() for db_id in SELECTED_DBS}

    with DETAIL_JSONL.open("w", encoding="utf-8") as detail_out:
        for ordinal, record in enumerate(records):
            db_id = record["db_id"]
            stats = summary[db_id]
            stats["total"] += 1
            detail = {**record, "ordinal": ordinal}
            try:
                sql_rows = normalize_rows(execute_sql(sqlite_paths, db_id, record["sql"]))
                stats["sql_executed"] += 1
            except Exception as exc:
                stats["sql_error"] += 1
                detail.update({"status": "sql_error", "failure_type": type(exc).__name__, "translated_cypher": None})
                detail_out.write(json.dumps(detail, ensure_ascii=False) + "\n")
                continue
            try:
                parsed = parse(record["sql"])
                json.dumps(parsed, ensure_ascii=False)
                stats["parsed"] += 1
            except Exception as exc:
                stats["parse_error"] += 1
                detail.update({"status": "parse_error", "failure_type": type(exc).__name__, "translated_cypher": None})
                detail_out.write(json.dumps(detail, ensure_ascii=False) + "\n")
                continue
            try:
                cypher = FairTranslator(db_id, schemas[db_id]).translate(parsed)
                stats["translated"] += 1
            except Exception as exc:
                stats["translation_error"] += 1
                detail.update(
                    {
                        "status": "translation_error",
                        "parsed_sql": parsed,
                        "failure_type": type(exc).__name__,
                        "failure_reason": str(exc),
                        "translated_cypher": None,
                    }
                )
                detail_out.write(json.dumps(detail, ensure_ascii=False) + "\n")
                continue
            try:
                cypher_rows = normalize_rows(graph.run(cypher).data())
                stats["cypher_executed"] += 1
            except Exception as exc:
                stats["cypher_error"] += 1
                detail.update(
                    {
                        "status": "cypher_error",
                        "parsed_sql": parsed,
                        "failure_type": type(exc).__name__,
                        "failure_reason": str(exc),
                        "translated_cypher": cypher,
                    }
                )
                detail_out.write(json.dumps(detail, ensure_ascii=False) + "\n")
                continue
            match = sql_rows == cypher_rows
            stats["correct" if match else "incorrect"] += 1
            detail.update(
                {
                    "status": "correct" if match else "incorrect",
                    "parsed_sql": parsed,
                    "translated_cypher": cypher,
                    "sql_result_sample": repr(sql_rows[:5]),
                    "cypher_result_sample": repr(cypher_rows[:5]),
                    "sql_result_size": len(sql_rows),
                    "cypher_result_size": len(cypher_rows),
                }
            )
            detail_out.write(json.dumps(detail, ensure_ascii=False) + "\n")

    rows = []
    totals = Counter()
    for db_id in SELECTED_DBS:
        stats = summary[db_id]
        totals.update(stats)
        valid_n = stats["cypher_executed"]
        failure_n = stats["parse_error"] + stats["translation_error"] + stats["cypher_error"]
        ea = stats["correct"] / valid_n if valid_n else 0.0
        vs = ea * (valid_n / (valid_n + failure_n)) if (valid_n + failure_n) else 0.0
        rows.append(
            {
                "db_id": db_id,
                **{key: stats[key] for key in [
                    "total", "sql_executed", "parsed", "translated", "cypher_executed",
                    "correct", "incorrect", "sql_error", "parse_error",
                    "translation_error", "cypher_error"
                ]},
                "valid_n": valid_n,
                "failure_n": failure_n,
                "execution_accuracy": ea,
                "valid_score": vs,
            }
        )
    valid_total = totals["cypher_executed"]
    failure_total = totals["parse_error"] + totals["translation_error"] + totals["cypher_error"]
    overall_ea = totals["correct"] / valid_total if valid_total else 0.0
    overall_vs = overall_ea * (valid_total / (valid_total + failure_total)) if valid_total + failure_total else 0.0
    output = {
        "method": "SQL2Cypher-Li + Fair Adapter",
        "target_neo4j_database": os.environ.get("NEO4J_DATABASE", "neo4j"),
        "sqlite_reconstruction_root": str(sqlite_root),
        "selected_databases": SELECTED_DBS,
        "definition": {
            "EA": "correct / successfully executable Cypher query pairs",
            "VS": "EA * N_valid / (N_valid + N_fail), where N_fail = parse + translation + Cypher execution failures",
        },
        "overall": {
            **{key: totals[key] for key in [
                "total", "sql_executed", "parsed", "translated", "cypher_executed",
                "correct", "incorrect", "sql_error", "parse_error",
                "translation_error", "cypher_error"
            ]},
            "valid_n": valid_total,
            "failure_n": failure_total,
            "execution_accuracy": overall_ea,
            "valid_score": overall_vs,
        },
        "per_database": rows,
    }
    SUMMARY_JSON.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    with SUMMARY_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return output


if __name__ == "__main__":
    result = evaluate()
    print(json.dumps(result["overall"], indent=2, ensure_ascii=False))
    print(json.dumps(result["per_database"], indent=2, ensure_ascii=False))
