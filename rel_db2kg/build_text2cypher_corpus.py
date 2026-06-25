#!/usr/bin/env python3
"""Build a text-to-Cypher corpus from Rel2KG query-level evaluation results."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any


DEFAULT_SOURCE = Path("/Users/leamonzea/Desktop/Rel2KG/rel_db2kg/query_details.jsonl")
DEFAULT_OUT_DIR = Path("/Users/leamonzea/Desktop/Rel2KG/rel_db2kg/text2cypher_corpus")


def load_correct_examples(source: Path) -> list[dict[str, Any]]:
    examples: list[dict[str, Any]] = []
    with source.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("status") != "correct" or not row.get("cypher"):
                continue
            split = row.get("split", "unknown")
            db_id = row.get("db_id", "unknown")
            split_index = row.get("split_index", line_no)
            examples.append(
                {
                    "id": f"{split}:{db_id}:{split_index}",
                    "task": "text2cypher",
                    "source": "rel2kg_sql2cypher_correct_execution",
                    "split": split,
                    "db_id": db_id,
                    "question": row.get("question"),
                    "sql": row.get("sql"),
                    "cypher": row.get("cypher"),
                    "sql_result_size": row.get("sql_result_size"),
                    "cypher_result_size": row.get("cypher_result_size"),
                    "ordinal": row.get("ordinal"),
                }
            )
    return examples


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "id",
        "task",
        "source",
        "split",
        "db_id",
        "question",
        "sql",
        "cypher",
        "sql_result_size",
        "cypher_result_size",
        "ordinal",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def config_rows(out_dir: Path) -> list[dict[str, str]]:
    all_path = out_dir / "text2cypher_correct_corpus.jsonl"
    train_path = out_dir / "text2cypher_train.jsonl"
    dev_path = out_dir / "text2cypher_dev.jsonl"
    output_root = out_dir / "model_outputs"
    return [
        {
            "provider": "Qwen",
            "model": "qwen-plus",
            "api_key_env": "QWEN_API_KEY",
            "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
            "input_data": str(all_path),
            "train_data": str(train_path),
            "eval_data": str(dev_path),
            "output_dir": str(output_root / "qwen"),
            "task": "text2cypher",
        },
        {
            "provider": "DeepSeek",
            "model": "deepseek-chat",
            "api_key_env": "DEEPSEEK_API_KEY",
            "base_url": "https://api.deepseek.com",
            "input_data": str(all_path),
            "train_data": str(train_path),
            "eval_data": str(dev_path),
            "output_dir": str(output_root / "deepseek"),
            "task": "text2cypher",
        },
        {
            "provider": "OpenAI GPT",
            "model": "gpt-4.1",
            "api_key_env": "OPENAI_API_KEY",
            "base_url": "https://api.openai.com/v1",
            "input_data": str(all_path),
            "train_data": str(train_path),
            "eval_data": str(dev_path),
            "output_dir": str(output_root / "gpt"),
            "task": "text2cypher",
        },
    ]


def write_config(out_dir: Path) -> None:
    rows = config_rows(out_dir)
    fields = [
        "provider",
        "model",
        "api_key_env",
        "base_url",
        "input_data",
        "train_data",
        "eval_data",
        "output_dir",
        "task",
    ]
    with (out_dir / "text2cypher_task_config.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    (out_dir / "text2cypher_task_config.json").write_text(
        json.dumps({"models": rows}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    lines = [
        "\\begin{table*}[htbp]",
        "\\centering",
        "\\small",
        "\\caption{Task parameter configuration for text-to-Cypher generation.}",
        "\\label{tab:text2cypher_task_config}",
        "\\resizebox{\\textwidth}{!}{%",
        "\\begin{tabular}{llllp{0.30\\textwidth}p{0.24\\textwidth}}",
        "\\toprule",
        "Provider & Model & API key & Base URL & Input data & Output directory \\\\",
        "\\midrule",
    ]
    for row in rows:
        values = [
            row["provider"],
            row["model"],
            "\\texttt{" + row["api_key_env"].replace("_", "\\_") + "}",
            "\\texttt{" + row["base_url"].replace("_", "\\_") + "}",
            "\\texttt{" + row["input_data"].replace("_", "\\_") + "}",
            "\\texttt{" + row["output_dir"].replace("_", "\\_") + "}",
        ]
        lines.append(" & ".join(values) + " \\\\")
    lines.extend(["\\bottomrule", "\\end{tabular}}", "\\end{table*}", ""])
    (out_dir / "text2cypher_task_config.tex").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    examples = load_correct_examples(args.source)
    train = [row for row in examples if row["split"] in {"train_spider", "train_others"}]
    dev = [row for row in examples if row["split"] == "dev"]

    write_jsonl(args.out_dir / "text2cypher_correct_corpus.jsonl", examples)
    write_jsonl(args.out_dir / "text2cypher_train.jsonl", train)
    write_jsonl(args.out_dir / "text2cypher_dev.jsonl", dev)
    write_csv(args.out_dir / "text2cypher_correct_corpus.csv", examples)
    write_config(args.out_dir)

    summary = {
        "source": str(args.source),
        "output_dir": str(args.out_dir),
        "total_correct_examples": len(examples),
        "split_counts": dict(Counter(row["split"] for row in examples)),
        "database_count": len({row["db_id"] for row in examples}),
        "files": {
            "all_jsonl": str(args.out_dir / "text2cypher_correct_corpus.jsonl"),
            "train_jsonl": str(args.out_dir / "text2cypher_train.jsonl"),
            "dev_jsonl": str(args.out_dir / "text2cypher_dev.jsonl"),
            "csv": str(args.out_dir / "text2cypher_correct_corpus.csv"),
            "config_csv": str(args.out_dir / "text2cypher_task_config.csv"),
            "config_json": str(args.out_dir / "text2cypher_task_config.json"),
            "config_tex": str(args.out_dir / "text2cypher_task_config.tex"),
        },
    }
    (args.out_dir / "text2cypher_corpus_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
