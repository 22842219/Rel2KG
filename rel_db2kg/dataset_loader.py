"""Dataset loading helpers for benchmark query files."""

import json
import os


SPIDER_DATASET_NAME = "xlangai/spider"
SPIDER_SPLIT_MAP = {
    "train": "train",
    "dev": "validation",
    "validation": "validation",
}


def load_benchmark_split(data_folder, benchmark, split):
    local_file = os.path.join(data_folder, "{}.json".format(split))
    if os.path.exists(local_file):
        with open(local_file, "r", encoding="utf-8") as f:
            return json.load(f)

    if benchmark.lower() == "spider":
        return load_spider_split(split)

    raise FileNotFoundError("Can not find benchmark split file: {}".format(local_file))


def load_spider_split(split):
    try:
        from datasets import load_dataset
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "The 'datasets' package is required when Spider JSON files are not present. "
            "Install it with: pip install datasets"
        ) from exc

    hf_split = SPIDER_SPLIT_MAP.get(split)
    if hf_split is None:
        raise ValueError("Unsupported Spider split: {}".format(split))

    dataset = load_dataset(SPIDER_DATASET_NAME, split=hf_split)
    return [normalize_spider_record(record) for record in dataset]


def normalize_spider_record(record):
    return {
        "db_id": record["db_id"],
        "query": record["query"],
        "question": record["question"],
        "query_toks": record.get("query_toks", []),
        "query_toks_no_value": record.get("query_toks_no_value", []),
        "question_toks": record.get("question_toks", []),
    }
