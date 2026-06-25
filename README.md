# Rel2KG

Rel2KG converts relational databases into a Neo4j property graph and translates SQL workloads into Cypher over that graph. The current code focuses on Spider-style SQLite benchmarks, SQL-to-Cypher execution evaluation, and Text-to-Cypher experiments built from successfully translated SQL examples.

## Main Components

- `rel_db2kg/schema2graph.py`: builds Neo4j graph nodes and relationships from SQLite databases. The updated builder supports explicit Neo4j URI/user/password/database options, batched node/relationship writes, batched graph clearing, and configurable database discovery.
- `rel_db2kg/sql2cypher.py`: translates parsed SQL into Cypher and includes a Spider train/dev execution evaluator that reports execution accuracy and valid score.
- `rel_db2kg/build_text2query_corpus.py`: creates Text-to-Cypher train/dev corpora from correct SQL-to-Cypher execution results.
- `rel_db2kg/run_text2cypher_qwen_experiment.py`: runs an OpenAI-compatible Qwen Text-to-Cypher generation experiment.
- `rel_db2kg/evaluate_text2cypher_predictions.py`: evaluates generated Cypher predictions by executing them against Neo4j and comparing with SQLite SQL results.
- `rel_db2kg/py_compat.py`: applies Python compatibility patches needed by legacy parser dependencies on newer Python versions.

Legacy MySQL/PostgreSQL conversion and UNSW parser files have been removed from this working tree. Use the Spider/SQLite and Neo4j workflows below.

## Requirements

- Python 3.10+ recommended
- Neo4j running locally or remotely
- Spider data with SQLite databases and `train_spider.json`, `train_others.json`, `dev.json`
- Python dependencies from `requirements.txt`

Install dependencies in a virtual environment:

```shell
cd <path-to-your-root>
python -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt
```

The current requirements include `datasets==5.0.0` for experiment/data utilities.

## Configuration

`config.ini` provides defaults used by the scripts:

```ini
[FILENAMES]
root = <path-to-your-root>
benchmark = spider
```

Neo4j settings can be passed as command-line arguments or environment variables:

```shell
export NEO4J_URI=bolt://127.0.0.1:7687
export NEO4J_USER=neo4j
export NEO4J_PASSWORD=<your-neo4j-password>
export NEO4J_DATABASE=rel2kg
```

`schema2graph.py` can also read a `.env` file at the project root. At minimum, provide:

```shell
GRAPH_PASSWORD=<your-neo4j-password>
```

Optional `.env` keys are `NEO4J_URI`, `NEO4J_USER`, and `NEO4J_DATABASE`.

## Data Layout

The default Spider path used by evaluation scripts is:

```text
/Rel2KG/raw_download/spider_data
```

Expected files:

```text
raw_download/spider_data/
  train_spider.json
  train_others.json
  dev.json
  database/
    <db_id>/
      <db_id>.sqlite
```

For graph construction, `schema2graph.py` searches these database folders in order unless `--db-folder` is supplied:

- `<root>/rel_db2kg/data/<benchmark>/database`
- `<root>/data/<benchmark>/database`
- `<root>/application/data/<benchmark>/database`
- `<root>/application/rel_db2kg/data/<benchmark>/database`

## Build The Neo4j Graph

Run graph construction from `rel_db2kg` so relative imports and `../config.ini` resolve correctly:

```shell
cd <path-to-your-root>
python schema2graph.py \
  --spider \
  --restart \
  --cased \
  --db-folder <path-to-the-loaded-database> \
  --neo4j-uri "$NEO4J_URI" \
  --neo4j-user "$NEO4J_USER" \
  --neo4j-password "$NEO4J_PASSWORD" \
  --neo4j-database "$NEO4J_DATABASE" \
  --batch-size 1000 \
  --delete-batch-size 10000
```

Notes:

- `--restart` clears the target Neo4j database in batches before loading.
- `--cased` preserves original database/table/property casing. Without it, labels and properties are lowercased.
- Relationships are created from foreign-key lookups using in-memory indexes instead of repeated Cypher matches, which is faster and avoids brittle value formatting.

## Evaluate SQL-to-Cypher On Spider

After the graph is loaded, run the Spider train/dev evaluator:

```shell
cd <path-to-your-rel_db2kg> 
python sql2cypher.py \
  --evaluate-spider \
  --spider-root <path-to-the-spider-database> \
  --output-dir <path-to-the-output-directory> \
  --neo4j-uri "$NEO4J_URI" \
  --neo4j-user "$NEO4J_USER" \
  --neo4j-password "$NEO4J_PASSWORD" \
  --neo4j-database "$NEO4J_DATABASE" \
  --checkpoint-interval 250
```

Outputs:

- `ea_vs_summary.json`
- `ea_vs_summary.csv`
- `query_details.jsonl`

Reported metrics:

- `execution_accuracy`: correct queries divided by all SQL queries in the workload.
- `valid_score`: executable-only execution accuracy weighted by the executable/failure ratio.
- `legacy_execution_accuracy_executable_only`: correct queries divided by executable Cypher queries only.

## Build Text-to-Cypher Corpus

Create a Text-to-Cypher corpus from the correct SQL-to-Cypher results:

```shell
cd <path-to-your-rel_db2kg> 
python build_text2query_corpus.py \
  --source <path-to-your-rel_db2kg/rel2kg_spider_train_dev_sql2cypher_eval/query_details.jsonl> \
  --out-dir <path-to-your-rel_db2kg/text2query_corpus>
```

Outputs include:

- `text2query_correct_corpus.jsonl`
- `text2query_train.jsonl`
- `text2query_dev.jsonl`
- `text2query_correct_corpus.csv`
- `text2query_task_config.json`
- `text2query_corpus_summary.json`

<!-- ## Run Qwen Text-to-Cypher Generation

Set the API key environment variable expected by the script:

```shell
export QWEN_API_KEY=<your-qwen-api-key>
```

Run generation:

```shell
cd <path-to-your-rel_db2kg> 
python run_text2cypher_qwen_experiment.py \
  --corpus-dir <path-to-your-rel_db2kg/text2query_corpus> \
  --output-dir <path-to-your-rel_db2kg/text2query_corpus/model_outputs/qwen3-coder-plus> \
  --model qwen3-coder-plus \
  --max-few-shot 3 \
  --temperature 0 \
  --resume
```

The script calls an OpenAI-compatible chat completions endpoint. Defaults target DashScope:

```text
https://dashscope.aliyuncs.com/compatible-mode/v1
```

Use `--base-url`, `--api-key-env`, `--limit`, `--timeout`, `--retries`, and `--sleep` to adapt the run.

Outputs:

- `text2cypher_predictions.jsonl`
- `text2cypher_summary.json`

## Evaluate Text-to-Cypher Predictions

Evaluate generated Cypher by execution:

```shell
cd /Users/leamonzea/Desktop/Rel2KG/rel_db2kg
python evaluate_text2cypher_predictions.py \
  --predictions /Users/leamonzea/Desktop/Rel2KG/rel_db2kg/text2query_corpus/model_outputs/qwen3-coder-plus/text2cypher_predictions.jsonl \
  --spider-root /Users/leamonzea/Desktop/Rel2KG/raw_download/spider_data \
  --neo4j-uri "$NEO4J_URI" \
  --neo4j-user "$NEO4J_USER" \
  --neo4j-password "$NEO4J_PASSWORD" \
  --neo4j-database "$NEO4J_DATABASE"
```

If model output uses unquoted dotted labels such as `:concert_singer.singer`, rerun with:

```shell
python evaluate_text2cypher_predictions.py --quote-dotted-labels
```

Outputs are written next to the prediction file:

- `text2cypher_execution_details_raw.jsonl`
- `text2cypher_execution_summary_raw.json`
- or `*_quote_dotted_labels.*` when `--quote-dotted-labels` is used. -->

## Troubleshooting

- If `schema2graph.py` reports that no SQLite databases were found, pass `--db-folder` explicitly.
- If Neo4j authentication fails, pass `--neo4j-password` directly or set `NEO4J_PASSWORD` / `GRAPH_PASSWORD`.
- If generated Cypher fails on dotted labels, use `--quote-dotted-labels` during Text-to-Cypher evaluation.
- If the legacy SQL parser fails on newer Python versions, ensure scripts import and call `rel_db2kg.py_compat.patch_legacy_collections()` before `moz_sql_parser` is imported.
