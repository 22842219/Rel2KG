# Baseline Database Selection

## Selection Criteria

- Has Spider SQL queries in `xlangai/spider`.
- Has non-trivial schema: at least 3 tables and foreign-key relationships.
- Has good data completeness for the main set.
- Covers easy, medium, hard, and stress-scale cases.
- Already exists in the built Neo4j `rel2kg` graph.

## Smoke Set

Use these first to verify Neo4j ETL, SQL2Cypher, and R2G end-to-end.

| db_id | tables | fks | rows | queries | schema_score | sql_score | completeness |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| department_management | 3 | 2 | 30 | 16 | 41.51 | 27.38 | 100.00 |
| concert_singer | 4 | 3 | 31 | 45 | 56.60 | 27.59 | 100.00 |
| car_1 | 6 | 5 | 890 | 92 | 84.50 | 36.33 | 100.00 |

## Main Benchmark Set

Use these for the primary comparison across all three baselines.

| db_id | tables | fks | rows | queries | schema_score | sql_score | completeness | neo4j_nodes | neo4j_rels |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
<!-- | world_1 | 3 | 2 | 5302 | 120 | 71.57 | 33.99 | 99.83 | 5303 | 5063 | -->
| store_1 | 11 | 11 | 15607 | 112 | 176.41 | 32.59 | 99.29 | 6902 | 24529 |
<!-- | college_2 | 11 | 13 | 34620 | 170 | 175.28 | 30.26 | 100.00 | 4320 | 6450 | -->
| college_3 |
| hospital_1 | 15 | 23 | 132 | 100 | 227.11 | 33.66 | 99.82 | 100 | 146 |
| tracking_software_problems | 6 | 7 | 65 | 48 | 89.13 | 42.25 | 100.00 | 65 | 105 |
<!-- | sakila_1 | 16 | 21 | 46263 | 82 | 263.36 | 29.70 | 93.61 | 39805 | 77026 | -->

<!-- ## Stress Set

Use these after the main set to test graph construction and query execution scalability.

| db_id | tables | fks | rows | queries | schema_score | sql_score | completeness | neo4j_nodes | neo4j_rels |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| formula_1 | 13 | 17 | 88380 | 80 | 246.13 | 40.14 | 96.15 | 88380 | 204404 |
| soccer_1 | 6 | 5 | 196817 | 14 | 172.43 | 31.82 | 99.78 | 196823 | 185625 |
| wta_1 | 3 | 3 | 531377 | 62 | 124.09 | 21.21 | 83.33 | 531099 | 510658 |
| baseball_1 | 26 | 20 | 553693 | 82 | 561.21 | 38.23 | 75.00 | 420758 | 595024 | -->

## Exclusions

- `academic`, `geo`, `imdb`, `new_concert_singer`, `new_orchestra`, `new_pets_1`, `restaurants`, `scholar`, and `yelp` have SQLite databases but no SQL queries in the loaded Spider split, so they are not suitable for SQL2Cypher/R2G query baselines.
- `baseball_1` and `wta_1` are useful for stress tests, but not recommended for first-pass baseline debugging because they are large and have lower completeness scores.
