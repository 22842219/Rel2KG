# Spider Complexity Report

## Scope

- Databases: 169
- SQL queries: 8034
- Average schema complexity score: 75.95
- Average SQL complexity score: 26.75
- Average data completeness score: 96.40

## Metric Definitions

- `schema_complexity_score`: weighted heuristic using table count, column count, foreign-key count, foreign-key columns, and row-volume scale.
- `data_completeness_score`: weighted heuristic using non-null value ratio, non-empty table ratio, primary-key table ratio, and SQLite existence.
- `avg_sql_complexity_score`: weighted heuristic using SQL token length, joins, nested selects, set operations, grouping, ordering, having, where, conditions, aggregations, and limits.
- `expected_graph_nodes` / `expected_graph_relationships`: estimated from the current Rel2KG schema-to-graph rules, not from raw relational rows only.
- `neo4j_*_completion_ratio`: observed Neo4j count divided by expected Rel2KG graph count when Neo4j counts are supplied.

## Top Schema Complexity

| rank | db_id | schema_score | tables | columns | rows | fks |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | baseball_1 | 561.21 | 26 | 352 | 553693 | 20 |
| 2 | cre_Drama_Workshop_Groups | 271.60 | 18 | 99 | 237 | 24 |
| 3 | sakila_1 | 263.36 | 16 | 89 | 46263 | 21 |
| 4 | formula_1 | 246.13 | 13 | 94 | 88380 | 17 |
| 5 | hospital_1 | 227.11 | 15 | 68 | 132 | 23 |
| 6 | assets_maintenance | 198.36 | 14 | 64 | 191 | 18 |
| 7 | chinook_1 | 176.41 | 11 | 64 | 15607 | 11 |
| 8 | store_1 | 176.41 | 11 | 64 | 15607 | 11 |
| 9 | college_2 | 175.28 | 11 | 46 | 34620 | 13 |
| 10 | cre_Theme_park | 172.99 | 16 | 52 | 172 | 14 |

## Top SQL Complexity

| rank | db_id | avg_sql_score | max_sql_score | queries | easy | medium | hard | extra |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | tracking_software_problems | 42.25 | 119.00 | 48 | 16 | 10 | 10 | 12 |
| 2 | formula_1 | 40.14 | 108.00 | 80 | 20 | 16 | 30 | 14 |
| 3 | small_bank_1 | 39.02 | 103.00 | 50 | 8 | 12 | 24 | 6 |
| 4 | icfp_1 | 38.85 | 71.00 | 66 | 20 | 0 | 42 | 4 |
| 5 | club_1 | 38.61 | 62.50 | 70 | 22 | 0 | 44 | 4 |
| 6 | baseball_1 | 38.23 | 117.00 | 82 | 12 | 22 | 42 | 6 |
| 7 | music_2 | 37.34 | 72.00 | 100 | 20 | 26 | 44 | 10 |
| 8 | pets_1 | 37.07 | 101.50 | 42 | 14 | 10 | 10 | 8 |
| 9 | tracking_grants_for_research | 37.05 | 103.50 | 78 | 20 | 16 | 32 | 10 |
| 10 | car_1 | 36.33 | 114.00 | 92 | 26 | 22 | 34 | 10 |

## Lowest Data Completeness

| rank | db_id | completeness | non_null | non_empty_tables | pk_table_ratio | empty_tables |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | geo | 71.43 | 1.000 | 0.000 | 0.857 | 7 |
| 2 | academic | 73.33 | 1.000 | 0.000 | 0.933 | 15 |
| 3 | imdb | 73.44 | 1.000 | 0.000 | 0.938 | 16 |
| 4 | baseball_1 | 75.00 | 1.000 | 1.000 | 0.000 | 0 |
| 5 | music_2 | 75.00 | 1.000 | 0.000 | 1.000 | 7 |
| 6 | restaurants | 75.00 | 1.000 | 0.000 | 1.000 | 3 |
| 7 | scholar | 75.00 | 1.000 | 0.000 | 1.000 | 10 |
| 8 | yelp | 75.00 | 1.000 | 0.000 | 1.000 | 7 |
| 9 | real_estate_properties | 79.06 | 0.544 | 1.000 | 0.800 | 0 |
| 10 | dorm_1 | 80.00 | 1.000 | 1.000 | 0.200 | 0 |
