# SQL2Cypher-Li Fair Adapter

This directory contains the fair-adapter evaluator for the Li/UNSW SQL2Cypher baseline.

The adapter keeps the SQL2Cypher-Li graph model and adds benchmark compatibility for alias binding, column resolution, aggregate rendering, predicates, grouping, HAVING, ORDER BY, and LIMIT. It does not use Rel2KG-specific schema repair, primary-key reconstruction, foreign-key repair, namespace repair, or data-type correction.

Run from the Rel2KG working directory with Neo4j available:

```bash
/Users/leamonzea/Desktop/myenv/bin/python /Users/leamonzea/Desktop/Rel2KG/sql2cypher_fair_adapter/evaluate_li_sql2cypher_fair_adapter_ea_vs.py
```
