#!/usr/bin/env python3
"""Download Neo4j JDBC full bundle used by the SQL2Cypher translator."""

from __future__ import annotations

from pathlib import Path

import requests


VERSION = "6.13.1"
URL = (
    "https://repo1.maven.org/maven2/org/neo4j/neo4j-jdbc-full-bundle/"
    f"{VERSION}/neo4j-jdbc-full-bundle-{VERSION}.jar"
)
OUT = Path(f"/Users/leamonzea/Desktop/Rel2KG/baselines/neo4j_etl/neo4j-jdbc-full-bundle-{VERSION}.jar")


def main() -> None:
    response = requests.get(URL, stream=True, timeout=120)
    response.raise_for_status()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("wb") as handle:
        for chunk in response.iter_content(1024 * 1024):
            if chunk:
                handle.write(chunk)
    print(OUT)
    print(OUT.stat().st_size)


if __name__ == "__main__":
    main()
