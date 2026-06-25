#!/usr/bin/env python3
"""Download the official Spider dataset zip from the Yale Spider page."""

from __future__ import annotations

import argparse
import html
import re
from pathlib import Path
from urllib.parse import urlencode

import requests


SPIDER_FILE_ID = "1403EGqzIDoHMdQF4c9Bkyl7dZLZ5Wt6J"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        default="/Users/leamonzea/Desktop/Rel2KG/baselines/neo4j_etl/raw_download",
    )
    args = parser.parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    output = out_dir / "spider_data.zip"

    url = f"https://drive.google.com/uc?export=download&id={SPIDER_FILE_ID}"
    session = requests.Session()
    response = session.get(url, stream=True)
    token = None
    for key, value in response.cookies.items():
        if key.startswith("download_warning"):
            token = value
            break
    if token:
        response = session.get(url + f"&confirm={token}", stream=True)
    content_type = response.headers.get("content-type", "")
    if "text/html" in content_type:
        text = response.text
        action_match = re.search(r'<form[^>]+id="download-form"[^>]+action="([^"]+)"', text)
        inputs = dict(
            (html.unescape(name), html.unescape(value))
            for name, value in re.findall(r'<input[^>]+name="([^"]+)"[^>]+value="([^"]*)"', text)
        )
        if not action_match or not inputs:
            raise RuntimeError("Could not parse Google Drive confirmation form")
        response = session.get(html.unescape(action_match.group(1)) + "?" + urlencode(inputs), stream=True)
    response.raise_for_status()
    with output.open("wb") as handle:
        for chunk in response.iter_content(1024 * 1024):
            if chunk:
                handle.write(chunk)
    print(output)
    print(output.stat().st_size)
    print(response.headers.get("content-type"))


if __name__ == "__main__":
    main()
