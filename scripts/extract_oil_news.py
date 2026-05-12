#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Extract country update arrays from oil-chart.html into data/oil_news.json."""

import json
import re
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
HTML_PATH = ROOT / "oil-chart.html"
OUT_PATH = ROOT / "data" / "oil_news.json"


def extract_country_blocks(text):
    start = text.find("const countries = [")
    end = text.find("const ports = [", start)
    if start == -1 or end == -1:
        raise SystemExit("未找到 oil-chart.html 中的 countries/ports 数据段")
    return text[start:end]


def parse_updates(array_text):
    updates = []
    for match in re.finditer(
        r'\{\s*date:\s*"([^"]*)",\s*title:\s*"([^"]*)",\s*content:\s*"((?:[^"\\]|\\.)*)"\s*\}',
        array_text,
        re.S,
    ):
        updates.append({
            "date": match.group(1),
            "title": match.group(2).replace('\\"', '"'),
            "content": match.group(3).replace('\\"', '"'),
        })
    return updates


def main():
    text = HTML_PATH.read_text(encoding="utf-8")
    block = extract_country_blocks(text)
    country_positions = list(re.finditer(r'name:\s*"([^"]+)",\s*nameEn:\s*"([^"]+)"', block))

    countries = {}
    for index, match in enumerate(country_positions):
        name = match.group(1)
        name_en = match.group(2)
        chunk_end = country_positions[index + 1].start() if index + 1 < len(country_positions) else len(block)
        chunk = block[match.start():chunk_end]
        updates_match = re.search(r'updates:\s*\[(.*?)\]\s*\n\s*\}', chunk, re.S)
        countries[name] = {
            "nameEn": name_en,
            "updates": parse_updates(updates_match.group(1)) if updates_match else [],
        }

    output = {
        "fetchTime": datetime.now().isoformat(timespec="seconds"),
        "countries": countries,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] extracted {sum(len(v['updates']) for v in countries.values())} updates to {OUT_PATH}")


if __name__ == "__main__":
    main()
