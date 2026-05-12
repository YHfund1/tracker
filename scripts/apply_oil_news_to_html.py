#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Apply data/oil_news.json updates back into oil-chart.html."""

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
HTML_PATH = ROOT / "oil-chart.html"
DATA_PATH = ROOT / "data" / "oil_news.json"


def js_string(value):
    return json.dumps(str(value or ""), ensure_ascii=False)


def render_updates(updates, indent="                        "):
    lines = []
    for item in updates:
        lines.append(
            f'{indent}{{ date: {js_string(item.get("date"))}, title: {js_string(item.get("title"))}, content: {js_string(item.get("content"))} }}'
        )
    return ",\n".join(lines)


def replace_country_updates(text, country, updates):
    name_pattern = re.escape(f'name: "{country}"')
    start_match = re.search(name_pattern, text)
    if not start_match:
        print(f"[WARN] country not found: {country}")
        return text

    next_country = re.search(r'\n\s*\{\s*\n\s*name:\s*"[^"]+",\s*nameEn:', text[start_match.end():])
    end_pos = start_match.end() + next_country.start() if next_country else text.find("const ports = [", start_match.end())
    chunk = text[start_match.start():end_pos]

    updates_match = re.search(r'updates:\s*\[(.*?)\]\s*(\n\s*\})', chunk, re.S)
    if not updates_match:
        print(f"[WARN] updates array not found: {country}")
        return text

    replacement = "updates: [\n" + render_updates(updates) + "\n                    ]" + updates_match.group(2)
    new_chunk = chunk[:updates_match.start()] + replacement + chunk[updates_match.end():]
    return text[:start_match.start()] + new_chunk + text[end_pos:]


def main():
    text = HTML_PATH.read_text(encoding="utf-8")
    data = json.loads(DATA_PATH.read_text(encoding="utf-8"))
    for country, payload in data.get("countries", {}).items():
        text = replace_country_updates(text, country, payload.get("updates", []))
    HTML_PATH.write_text(text, encoding="utf-8")
    print(f"[OK] applied oil news to {HTML_PATH}")


if __name__ == "__main__":
    main()
