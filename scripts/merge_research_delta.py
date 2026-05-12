#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Merge AI-produced research deltas into data/research_items.json.

Expected AI output:
  cache/ai_outputs/research_delta.json

The AI should only read cache/ai_inputs/research_candidates.json and return
selected, translated, scored items. This script handles history and dedupe.
"""

import json
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "data" / "research_items.json"
DELTA_PATH = ROOT / "cache" / "ai_outputs" / "research_delta.json"


def load_json(path: Path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def normalize_item(item):
    return {
        "id": str(item.get("id", "")).strip(),
        "date": str(item.get("date", ""))[:10],
        "source": str(item.get("source", "")).strip(),
        "source_zh": str(item.get("source_zh", "")).strip(),
        "source_type": str(item.get("source_type", "news")).strip(),
        "title_zh": str(item.get("title_zh", "")).strip(),
        "original_title": str(item.get("original_title", "")).strip(),
        "summary_zh": str(item.get("summary_zh", "")).strip(),
        "sentiment": str(item.get("sentiment", "neutral")).strip(),
        "relevance_score": int(item.get("relevance_score", 1) or 1),
        "link": str(item.get("link", "")).strip(),
    }


def item_key(item):
    link = item.get("link", "")
    if link:
        return ("link", link)
    return (
        "fallback",
        item.get("source", "").lower(),
        item.get("original_title", "").lower() or item.get("title_zh", "").lower(),
    )


def main():
    data = load_json(DATA_PATH, {"items": []})
    if not DELTA_PATH.exists():
        raise SystemExit(f"未找到AI输出: {DELTA_PATH}")
    delta = load_json(DELTA_PATH, {"items": []})
    delta_items = delta.get("items", [])
    if not isinstance(delta_items, list):
        raise SystemExit("research_delta.json 的 items 必须是数组")

    merged = {}
    for item in data.get("items", []):
        normalized = normalize_item(item)
        merged[item_key(normalized)] = normalized

    added = 0
    updated = 0
    for item in delta_items:
        normalized = normalize_item(item)
        if not normalized["title_zh"] or not normalized["summary_zh"]:
            continue
        key = item_key(normalized)
        if key in merged:
            merged[key].update({k: v for k, v in normalized.items() if v})
            updated += 1
        else:
            merged[key] = normalized
            added += 1

    items = sorted(
        merged.values(),
        key=lambda x: (x.get("date", ""), x.get("relevance_score", 0)),
        reverse=True,
    )

    output = {
        "fetchTime": datetime.now().isoformat(timespec="seconds"),
        "totalItems": len(items),
        "items": items,
    }
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"[OK] merged research delta: +{added}, updated {updated}, total {len(items)}")
    print(f"[OK] wrote {DATA_PATH}")


if __name__ == "__main__":
    main()
