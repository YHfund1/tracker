#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Merge AI-produced central-bank statement deltas into data/cb-statements.json.

Expected AI output:
  cache/ai_outputs/cb_delta.json

The AI should only process the latest small packet in cache/ai_inputs/cb_new_items.json.
This script keeps the full historical JSON merge deterministic and cheap.
"""

import json
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "data" / "cb-statements.json"
DELTA_PATH = ROOT / "cache" / "ai_outputs" / "cb_delta.json"


def load_json(path: Path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def normalize_points(points):
    if not isinstance(points, list):
        return []
    normalized = []
    seen = set()
    for point in points:
        text = str(point).strip()
        if text and text not in seen:
            normalized.append(text)
            seen.add(text)
    return normalized


def merge_items(existing_items, delta_items):
    merged = {}

    for item in existing_items:
        key = (item.get("date", ""), item.get("official", ""))
        merged[key] = item

    added = 0
    updated = 0

    for item in delta_items:
        date = item.get("date", "")
        official = item.get("official", "")
        if not date or not official:
            continue

        key = (date, official)
        item["points"] = normalize_points(item.get("points", []))

        if key in merged:
            current = merged[key]
            current_points = normalize_points(current.get("points", []))
            combined_points = current_points[:]
            for point in item["points"]:
                if point not in combined_points:
                    combined_points.append(point)
            current["points"] = combined_points
            current["mergedCount"] = max(
                int(current.get("mergedCount", 1) or 1),
                int(item.get("mergedCount", 1) or 1),
            )
            current["link"] = current.get("link") or item.get("link", "")
            updated += 1
        else:
            merged[key] = {
                "date": date,
                "bank": item.get("bank", ""),
                "official": official,
                "title": item.get("title", ""),
                "mergedCount": int(item.get("mergedCount", 1) or 1),
                "points": item["points"],
                "link": item.get("link", ""),
            }
            added += 1

    items = sorted(merged.values(), key=lambda x: x.get("date", ""), reverse=True)
    return items, added, updated


def main():
    data = load_json(DATA_PATH, {"items": []})
    if not DELTA_PATH.exists():
        raise SystemExit(f"未找到AI输出: {DELTA_PATH}")
    delta = load_json(DELTA_PATH, {"items": []})
    delta_items = delta.get("items", [])

    if not isinstance(delta_items, list):
        raise SystemExit("cb_delta.json 的 items 必须是数组")

    items, added, updated = merge_items(data.get("items", []), delta_items)
    output = {
        "fetchTime": datetime.now().isoformat(timespec="seconds"),
        "totalItems": len(items),
        "items": items,
    }

    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"[OK] merged cb delta: +{added}, updated {updated}, total {len(items)}")
    print(f"[OK] wrote {DATA_PATH}")


if __name__ == "__main__":
    main()
