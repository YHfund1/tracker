#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Merge AI-produced country oil-news deltas into data/oil_news.json.

Expected AI output:
  cache/ai_outputs/oil_news_delta.json
"""

import json
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "data" / "oil_news.json"
DELTA_PATH = ROOT / "cache" / "ai_outputs" / "oil_news_delta.json"
COUNTRIES = ["沙特阿拉伯", "伊朗", "伊拉克", "阿联酋", "科威特", "卡塔尔", "阿曼", "巴林"]


def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def key(update):
    return (
        update.get("date", ""),
        update.get("title", "").strip().lower(),
    )


def main():
    data = load_json(DATA_PATH, {"countries": {}})
    if not DELTA_PATH.exists():
        raise SystemExit(f"未找到AI输出: {DELTA_PATH}")
    delta = load_json(DELTA_PATH, {"countries": {}})

    for country in COUNTRIES:
        data.setdefault("countries", {}).setdefault(country, {"updates": []})

    added = 0
    for country, payload in delta.get("countries", {}).items():
        if country not in COUNTRIES:
            continue
        existing = data["countries"].setdefault(country, {"updates": []}).setdefault("updates", [])
        seen = {key(item) for item in existing}
        for item in payload.get("updates", []):
            normalized = {
                "date": str(item.get("date", ""))[:10],
                "title": str(item.get("title", "")).strip(),
                "content": str(item.get("content", "")).strip(),
            }
            if not normalized["date"] or not normalized["title"] or not normalized["content"]:
                continue
            item_key = key(normalized)
            if item_key in seen:
                continue
            existing.append(normalized)
            seen.add(item_key)
            added += 1
        existing.sort(key=lambda x: x.get("date", ""), reverse=True)

    data["fetchTime"] = datetime.now().isoformat(timespec="seconds")
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    DATA_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] merged oil news delta: +{added}")
    print(f"[OK] wrote {DATA_PATH}")


if __name__ == "__main__":
    main()
