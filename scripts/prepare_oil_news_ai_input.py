#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prepare a compact oil-chart AI input packet from data/oil_news.json."""

import json
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "data" / "oil_news.json"
OUT_PATH = ROOT / "cache" / "ai_inputs" / "oil_news_recent.json"


def main():
    data = json.loads(DATA_PATH.read_text(encoding="utf-8")) if DATA_PATH.exists() else {"countries": {}}
    countries = {}
    for country, payload in data.get("countries", {}).items():
        countries[country] = {
            "recentUpdates": payload.get("updates", [])[:10]
        }

    packet = {
        "generatedAt": datetime.now().isoformat(timespec="seconds"),
        "purpose": "原油图谱国家动态增量AI输入；只添加近72小时新增事件，不要读取oil-chart.html全文。",
        "countries": countries,
        "aiOutputPath": "cache/ai_outputs/oil_news_delta.json",
        "aiOutputSchema": {
            "countries": {
                "国家名": {
                    "updates": [
                        {
                            "date": "YYYY-MM-DD",
                            "title": "一句话标题",
                            "content": "1-2句中文说明，包含来源和影响"
                        }
                    ]
                }
            }
        }
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    (ROOT / "cache" / "ai_outputs").mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(packet, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
