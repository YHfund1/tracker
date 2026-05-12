#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prepare compact supply-chain AI input from data/supply-chain.json."""

import json
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "data" / "supply-chain.json"
OUT_PATH = ROOT / "cache" / "ai_inputs" / "supply_chain_recent.json"


def main():
    data = json.loads(DATA_PATH.read_text(encoding="utf-8")) if DATA_PATH.exists() else {}
    packet = {
        "generatedAt": datetime.now().isoformat(timespec="seconds"),
        "purpose": "供应链跟踪增量AI输入；只用recentIndex去重，只输出新增/修正delta，不要输出完整supply-chain.json。",
        "recentIndex": {
            "energy": data.get("energy", [])[:30],
            "chain": data.get("chain", [])[:30],
        },
        "aiOutputPath": "cache/ai_outputs/supply_chain_delta.json",
        "aiOutputSchema": {
            "energy": [
                {
                    "date": "YYYY-MM-DD",
                    "region": "国家/地区",
                    "facility": "设施名称",
                    "type": "设施类型",
                    "owner": "所属企业",
                    "event": "事件描述",
                    "status": "当前状态",
                    "impact": "影响评估",
                    "source": "信息来源"
                }
            ],
            "chain": [
                {
                    "date": "YYYY-MM-DD",
                    "region": "国家/地区",
                    "company": "企业/机构",
                    "industry": "行业/产品",
                    "event": "事件描述",
                    "transmission": "影响传导链",
                    "scale": "停产/减产规模",
                    "chinaImpact": "对中国影响",
                    "recovery": "恢复预期",
                    "source": "信息来源"
                }
            ]
        }
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    (ROOT / "cache" / "ai_outputs").mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(packet, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
