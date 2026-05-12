#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Merge AI-produced supply-chain deltas into data/supply-chain.json."""

import json
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "data" / "supply-chain.json"
DELTA_PATH = ROOT / "cache" / "ai_outputs" / "supply_chain_delta.json"


def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def energy_key(item):
    return (
        item.get("date", ""),
        item.get("region", ""),
        item.get("facility", ""),
    )


def chain_key(item):
    return (
        item.get("date", ""),
        item.get("region", ""),
        item.get("company", ""),
        item.get("industry", ""),
    )


def merge_list(existing, delta, key_fn):
    merged = {key_fn(item): item for item in existing}
    added = 0
    updated = 0
    for item in delta:
        key = key_fn(item)
        if not key[0] or not any(key[1:]):
            continue
        if key in merged:
            merged[key].update({k: v for k, v in item.items() if v})
            updated += 1
        else:
            merged[key] = item
            added += 1
    items = sorted(merged.values(), key=lambda x: x.get("date", ""), reverse=True)
    return items, added, updated


def main():
    data = load_json(DATA_PATH, {"energy": [], "chain": []})
    if not DELTA_PATH.exists():
        raise SystemExit(f"未找到AI输出: {DELTA_PATH}")
    delta = load_json(DELTA_PATH, {"energy": [], "chain": []})

    energy, energy_added, energy_updated = merge_list(data.get("energy", []), delta.get("energy", []), energy_key)
    chain, chain_added, chain_updated = merge_list(data.get("chain", []), delta.get("chain", []), chain_key)

    output = {
        "fetchTime": datetime.now().isoformat(timespec="seconds"),
        "energy": energy,
        "chain": chain,
    }
    DATA_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] merged supply-chain delta: energy +{energy_added}/{energy_updated} updated, chain +{chain_added}/{chain_updated} updated")
    print(f"[OK] wrote {DATA_PATH}")


if __name__ == "__main__":
    main()
