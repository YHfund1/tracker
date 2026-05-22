#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从Polymarket获取数据并保存到JSON文件
HTML页面会自动从JSON文件加载数据，不需要重新生成HTML
"""

import requests
import json
import time
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List

# Polymarket APIs
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

# 当前用于页面展示的事件。优先选择未过期、仍有活跃市场的美伊/伊朗核谈判/霍尔木兹相关盘口。
EVENT_CONFIGS = [
    {
        "slug": "us-x-iran-permanent-peace-deal-by",
        "displayTitle": "美伊永久和平协议时间",
        "subtitle": "不同截止日期前达成永久和平协议的概率",
        "kind": "series",
    },
    {
        "slug": "us-obtains-iranian-enriched-uranium-by",
        "displayTitle": "美国取得伊朗浓缩铀时间",
        "subtitle": "美国在不同截止日期前取得伊朗浓缩铀的概率",
        "kind": "series",
    },
    {
        "slug": "iran-agrees-to-surrender-enriched-uranium-stockpile-by",
        "displayTitle": "伊朗同意交出浓缩铀库存时间",
        "subtitle": "伊朗在不同截止日期前同意交出浓缩铀库存的概率",
        "kind": "series",
    },
    {
        "slug": "trump-announces-us-blockade-of-hormuz-lifted-by",
        "displayTitle": "特朗普宣布解除霍尔木兹封锁时间",
        "subtitle": "美国宣布解除霍尔木兹封锁的时间窗口",
        "kind": "series",
    },
    {
        "slug": "strait-of-hormuz-traffic-returns-to-normal-by-end-of-may",
        "displayTitle": "霍尔木兹交通5月底前恢复正常",
        "subtitle": "海峡通行在5月底前恢复正常的概率",
        "kind": "simple",
    },
    {
        "slug": "strait-of-hormuz-traffic-returns-to-normal-by-end-of-june",
        "displayTitle": "霍尔木兹交通6月底前恢复正常",
        "subtitle": "海峡通行在6月底前恢复正常的概率",
        "kind": "simple",
    },
    {
        "slug": "will-the-us-invade-iran-before-2027",
        "displayTitle": "美国2027年前入侵伊朗",
        "subtitle": "美国在2027年前入侵伊朗的概率",
        "kind": "simple",
    },
    {
        "slug": "will-the-us-officially-declare-war-on-iran-by",
        "displayTitle": "美国正式对伊宣战时间",
        "subtitle": "美国在不同截止日期前正式对伊朗宣战的概率",
        "kind": "series",
    },
    {
        "slug": "where-will-the-next-us-iran-diplomatic-meeting-happen-455",
        "displayTitle": "下一次美伊外交会谈地点",
        "subtitle": "下一次合格美伊外交会谈地点的概率分布",
        "kind": "ranked",
    },
    {
        "slug": "iran-closes-its-airspace-by",
        "displayTitle": "伊朗关闭领空时间",
        "subtitle": "伊朗在不同截止日期前关闭领空的概率",
        "kind": "series",
    },
]

EVENT_SLUGS = [item["slug"] for item in EVENT_CONFIGS]

# 请求头
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Origin": "https://polymarket.com",
    "Referer": "https://polymarket.com/",
}


def get_event_by_slug(slug: str, max_retries: int = 2) -> Dict:
    """通过slug获取事件数据，带重试机制"""
    url = f"{GAMMA_API}/events"
    params = {"slug": slug, "_s": "slug"}

    for attempt in range(max_retries):
        try:
            time.sleep(0.3)  # 减少延迟
            resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                event = data[0]
                event_id = event.get("id")
                if event_id:
                    time.sleep(0.2)  # 减少延迟
                    detail_url = f"{GAMMA_API}/events/{event_id}"
                    detail_resp = requests.get(detail_url, headers=HEADERS, timeout=15)
                    if detail_resp.status_code == 200:
                        return detail_resp.json()
                return event
            return {}
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  获取失败 {slug} (尝试 {attempt+1}/{max_retries}): {e}")
                time.sleep(1)  # 失败后等待
            else:
                print(f"  获取事件失败 {slug}: {e}")
                return {}


def get_price_history(token_id: str, interval: str = "all", max_retries: int = 1) -> List[Dict]:
    """获取价格历史数据，带重试机制"""
    url = f"{CLOB_API}/prices-history"
    params = {"market": token_id, "interval": interval}

    for attempt in range(max_retries):
        try:
            time.sleep(0.1)  # 减少延迟
            resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            return data.get("history", [])
        except:
            if attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            # 尝试其他interval
            for alt_interval in ["max", "1d"]:
                try:
                    params["interval"] = alt_interval
                    resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
                    if resp.status_code == 200:
                        data = resp.json()
                        return data.get("history", [])
                except:
                    pass
            return []


def parse_json_field(field) -> List:
    """解析JSON字段"""
    if isinstance(field, str):
        try:
            return json.loads(field)
        except:
            return []
    elif isinstance(field, list):
        return field
    return []


def event_url(slug: str) -> str:
    """Return the public Polymarket event URL."""
    return f"https://polymarket.com/event/{slug}"


def get_event_config(slug: str) -> Dict:
    """Get display metadata for an event slug."""
    return next((item for item in EVENT_CONFIGS if item["slug"] == slug), {"slug": slug, "kind": "series"})


def is_active_market(market: Dict) -> bool:
    """Only keep markets that can still trade/render as current."""
    return bool(market.get("active", True)) and not bool(market.get("closed", False))


def fetch_all_events_data() -> Dict:
    """获取所有事件数据"""
    # 尝试加载之前保存的数据
    prev_data = {}
    try:
        if os.path.exists("data/polymarket_data.json"):
            with open("data/polymarket_data.json", "r", encoding="utf-8") as f:
                saved = json.load(f)
                prev_data = saved.get("events", {})
    except:
        pass

    all_data = {}

    for slug in EVENT_SLUGS:
        print(f"获取: {slug}")
        event = get_event_by_slug(slug)
        if not event:
            # 如果获取失败，使用之前保存的数据
            if slug in prev_data:
                print(f"  使用缓存数据")
                all_data[slug] = prev_data[slug]
            continue

        config = get_event_config(slug)
        event_id = event.get("id")
        event_title = event.get("title", "")
        markets = event.get("markets", [])

        markets_data = []
        for m in markets:
            if not is_active_market(m):
                continue

            question = m.get("question", "")
            outcomes = parse_json_field(m.get("outcomes"))
            outcome_prices = parse_json_field(m.get("outcomePrices"))
            clob_token_ids = parse_json_field(m.get("clobTokenIds"))
            volume = m.get("volume", "0")

            market_info = {
                "question": question,
                "slug": m.get("slug", ""),
                "outcomes": {},
                "volume": volume,
                "closed": m.get("closed", False),
                "active": m.get("active", True),
                "endDate": m.get("endDate") or m.get("endDateIso") or "",
            }

            for i, outcome_name in enumerate(outcomes):
                token_id = clob_token_ids[i] if i < len(clob_token_ids) else ""
                current_price = 0.0
                if i < len(outcome_prices):
                    try:
                        current_price = float(outcome_prices[i])
                    except:
                        pass

                # 获取价格历史
                price_history = []
                if token_id and str(outcome_name).lower() == "yes":
                    history = get_price_history(token_id)
                    for h in history:
                        try:
                            ts = h.get("t", 0)
                            price = h.get("p", 0)
                            beijing_tz = ZoneInfo("Asia/Shanghai")
                            dt = datetime.fromtimestamp(ts, tz=beijing_tz)
                            price_history.append({
                                "time": dt.strftime('%m-%d %H:%M'),
                                "timestamp": ts,
                                "price": round(price * 100, 2)
                            })
                        except:
                            pass
                    # 移除多余延迟

                market_info["outcomes"][outcome_name] = {
                    "currentPrice": round(current_price * 100, 2),
                    "priceHistory": price_history
                }

            markets_data.append(market_info)

        all_data[slug] = {
            "title": event_title,
            "displayTitle": config.get("displayTitle") or event_title,
            "subtitle": config.get("subtitle", ""),
            "kind": config.get("kind", "series"),
            "url": event_url(slug),
            "markets": markets_data
        }

    return all_data



def main():
    print("="*60)
    print("Polymarket 数据更新器")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    try:
        # 获取数据
        print("\n正在获取Polymarket数据...")
        data = fetch_all_events_data()

        # 保存JSON数据到data目录
        json_filename = "data/polymarket_data.json"
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump({
                "fetchedAt": datetime.now().isoformat(),
                "eventOrder": EVENT_SLUGS,
                "events": data
            }, f, ensure_ascii=False, indent=2)
        print(f"数据已保存: {json_filename}")
        print("HTML页面会自动从JSON文件加载数据，无需重新生成HTML")

        print("\n" + "="*60)
        print("完成!")
        print("="*60)
        return 0
    except Exception as e:
        print(f"\n[错误] 更新失败: {e}")
        import traceback
        traceback.print_exc()
        print("\n不标记为失败，继续执行")
        return 0


if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code)
