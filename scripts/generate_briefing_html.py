#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate briefing.html from briefing_data.json."""

import html
import json
import re
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "briefing_data.json"
HTML_PATH = ROOT / "briefing.html"

TYPE_LABELS = {
    "military": "军事",
    "diplomacy": "外交",
    "shipping": "航运",
    "market": "市场",
    "energy": "能源",
}
POSITION_LABELS = {
    "us": "🇺🇸 美国/特朗普",
    "iran": "🇮🇷 伊朗",
    "gulf": "🇶🇦 海湾国家",
    "others": "🌐 其他",
}


def esc(value):
    return html.escape(str(value or ""), quote=True)


def load_data():
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def clean_summary(summary):
    return re.sub(r"^【[^】]+】\s*\d{4}年\d{1,2}月\d{1,2}日。?", "", summary or "").strip()


def render_progress(items):
    out = []
    for item in items:
        item_type = item.get("type", "market")
        out.append(f'''
            <div class="progress-item">
                <div class="title">{esc(item.get("title"))} <span class="type-badge type-{esc(item_type)}">{esc(TYPE_LABELS.get(item_type, item_type))}</span></div>
                <div class="content">{esc(item.get("content"))}</div>
            </div>''')
    return "\n".join(out)


def render_positions(positions):
    out = []
    for key, label in POSITION_LABELS.items():
        out.append(f'<div class="position-card"><h3>{esc(label)}</h3><p>{esc(positions.get(key, ""))}</p></div>')
    return "\n".join(out)


def render_supply(items):
    return "\n".join(
        f'''<div class="supply-item">
                <div class="sector">{esc(item.get("sector"))}</div>
                <div class="event">{esc(item.get("event"))}</div>
                <div class="impact">影响：{esc(item.get("impact"))}</div>
            </div>'''
        for item in items
    )


def render_bank_views(items):
    return "\n".join(
        f'<div class="bank-view"><div class="bank">{esc(item.get("bank"))}</div><div class="view">{esc(item.get("view"))}</div></div>'
        for item in items
    )


def render_market(data):
    labels = [
        ("brent", "布伦特原油", "$", "/桶"),
        ("wti", "WTI原油", "$", "/桶"),
        ("lme_aluminum", "LME铝(3M)", "$", "/吨"),
        ("urea", "尿素期货", "", "元/吨"),
        ("strait_pressure", "通行压力", "", "%"),
        ("ships_total", "海域船只", "", "艘"),
        ("passing_now", "正在通过", "", "艘"),
    ]
    out = []
    for key, label, prefix, suffix in labels:
        if key not in data:
            continue
        out.append(f'<div class="market-item"><div class="label">{esc(label)}</div><div class="value">{esc(prefix)}{esc(data.get(key))}{esc(suffix)}</div></div>')
    return "\n".join(out)


def main():
    data = load_data()
    date = data.get("date") or datetime.now().strftime("%Y-%m-%d")
    conflict_day = data.get("conflict_day")
    blockade_day = data.get("blockade_day")
    summary = clean_summary(data.get("summary", ""))
    sources = "、".join(data.get("sources", []))

    strait = data.get("strait_status", {})
    market = data.get("market_data", {})

    strait_status = strait.get("status", "")
    pressure = market.get("strait_pressure", "")
    passing_now = market.get("passing_now", "")
    ships_total = market.get("ships_total", "")

    html_text = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>中东地缘跟踪 - 美以伊冲突每日简报</title>
    <style>
        *{{margin:0;padding:0;box-sizing:border-box;}}
        body{{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;background:#f8fafc;color:#1e293b;line-height:1.8;}}
        .header{{background:#fff;color:#1e293b;padding:0;box-shadow:0 1px 3px rgba(0,0,0,.08);border-bottom:1px solid #e2e8f0;position:sticky;top:0;z-index:100;}}
        .header-main{{display:flex;align-items:center;max-width:1400px;margin:0 auto;padding:0 20px;position:relative;}}
        .header-logo{{display:flex;align-items:center;gap:10px;position:absolute;left:20px;}}
        .header-logo img{{height:30px;width:auto;display:block;}}
        .logo-text{{font-size:1.25rem;font-weight:600;color:#c41230;letter-spacing:1px;}}
        .header-nav{{display:flex;gap:0;margin:0 auto;}}
        .nav-btn{{color:#64748b;text-decoration:none;padding:12px 14px;font-size:.85rem;transition:all .2s;white-space:nowrap;border-bottom:3px solid transparent;}}
        .nav-btn:hover{{background:#f1f5f9;color:#991b1b;}}
        .nav-btn.active{{background:#f8fafc;color:#991b1b;border-bottom-color:#dc2626;font-weight:500;}}
        .container{{max-width:900px;margin:0 auto;padding:24px 20px;}}
        .briefing-header{{background:linear-gradient(135deg,#fee2e2 0%,#fecaca 100%);border:1px solid #dc2626;border-radius:12px;padding:24px;margin-bottom:24px;}}
        .briefing-header h1{{font-size:1.5rem;color:#991b1b;margin-bottom:8px;}}
        .briefing-header .date{{color:#b91c1c;font-size:.9rem;}}
        .section{{background:#fff;border-radius:12px;padding:24px;margin-bottom:20px;box-shadow:0 1px 3px rgba(0,0,0,.08);}}
        .section h2{{font-size:1.1rem;color:#1e293b;margin-bottom:16px;padding-bottom:8px;border-bottom:2px solid #e2e8f0;}}
        .progress-item,.supply-item{{padding:12px 0;border-bottom:1px solid #f1f5f9;}}
        .progress-item:last-child,.supply-item:last-child{{border-bottom:none;}}
        .progress-item .title,.supply-item .sector{{font-weight:600;color:#1e293b;margin-bottom:4px;}}
        .progress-item .content,.supply-item .event{{color:#64748b;font-size:.9rem;}}
        .supply-item .impact{{color:#dc2626;font-size:.85rem;margin-top:4px;}}
        .type-badge{{display:inline-block;padding:2px 8px;border-radius:4px;font-size:.75rem;margin-left:8px;}}
        .type-military{{background:#fee2e2;color:#991b1b;}}.type-diplomacy{{background:#dbeafe;color:#1e40af;}}.type-shipping{{background:#fef3c7;color:#92400e;}}.type-market{{background:#d1fae5;color:#065f46;}}.type-energy{{background:#e0e7ff;color:#3730a3;}}
        .position-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;}}
        .position-card{{background:#f8fafc;border-radius:8px;padding:16px;}}
        .position-card h3{{font-size:.9rem;color:#64748b;margin-bottom:8px;}}
        .position-card p{{font-size:.85rem;color:#1e293b;}}
        .strait-status{{background:linear-gradient(135deg,#fee2e2 0%,#fecaca 100%);border:1px solid #dc2626;border-radius:12px;padding:20px;margin-bottom:20px;}}
        .strait-status h3{{color:#991b1b;margin-bottom:12px;}}
        .strait-data{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-top:12px;}}
        .strait-data .item{{text-align:center;padding:12px;background:rgba(255,255,255,.7);border-radius:8px;}}
        .strait-data .value{{font-size:1.5rem;font-weight:700;color:#991b1b;}}
        .strait-data .label{{font-size:.8rem;color:#4b5563;}}
        .bank-view{{padding:12px;background:#f8fafc;border-radius:8px;margin-bottom:12px;}}
        .bank-view .bank{{font-weight:600;color:#1e293b;}}
        .bank-view .view{{color:#64748b;font-size:.9rem;margin-top:4px;}}
        .market-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;}}
        .market-item{{background:#f8fafc;border-radius:8px;padding:12px;text-align:center;}}
        .market-item .label{{font-size:.8rem;color:#64748b;}}
        .market-item .value{{font-size:1.1rem;font-weight:600;color:#1e293b;margin-top:4px;}}
        .watch-list{{list-style:none;}}
        .watch-list li{{padding:8px 0;padding-left:20px;position:relative;color:#4b5563;}}
        .watch-list li:before{{content:"->";position:absolute;left:0;color:#64748b;}}
        .footer{{text-align:center;padding:20px;color:#64748b;font-size:.85rem;margin-top:30px;}}
        @media(max-width:768px){{.header-main{{flex-direction:column;padding:0;}}.header-logo{{padding:10px 16px;border-bottom:1px solid #e2e8f0;width:100%;position:static;}}.header-nav{{width:100%;overflow-x:auto;scrollbar-width:none;padding:0 8px;}}.nav-btn{{padding:10px 12px;font-size:.8rem;}}}}
    </style>
</head>
<body>
    <header class="header"><div class="header-main"><div class="header-logo"><img src="images/header.png" alt="Logo"><span class="logo-text">中东地缘跟踪</span></div><nav class="header-nav" id="navCenter"><a href="index.html" class="nav-btn">海峡跟踪</a><a href="data-tracking.html" class="nav-btn">全球市场</a><a href="war-situation.html" class="nav-btn">战局形势</a><a href="briefing.html" class="nav-btn active">每日简报</a><a href="news.html" class="nav-btn">实时新闻</a><a href="central-bank-tracker.html" class="nav-btn">央行表态</a><a href="eco-track.html" class="nav-btn">经济数据</a><a href="research.html" class="nav-btn">研究视点</a><a href="polymarket.html" class="nav-btn">Polymarket</a><a href="oil-chart.html" class="nav-btn">原油图谱</a></nav></div></header>
    <main class="container">
        <div class="briefing-header"><h1>中东地缘政治每日简报</h1><div class="date">{esc(date)} | 冲突第{esc(conflict_day or "-")}天 | 封锁第{esc(blockade_day or "-")}天 | 数据来源：{esc(sources)}</div></div>
        <div class="section"><h2>今日摘要</h2><p>{esc(summary)}</p></div>
        <div class="section"><h2>冲突进展</h2>{render_progress(data.get("conflict_progress", []))}</div>
        <div class="strait-status"><h3>霍尔木兹海峡状况</h3><p><strong>状态：</strong>{esc(strait_status)}</p><p style="margin-top:8px;font-size:.9rem;color:#991b1b;">{esc(strait.get("transit_data", ""))}</p><div class="strait-data"><div class="item"><div class="value">{esc(strait_status)}</div><div class="label">海峡状态</div></div><div class="item"><div class="value">{esc(pressure)}%</div><div class="label">通行压力系数</div></div><div class="item"><div class="value">第{esc(blockade_day or "-")}天</div><div class="label">封锁持续天数</div></div><div class="item"><div class="value">{esc(passing_now)}艘</div><div class="label">当前正在通过</div></div></div></div>
        <div class="section"><h2>各方立场</h2><div class="position-grid">{render_positions(data.get("positions", {}))}</div></div>
        <div class="section"><h2>供应链影响</h2>{render_supply(data.get("supply_chain", []))}</div>
        <div class="section"><h2>机构观点</h2>{render_bank_views(data.get("bank_views", []))}</div>
        <div class="section"><h2>市场数据</h2><div class="market-grid">{render_market(market)}</div></div>
        <div class="section"><h2>明日关注</h2><ul class="watch-list">{"".join(f"<li>{esc(item)}</li>" for item in data.get("next_watch", []))}</ul></div>
    </main>
    <footer class="footer"><p>数据更新时间: {esc(date)} | 海域船只: {esc(ships_total)} | 数据来源: {esc(sources)}</p></footer>
</body>
</html>
'''
    HTML_PATH.write_text(html_text, encoding="utf-8")
    print(f"[OK] generated {HTML_PATH}")


if __name__ == "__main__":
    main()
