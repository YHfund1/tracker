#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate research.html from data/research_items.json."""

import html
import json
from collections import Counter
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "data" / "research_items.json"
HTML_PATH = ROOT / "research.html"

TYPE_LABELS = {
    "investment_bank": "投资银行",
    "asset_manager": "资管机构",
    "news": "财经媒体",
    "think_tank": "智库",
    "institution": "国际机构",
    "analyst": "分析",
}
SENTIMENT_LABELS = {
    "bullish": "看多/风险溢价",
    "bearish": "风险警示",
    "neutral": "中性",
}
SOURCE_SHORTS = {
    "Goldman Sachs": "高盛",
    "Morgan Stanley": "大摩",
    "JPMorgan": "摩根大通",
    "BlackRock": "贝莱德",
    "Reuters": "路透",
    "Bloomberg": "彭博",
}


def esc(value):
    return html.escape(str(value or ""), quote=True)


def load_items():
    if DATA_PATH.exists():
        data = json.loads(DATA_PATH.read_text(encoding="utf-8"))
        return data.get("items", []), data.get("fetchTime", "")

    # Backfill a structured source from current raw candidates so the page can
    # still be generated before the first AI delta is merged.
    raw_path = ROOT / "data" / "research_raw_data.json"
    if not raw_path.exists():
        return [], ""
    raw = json.loads(raw_path.read_text(encoding="utf-8"))
    candidates = raw.get("think_tank_entries", []) + raw.get("ib_search_results", [])
    items = []
    for item in candidates:
        if item.get("relevance_hint", 0) < 3:
            continue
        items.append({
            "id": item.get("id", ""),
            "date": item.get("pub_date", "")[:10],
            "source": item.get("source", ""),
            "source_zh": item.get("source_zh", ""),
            "source_type": item.get("source_type", "news"),
            "title_zh": item.get("title", ""),
            "original_title": item.get("title", ""),
            "summary_zh": item.get("summary", ""),
            "sentiment": "neutral",
            "relevance_score": item.get("relevance_hint", 1),
            "link": item.get("link", ""),
        })
    return items, raw.get("fetch_time", "")


def render_card(item):
    stype = item.get("source_type", "news")
    sentiment = item.get("sentiment", "neutral")
    source = item.get("source", "")
    source_label = item.get("source_zh") or SOURCE_SHORTS.get(source) or source
    score = item.get("relevance_score", "")
    return f'''
            <div class="card" data-type="{esc(stype)}" data-source="{esc(source)}">
                <div class="card-header">
                    <span class="source-badge">{esc(source_label)}</span>
                    <span class="sentiment {esc(sentiment)}">{esc(SENTIMENT_LABELS.get(sentiment, "中性"))}</span>
                </div>
                <h3>{esc(item.get("title_zh"))}</h3>
                <p class="original-title">{esc(item.get("original_title"))}</p>
                <p class="summary">{esc(item.get("summary_zh"))}</p>
                <div class="meta">
                    <span>{esc(item.get("date"))}</span>
                    <span class="relevance-score">相关度 {esc(score)}/5</span>
                </div>
                <a href="{esc(item.get("link"))}" target="_blank" class="read-more">查看原文 -></a>
            </div>'''


def main():
    items, fetch_time = load_items()
    items = sorted(items, key=lambda x: (x.get("date", ""), x.get("relevance_score", 0)), reverse=True)
    source_counts = Counter(i.get("source", "") for i in items if i.get("source"))
    type_counts = Counter(i.get("source_type", "news") for i in items)
    updated = fetch_time.replace("T", " ")[:16] if fetch_time else datetime.now().strftime("%Y-%m-%d %H:%M")

    cards = "\n".join(render_card(item) for item in items)
    type_buttons = [
        ("all", "全部"),
        ("investment_bank", "投资银行"),
        ("asset_manager", "资管机构"),
        ("news", "财经媒体"),
        ("think_tank", "智库"),
        ("institution", "国际机构"),
    ]
    buttons_html = "".join(
        f'<button class="filter-btn{" active" if key == "all" else ""}" data-filter="{key}" onclick="filterCards(\'{key}\')">{label}</button>'
        for key, label in type_buttons
    )

    html_text = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>中东地缘研究观点 | 伊朗战争与能源安全</title>
    <style>
        *{{margin:0;padding:0;box-sizing:border-box;}}
        body{{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;background:#f8fafc;color:#1e293b;line-height:1.6;}}
        .header{{background:#fff;color:#1e293b;padding:0;box-shadow:0 1px 3px rgba(0,0,0,.08);border-bottom:1px solid #e2e8f0;position:sticky;top:0;z-index:100;}}
        .header-main{{display:flex;align-items:center;max-width:1400px;margin:0 auto;padding:0 20px;position:relative;}}
        .header-logo{{display:flex;align-items:center;gap:10px;position:absolute;left:20px;}}
        .header-logo img{{height:30px;width:auto;display:block;}}
        .logo-text{{font-size:1.25rem;font-weight:600;color:#c41230;letter-spacing:1px;}}
        .header-nav{{display:flex;gap:0;margin:0 auto;}}
        .nav-btn{{color:#64748b;text-decoration:none;padding:12px 14px;font-size:.85rem;transition:all .2s;white-space:nowrap;border-bottom:3px solid transparent;}}
        .nav-btn:hover{{background:#f1f5f9;color:#991b1b;}}
        .nav-btn.active{{background:#f8fafc;color:#991b1b;border-bottom-color:#dc2626;font-weight:500;}}
        .container{{max-width:1280px;margin:0 auto;padding:24px 20px;}}
        .research-header{{background:linear-gradient(135deg,#1e3a5f 0%,#2d5a87 100%);color:white;padding:28px 24px;border-radius:12px;margin-bottom:24px;}}
        .research-header h1{{font-size:1.6rem;margin-bottom:8px;}}
        .research-header p{{opacity:.9;font-size:.95rem;}}
        .stats{{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:20px;}}
        .stat-card{{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:18px;text-align:center;box-shadow:0 1px 3px rgba(0,0,0,.08);}}
        .stat-card .number{{font-size:2rem;font-weight:700;color:#1e3a5f;}}
        .stat-card .label{{font-size:.85rem;color:#64748b;margin-top:4px;}}
        .filter-section{{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:16px;margin-bottom:20px;}}
        .filter-section h3{{font-size:.95rem;margin-bottom:12px;color:#475569;}}
        .filter-buttons{{display:flex;gap:8px;flex-wrap:wrap;}}
        .filter-btn{{padding:7px 14px;border:1px solid #e2e8f0;background:#f8fafc;border-radius:20px;cursor:pointer;color:#475569;}}
        .filter-btn.active{{background:#1e3a5f;color:white;border-color:#1e3a5f;}}
        .cards-grid{{display:grid;grid-template-columns:repeat(2,1fr);gap:18px;}}
        .card{{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:18px;box-shadow:0 1px 3px rgba(0,0,0,.08);display:flex;flex-direction:column;min-height:260px;}}
        .card.hidden{{display:none;}}
        .card-header{{display:flex;justify-content:space-between;gap:10px;align-items:flex-start;margin-bottom:12px;}}
        .source-badge{{background:#eff6ff;color:#1d4ed8;border-radius:20px;padding:4px 10px;font-size:.8rem;font-weight:600;}}
        .sentiment{{border-radius:20px;padding:4px 10px;font-size:.78rem;font-weight:600;white-space:nowrap;}}
        .sentiment.bearish{{background:#fee2e2;color:#991b1b;}}
        .sentiment.bullish{{background:#dcfce7;color:#166534;}}
        .sentiment.neutral{{background:#f1f5f9;color:#475569;}}
        .card h3{{font-size:1.05rem;color:#1e40af;line-height:1.5;margin-bottom:8px;}}
        .original-title{{font-size:.78rem;color:#94a3b8;margin-bottom:10px;}}
        .summary{{font-size:.9rem;color:#475569;line-height:1.65;}}
        .meta{{display:flex;justify-content:space-between;gap:10px;color:#64748b;font-size:.8rem;margin-top:auto;padding-top:14px;border-top:1px solid #e2e8f0;}}
        .read-more{{margin-top:10px;color:#1e40af;text-decoration:none;font-size:.88rem;font-weight:600;}}
        .footer{{text-align:center;color:#94a3b8;font-size:.8rem;padding:24px;}}
        @media(max-width:768px){{.header-main{{flex-direction:column;padding:0;}}.header-logo{{position:static;width:100%;padding:10px 16px;border-bottom:1px solid #e2e8f0;}}.header-nav{{width:100%;overflow-x:auto;padding:0 8px;}}.stats{{grid-template-columns:repeat(2,1fr);}}.cards-grid{{grid-template-columns:1fr;}}}}
    </style>
</head>
<body>
    <div class="header"><div class="header-main"><div class="header-logo"><img src="images/header.png" alt="Logo"><span class="logo-text">中东地缘跟踪</span></div><nav class="header-nav" id="navCenter"><a href="index.html" class="nav-btn">海峡跟踪</a><a href="data-tracking.html" class="nav-btn">全球市场</a><a href="war-situation.html" class="nav-btn">战局形势</a><a href="briefing.html" class="nav-btn">每日简报</a><a href="news.html" class="nav-btn">实时新闻</a><a href="central-bank-tracker.html" class="nav-btn">央行表态</a><a href="eco-track.html" class="nav-btn">经济数据</a><a href="research.html" class="nav-btn active">研究视点</a><a href="polymarket.html" class="nav-btn">Polymarket</a><a href="oil-chart.html" class="nav-btn">原油图谱</a></nav></div></div>
    <main class="container">
        <div class="research-header"><h1>智库 & 投行研究观点</h1><p>聚焦伊朗战争、霍尔木兹海峡、油价、能源安全与宏观传导。更新时间：{esc(updated)}</p></div>
        <div class="stats"><div class="stat-card"><div class="number">{len(items)}</div><div class="label">核心观点</div></div><div class="stat-card"><div class="number">{len(source_counts)}</div><div class="label">来源机构</div></div><div class="stat-card"><div class="number">{type_counts.get("think_tank", 0) + type_counts.get("institution", 0)}</div><div class="label">智库/机构</div></div><div class="stat-card"><div class="number">{type_counts.get("investment_bank", 0) + type_counts.get("asset_manager", 0) + type_counts.get("news", 0)}</div><div class="label">投行&媒体</div></div></div>
        <div class="filter-section"><h3>类型筛选</h3><div class="filter-buttons">{buttons_html}</div></div>
        <div class="cards-grid" id="cardsGrid">{cards}
        </div>
    </main>
    <footer class="footer">数据来源：研究机构、投行与财经媒体公开页面</footer>
    <script>
        function filterCards(type) {{
            document.querySelectorAll('.filter-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.filter === type));
            document.querySelectorAll('.card').forEach(card => {{
                const show = type === 'all' || card.dataset.type === type;
                card.classList.toggle('hidden', !show);
            }});
        }}
    </script>
</body>
</html>
'''
    HTML_PATH.write_text(html_text, encoding="utf-8")
    print(f"[OK] generated {HTML_PATH} with {len(items)} items")


if __name__ == "__main__":
    main()
