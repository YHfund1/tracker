#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Extract current research.html cards into data/research_items.json."""

import html
import json
import re
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
HTML_PATH = ROOT / "research.html"
OUT_PATH = ROOT / "data" / "research_items.json"


def strip_tags(value):
    value = re.sub(r"<[^>]+>", "", value or "")
    return html.unescape(value).strip()


def classify_sentiment(text):
    if "风险警示" in text or "看空" in text:
        return "bearish"
    if "看多" in text or "风险溢价" in text:
        return "bullish"
    return "neutral"


def main():
    text = HTML_PATH.read_text(encoding="utf-8")
    card_pattern = re.compile(r'<div class="card" data-type="([^"]*)" data-source="([^"]*)">')
    matches = list(card_pattern.finditer(text))
    items = []

    for idx, match in enumerate(matches, 1):
        source_type = match.group(1)
        source = match.group(2)
        end = matches[idx].start() if idx < len(matches) else text.find("</div>\n    </main>", match.end())
        if end == -1:
            end = len(text)
        body = text[match.end():end]

        title_match = re.search(r"<h3>(.*?)</h3>", body, re.S)
        original_match = re.search(r'<p class="original-title">(.*?)</p>', body, re.S)
        summary_match = re.search(r'<p class="summary">(.*?)</p>', body, re.S)
        sentiment_match = re.search(r'<span class="sentiment[^"]*">(.*?)</span>', body, re.S)
        source_zh_match = re.search(r'<span class="source-badge">(.*?)</span>', body, re.S)
        date_match = re.search(r"<span>(\d{4}-\d{2}-\d{2})</span>", body)
        score_match = re.search(r"相关度\s*(\d+)/5", body)
        link_match = re.search(r'<a href="(.*?)"', body, re.S)

        title_zh = strip_tags(title_match.group(1) if title_match else "")
        summary_zh = strip_tags(summary_match.group(1) if summary_match else "")
        original_title = strip_tags(original_match.group(1) if original_match else title_zh)
        if not summary_zh:
            summary_zh = original_title or "暂无摘要"
        if not title_zh:
            continue

        items.append({
            "id": f"existing-{idx:04d}",
            "date": date_match.group(1) if date_match else "",
            "source": html.unescape(source),
            "source_zh": strip_tags(source_zh_match.group(1) if source_zh_match else ""),
            "source_type": html.unescape(source_type),
            "title_zh": title_zh,
            "original_title": original_title,
            "summary_zh": summary_zh,
            "sentiment": classify_sentiment(strip_tags(sentiment_match.group(1) if sentiment_match else "")),
            "relevance_score": int(score_match.group(1)) if score_match else 3,
            "link": html.unescape(link_match.group(1) if link_match else ""),
        })

    output = {
        "fetchTime": datetime.now().isoformat(timespec="seconds"),
        "totalItems": len(items),
        "items": items,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] extracted {len(items)} research items to {OUT_PATH}")


if __name__ == "__main__":
    main()
