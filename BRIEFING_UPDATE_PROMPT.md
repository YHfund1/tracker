# 每日简报低Token更新流程

## 核心原则

- AI只更新 `briefing_data.json`，不要输出完整 `briefing.html`。
- 页面由 `python scripts/generate_briefing_html.py` 生成。
- 搜索结果只保留最终采用的事实，不要把大量新闻全文塞进上下文。

## Step 1：搜索最新信息

搜索近36小时与以下主题有关的可靠信息：

- 战局进展：Israel Iran war latest、US/Iran strikes、missile/drone attack
- 各方表态：US/Iran/Gulf statements、Trump Iran、Khamenei statement
- 海峡通行：Strait of Hormuz shipping、tanker、LNG、UKMTO
- 供应链：Middle East supply chain、LNG disruption、fertilizer、aluminum、jet fuel
- 机构观点：Goldman/JPMorgan/Morgan Stanley oil forecast、shipping insurance

## Step 2：只输出 JSON

直接更新 `briefing_data.json`，字段保持如下结构：

```json
{
  "date": "YYYY-MM-DD",
  "conflict_day": 0,
  "blockade_day": 0,
  "summary": "一段中文摘要",
  "conflict_progress": [
    {"title": "标题", "content": "事实描述", "type": "military|diplomacy|shipping|market|energy"}
  ],
  "positions": {
    "us": "美国/特朗普立场",
    "iran": "伊朗立场",
    "gulf": "海湾国家立场",
    "others": "其他相关方"
  },
  "strait_status": {
    "status": "海峡状态",
    "transit_data": "通行数据说明",
    "key_events": ["关键事件"]
  },
  "supply_chain": [
    {"sector": "行业", "event": "事件", "impact": "影响"}
  ],
  "bank_views": [
    {"bank": "机构", "view": "观点"}
  ],
  "market_data": {
    "brent": 0,
    "wti": 0,
    "lme_aluminum": 0,
    "urea": 0,
    "ny_gas": 0,
    "strait_pressure": 0,
    "ships_total": 0,
    "passing_now": 0
  },
  "next_watch": ["明日关注点"],
  "sources": ["来源名称"]
}
```

## Step 3：生成 HTML

```bash
python scripts/generate_briefing_html.py
```

## 控制要点

- `conflict_progress` 保持 4-7 条。
- `supply_chain` 和 `bank_views` 各保持 3-5 条。
- 每条内容用 1-2 句，保留时间、地点、来源和影响。
- 不要复制旧 HTML，不要输出 CSS/HTML。
