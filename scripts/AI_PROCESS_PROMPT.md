# 央行表态 + FedWatch 低Token更新流程

## 核心原则

- 不要读取 `data/jin10_cb_for_ai.json` 全量历史。
- AI只读取 `cache/ai_inputs/cb_new_items.json` 里的 `newItems` 和少量 `existingRecentIndex`。
- AI只输出 `cache/ai_outputs/cb_delta.json`。
- 历史合并由 `python scripts/merge_cb_delta.py` 完成。

## Step 1：抓取金十数据

```bash
python scripts/jin10_fetch.py
```

脚本会同时更新：

- `data/jin10_cb_for_ai.json`：全量历史，脚本使用，不给AI全文读取
- `cache/ai_inputs/cb_new_items.json`：本次新增小输入，给AI处理

## Step 2：AI处理新增快讯

读取 `cache/ai_inputs/cb_new_items.json`，只处理其中 `newItems`。

处理规则：

- 只保留现任央行货币政策决策机构成员本人发言。
- 排除前官员、分析师、记者、市场评论。
- 同一官员同一天或相邻一天、主题相关的快讯合并为一条。
- points 按话题提炼，避免逐条堆砌。
- 保留关键数据、明确政策表态、政策暗示和能源/通胀冲击。

## Step 3：只输出 Delta JSON

将结果写入 `cache/ai_outputs/cb_delta.json`：

```json
{
  "items": [
    {
      "date": "YYYY-MM-DD",
      "bank": "美联储",
      "official": "鲍威尔（Jerome Powell，主席）",
      "title": "主席",
      "mergedCount": 3,
      "points": [
        "通胀：...",
        "政策立场：..."
      ],
      "link": "https://www.jin10.com/flash/..."
    }
  ]
}
```

## Step 4：合并正式数据

```bash
python scripts/merge_cb_delta.py
python scripts/fetch_fedwatch.py
```

`central-bank-tracker.html` 会自动读取 `data/cb-statements.json` 和 `data/fedwatch.json`，无需重写 HTML。
