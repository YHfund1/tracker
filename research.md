# 研究视点低Token更新流程

## 核心原则

- 不要复制 `data/research_raw_data.json` 全文给 AI。
- 不要让 AI 生成完整 `research.html`。
- AI只读取 `cache/ai_inputs/research_candidates.json`，只输出 `cache/ai_outputs/research_delta.json`。
- 正式历史保存在 `data/research_items.json`，页面由脚本生成。

## Step 1：抓取候选数据

```bash
python scripts/update_research_data.py
```

脚本输出：

- `data/research_raw_data.json`：原始抓取备份，脚本/排错使用
- `cache/ai_inputs/research_candidates.json`：给AI的小候选包

## Step 2：AI筛选新增观点

读取 `cache/ai_inputs/research_candidates.json`：

- 只处理 `candidates`。
- 使用 `existingIndex` 去重，不要删除旧观点。
- 只保留与伊朗战争、霍尔木兹、油价、能源安全、中东供应链和宏观传导直接相关的内容。
- 排除纯政治、通用国家介绍、旧背景百科、非原创转引、与能源/经济无关的军事战术内容。

## 输出格式

将结果写入 `cache/ai_outputs/research_delta.json`：

```json
{
  "items": [
    {
      "id": "沿用候选id",
      "date": "YYYY-MM-DD",
      "source": "Morgan Stanley",
      "source_zh": "大摩",
      "source_type": "investment_bank",
      "title_zh": "中文标题",
      "original_title": "英文原标题",
      "summary_zh": "中文摘要，1-2句",
      "sentiment": "bullish|bearish|neutral",
      "relevance_score": 1,
      "link": "原文链接"
    }
  ]
}
```

## Step 3：合并并生成页面

```bash
python scripts/merge_research_delta.py
python scripts/generate_research_html.py
```

## 字段说明

- `source_type` 可用：`think_tank`、`investment_bank`、`asset_manager`、`news`、`institution`、`analyst`
- `sentiment`：
  - `bullish`：推高油价、风险溢价上升、供应冲击
  - `bearish`：油价下行、风险缓解、需求走弱
  - `neutral`：平衡分析或结构性观点
- `relevance_score`：1-5，5为直接讨论霍尔木兹/伊朗战争/能源供应冲击。

## 维护命令

如果第一次启用或需要从当前页面回填结构化数据：

```bash
python scripts/extract_research_items.py
```
