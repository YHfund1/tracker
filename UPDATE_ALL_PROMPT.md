# 中东地缘跟踪器全量更新提示词

## 执行前准备
1. 确保所有Python依赖已安装
2. 确保`全球市场.xlsx`文件已更新到最新数据
3. 确保GitHub仓库已配置

---
更新过程尽量不生成新的脚本，如果需要生成新的测试脚本和测试文件，请在更新完成后删除测试脚本和测试文件

## 低Token更新原则（强制）

- 不要把大文件全文贴给AI，包括 `news.html`、`oil-chart.html`、`data/jin10_cb_for_ai.json`、`data/polymarket_data.json`、`data/eco_data.json`、`market_data.json`。
- AI只处理 `cache/ai_inputs/` 里的本次新增小文件，并只输出 `cache/ai_outputs/` 里的 delta JSON。
- 正式历史数据和HTML由脚本合并/生成；除非调试脚本，否则不要让AI直接输出整页HTML。
- 推荐执行顺序：抓取脚本 -> 生成AI小输入 -> AI输出delta -> 合并脚本 -> HTML生成脚本。

## 缓存与冗余文件约定

- 可随时删除的临时文件统一放入 `cache/`，不要散落在项目根目录。
- `cache/` 用于调试截图、旧快照、临时搜索结果、交付压缩包/解压包、Python `__pycache__` 等与当前网页运行无关的副产物。
- 更新流程不要读取 `cache/` 作为正式数据源；如果某个文件会被网页或脚本读取，应保留在原路径。
- 当前正式数据源仍包括：`全球市场.xlsx`、`历史.csv`、`data/*.json`、`briefing_data.json`、`market_data.json`、`strait_data.json`、`jin10_strait_data.json`、网页 HTML、脚本和提示词文件。
- 抓取失败或调试产生的新截图请保存到 `cache/debug/`。

## 1. 海峡跟踪网页 (index.html) 更新

### 上方数据更新
```bash
python update_strait_data.py
python generate_timelapse_video.py
```

### 下方供应链跟踪
**参考文件**: `scripts/SUPPLY_CHAIN_PROMPT.md`
- 生成近期索引，AI只输出delta：
```bash
python scripts/prepare_supply_chain_ai_input.py
```
- AI输出 `cache/ai_outputs/supply_chain_delta.json` 后合并：
```bash
python scripts/merge_supply_chain_delta.py
```
- `index.html` 会自动读取 `data/supply-chain.json`

---

## 2. 全球市场 & 经济数据网页更新

```bash
python update_data_from_excel.py
```

**更新内容**:
- `data-tracking.html` - 全球市场数据（商品价格、流动性、股市、债市、总览）
- `eco-track.html` - 各国经济数据（美国、欧元区、日本、英国等8国）

**数据源**: `全球市场.xlsx`

---

## 3. 战局形势网页 (war-situation.html) 更新

### 步骤1：获取最新报告
1. 访问 https://understandingwar.org/
2. 搜索最新的 Iran Update Special Report（伊朗特别更新报告）
3. 提取报告全文和所有图片链接

### 步骤2：翻译处理（手动翻译，不使用脚本）
**必须完整翻译的内容**:
- ✅ Key Takeaways（关键要点）- 全部保留翻译
- ✅ 所有图表/地图 - 全部保留
- ✅ 图表对应的正文内容 - 翻译使用

### 步骤3：更新网页
- 保留war-situation.html原有结构
- 替换为最新翻译内容
- 确保图片链接正确（使用原始URL或下载到本地）

---

## 4. 每日简报网页 (briefing.html) 更新

**参考文件**: `BRIEFING_UPDATE_PROMPT.md`
- AI只更新 `briefing_data.json`，不要输出完整 `briefing.html`
- 生成网页：
```bash
python scripts/generate_briefing_html.py
```

---

## 5. 实时新闻网页 (news.html) 更新

```bash
python scrape_cls_final.py
```

**更新内容**:
- 抓取财联社最新新闻
- 更新news.html的新闻列表

---

## 6. 央行表态网页 (central-bank-tracker.html) 更新

**参考文件**: `scripts/AI_PROCESS_PROMPT.md`
- 运行抓取脚本后，只读取 `cache/ai_inputs/cb_new_items.json`
- AI输出 `cache/ai_outputs/cb_delta.json`
- 合并央行表态：
```bash
python scripts/merge_cb_delta.py
```
- `central-bank-tracker.html` 自动读取 `data/cb-statements.json` 和 `data/fedwatch.json`，无需重写HTML

---

## 7. 研究视点网页 (research.html) 更新

**参考文件**: `research.md`
- 运行抓取脚本后，只读取 `cache/ai_inputs/research_candidates.json`
- AI输出 `cache/ai_outputs/research_delta.json`
- 合并并生成网页：
```bash
python scripts/merge_research_delta.py
python scripts/generate_research_html.py
```

---

## 8. Polymarket网页 (polymarket.html) 更新

```bash
python update_polymarket_html.py
```

**更新内容**:
- 抓取Polymarket中东相关预测市场数据
- 保存到 `data/polymarket_data.json`
- polymarket.html 会自动从JSON加载数据，无需重新生成HTML

---

## 9. 海湾原油图谱网页 (oil-chart.html) 更新

### 更新要求：
1. 生成近期历史小输入：
```bash
python scripts/prepare_oil_news_ai_input.py
```

2. **使用全网搜索功能**搜索以下国家能源设施相关新闻：
   - 沙特阿拉伯
   - 伊朗
   - 伊拉克
   - 阿联酋
   - 科威特
   - 卡塔尔
   - 阿曼
   - 巴林

3. **时间范围**: 近72小时内的最新信息

4. **更新规则**:
   - ✅ 添加新增的新闻
   - ✅ 保留旧的新闻（不要删除）
   - ❌ 内容大体相同的重复新闻不重复添加
   - 📅 按时间从新到早排序

5. AI只输出 `cache/ai_outputs/oil_news_delta.json`，然后执行：
```bash
python scripts/merge_oil_news_delta.py
python scripts/apply_oil_news_to_html.py
```

---

## 10. 推送到GitHub (使用SSH)

> ⚠️ **重要**：本项目使用SSH方式推送，避免HTTPS的SSL/TLS连接问题

### 前置检查
确保本地已配置SSH密钥并添加到GitHub：
```bash
# 检查SSH密钥是否存在
ls ~/.ssh/id_rsa.pub

# 测试GitHub SSH连接
ssh -T git@github.com
```

### 修改远程仓库URL为SSH（如需要）
```bash
# 查看当前远程URL
git remote -v

# 如显示为 https://github.com/...，需改为SSH
git remote set-url origin git@github.com:YHfund1/tracker.git
```

### 推送步骤
```bash
# 添加所有更改
git add .

# 提交更新
git commit -m "update: 全量数据更新 [日期]"

# 先拉取远程更新（避免冲突）
git pull origin main --rebase

# 推送到远程（使用SSH）
git push origin main
```

### 常见问题
- **权限被拒绝**: 检查SSH密钥是否正确添加到GitHub账户
- **连接超时**: 检查网络连接，或稍后重试
- **冲突**: 执行 `git pull origin main --rebase` 解决冲突后再推送

---

## 更新检查清单

- [ ] index.html - 海峡跟踪数据已更新
- [ ] index.html - 供应链跟踪已更新
- [ ] data-tracking.html - 全球市场数据已更新
- [ ] eco-track.html - 经济数据已更新
- [ ] war-situation.html - ISW战局报告已翻译更新
- [ ] briefing.html - 每日简报已更新
- [ ] news.html - 实时新闻已更新
- [ ] central-bank-tracker.html - 央行表态已更新
- [ ] research.html - 研究视点已更新
- [ ] polymarket.html - 预测市场数据已更新
- [ ] oil-chart.html - 海湾原油动态已更新
- [ ] GitHub推送已完成

---

## 执行顺序建议

```
步骤1: 数据收集
  ├── 执行 update_data_from_excel.py
  ├── 执行 scrape_cls_final.py
  ├── 执行 update_strait_data.py
  └── 搜索oil-chart需要的各国新闻

步骤2: AI处理
  ├── 处理briefing更新
  ├── 处理央行表态更新
  ├── 处理war-situation翻译
  └── 处理research更新

步骤3: 生成视频
  └── 执行 generate_timelapse_video.py

步骤4: 验证检查
  └── 检查所有网页显示正常

步骤5: 提交推送
  └── git commit & push
```

---

## 注意事项

1. **war-situation.html**: 必须手动翻译，确保中文流畅准确
2. **oil-chart**: 只更新 `data/oil_news.json` 的新增动态，再由脚本写回HTML
3. **Excel文件**: 确保`全球市场.xlsx`已更新到最新数据后再运行脚本
4. **图片链接**: war-situation中的图片如无法显示，需下载到本地或替换为可用链接
5. **Git提交**: 提交信息建议包含更新日期和主要内容
