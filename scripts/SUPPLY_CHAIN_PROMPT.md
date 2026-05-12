# 供应链跟踪低Token更新流程

## 核心原则

- 不要让AI读取完整 `data/supply-chain.json`。
- 先生成近期索引小文件，再让AI只输出新增/修正 delta。
- 正式合并由脚本完成，网页 `index.html` 会读取 `data/supply-chain.json`。

## Step 1：准备近期索引

```bash
python scripts/prepare_supply_chain_ai_input.py
```

AI只读取：

```text
cache/ai_inputs/supply_chain_recent.json
```

## Step 2：搜索最新新闻

关注近72小时中东地缘局势相关新闻。

能源基础设施关键词：

- `中东 炼油厂 袭击`
- `中东 LNG 遭袭`
- `伊朗 气田 轰炸`
- `卡塔尔 油轮 导弹`
- `沙特 石油设施 攻击`
- `oil refinery attack Middle East`
- `LNG facility strike`
- `gas field bombing Iran`
- `tanker missile Qatar`
- `oil terminal damage`

产业链影响关键词：

- `中东 化工 停产`
- `伊朗 钢铁 不可抗力`
- `石化 降负荷`
- `化肥 供应中断`
- `铝业 减产 中东`
- `force majeure petrochemical`
- `chemical plant shutdown Middle East`
- `steel production halt Iran`
- `supply shortage fertilizer aluminum`

## 纳入标准

能源基础设施 `energy`：

- 油气田、油气处理设施、炼油厂、石化厂、LNG终端、储油储气设施
- 油轮/LNG船被导弹、无人机、水雷攻击
- 石油/天然气出口终端、港口码头、能源运输通道、管道、电网/发电厂
- 核设施或能源密集工业区遭袭

产业链影响 `chain`：

- 具体企业停产、降负荷、不可抗力
- 行业供应中断或原材料断供导致减产
- 政府能源应急措施
- 中东工业设施受损造成全球产业链冲击

排除：

- 民用设施袭击
- 纯军事设施，除非同时影响能源设施
- 纯金融市场价格波动
- 纯宏观预测、尚未发生的风险推测

## Step 3：只输出 Delta JSON

写入 `cache/ai_outputs/supply_chain_delta.json`：

```json
{
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
```

## Step 4：合并正式数据

```bash
python scripts/merge_supply_chain_delta.py
```

## 注意

- 只新增或修正，不删除旧条目。
- 日期使用事件发生日期，不是新闻发布日期。
- 同设施/企业 + 同日事件视为重复，应合并。
