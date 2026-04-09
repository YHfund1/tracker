#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
研究数据抓取脚本 v2 - 增强版
- 22个智库/机构RSS源
- 90+搜索任务覆盖40+金融机构
- DuckDuckGo + Google双引擎
- 代理支持
- 粗筛预过滤
"""

import json
import os
import hashlib
import re
from datetime import datetime, timedelta
import feedparser
import time

# ===== 代理配置 =====
PROXY_HOST = os.environ.get("RESEARCH_PROXY", "http://127.0.0.1:7897")
PROXY_CONFIG = {"http": PROXY_HOST, "https": PROXY_HOST}
USE_PROXY = os.environ.get("USE_PROXY", "1") == "1"

def get_proxies():
    return PROXY_CONFIG if USE_PROXY else None

# 设置环境变量让 feedparser 也走代理
if USE_PROXY:
    os.environ["HTTP_PROXY"] = PROXY_HOST
    os.environ["HTTPS_PROXY"] = PROXY_HOST

# DuckDuckGo 搜索
try:
    from duckduckgo_search import DDGS
    DDGS_AVAILABLE = True
except ImportError:
    DDGS_AVAILABLE = False
    print("[WARN] duckduckgo-search not installed, search will be skipped")

# Google 搜索（备选）
try:
    from googlesearch import search as google_search
    GOOGLE_AVAILABLE = True
except ImportError:
    GOOGLE_AVAILABLE = False
    print("[WARN] googlesearch-python not installed, Google fallback disabled")

# ===== 粗筛关键词 =====
RELEVANCE_KEYWORDS = [
    # 能源/商品
    'iran', 'hormuz', 'oil', 'energy', 'brent', 'crude', 'petroleum',
    'gas', 'lng', 'commodity', 'opec', 'persian gulf', 'strait',
    # 中东/冲突
    'middle east', 'israel', 'saudi', 'gulf', 'ceasefire', 'war',
    'conflict', 'geopolitical', 'sanctions', 'hezbollah', 'missile',
    'geopolitics', 'nuclear',
    # 宏观
    'inflation', 'fed', 'ecb', 'central bank', 'interest rate',
    'recession', 'stagflation', 'supply chain', 'tariff', 'fiscal',
    # 机构标记
    'goldman', 'morgan', 'jpmorgan', 'citi', 'blackrock', 'pimco',
    'treasury', 'sovereign', 'emerging market',
]

# ===== 排除来源 =====
EXCLUDED_DOMAINS = [
    'news.com.au',
    'abc.net.au',      # ABC News Australia
    'aljazeera.com',   # Al Jazeera
]
EXCLUDED_URL_PATTERNS = [
    'in-the-news',     # Atlantic Council "In the News" citations
]

# ==========================================
# 智库/机构RSS源（22个）
# ==========================================

THINK_TANK_SOURCES = [
    # --- 原有12个 ---
    {"name": "Brookings Institution", "name_zh": "布鲁金斯学会", "rss": "https://www.brookings.edu/feed/", "region": "us", "type": "think_tank"},
    {"name": "CSIS", "name_zh": "战略与国际研究中心", "rss": "https://www.csis.org/rss.xml", "region": "us", "type": "think_tank"},
    {"name": "CFR", "name_zh": "外交关系委员会", "rss": "https://www.cfr.org/rss.xml", "region": "us", "type": "think_tank"},
    {"name": "PIIE", "name_zh": "彼得森国际经济研究所", "rss": "https://www.piie.com/rss.xml", "region": "us", "type": "think_tank"},
    {"name": "Carnegie Endowment", "name_zh": "卡内基国际和平基金会", "rss": "https://carnegieendowment.org/rss.xml", "region": "us", "type": "think_tank"},
    {"name": "RAND Corporation", "name_zh": "兰德公司", "rss": "https://www.rand.org/rss.xml", "region": "us", "type": "think_tank"},
    {"name": "Atlantic Council", "name_zh": "大西洋理事会", "rss": "https://www.atlanticcouncil.org/feed/", "region": "us", "type": "think_tank"},
    {"name": "Chatham House", "name_zh": "查塔姆研究所", "rss": "https://www.chathamhouse.org/feed", "region": "eu", "type": "think_tank"},
    {"name": "ECFR", "name_zh": "欧洲外交关系委员会", "rss": "https://ecfr.eu/feed/", "region": "eu", "type": "think_tank"},
    {"name": "GMF", "name_zh": "德国马歇尔基金会", "rss": "https://www.gmfus.org/rss.xml", "region": "eu", "type": "think_tank"},
    {"name": "IMF", "name_zh": "国际货币基金组织", "rss": "https://www.imf.org/en/Publications/RSS", "region": "global", "type": "institution"},
    {"name": "BIS", "name_zh": "国际清算银行", "rss": "https://www.bis.org/doclist/bis_fsi_publs.rss", "region": "global", "type": "institution"},

    # --- 新增: 国际能源/经济机构 ---
    {"name": "IEA", "name_zh": "国际能源署", "rss": "https://www.iea.org/feed", "region": "global", "type": "institution"},
    {"name": "World Bank", "name_zh": "世界银行", "rss": "http://feeds.feedburner.com/PSDBlog", "region": "global", "type": "institution"},
    {"name": "OECD", "name_zh": "经合组织", "rss": "https://www.oecd.org/en/rss.html", "region": "global", "type": "institution"},
    {"name": "ADB", "name_zh": "亚洲开发银行", "rss": "https://www.adb.org/rss", "region": "asia", "type": "institution"},

    # --- 新增: 美国智库 ---
    {"name": "Foreign Affairs", "name_zh": "外交事务", "rss": "https://www.foreignaffairs.com/rss.xml", "region": "us", "type": "think_tank"},
    {"name": "Hudson Institute", "name_zh": "哈德逊研究所", "rss": "https://feeds.simplecast.com/_011BktN", "region": "us", "type": "think_tank"},
    {"name": "War on the Rocks", "name_zh": "战争岩石", "rss": "https://warontherocks.com/feed/", "region": "us", "type": "think_tank"},

    # --- 新增: 欧洲智库 ---
    {"name": "Bruegel", "name_zh": "布鲁盖尔研究所", "rss": "https://www.bruegel.org/rss.xml", "region": "eu", "type": "think_tank"},
    {"name": "EUISS", "name_zh": "欧盟安全研究所", "rss": "https://www.iss.europa.eu/rss.xml", "region": "eu", "type": "think_tank"},
    {"name": "SWP", "name_zh": "德国国际安全事务研究所", "rss": "https://www.swp-berlin.org/rss.xml", "region": "eu", "type": "think_tank"},
]

# ==========================================
# 搜索任务配置（90+条）
# ==========================================

IB_RESEARCH_SEARCHES = [
    # ============================================================
    # Tier 1: 顶级投行 - 官网+媒体报道
    # ============================================================
    {"query": "Goldman Sachs oil price forecast brent 2026", "source": "Goldman Sachs", "source_zh": "高盛", "priority": 1},
    {"query": "Goldman Sachs commodity outlook 2026", "source": "Goldman Sachs", "source_zh": "高盛", "priority": 1},
    {"query": "Morgan Stanley energy outlook oil forecast", "source": "Morgan Stanley", "source_zh": "大摩", "priority": 1},
    {"query": "Morgan Stanley oil 2026", "source": "Morgan Stanley", "source_zh": "大摩", "priority": 1},
    {"query": "JPMorgan oil supply disruption forecast", "source": "JPMorgan", "source_zh": "小摩", "priority": 1},
    {"query": "JPMorgan energy research 2026", "source": "JPMorgan", "source_zh": "小摩", "priority": 1},
    {"query": "Citi commodity outlook oil price target", "source": "Citi", "source_zh": "花旗", "priority": 1},
    {"query": "Citi oil price target 2026", "source": "Citi", "source_zh": "花旗", "priority": 1},
    {"query": "Bank of America energy outlook oil forecast", "source": "Bank of America", "source_zh": "美银", "priority": 1},
    {"query": "Wells Fargo energy oil price forecast", "source": "Wells Fargo", "source_zh": "富国银行", "priority": 1},

    # ============================================================
    # Tier 2: 欧洲投行
    # ============================================================
    {"query": "Deutsche Bank oil price forecast commodity", "source": "Deutsche Bank", "source_zh": "德银", "priority": 2},
    {"query": "Barclays oil price target energy research", "source": "Barclays", "source_zh": "巴克莱", "priority": 2},
    {"query": "UBS commodity outlook oil forecast", "source": "UBS", "source_zh": "瑞银", "priority": 2},
    {"query": "Credit Suisse oil market outlook", "source": "Credit Suisse", "source_zh": "瑞信", "priority": 2},
    {"query": "Standard Chartered oil forecast energy", "source": "Standard Chartered", "source_zh": "渣打", "priority": 2},

    # ============================================================
    # Tier 3: 其他重要投行/券商
    # ============================================================
    {"query": "HSBC oil price forecast commodity", "source": "HSBC", "source_zh": "汇丰", "priority": 3},
    {"query": "Societe Generale oil forecast energy", "source": "Societe Generale", "source_zh": "法兴", "priority": 3},
    {"query": "Nomura oil price forecast energy", "source": "Nomura", "source_zh": "野村", "priority": 3},
    {"query": "Macquarie commodity outlook oil", "source": "Macquarie", "source_zh": "麦格理", "priority": 3},
    {"query": "Jefferies energy oil price forecast", "source": "Jefferies", "source_zh": "杰富瑞", "priority": 2},
    {"query": "Bernstein oil energy research forecast", "source": "Bernstein", "source_zh": "伯恩斯坦", "priority": 2},
    {"query": "Evercore ISI energy oil outlook", "source": "Evercore ISI", "source_zh": "Evercore", "priority": 2},
    {"query": "Oppenheimer oil price energy forecast", "source": "Oppenheimer", "source_zh": "奥本海默", "priority": 2},
    {"query": "Lazard energy market outlook oil", "source": "Lazard", "source_zh": "拉扎德", "priority": 2},
    {"query": "RBC Capital Markets oil energy forecast", "source": "RBC", "source_zh": "加拿大皇家银行", "priority": 2},
    {"query": "BMO Capital oil commodity outlook", "source": "BMO", "source_zh": "蒙特利尔银行", "priority": 2},
    {"query": "TD Securities energy oil research", "source": "TD Securities", "source_zh": "道明证券", "priority": 2},
    {"query": "ANZ Bank oil price commodity forecast", "source": "ANZ", "source_zh": "澳新银行", "priority": 2},

    # ============================================================
    # Tier 4: 大型资管公司
    # ============================================================
    {"query": "BlackRock oil energy outlook 2026", "source": "BlackRock", "source_zh": "贝莱德", "priority": 1},
    {"query": "BlackRock geopolitical risk market impact", "source": "BlackRock", "source_zh": "贝莱德", "priority": 1},
    {"query": "PIMCO commodity outlook oil energy", "source": "PIMCO", "source_zh": "太平洋投资", "priority": 1},
    {"query": "PIMCO macro outlook energy inflation 2026", "source": "PIMCO", "source_zh": "太平洋投资", "priority": 1},
    {"query": "Fidelity oil price energy market forecast", "source": "Fidelity", "source_zh": "富达", "priority": 2},
    {"query": "Vanguard energy market outlook oil", "source": "Vanguard", "source_zh": "先锋", "priority": 2},
    {"query": "Schroders oil energy commodity forecast", "source": "Schroders", "source_zh": "施罗德", "priority": 2},
    {"query": "T Rowe Price energy oil market outlook", "source": "T. Rowe Price", "source_zh": "普信", "priority": 2},
    {"query": "Invesco oil commodity energy outlook", "source": "Invesco", "source_zh": "景顺", "priority": 2},
    {"query": "Allianz energy outlook oil geopolitics", "source": "Allianz", "source_zh": "安联", "priority": 2},
    {"query": "Aberdeen oil energy market outlook", "source": "Abrdn", "source_zh": "安本", "priority": 2},
    {"query": "Goldman Sachs asset management oil energy", "source": "GSAM", "source_zh": "高盛资管", "priority": 2},
    {"query": "JP Morgan Asset Management energy outlook", "source": "JPMAM", "source_zh": "小摩资管", "priority": 2},
    {"query": "State Street global energy market outlook", "source": "State Street", "source_zh": "道富", "priority": 2},

    # ============================================================
    # Tier 5: 财经媒体报道的投行观点
    # ============================================================
    {"query": "Bloomberg Goldman Sachs oil price forecast", "source": "Bloomberg/Goldman", "source_zh": "彭博/高盛", "priority": 1},
    {"query": "Bloomberg analyst oil price target", "source": "Bloomberg", "source_zh": "彭博", "priority": 1},
    {"query": "Reuters investment bank oil forecast", "source": "Reuters", "source_zh": "路透", "priority": 1},
    {"query": "Reuters analyst oil price middle east", "source": "Reuters", "source_zh": "路透", "priority": 1},
    {"query": "Financial Times oil price forecast analyst", "source": "FT", "source_zh": "金融时报", "priority": 1},
    {"query": "Wall Street Journal energy analysts forecast", "source": "WSJ", "source_zh": "华尔街日报", "priority": 1},
    {"query": "CNBC oil price forecast analyst", "source": "CNBC", "source_zh": "CNBC", "priority": 2},
    {"query": "MarketWatch oil forecast investment bank", "source": "MarketWatch", "source_zh": "MarketWatch", "priority": 2},
    {"query": "oil analyst upgrade downgrade", "source": "Media", "source_zh": "财经媒体", "priority": 1},
    {"query": "bank research note oil energy", "source": "Media", "source_zh": "财经媒体", "priority": 1},

    # ============================================================
    # Tier 6: 地缘政治/能源主题（模糊宽泛搜索）
    # ============================================================
    # 通用油价
    {"query": "oil price forecast", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "Brent crude 2026 forecast", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "oil supply disruption Middle East 2026", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "Hormuz strait closure impact", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "energy crisis oil shock 2026", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "geopolitical risk oil market", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "inflation oil prices central bank", "source": "Analysts", "source_zh": "分析师", "priority": 2},
    {"query": "commodity supercycle oil 2026", "source": "Analysts", "source_zh": "分析师", "priority": 2},
    {"query": "WTI oil price target investment bank", "source": "Investment Banks", "source_zh": "投行", "priority": 1},
    {"query": "oil price outlook geopolitical risk", "source": "Analysts", "source_zh": "分析师", "priority": 2},

    # 美伊关系/停火/谈判
    {"query": "US Iran ceasefire deal negotiations 2026", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "Iran war ceasefire Pakistan mediation", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "Iran nuclear deal 2026 analyst", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "US Iran sanctions oil impact 2026", "source": "Analysts", "source_zh": "分析师", "priority": 1},

    # 霍尔木兹海峡/航运
    {"query": "Strait of Hormuz shipping insurance rates 2026", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "Hormuz strait tanker rates disruption", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "Persian Gulf shipping disruption analyst", "source": "Analysts", "source_zh": "分析师", "priority": 1},

    # 中东冲突外溢
    {"query": "Middle East war energy market impact 2026", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "Israel Iran conflict oil supply risk", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "Gulf states energy security Iran threat", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "Saudi Arabia oil facility attack risk 2026", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "OPEC production cut Middle East war", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "Iran oil exports sanctions evasion 2026", "source": "Analysts", "source_zh": "分析师", "priority": 1},

    # 宏观影响
    {"query": "oil shock recession risk 2026", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "energy inflation global economy forecast", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "stagflation risk oil price 2026", "source": "Analysts", "source_zh": "分析师", "priority": 1},
    {"query": "Middle East conflict market volatility", "source": "Analysts", "source_zh": "分析师", "priority": 2},

    # ============================================================
    # Tier 7: 智库/国际组织（搜索补充）
    # ============================================================
    {"query": "IMF Iran war oil supply Middle East 2026", "source": "IMF", "source_zh": "国际货币基金组织", "priority": 1},
    {"query": "IMF World Bank Iran conflict energy shock", "source": "IMF", "source_zh": "国际货币基金组织", "priority": 1},
    {"query": "IMF Middle East oil supply disruption forecast", "source": "IMF", "source_zh": "国际货币基金组织", "priority": 1},
    {"query": "BIS Iran war oil supply financial stability", "source": "BIS", "source_zh": "国际清算银行", "priority": 1},
    {"query": "BIS energy supply shock commodity prices", "source": "BIS", "source_zh": "国际清算银行", "priority": 1},
    {"query": "World Bank Iran war oil Middle East economy", "source": "World Bank", "source_zh": "世界银行", "priority": 1},
    {"query": "IEA International Energy Agency Iran oil supply", "source": "IEA", "source_zh": "国际能源署", "priority": 1},
    {"query": "IEA Hormuz Strait oil disruption forecast", "source": "IEA", "source_zh": "国际能源署", "priority": 1},
    {"query": "PIIE Peterson Institute Iran oil sanctions", "source": "PIIE", "source_zh": "彼得森国际经济研究所", "priority": 1},
    {"query": "PIIE oil price Middle East supply shock 2026", "source": "PIIE", "source_zh": "彼得森国际经济研究所", "priority": 1},
    {"query": "Brookings Institution Iran war oil Middle East", "source": "Brookings", "source_zh": "布鲁金斯学会", "priority": 1},
    {"query": "Brookings energy security Hormuz Strait", "source": "Brookings", "source_zh": "布鲁金斯学会", "priority": 1},
    {"query": "CSIS energy security Iran Hormuz Strait", "source": "CSIS", "source_zh": "战略与国际研究中心", "priority": 1},
    {"query": "CSIS Middle East oil supply chain disruption", "source": "CSIS", "source_zh": "战略与国际研究中心", "priority": 1},
    {"query": "CFR Iran war oil price energy outlook", "source": "CFR", "source_zh": "外交关系委员会", "priority": 1},
    {"query": "CFR Council Foreign Relations Hormuz oil", "source": "CFR", "source_zh": "外交关系委员会", "priority": 1},
    {"query": "RAND Corporation Iran energy security", "source": "RAND", "source_zh": "兰德公司", "priority": 1},
    {"query": "Carnegie Endowment Iran oil Middle East war", "source": "Carnegie", "source_zh": "卡内基国际和平基金会", "priority": 1},
    {"query": "Chatham House Hormuz oil Middle East", "source": "Chatham House", "source_zh": "查塔姆研究所", "priority": 1},
]


# ==========================================
# 工具函数
# ==========================================

def log(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}")

def parse_date(entry):
    """解析日期"""
    for field in ['published_parsed', 'updated_parsed', 'created_parsed']:
        if hasattr(entry, field) and getattr(entry, field):
            return datetime(*getattr(entry, field)[:6])
    return None

def generate_id(text):
    return hashlib.md5(text.encode()).hexdigest()[:12]

def compute_relevance_hint(title, summary):
    """粗筛：计算与中东/能源主题的相关度提示（0-5分）"""
    text = (title + ' ' + summary).lower()
    hits = sum(1 for kw in RELEVANCE_KEYWORDS if kw in text)
    return min(hits, 5)

def is_excluded_source(url):
    """检查URL是否属于排除来源"""
    if not url:
        return False
    url_lower = url.lower()
    for domain in EXCLUDED_DOMAINS:
        if domain in url_lower:
            return True
    for pattern in EXCLUDED_URL_PATTERNS:
        if pattern in url_lower:
            return True
    return False

def clean_html(text):
    """去除HTML标签"""
    return re.sub(r'<[^>]+>', '', text).strip()


# ==========================================
# RSS 抓取
# ==========================================

def fetch_rss_source(source, days=15):
    """抓取单个RSS源"""
    log(f"[RSS] {source['name_zh']}")

    try:
        feed = feedparser.parse(source['rss'])
        if feed.bozo and not feed.entries:
            log(f"  ERR feed parse failed: {feed.bozo_exception}")
            return []

        entries = []
        cutoff = datetime.now() - timedelta(days=days)

        for entry in feed.entries:
            entry_date = parse_date(entry)
            if entry_date and entry_date < cutoff:
                continue

            title = entry.get('title', '')
            summary = clean_html(entry.get('summary', '') or entry.get('description', ''))

            # 粗筛：至少匹配1个关键词才保留
            hint = compute_relevance_hint(title, summary)
            if hint == 0:
                continue

            # 排除指定来源
            link = entry.get('link', '')
            if is_excluded_source(link):
                continue

            item = {
                "id": generate_id(f"{source['name']}_{title}"),
                "title": title,
                "summary": summary[:500],
                "link": entry.get('link', ''),
                "pub_date": entry_date.isoformat() if entry_date else datetime.now().isoformat(),
                "source": source['name'],
                "source_zh": source['name_zh'],
                "source_type": source['type'],
                "region": source['region'],
                "fetch_method": "rss",
                "relevance_hint": hint,
            }
            entries.append(item)

        log(f"  OK {len(entries)} entries (filtered)")
        return entries

    except Exception as e:
        log(f"  ERR {e}")
        return []

def fetch_all_rss(days=15):
    """抓取所有RSS"""
    all_entries = []
    for source in THINK_TANK_SOURCES:
        entries = fetch_rss_source(source, days)
        all_entries.extend(entries)
    return all_entries


# ==========================================
# DuckDuckGo 搜索
# ==========================================

def search_ddg(query, max_results=8, timelimit='w'):
    """DuckDuckGo搜索，返回结果列表"""
    try:
        with DDGS(proxy=PROXY_HOST if USE_PROXY else None) as ddgs:
            results = list(ddgs.text(query, max_results=max_results, timelimit=timelimit))
            return results
    except Exception as e:
        log(f"  DDG ERR {str(e)[:60]}")
        return []

def search_google_fallback(query, max_results=5):
    """Google备选搜索"""
    if not GOOGLE_AVAILABLE:
        return []
    try:
        results = []
        for url in google_search(query, num_results=max_results, lang='en', proxy=PROXY_HOST if USE_PROXY else None):
            results.append({"href": url, "title": "", "body": ""})
        return results
    except Exception as e:
        log(f"  Google ERR {str(e)[:60]}")
        return []

def search_investment_banks(max_results=8):
    """
    搜索投行/资管研报
    DuckDuckGo主搜索，Google备选
    """
    if not DDGS_AVAILABLE and not GOOGLE_AVAILABLE:
        log("[WARN] No search engine available")
        return []

    log(f"\n[Search] Starting search ({len(IB_RESEARCH_SEARCHES)} tasks)...")
    log(f"[Search] DDG={'ON' if DDGS_AVAILABLE else 'OFF'} | Google={'ON' if GOOGLE_AVAILABLE else 'OFF'} | Proxy={'ON' if USE_PROXY else 'OFF'}")

    all_results = []
    timelimit = 'w'  # 最近一周

    for i, task in enumerate(IB_RESEARCH_SEARCHES, 1):
        query = task["query"]
        source = task["source"]
        priority = task["priority"]

        log(f"[{i}/{len(IB_RESEARCH_SEARCHES)}] [P{priority}] {source}: {query[:50]}...")

        # 主搜索: DuckDuckGo
        results = []
        if DDGS_AVAILABLE:
            results = search_ddg(query, max_results=max_results, timelimit=timelimit)

        # 如果DDG无结果且为高优先级，尝试Google
        if not results and priority == 1 and GOOGLE_AVAILABLE:
            log(f"  DDG empty, trying Google...")
            google_results = search_google_fallback(query, max_results=5)
            if google_results:
                for r in google_results:
                    r['_from_google'] = True
                results = google_results

        for r in results:
            href = r.get('href', r.get('url', ''))
            title = r.get('title', '')
            body = r.get('body', r.get('snippet', ''))

            if not href:
                continue

            # 排除指定来源
            if is_excluded_source(href):
                continue

            # 提取域名
            domain_match = re.search(r'https?://(?:www\.)?([^/]+)', href)
            domain = domain_match.group(1) if domain_match else "unknown"

            # 判断source_type
            media_sources = {"Bloomberg", "Reuters", "FT", "WSJ", "CNBC", "MarketWatch", "Bloomberg/Goldman", "Media"}
            source_type = "news" if source in media_sources else (
                "investment_bank" if any(x in source for x in ["Goldman", "Morgan", "JPM", "Citi", "Bank of", "Deutsche", "Barclays", "UBS", "HSBC", "Credit", "Societe", "Nomura", "Macquarie", "Jefferies", "Bernstein", "Evercore", "Oppenheimer", "Lazard", "RBC", "BMO", "TD", "ANZ", "Standard", "Wells"]) else (
                "asset_manager" if any(x in source for x in ["BlackRock", "PIMCO", "Fidelity", "Vanguard", "Schroders", "T. Rowe", "Invesco", "Allianz", "Abrdn", "GSAM", "JPMAM", "State Street"]) else "analyst"
            ))

            hint = compute_relevance_hint(title, body)

            item = {
                "id": generate_id(f"search_{href}_{title}"),
                "title": title,
                "summary": body[:500] if body else "",
                "link": href,
                "pub_date": datetime.now().isoformat(),
                "source": source,
                "source_zh": task["source_zh"],
                "source_type": source_type,
                "region": "global",
                "fetch_method": "google" if r.get('_from_google') else "duckduckgo",
                "search_query": query,
                "priority": priority,
                "domain": domain,
                "relevance_hint": hint,
            }
            all_results.append(item)

        log(f"  OK {len(results)} results")

        # 延迟（避免被封）
        time.sleep(0.5 if DDGS_AVAILABLE else 1.0)

    # 去重（基于链接）
    seen_links = set()
    unique_results = []
    for item in all_results:
        if item['link'] not in seen_links:
            seen_links.add(item['link'])
            unique_results.append(item)

    log(f"[Search] Total unique: {len(unique_results)} (from {len(all_results)} raw)")
    return unique_results


# ==========================================
# 保存
# ==========================================

def save_raw_data(think_tank_entries, search_results):
    """保存原始数据"""
    output = {
        "fetch_time": datetime.now().isoformat(),
        "date_range": {
            "from": (datetime.now() - timedelta(days=15)).isoformat(),
            "to": datetime.now().isoformat()
        },
        "config": {
            "rss_sources": len(THINK_TANK_SOURCES),
            "search_tasks": len(IB_RESEARCH_SEARCHES),
            "proxy": PROXY_HOST if USE_PROXY else "disabled",
            "engines": {
                "duckduckgo": DDGS_AVAILABLE,
                "google": GOOGLE_AVAILABLE
            }
        },
        "think_tank_entries": think_tank_entries,
        "ib_search_results": search_results,
        "total_raw_entries": len(think_tank_entries) + len(search_results),
        "search_summary": {
            "search_tasks": len(IB_RESEARCH_SEARCHES),
            "total_results": len(search_results),
            "ddg_available": DDGS_AVAILABLE,
            "google_available": GOOGLE_AVAILABLE,
        }
    }

    os.makedirs("data", exist_ok=True)
    filepath = "data/research_raw_data.json"

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log(f"\n[Saved] {filepath}")
    log(f"  - Think tank RSS: {len(think_tank_entries)} (from {len(THINK_TANK_SOURCES)} sources)")
    log(f"  - IB/Search: {len(search_results)}")
    log(f"  - Total: {len(think_tank_entries) + len(search_results)}")

    return filepath


# ==========================================
# 主流程
# ==========================================

def main():
    log("=" * 60)
    log("Research Data Fetcher v2 - Enhanced")
    log(f"Proxy: {'ON (' + PROXY_HOST + ')' if USE_PROXY else 'OFF'}")
    log(f"Engines: DDG={'ON' if DDGS_AVAILABLE else 'OFF'} Google={'ON' if GOOGLE_AVAILABLE else 'OFF'}")
    log(f"RSS sources: {len(THINK_TANK_SOURCES)} | Search tasks: {len(IB_RESEARCH_SEARCHES)}")
    log("=" * 60)

    # 1. 抓取智库RSS
    log("\n[Step 1] Fetching RSS feeds...")
    think_tank_entries = fetch_all_rss(days=15)

    # 2. 搜索投行/资管研报
    log("\n[Step 2] Searching IB/AM research...")
    search_results = search_investment_banks(max_results=8)

    # 3. 保存
    log("\n[Step 3] Saving...")
    filepath = save_raw_data(think_tank_entries, search_results)

    # 统计
    hint_dist = {}
    for e in think_tank_entries + search_results:
        h = e.get('relevance_hint', 0)
        hint_dist[h] = hint_dist.get(h, 0) + 1

    log(f"\n[Stats] Relevance distribution:")
    for score in sorted(hint_dist.keys(), reverse=True):
        log(f"  Score {score}: {hint_dist[score]} entries")

    log("\n" + "=" * 60)
    log(f"Complete! Total: {len(think_tank_entries) + len(search_results)}")
    log("=" * 60)

if __name__ == "__main__":
    main()
