#!/usr/bin/env python3
"""prospector: 每日需求信号 → LLM 评分 → 机会日报。

抓取(HN/Reddit/RSS/AppStore差评) → SQLite 去重 → 规则预筛 → LLM 按 rubric 打分
→ reports/YYYY-MM-DD.md。本地跑: python pipeline.py [--dry-run]。
GitHub Actions 负责 cron 调度和把报告 commit 回仓库（见 .github/workflows/daily.yml）。
"""
import argparse
import hashlib
import html
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import feedparser
import requests
import yaml

ROOT = Path(__file__).parent
DB_PATH = ROOT / "data" / "signals.db"
REPORTS_DIR = ROOT / "reports"
TZ = timezone(timedelta(hours=8))  # 北京时间
UA = "prospector/0.1 (personal research pipeline)"
BROWSER_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

RUBRIC = """先为每条信号分类（track）：
- product：适合做产品/付费工具/SaaS 订阅——本质是"有人会为解决这个痛付钱"
- traffic：适合做流量站/工具页/数据页，靠展示广告或 affiliate 变现——本质是"有人会反复搜索并使用"
- minigame：微信小游戏方向的信号（玩法/品类/平台政策/爆款拆解/成败复盘），按个人主体 IAA 变现评估
- none：纯新闻/随笔/无机会
两道以上皆可时选主赛道，并在 reason 里注明其他可能性。

【product 赛道】六维各 1-5 分，总分 30：
B1 付费意愿证据：5=竞品收费多年且社区有人主动求替代/抱怨涨价；3=2-3 家竞品有公开定价页和真实评价；1=无付费竞品且人群是价格敏感 C 端
B2 竞争结构：5=有竞品(市场被验证)且存在可命名的 feature-gap，差异有私有数据/垂直工作流护城河；3=强竞品但差评主题指向其架构/商业模式难以快速修补的缺口；1=核心功能已被大厂(Apple/Google/Microsoft/Notion/Canva/OpenAI)免费内置
B3 获客通道：5=存在迁移窗口(竞品关站/收费/涨价)或有自有分发渠道；4="alternative to 竞品"有真实搜索量或人群聚集在可参与社区；1=无搜索意图、无社区，只能投广告/冷邮件
B4 付费人群质量：5=B2B 有预算线、痛点挂钩收入/合规、可付 $100+/月；3=prosumer/独立创业者可付 $5-15/月；1=免费习惯的 C 端
B5 时机窗口（非 shutdown/迁移型机会默认 3 分）：5=尚未公告但可从免费→付费信号提前布局，或公告当日即可上线；4=距关站 4-9 个月，可备齐"产品+一键迁移+定向优惠"三件套；2=距关站<2 个月来不及建迁移工具；1=关站已超 6 个月迁移潮被接走
B6 独立交付：5=MVP<4 周、成本边际化、无单点依赖、可沉淀私有数据复利；3=MVP 2-3 个月、上游可替换；1=MVP 6+ 月或核心依赖单一上游 API 且无 fallback
product 一票否决（命中则 track=none 并注明）：核心功能已被大厂免费内置；无付费竞品+无预算 C 端+非迁移窗口三者同时成立；单点上游依赖无切换方案；3-4h/天约束下 MVP 三个月内无法上线收费；shutdown 型但距关站<6 周

【traffic 赛道】六维各 1-5 分，总分 30：
A1 页面意图抗性：5=纯 tool intent（calculator/converter/generator/tracker/checker/数据查询），AIO 无法替代交互；3=词簇一半是 info，可改造为"info+内嵌工具"页；1=纯 info 词且 SERP 已出 AIO 且答案完整
A2 关键词可赢性（新域名视角）：5=KD<20 且 SERP 前排有近两年新建小站排上去的先例；3=KD 20-30 或前排有 UGC/论坛帖穿插；1=KD>50 或前排全是权威站
A3 变现效率：5=词簇 CPC>$15（金融/法律/保险/B2B）且流量以美英加为主；3=CPC $3-5；1=CPC<$1 泛娱乐
A4 需求规模：5=词簇合计>5 万 mv，可扩 100+ 含独有数据的页面；3=2 千-1 万 mv 或 10+ 同构长尾页；1=词簇<500 mv 且无相邻扩展
A5 独有数据/交互深度：5=专有数据管道或用户生成数据，页面价值随时间自增；3=简单交互或自建小型数据集；1=静态文字，可被 AIO 一段话复述
A6 达门槛可行性（3-4h/天，12 个月）：5=可破 10 万 PV/月（Raptive 档，约 $1500-4500/月）；4=5-10 万 sessions（Mediavine 档）；3=1-5 万 sessions；1=到不了 Journey 门槛（1k sessions/月）
traffic 一票否决（命中则 track=none 并注明）：词簇已被 AIO 覆盖且页面形态无法提供交互/独有数据；寄生 SEO（借高权域名挂第三方内容）；无人工介入的纯 AI 批量铺页；SERP 前排被 Reddit/YouTube/大厂官方占 ≥5 席且无法用 tool intent 差异化

【minigame 赛道：微信小游戏玩法方向（个人 IAA）】本赛道输出的是"可立项的游戏方向/点子"，不是新闻摘要：
- 先判定机会类型，angle 必须以此开头：[迁移]=其他平台（Steam/手游/海外超休闲）已验证玩法→小游戏化；[改造]=已有小游戏玩法换题材/轻量化；[整合]=两个已验证玩法融合。angle 还需含具体玩法方向与预估开发周数（如"[迁移] 竖屏单局化+合成养成，约 4 周"）。
- 纯行业新闻、政策解读、投融资、市场数据、无具体玩法可提炼的内容 → 一律 track=none，无论题材多重要。
六维各 1-5 分，总分 30：
C1 玩法验证度与吸量：5=来源玩法在其他平台已被市场验证（销量/榜单/流水可查）且天然可视化爆点（物理效果/大数字/破坏反馈），30 秒素材可演示；3=有成熟吸量原型（割草/合成/解压）可借鉴但需换皮；1=未验证的自创玩法，或纯数值挂机无画面爆点
C2 竞争与拥挤度（看微信小游戏生态内）：5=微信内同类<5 款且来源玩法验证于 6 个月内（时间差窗口）；3=品类活跃但有明显细分空位（题材/融合差异）；1=微信内已泛滥（咸鱼like/无尽冬日like/成熟 SLG 的正面复刻）
C3 制作成本（个人 3-4h/天）：5=单场景可复用玩法（Roguelike/物理/合成），AI 可生成大部分美术，≤4 周出可测版；3=2D 可行但内容量大（百关+）或需 4-8 周；1=需 3D 精建模/大体量关卡/多人服务端
C4 IAA 变现效率：5=死亡复活/加速/翻倍类强激励场景，人均广告位 5+ 次/日，且可自然插入屏（插屏约占休闲小游戏收入 44%，别只做激励视频）；3=3-5 次自然广告位、激励视频有明确价值交换；1=人均广告曝光<2 次/日
C5 流量获取路径：5=适合视频号直播挂载（规则简单、强互动、观众可指挥主播；直播 CP 分成最高 90%）或 PC 多窗口挂机（PC 用户时长高近一倍+额外 10% 广告金）；3=可蹭一个明确入口（搜索词/游戏圈）；1=纯自然长尾、无入口红利
C6 合规与上线摩擦：5=纯 IAA+中文命名免软著、已备案账号可复用、2-3 周可上线；3=标准 IAA 流程软著在办（全周期 3-6 周）；1=类目敏感（棋牌/捕鱼/真人竞技）或备案预期>2 个月
minigame 一票否决（命中则 track=none 并注明）：依赖 IAP/虚拟支付（个人无版号不可能合规）；可测版本开发超 8 周；玩法 30 秒内无法演示；正面复刻头部品类；依赖买量回本、周期>3 个月；涉现金/实物奖励诱导（涉诈红线）
minigame 校准：个人 IAA 常态是月几百-几千元，吃到直播/PC 入口红利才上万。Steam 畅销榜上的 AAA/重叙事/重操作游戏若无法轻量化为"竖屏、30 秒可玩、单局 1-3 分钟"的小游戏形态，视为迁移不成立（C1/C3 打低分）。

通用评分纪律：点赞和"想要"不是付费意愿，waitlist 无意义；竞品收费且差评多是好信号；搜索量/CPC/KD 看不到时按品类常识估并在 action 里写明"需 Glimpse/Ahrefs 验证"。"""


def load_config():
    return yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))


def strip_html(text):
    return html.unescape(re.sub(r"<[^>]+>", " ", text or "")).strip()


def make_item(source, ext_id, title, body, url, meta):
    return {"source": source, "ext_id": str(ext_id), "title": strip_html(title),
            "body": strip_html(body)[:800], "url": url, "meta": meta}


# ---------- 抓取层 ----------

def fetch_hn(cfg):
    src = cfg["sources"]["hacker_news"]
    since = int(time.time()) - src.get("lookback_hours", 48) * 3600
    items = []
    for q in src.get("queries", []):
        try:
            r = requests.get(
                "https://hn.algolia.com/api/v1/search",
                params={"query": q, "tags": "ask_hn",
                        "numericFilters": f"created_at_i>{since}", "hitsPerPage": 30},
                timeout=30)
            r.raise_for_status()
            for h in r.json().get("hits", []):
                items.append(make_item(
                    "hn", h["objectID"], h.get("title") or "", h.get("story_text") or "",
                    f"https://news.ycombinator.com/item?id={h['objectID']}",
                    f"AskHN {h.get('points', 0)}p/{h.get('num_comments', 0)}c"))
        except Exception as e:
            print(f"[warn] hn query '{q}' failed: {e}")
    return items


def reddit_headers():
    cid = os.environ.get("REDDIT_CLIENT_ID")
    sec = os.environ.get("REDDIT_CLIENT_SECRET")
    if not (cid and sec):
        return None
    r = requests.post("https://www.reddit.com/api/v1/access_token",
                      auth=(cid, sec), data={"grant_type": "client_credentials"},
                      headers={"User-Agent": UA}, timeout=30)
    r.raise_for_status()
    return {"Authorization": "bearer " + r.json()["access_token"], "User-Agent": UA}


def fetch_reddit(cfg):
    headers = reddit_headers()
    src = cfg["sources"]["reddit"]
    items = []
    if headers is None:
        # 免凭证 .rss 降级：实测连发第二个请求即 429，必须串行 + 硬间隔
        # ponytail: OAuth 凭证就绪后走下面正式档（100 QPM），此分支可删
        print("[warn] REDDIT_CLIENT_ID/SECRET 未设置，走免凭证 .rss 慢速模式（串行+15s，部分 sub 可能被限流跳过）")
        s = requests.Session()
        for sub in src.get("subs", []):
            try:
                r = s.get(f"https://www.reddit.com/r/{sub}/new/.rss",
                          params={"limit": 50}, headers={"User-Agent": BROWSER_UA}, timeout=30)
                if r.status_code == 429:
                    time.sleep(30)
                    r = s.get(f"https://www.reddit.com/r/{sub}/new/.rss",
                              params={"limit": 50}, headers={"User-Agent": BROWSER_UA}, timeout=30)
                r.raise_for_status()
                f = feedparser.parse(r.content)
                for e in f.entries[:30]:
                    items.append(make_item(
                        "reddit", e.get("id") or e.get("link"), e.get("title") or "",
                        e.get("summary") or "", e.get("link") or "", f"r/{sub}"))
            except Exception as e:
                print(f"[warn] reddit r/{sub} rss failed: {e}")
            time.sleep(15)
        return items
    for sub in src.get("subs", []):
        try:
            r = requests.get(f"https://oauth.reddit.com/r/{sub}/new",
                             params={"limit": src.get("limit", 50)},
                             headers=headers, timeout=30)
            r.raise_for_status()
            for c in r.json().get("data", {}).get("children", []):
                d = c["data"]
                if d.get("stickied") or d.get("removed_by_category"):
                    continue
                # r/SomebodyMakeThis 对已存在创意打 "Already Exists" 类 flair
                if "exists" in (d.get("link_flair_text") or "").lower():
                    continue
                items.append(make_item(
                    "reddit", d["id"], d.get("title") or "", d.get("selftext") or "",
                    "https://reddit.com" + d.get("permalink", ""),
                    f"r/{sub} {d.get('score', 0)}p/{d.get('num_comments', 0)}c"))
        except Exception as e:
            print(f"[warn] reddit r/{sub} failed: {e}")
    return items


def fetch_rss(cfg):
    items = []
    for feed in cfg["sources"]["rss"].get("feeds", []):
        if isinstance(feed, dict):
            url = feed["url"]
            kws = [k.lower() for k in feed.get("keywords", [])]
        else:
            url, kws = feed, []
        try:
            f = feedparser.parse(url)
            if f.get("bozo") and not f.entries:
                print(f"[warn] rss {url} unreachable/invalid (bozo)")
                continue
            label = f.feed.get("title", url)
            for e in f.entries[:30]:
                title = e.get("title") or ""
                body = strip_html(e.get("summary") or "")
                if kws and not any(k in (title + body).lower() for k in kws):
                    continue
                items.append(make_item(
                    "rss", e.get("id") or e.get("link") or title, title,
                    body, e.get("link") or url, label))
        except Exception as e:
            print(f"[warn] rss {url} failed: {e}")
    return items


def fetch_tuoluo(cfg):
    src = cfg["sources"].get("tuoluo") or {}
    channel = src.get("channel", "https://www.youxituoluo.com/html5game")
    try:
        r = requests.get(channel, timeout=30, headers={"User-Agent": BROWSER_UA})
        r.raise_for_status()
    except Exception as e:
        print(f"[warn] tuoluo channel failed: {e}")
        return []
    paths = list(dict.fromkeys(re.findall(r'href="(/\d+\.html)"', r.text)))
    items = []
    for path in paths[:src.get("max_details", 12)]:
        url = "https://www.youxituoluo.com" + path
        try:
            d = requests.get(url, timeout=30, headers={"User-Agent": BROWSER_UA}).text
            t = re.search(r"<title>([^<]+)</title>", d)
            desc = (re.search(r'property="og:description"\s+content="([^"]+)"', d)
                    or re.search(r'name="description"\s+content="([^"]+)"', d))
            title = t.group(1).replace("_游戏陀螺", "").strip() if t else ""
            if title:
                items.append(make_item("tuoluo", path.removeprefix("/").removesuffix(".html"),
                                       title, desc.group(1) if desc else "", url, "游戏陀螺·小游戏"))
        except Exception as e:
            print(f"[warn] tuoluo {path} failed: {e}")
    return items


def fetch_appstore(cfg):
    items = []
    for app in cfg["sources"].get("appstore") or []:
        url = (f"https://itunes.apple.com/{app['country']}/rss/customerreviews/"
               f"id={app['id']}/sortby=mostrecent/page=1/json")
        try:
            entries = requests.get(url, timeout=30).json().get("feed", {}).get("entry", [])
            if isinstance(entries, dict):  # 只有一条时 Apple 返回对象而非数组
                entries = [entries]
            for e in entries:
                try:
                    rating = int(e["im:rating"]["label"])
                except (KeyError, ValueError, TypeError):
                    continue
                if rating > 3:
                    continue  # 只要差评
                ext = e.get("id", {}).get("attributes", {}).get("im:id") or e["title"]["label"]
                items.append(make_item(
                    "appstore", f"{app['id']}-{ext}",
                    f"[{app['name']} {rating}★差评] {e['title']['label']}",
                    e["content"]["label"], f"https://apps.apple.com/{app['country']}/app/id{app['id']}",
                    f"{app['name']} {rating}★ review"))
        except Exception as e:
            print(f"[warn] appstore {app.get('name')} failed: {e}")
    return items


def fetch_steam(cfg):
    src = cfg["sources"].get("steam") or {}
    s = requests.Session()
    s.trust_env = False  # 本机代理访问 steam 不稳，直连更可靠
    try:
        r = s.get("https://store.steampowered.com/api/featuredcategories/", timeout=30,
                  headers={"User-Agent": BROWSER_UA})
        r.raise_for_status()
        d = r.json()
    except Exception as e:
        print(f"[warn] steam failed: {e}")
        return []
    items = []
    for cat, label in (("top_sellers", "畅销榜"), ("new_releases", "新品热门")):
        for it in d.get(cat, {}).get("items", [])[:src.get("per_category", 15)]:
            appid = it.get("id")
            name = it.get("name", "")
            # 硬件条目（Steam Machine/Frame/Deck 等）也有 appid，按名单排除
            if not appid or not name or name.startswith(("Steam Machine", "Steam Frame", "Steam Deck")):
                continue
            desc = (it.get("short_description")
                    or f"Steam {label}上榜（无简介，按游戏名与已知玩法判断迁移潜力）")
            items.append(make_item("steam", f"{appid}-{cat}", name, desc,
                                   f"https://store.steampowered.com/app/{appid}",
                                   f"Steam {label}"))
    # 标签新品榜（search infinite=1 端点，结果内嵌 HTML 需正则解析）
    for tag in src.get("tags", []):
        try:
            r2 = s.get("https://store.steampowered.com/search/results/",
                       params={"json": 1, "start": 0, "count": 30, "infinite": 1,
                               "tags": tag, "sort_by": "Released_DESC", "cc": "US", "l": "english"},
                       timeout=30, headers={"User-Agent": BROWSER_UA})
            html_rows = r2.json().get("results_html", "")
            for m in re.finditer(r'data-ds-appid="(\d+)"(.{0,1500}?)<span class="title">\s*([^<]+)</span>',
                                 html_rows, re.S):
                appid, _, name = m.group(1), m.group(2), m.group(3).strip()
                items.append(make_item("steam", f"{appid}-tag-{tag}", name,
                                       f"Steam 标签[{tag}]新品榜（按名称与已知玩法判断迁移潜力）",
                                       f"https://store.steampowered.com/app/{appid}",
                                       f"Steam {tag}新品"))
        except Exception as e:
            print(f"[warn] steam tag '{tag}' failed: {e}")
    return items


def fetch_eol(cfg):
    # ponytail: endoflife.date 的 all.json 只是产品名列表，周期详情需逐产品拉；
    # 默认白名单为空=不请求，需要时在 config 填产品 slug（如 nodejs）。关停信号主力是
    # killedbygoogle + Google News 检索式 RSS，不依赖这里。
    window = (cfg["sources"].get("eol") or {}).get("window_days", 14)
    earliest = datetime.now(TZ) - timedelta(days=window)
    horizon = datetime.now(TZ) + timedelta(
        days=(cfg["sources"].get("eol") or {}).get("horizon_days", 270))
    items = []
    for slug in (cfg["sources"].get("eol") or {}).get("products", []):
        try:
            cycles = requests.get(f"https://endoflife.date/api/{slug}.json", timeout=30,
                                  headers={"User-Agent": BROWSER_UA}).json()
            for c in cycles:
                eol = c.get("eol")
                if not isinstance(eol, str):
                    continue
                try:
                    d = datetime.fromisoformat(eol[:10]).replace(tzinfo=TZ)
                except ValueError:
                    continue
                if earliest <= d <= horizon:
                    items.append(make_item(
                        "eol", f"{slug}-{c.get('cycle')}-{eol}",
                        f"[EOL] {slug} {c.get('cycle','')} 将于 {d:%Y-%m-%d} 停止支持",
                        f"{slug} {c.get('cycle','')} 生命周期结束日期 {d:%Y-%m-%d}（endoflife.date 官方周期数据），"
                        f"下游用户面临升级/迁移压力。",
                        f"https://endoflife.date/{slug}", "endoflife.date"))
                    break
        except Exception as e:
            print(f"[warn] endoflife {slug} failed: {e}")
    try:
        grave = requests.get(
            "https://raw.githubusercontent.com/codyogden/killedbygoogle/main/graveyard.json",
            timeout=30, headers={"User-Agent": BROWSER_UA}).json()
        for x in grave:
            dc = (x.get("dateClose") or "")[:10]
            try:
                d = datetime.strptime(dc, "%Y-%m-%d").replace(tzinfo=TZ)
            except ValueError:
                continue
            if earliest <= d <= horizon:
                items.append(make_item(
                    "eol", f"kbg-{x.get('name')}",
                    f"[关停] Google {x.get('name')} 将于 {d:%Y-%m-%d} 关闭",
                    (x.get("description") or "")[:300],
                    x.get("link") or "https://killedbygoogle.com", "Killed by Google"))
    except Exception as e:
        print(f"[warn] killedbygoogle failed: {e}")
    return items


def fetch_v2ex(cfg):
    items = []
    for node in (cfg["sources"].get("v2ex") or {}).get("nodes", []):
        try:
            r = requests.get("https://www.v2ex.com/api/topics/show.json",
                             params={"node_name": node}, timeout=30,
                             headers={"User-Agent": BROWSER_UA})
            r.raise_for_status()
            for t in r.json():
                items.append(make_item(
                    "v2ex", t.get("id"), t.get("title") or "", t.get("content") or "",
                    t.get("url") or "https://v2ex.com/go/create", f"V2EX/{node}"))
        except Exception as e:
            print(f"[warn] v2ex {node} failed: {e}")
    return items


# ---------- 去重 ----------

def dedupe(items, cooldown_days):
    # ponytail: 标题规范化 hash 去重；近似重复(同义改写)漏网时再上 SimHash
    DB_PATH.parent.mkdir(exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS seen(source TEXT, ext_id TEXT, first_seen TEXT,"
                " PRIMARY KEY(source, ext_id))")
    con.execute("CREATE TABLE IF NOT EXISTS titlehash(h TEXT PRIMARY KEY, first_seen TEXT)")
    now = datetime.now(TZ)
    fresh = []
    for it in items:
        h = hashlib.md5(re.sub(r"\W+", " ", it["title"].lower()).strip().encode()).hexdigest()
        row = con.execute("SELECT first_seen FROM titlehash WHERE h=?", (h,)).fetchone()
        if row and (now - datetime.fromisoformat(row[0])).days < cooldown_days:
            continue
        if con.execute("SELECT 1 FROM seen WHERE source=? AND ext_id=?",
                       (it["source"], it["ext_id"])).fetchone():
            continue
        con.execute("INSERT OR REPLACE INTO seen VALUES (?,?,?)",
                    (it["source"], it["ext_id"], now.isoformat()))
        con.execute("INSERT OR IGNORE INTO titlehash VALUES (?,?)", (h, now.isoformat()))
        fresh.append(it)
    con.commit()
    con.close()
    return fresh


def prefilter(items, cfg):
    f = cfg["filters"]
    neg = [w.lower() for w in f.get("negative_keywords", [])]
    keep = []
    for it in items:
        text = (it["title"] + " " + it["body"]).lower()
        if len(it["title"]) < 8 or len(text) < f.get("min_text_len", 40):
            continue
        if any(w in text for w in neg):
            continue
        keep.append(it)
    return keep


# ---------- LLM ----------

def llm_json(cfg, messages):
    base = os.environ.get("LLM_BASE_URL", cfg["llm"]["base_url"]).rstrip("/")
    key = os.environ.get("LLM_API_KEY")
    model = os.environ.get("LLM_MODEL", cfg["llm"]["model"])
    if not key:
        sys.exit("[error] 未设置 LLM_API_KEY")
    body = {"model": model, "messages": messages, "temperature": 0.3,
            "response_format": {"type": "json_object"}}
    for attempt_json in (True, False):  # 个别兼容端点不支持 json_object，降级重试
        if not attempt_json:
            body.pop("response_format", None)
        r = requests.post(f"{base}/chat/completions",
                          headers={"Authorization": f"Bearer {key}"}, json=body, timeout=300)
        if r.status_code == 400 and attempt_json:
            continue
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"].strip()
        content = re.sub(r"^```(json)?\s*|\s*```$", "", content)
        return json.loads(content)
    raise RuntimeError("LLM call failed")


def score_items(cfg, items):
    scored = []
    for i in range(0, len(items), 25):
        batch = items[i:i + 25]
        listing = "\n\n".join(
            f"[{j}] {it['title']}\n{it['body'][:500]}\n({it['meta']})"
            for j, it in enumerate(batch))
        try:
            data = llm_json(cfg, [
                {"role": "system",
                 "content": f"你为一位独立开发者（海外市场，每天 3-4 小时）筛选机会。{RUBRIC} 只输出 JSON。"},
                {"role": "user",
                 "content": f"以下是 {len(batch)} 条来自互联网的信号（社区帖子/愿望/差评/趋势）。"
                            f"对每条：先定 track（product/traffic/minigame/none，注意各赛道一票否决项），"
                            f"product 输出 b1-b6 六个维度分，traffic 输出 a1-a6 六个维度分，"
                            f"minigame 输出 c1-c6 六个维度分，none 只给 reason。\n\n{listing}\n\n"
                            f'输出格式：{{"items":[{{"i":0,"track":"product",'
                            f'"scores":{{"b1":3,"b2":4,"b3":3,"b4":4,"b5":3,"b6":4}},"total":21,'
                            f'"reason":"一句话理由","angle":"一句话切入角度",'
                            f'"kw":"建议主词簇(仅traffic)","action":"一个具体的下一步验证动作"}}]}}'}])
            for row in data.get("items", []) if isinstance(data.get("items"), list) else []:
                j = row.get("i", -1)
                if isinstance(j, int) and 0 <= j < len(batch):
                    b = dict(batch[j])
                    b["track"] = row.get("track", "none")
                    sc = row.get("scores") or {}
                    b["scores"] = {k: int(v) for k, v in sc.items()
                                   if isinstance(v, (int, float)) and not isinstance(v, bool)}
                    b["total"] = int(row.get("total") or sum(b["scores"].values()))
                    b.update({k: row.get(k, "") for k in ("reason", "angle", "action", "kw")})
                    scored.append(b)
        except Exception as e:
            print(f"[warn] scoring batch {i//25} failed: {e}")
    return scored


# ---------- 报告 ----------

TRACK_TITLES = {"product": "产品订阅方向（SaaS/付费工具）",
                "traffic": "流量站方向（工具页/数据页 + 展示广告或 affiliate）",
                "minigame": "微信小游戏方向（个人 IAA·玩法方向提案）"}

# 评分行用中文名，不用 B1/A1 代号（读者不需要记 schema）
DIM_LABELS = {
    "b1": "付费意愿", "b2": "竞争结构", "b3": "获客通道", "b4": "人群质量",
    "b5": "时机窗口", "b6": "独立交付",
    "a1": "意图抗性", "a2": "词可赢性", "a3": "变现效率", "a4": "需求规模",
    "a5": "独有数据", "a6": "达门槛",
    "c1": "玩法验证", "c2": "竞争拥挤", "c3": "制作成本", "c4": "IAA变现",
    "c5": "流量入口", "c6": "合规上线",
}


def write_report(cfg, scored, stats, date_str):
    top_n = cfg["report"]["top_n"]
    lines = [
        f"# 每日机会推荐 {date_str}",
        "",
        f"今日抓取 {stats['fetched']} 条信号，去重后 {stats['fresh']} 条，预筛后送评 {stats['scored']} 条。",
        "",
    ]
    top_all = []
    for track in ("product", "traffic", "minigame"):
        pool = [x for x in scored if x.get("track") == track]
        top = sorted(pool, key=lambda x: -x.get("total", 0))[:top_n]
        lines += [f"## {TRACK_TITLES[track]}", ""]
        if not top:
            lines += ["今日无符合该赛道的机会。", ""]
        for n, it in enumerate(top, 1):
            dims = " · ".join(f"{DIM_LABELS.get(k, k)} {v}" for k, v in it.get("scores", {}).items())
            lines += [
                f"### {n}. {it['title']}（{it['total']}/30）",
                f"- 来源：{it['meta']} ｜ [原文]({it['url']})",
                f"- 评分：{dims}",
            ]
            if it.get("kw"):
                lines.append(f"- 建议主词簇：{it['kw']}（搜索量/CPC/KD 需 Glimpse/Ahrefs 验证）")
            lines += [
                f"- 理由：{it.get('reason', '')}",
                f"- 切入：{it.get('angle', '')}",
                f"- 下一步：{it.get('action', '')}",
                "",
            ]
        top_all += top
    lines += ["---", "",
              "> 定位提醒：这是灵感催化剂不是点子贩卖机——最好的判断仍来自你读原文后的直觉。",
              "> 觉得某条不靠谱？直接改 pipeline.py 里的 RUBRIC，这就是自建的意义。"]
    REPORTS_DIR.mkdir(exist_ok=True)
    path = REPORTS_DIR / f"{date_str}.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path, top_all


def notify_telegram(top):
    tok = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat = os.environ.get("TELEGRAM_CHAT_ID")
    if not (tok and chat) or not top:
        return
    text = "⛏ prospector 日报 Top5:\n" + "\n".join(
        f"{n}. {it['title'][:60]}（{it['total']}分） {it['url']}"
        for n, it in enumerate(top[:5], 1))
    try:
        requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                      json={"chat_id": chat, "text": text[:4000]}, timeout=30)
    except Exception as e:
        print(f"[warn] telegram failed: {e}")


def interleave_by_source(items):
    # 按源轮转重排：防止高产量源（reddit/rss）独占 max_score_per_day 上限，
    # 让 eol/steam/v2ex/陀螺 等尾部源也有名额
    by_src = {}
    for it in items:
        by_src.setdefault(it["source"], []).append(it)
    out = []
    while by_src:
        for k in list(by_src):
            out.append(by_src[k].pop(0))
            if not by_src[k]:
                del by_src[k]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="只跑抓取+去重+预筛，不调 LLM")
    ap.add_argument("--signals-only", action="store_true",
                    help="抓取+去重+预筛后导出 data/pending.json，评分交给外部（ZCode agent），不调 LLM API")
    args = ap.parse_args()

    cfg = load_config()
    items = (fetch_hn(cfg) + fetch_reddit(cfg) + fetch_rss(cfg)
             + fetch_tuoluo(cfg) + fetch_steam(cfg) + fetch_eol(cfg)
             + fetch_v2ex(cfg) + fetch_appstore(cfg))
    fresh = dedupe(items, cfg["filters"].get("cooldown_days", 14))
    keep = prefilter(fresh, cfg)
    keep = interleave_by_source(keep)
    stats = {"fetched": len(items), "fresh": len(fresh), "scored": len(keep)}
    print(f"fetched={stats['fetched']} fresh={stats['fresh']} kept={stats['scored']}")

    if args.dry_run:
        for it in keep[:5]:
            print(f"  - [{it['source']}] {it['title'][:70]} ({it['meta']})")
        return

    keep = keep[:cfg["filters"].get("max_score_per_day", 150)]
    stats["scored"] = len(keep)
    if not keep:
        print("[info] 今日无新信号")
        return

    if args.signals_only:
        payload = {"date": datetime.now(TZ).strftime("%Y-%m-%d"),
                   "stats": stats, "items": keep}
        (ROOT / "data").mkdir(exist_ok=True)
        path = ROOT / "data" / "pending.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=1),
                        encoding="utf-8")
        print(f"pending -> data/pending.json ({len(keep)} items, 待 ZCode agent 评分)")
        return

    scored = score_items(cfg, keep)
    if not scored:
        print("[error] LLM 评分全部失败")
        sys.exit(1)
    date_str = datetime.now(TZ).strftime("%Y-%m-%d")
    path, top = write_report(cfg, scored, stats, date_str)
    print(f"report -> {path}")
    notify_telegram(top)


if __name__ == "__main__":
    main()
