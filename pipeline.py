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

RUBRIC = """先为每条信号分类（track）：
- product：适合做产品/付费工具/SaaS 订阅——本质是"有人会为解决这个痛付钱"
- traffic：适合做流量站/工具页/数据页，靠展示广告或 affiliate 变现——本质是"有人会反复搜索并使用"
- none：纯新闻/随笔/无机会
两道皆可时选主赛道，并在 reason 里注明另一道的可能性。

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
    if headers is None:
        print("[warn] REDDIT_CLIENT_ID/SECRET 未设置，跳过 Reddit 源")
        return []
    src = cfg["sources"]["reddit"]
    items = []
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
    for url in cfg["sources"]["rss"].get("feeds", []):
        try:
            f = feedparser.parse(url)
            if f.get("bozo") and not f.entries:
                print(f"[warn] rss {url} unreachable/invalid (bozo)")
                continue
            label = f.feed.get("title", url)
            for e in f.entries[:30]:
                items.append(make_item(
                    "rss", e.get("id") or e.get("link") or e.get("title"), e.get("title") or "",
                    e.get("summary") or "", e.get("link") or url, label))
        except Exception as e:
            print(f"[warn] rss {url} failed: {e}")
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
                            f"对每条：先定 track（product/traffic/none，注意一票否决项），"
                            f"product 输出 b1-b6 六个维度分，traffic 输出 a1-a6 六个维度分，"
                            f"none 只给 reason。\n\n{listing}\n\n"
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
                "traffic": "流量站方向（工具页/数据页 + 展示广告或 affiliate）"}


def write_report(cfg, scored, stats, date_str):
    top_n = cfg["report"]["top_n"]
    lines = [
        f"# 每日机会推荐 {date_str}",
        "",
        f"今日抓取 {stats['fetched']} 条信号，去重后 {stats['fresh']} 条，预筛后送评 {stats['scored']} 条。",
        "",
    ]
    top_all = []
    for track in ("product", "traffic"):
        pool = [x for x in scored if x.get("track") == track]
        top = sorted(pool, key=lambda x: -x.get("total", 0))[:top_n]
        lines += [f"## {TRACK_TITLES[track]}", ""]
        if not top:
            lines += ["今日无符合该赛道的机会。", ""]
        for n, it in enumerate(top, 1):
            dims = " · ".join(f"{k.upper()} {v}" for k, v in it.get("scores", {}).items())
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="只跑抓取+去重+预筛，不调 LLM")
    ap.add_argument("--signals-only", action="store_true",
                    help="抓取+去重+预筛后导出 data/pending.json，评分交给外部（ZCode agent），不调 LLM API")
    args = ap.parse_args()

    cfg = load_config()
    items = fetch_hn(cfg) + fetch_reddit(cfg) + fetch_rss(cfg) + fetch_appstore(cfg)
    fresh = dedupe(items, cfg["filters"].get("cooldown_days", 14))
    keep = prefilter(fresh, cfg)
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
