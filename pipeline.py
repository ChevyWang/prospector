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

RUBRIC = """评分维度（各 1-5 分，分高更好）：
pain: 痛点是否清晰、强烈、具体（泛泛的"希望有个AI帮我生活"=1分）
pay: 付费人群与付费意愿是否明确（B2B/专业人士/已有人在为类似问题花钱=高分；学生/娱乐=低分）
competition: 市场空隙（5=几乎无现成方案或现有方案差评多；1=大厂免费产品已覆盖）
fit: 适合个人开发者快速做出 B2B micro-SaaS/工具/API（需要硬件/牌照/重运营=低分）
total = 四项之和"""


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
                 "content": f"你为一位独立开发者（方向：海外 B2B micro-SaaS/付费 API/工具站）"
                            f"筛选产品机会。{RUBRIC}。只输出 JSON。"},
                {"role": "user",
                 "content": f"以下是 {len(batch)} 条来自互联网的信号（社区帖子/愿望/差评）。"
                            f"逐条按序号评分。注意：点赞数和'想要'不是付费意愿；"
                            f"已有竞品在收费且差评多反而是好信号(competition 高分)。\n\n{listing}\n\n"
                            f'输出格式：{{"items":[{{"i":0,"pain":1,"pay":1,"competition":1,'
                            f'"fit":1,"total":4,"reason":"一句话理由",'
                            f'"angle":"一句话产品切入角度","action":"一个具体的下一步验证动作"}}]}}'}])
            for row in data.get("items", []) if isinstance(data.get("items"), list) else []:
                j = row.get("i", -1)
                if isinstance(j, int) and 0 <= j < len(batch):
                    b = dict(batch[j])
                    for k in ("pain", "pay", "competition", "fit", "total"):
                        b[k] = int(row.get(k, 0))
                    b.update({k: row.get(k, "") for k in ("reason", "angle", "action")})
                    if not b.get("total"):
                        b["total"] = b["pain"] + b["pay"] + b["competition"] + b["fit"]
                    scored.append(b)
        except Exception as e:
            print(f"[warn] scoring batch {i//25} failed: {e}")
    return scored


# ---------- 报告 ----------

def write_report(cfg, scored, stats, date_str):
    top = sorted(scored, key=lambda x: -x.get("total", 0))[:cfg["report"]["top_n"]]
    lines = [
        f"# 每日机会推荐 {date_str}",
        "",
        f"今日抓取 {stats['fetched']} 条信号，去重后 {stats['fresh']} 条，"
        f"预筛后送评 {stats['scored']} 条，推荐以下 {len(top)} 条。",
        "",
    ]
    for n, it in enumerate(top, 1):
        lines += [
            f"## {n}. {it['title']}（{it['total']} 分）",
            f"- 来源：{it['meta']} ｜ [原文]({it['url']})",
            f"- 评分：痛点 {it['pain']} · 付费 {it['pay']} · 空隙 {it['competition']} · 适配 {it['fit']}",
            f"- 值得做的理由：{it.get('reason', '')}",
            f"- 产品切入：{it.get('angle', '')}",
            f"- 下一步：{it.get('action', '')}",
            "",
        ]
    lines += ["---", "", "> 定位提醒：这是灵感催化剂不是点子贩卖机——最好的判断仍来自你读原文后的直觉。",
              "> 觉得某条不靠谱？直接改 pipeline.py 里的 RUBRIC 权重/措辞，这就是自建的意义。"]
    REPORTS_DIR.mkdir(exist_ok=True)
    path = REPORTS_DIR / f"{date_str}.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path, top


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
        print("[info] 今日无新信号，跳过 LLM")
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
