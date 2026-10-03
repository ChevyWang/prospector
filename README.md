# prospector：LLM 每日选题雷达

每天自动抓取海外需求信号（社区愿望帖 / 竞品差评 / RSS），LLM 按你自己的 rubric 打分，
把值得看的机会写成日报 commit 回仓库。

**双模式**：
- **ZCode 定时任务（主）**：每天由 ZCode 自动化触发 `uv run python pipeline.py --signals-only`
  导出当日信号，agent 直接读 `data/pending.json` 按 RUBRIC 评分、写 `reports/YYYY-MM-DD.md`
  并 push——LLM 消耗走 ZCode 订阅额度，无需任何付费 API key。GitHub 仓库只是落地存储。
- **GitHub Actions（备用）**：配好 `LLM_API_KEY` Secret 后可手动 Run workflow 走 API 评分
  （`uv run python pipeline.py` 完整模式），适合本机不在身边的场景。

```
每日（ZCode 定时触发）
  → 抓取: HN Algolia + Reddit(OAuth) + RSS（Lobsters/dev.to/GameLook 小游戏关键词过滤）
          + 游戏陀螺·小游戏频道 + App Store 竞品差评
  → SQLite 去重（14 天同题冷却）+ 规则预筛 → data/pending.json
  → agent 按 RUBRIC 分类评分：每条信号先定赛道
     · product 产品订阅：付费意愿/竞争结构/获客通道/人群质量/时机窗口/独立交付（B1-B6）
     · traffic 流量站：意图抗性/词可赢性/变现效率/需求规模/独有数据/达门槛（A1-A6）
     · minigame 微信小游戏（个人 IAA）：吸量潜力/品类拥挤度/制作成本/变现效率/流量入口/合规摩擦（C1-C6）
     · 三道各有"一票否决项"（大厂免费内置、AIO 吞噬无交互、IAP 依赖无版号等）
  → reports/YYYY-MM-DD.md 按赛道分组输出（每赛道前 5）+ commit 回仓库
```

## 部署（10 分钟）

1. **建一个私有 GitHub 仓库**（私有即免泄露报告，Actions 免费额度 2000 分钟/月，本任务每天约 3-5 分钟）并推送本目录：

   ```bash
   cd prospector
   git remote add origin git@github.com:<你>/prospector.git
   git push -u origin main
   ```

2. **仓库 Settings → Secrets and variables → Actions** 添加：

   | Secret | 必填 | 说明 |
   |---|---|---|
   | `LLM_API_KEY` | ✅ | 任意 OpenAI 兼容服务的 key |
   | `LLM_BASE_URL` | 可选 | 覆盖 config.yaml，如 `https://api.deepseek.com/v1` |
   | `LLM_MODEL` | 可选 | 覆盖 config.yaml，如 `deepseek-chat` / `glm-4-flash` |
   | `REDDIT_CLIENT_ID` / `REDDIT_CLIENT_SECRET` | 推荐 | 见下方注册步骤；不设则跳过 Reddit 源 |
   | `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | 可选 | 每天推送 Top5；不设则只看仓库报告 |

3. **编辑 `config.yaml`**：换成你关注的 subreddit / RSS / 竞品 App Store ID。垂直行业 sub（accounting、veterinary 之类）是 B2B 信号金矿。

4. **手动触发一次验证**：仓库 Actions 标签 → daily → Run workflow。成功后 `reports/` 里出现当天报告。

Reddit 应用注册（5 分钟）：reddit.com/prefs/apps → create app → 类型选 **script** → 名称随意、redirect uri 填 `http://localhost:8080` → 创建后得到 client_id（应用名下方短串）和 secret。免费档限 100 请求/分钟，本项目每天几十个请求，远够。

## 本地运行

```bash
uv sync
uv run python pipeline.py --dry-run       # 只验证抓取/去重，不调 LLM、不需要 key
LLM_API_KEY=sk-... uv run python pipeline.py   # 完整跑一次
```

## 日报怎么用（反馈回路）

- 每天给条目标「看 / 不看」，两周后回看：不靠谱的推荐集中在哪个维度失真？
  直接改 `pipeline.py` 里的 `RUBRIC` 措辞和评分指令——这是自建相对 IdeaBrowser 们的核心价值。
- 报告里每条都挂原文链接，**决定前点开读原帖**：点赞和「想要」不是付费意愿，已有竞品收费且差评多才是好信号。
- 定位：每天 10 分钟的灵感催化剂 + 趋势雷达，不是想法自动贩卖机。

## 成本与限制

- LLM：150 条/天 × gpt-4o-mini 级约 $1-3/月；换 DeepSeek/GLM 更低。
- cron 是 best-effort，可能延迟几分钟到几十分钟；手动 `workflow_dispatch` 随时可跑。
- 每日 commit 同时解决了「60 天无活动仓库定时任务被禁用」的问题。
- Reddit 必须走 OAuth（免认证 `.json` 已 403）；X 官方 API、G2/Capterra 抓取、Play 竞品差评被刻意排除（贵 / ToS 禁止 / 无官方途径），G2/Capterra 建议每周人工浏览。
- 去重是标题规范化 hash：同义改写的近似重复会漏网，重复干扰明显时再升级 SimHash。
