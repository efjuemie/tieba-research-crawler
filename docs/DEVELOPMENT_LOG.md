# 开发记录

本文记录已执行的建仓、实现和验证，区分公开提交、当前工作树和仍待验收的行为。不会把合成样本或单次 API 响应说成全量采集结果。

本记录更新时，公开 `origin/main` 最新代码提交为 `4142791 feat: add resumable research collection and validated CSV exports`。本轮 README、架构、字段审计、开发记录及 IDE 指南更新仍未提交；真实导出证据已补齐，备份恢复尚未验证。

## 2026-11-14

### 阶段 0：公开仓库准备 — 已推送

- 创建公开仓库 [efjuemie/tieba-research-crawler](https://github.com/efjuemie/tieba-research-crawler)，默认分支 `main`。
- 保留 TiebaMeow/TiebaScraper 上游 Git 历史和 MIT `LICENSE`；`origin` 指向本项目，`upstream` 指向原仓库。
- 提交 `952406d chore: secure local data and pin compatible dependencies` 增加本机数据与凭据忽略规则，并固定 `aiotieba==4.8.2`、`tiebameow==0.5.0`。
- `uv sync` 和核心包导入成功；该次环境使用 Python 3.12.16。
- 配置、数据、导出、日志、截图、数据库转储、HAR 和凭据示例路径均执行过 `git check-ignore` 检查。

### 阶段 1：字段来源审计 — 已推送，后续持续更新

- 提交 `646e3d2 docs: audit field source and persistence`。
- 检查本机七张参考截图，只记录页面结构线索；截图和其中的帖子正文、用户标识、图片内容没有上传或复制进公开文档。
- 对照安装版 aiotieba/tiebameow 源码梳理 raw API 对象、DTO 和 ORM 路径。原始 `get_posts.Thread_p` 的首楼、收藏计数等字段在上游 DTO 转换中会被丢弃，项目现由 `src/research` 保留需要的研究字段。
- 当时没有请求线上 API；本文件后面的有界字段检查是之后单独进行的，不改变静态源码审计的证据范围。

### 阶段 2：CREATE 回溯与公平分页 — 已推送

- 提交 `ab7666b feat: schedule fair bounded CREATE backfill pages`。
- 历史回溯按主题创建时间读取普通与精华两条列表流；周期首页仍按回复时间排序。回溯从第 1 页开始，`max_backfill_pages = 0` 表示不设页数上限。
- 每条列表流等待一页被工作器处理并 ACK，再投递下一页；调度器在贴吧及普通/精华流之间轮转。
- 运行时检查页码、分页结束元数据、空页、创建时间顺序和重复页指纹。分页错误进入失败或未完成状态，不作为正常结束。
- 列表恢复会重放最近已成功的非终止页一次，降低接口分页位移造成漏项的风险。

### 阶段 3/4：研究持久化、认证和导出 — 代码已推送，数据验收仍进行中

- `ResearchClient` 保留单次请求的 raw response 和 DTO。`get_threads`、`get_comments` 的 raw page `total_count` 会在 DTO 转换后补回；aiotieba `get_posts` 的 `Page_p` 不提供该字段，checkpoint 会记录缺失原因。研究扩展表包括 `research_thread`、`research_post`、`research_comment`、`research_user`、`research_user_forum_snapshot`、`research_media_asset`、`research_content_observation`、`research_task_scope`；run 与页级 checkpoint 使用 `research_run`、`research_checkpoint`。
- `research_content_observation` 记录完整正文/媒体观测水位。正文字段与整组媒体在事务中更新；新空媒体集合也会推进水位，拒绝较旧响应重放。
- 登录扩展仅在 `fetch_user_forum_info = true` 时启用。缺少或失效的本机凭据会暂停等待；用户可通过临时浏览器手动完成百度官方登录，或用隐藏输入提示提供本机凭据。
- 导出实现生成唯一本机 all-runs 批次目录，内含七类 CSV 与一个 manifest。质量报告可按 run 筛选；其余六类实体仍导出全数据库。
- 三次真实 CLI 导出均成功生成不同时间戳加 UUID 的批次目录，旧批次保留且未被覆盖。最终批次六类实体 CSV 覆盖全数据库，CSV 行数与 SQL 查询和 manifest 一致：`threads=89`、`posts=2`、`comments=50`、`users=80`、`user_forum_snapshots=119`、`media_assets=31`；manifest `entity_scope=all_database_rows`、`validation_status=passed`，`crawl_quality_report` 覆盖全部 runs 共 98 行。前两次命令传入 comments run 的 `--run-id`，所以质量报告各为 2 行。备份恢复尚未验证。
- 最终全仓回归 236 passed、10 个默认门控用例 skipped；质量 SQL 定向用例 17 passed；专用 PostgreSQL scope 双事务锁竞态两种顺序 2 passed。Ruff、compileall、diff-check 通过。独立 Sol 对最终质量 SQL 的定向复核无新发现。上述测试均不能证明真实平台数据完整。
- 一次真实主入口 SIGINT/恢复演练：普通 CREATE 流第 1 页 checkpoint 保存 30 条后发送 SIGINT，数据库 run 状态为 `interrupted`。同一正式 run 以 `--resume` 和 `max_backfill_pages=1` 恢复，重放第 1 页并更新 checkpoint，再读取第 2 页并保存 30 条后再次 SIGINT；第二次从发送信号到进程退出约 14 秒。接口报告 `total_pages=334` 只是该次响应元数据，不代表已扫描 334 页。演练时详情 checkpoint 为 0，仍有未完成详情 scope，因此只验证主入口列表 checkpoint 恢复，不验证详情队列完成。进程 exit code 0 不表示 run 正常完成；以数据库中 `interrupted` 状态为准。
- 该主入口演练使用 `fetch_user_forum_info=false`、HTTP 并发上限 1 和 `global_rps=0.2`；没有观察到 429 或认证暂停。VS Code/PyCharm GUI 尚未操作。

### 已做的有界真实 API 检查

- 用户手动完成百度官方登录后，进行过一次有界 `get_user_forum_info` 查询。响应状态为 `available`，请求和响应用户匹配，`forum_level`、`level_name`、`role_name` 均非空。没有在仓库记录原始 ID 或字段值。这只证明该次当前响应可用，不代表历史发帖状态。
- 在一个 10 条 `get_posts` 作者样本中，`author.ip` 为非空中文地域文本 10/10，符合 IPv4/IPv6 字面格式 0/10。只记录聚合数；没有记录原值、用户标识或正文。样本不能证明全站覆盖率或永久地理位置。
- 一个小主题的 `get_posts` 请求读取一页并以终止状态完成；raw/DTO 含首楼和一条普通一级回复两个楼层 ID，数据库分别保存首楼和一条 `research_post`，checkpoint 为 `completed`。该次启用了吧内快照查询并产生 4 条用户吧内快照。主题 `reply_num` 报告为 2，而本次保存的唯一普通一级回复为 1；字段语义和观察时间无法确定，未推测差异原因。
- 一条楼中楼扫描跨两页返回 30+20 个唯一评论，共 50 条且已确认持久化。两页各自的 raw、DTO 和 checkpoint `total_count` 均为 51；修复后的 SQL 质量报告显示报告值 51、唯一记录 50、`count_mismatch=true`。差异原因未知。第二页独立 DTO ID 指纹未留存，不能说已逐 ID 独立验证；两页 raw/DTO 数量一致。
- 以上是有限 scope 的在线检查，不代表全吧采集、并发负载、超过 10 页或详情队列完整。三次真实导出已验证六类实体 CSV 的全数据库行数、全 runs 质量报告和 manifest；备份恢复尚未验证。完整媒体范围、跨页完整率以及 IDE GUI 停止未验证。

## 最终验收待办

- 备份恢复尚未验证；这不影响已完成的 CSV/manifest 验收。
- 在完整 detail scope drain、超过 10 页、完整回复对账、媒体范围和 IDE GUI 停止得到验证前，不宣称全吧完整采集或 IDE 端到端验收。
- Root 提供线上导出最终证据后更新文档；文档/IDE 变更另行审查后再提交。

## 发布纪律

- GitHub 公开仓库只放代码、非敏感配置模板及不含真实用户数据的文档/测试。
- PostgreSQL 数据、备份、CSV、日志、参考截图、BDUSS/Cookie 和 `config.toml` 留在本机。
- VS Code 共享配置虽已跟踪，也不能包含数据库密码或账号凭据。提交前逐项核对 `git status`、暂存文件清单、忽略规则和 staged diff；不要使用 `git add -A`。
