# 架构与数据边界

本文对应当前工作树。采集、登录和导出功能已有代码与合成测试；有限真实主题/楼中楼 scope、主入口列表 checkpoint 的 SIGINT/同 run 恢复，以及七类 CSV/manifest 的三次真实导出已验证。完整详情队列、超过 10 页运行和备份恢复仍待验收。

## 请求与数据流

```mermaid
flowchart LR
  API[贴吧 API] --> C[ResearchClient]
  C -->|一次请求的 raw + DTO| W[线程 / 帖子 / 评论工作器]
  S[回溯 / 周期调度器] --> W
  W --> D[DataStore]
  D --> PG[(本机 PostgreSQL)]
  PG --> X[本机 CSV 批次导出]
  A[可选本机账号文件] --> G[认证等待与验证]
  G --> C
```

应用由 `main.py` 创建配置、Container、PostgreSQL 会话工厂、调度器和分通道工作器。`src/research/client.py` 在一次 API 调用中保留 aiotieba raw 响应与 tiebameow DTO，供基础 ORM 和项目研究扩展共同使用。`DataStore` 保存基础表、研究表、checkpoint 和质量状态。

默认 `consumer.transport = "none"`、`cache.backend = "memory"`。本机运行只需要 PostgreSQL 和访问贴吧 API 的网络；不需要 Docker、Redis、TimescaleDB 或远程数据库。只有用户主动改用 Redis/WebSocket 消费者或 Redis 缓存时才需要相应服务。

研究客户端的全局请求预算默认 `global_rps = 0.2`。通过当前 Container 创建的 `ResearchClient` 请求共用此间隔，包含用户吧内资料查询。`threads_rps`、`posts_rps` 和 `comments_rps` 另有限制各自接口。该配置不是贴吧给出的官方限额，也不限制其他程序自行发出的请求。

## 回溯、周期监控与恢复

- `backfill` 对普通主题和精华主题各开一条列表流，从第 1 页开始，使用 `ThreadSortType.CREATE`。每条流一次只请求一页，收到工作器 ACK 后再推进；多贴吧与普通/精华流轮转处理。
- `periodic` 监控首页，使用 `ThreadSortType.REPLY`。
- `hybrid` 同时运行周期首页监控和一轮 CREATE 回溯。REPLY 首页不会充当 CREATE 流的第 1 页。
- `[tieba] max_backfill_pages = 0` 表示回溯流没有配置页数上限。正数限制每条列表流在本次运行中读取的页数；达到上限时记录 `paused_limit`。
- 回溯会检查页码、空页元数据、页内和跨页创建时间顺序、重复页指纹及服务端 `has_more`。这些检查失败时该流记录失败/未完成状态，不把异常空页当作正常终止页。

每个研究 run 由 `research_run` 记录；`research_checkpoint` 按 run、任务键和页码保存列表、帖子详情和评论页状态、尝试次数、下一页、服务端报告数、游标、错误及 scope 状态。论坛范围必须与恢复目标兼容。`--resume` 选择最新可恢复 run；`--run-id` 可指定精确 run，且必须与 `--resume` 同用。

列表流恢复时，最近一个成功但仍有后续页的非终止页会重放一次，以吸收顶部删帖或更新造成的页码位移。内容扩展按实体 ID 幂等写入。帖子与评论详情从成功页之后继续，失败或未完成页可重试。一次本机真实主入口演练以 `max_backfill_pages = 1` 恢复同一 run，重放普通 CREATE 流第 1 页并推进到第 2 页后收到 SIGINT；run 状态保留为 `interrupted`。该演练只验证列表 checkpoint 的重放与推进，不代表详情 scope 已完成，也不证明完整回溯、IDE GUI 停止或长页现场恢复。

### 详情任务代际、原子提交与质量状态

`research_task_scope` 以 `(run_id, logical_task_key)` 保存每个详情扫描的当前代际、状态和恢复参数。主题回复扫描的逻辑键为 `thread_replies:<tid>`；楼中楼扫描的逻辑键为 `post_comments:<tid>:<pid>`。初始代际为 0；代际大于 0 时，详情 checkpoint 的任务键带 `:g<generation>`，不同代的页状态不会混在一起。checkpoint 保存页级游标和服务端报告数，scope 指向当前应继续处理的代。

楼中楼 scope 还记录父主题回复扫描的逻辑键和 `parent_generation`。刷新主题回复扫描时，其代际递增；现有子 scope 也更新到新父代际并重新置为待处理。评论任务携带自身代际和父代际。工作器在数据库事务内锁定相关 scope 行并核对代际；旧任务若在刷新提交后才尝试提交页面，会因父/子代际已变化而被拒绝。

每页的研究映射行、基础 ORM 行、子 scope 与 pending 标记、页 checkpoint 和 scope 状态在同一数据库事务中提交。子任务仅在事务提交后投递到内存队列；若进程在提交后、投递前退出，启动恢复仍可从 PostgreSQL 的 scope 状态重建任务。启动时先将旧 pending 扫描和旧 checkpoint 纳入当前 run 的 scope，再按每批最多 500 条、楼中楼优先的顺序重新入队。详情任务从当前代际最后一个成功页的下一页继续；没有页 checkpoint 时从第 1 页开始。失败页会从记录页重试。

`crawl_quality_report` 按每个当前 `thread_replies` 或 `post_comments` scope 及其当前代际的 checkpoint 汇总状态、成功/失败页和服务端报告数与数据库唯一回复数的差异。楼中楼 scope 优先用当前代最近一个有可信报告数的完成 checkpoint；没有此值时，首楼回退 `research_thread.source_payload_json["first_post"]["subreply_count_reported"]`，普通回复楼回退 `research_post.subreply_count_reported`，不回退到主题总回复数。一级回复对账使用主题回复报告数与已保存唯一一级回复数。该报告是当前扫描范围的计数与任务状态检查，不是平台数据完整性证明。列表流仍以 `thread_list` checkpoint 保存分页恢复状态。run 在队列排空后，会结合最新列表 checkpoint 和当前详情 scope 状态更新整体状态。

`get_threads` 与 `get_comments` 的 raw page 提供 `total_count`；`ResearchClient` 在 DTO 转换后复制该值，供分页 checkpoint 使用。aiotieba `get_posts` 的 `Page_p` 不提供 `total_count`，相关 checkpoint 会记录缺失原因，不能把 DTO 默认 `0` 当成服务端报告数。主题 `reply_count`、父楼 `subreply_count_reported` 与分页 `total_count` 含义不同，质量汇总按具体 scope 选择来源。

## 基础表与研究扩展表

`tiebameow` 管理基础 `thread`、`post`、`comment`、`user`、`forum` 等表。项目在同一数据库增加以下表；研究表不依赖基础表外键，兼容现有分区/组合主键结构。

| 表 | 保存内容 |
|---|---|
| `research_thread` | 主题字段和首楼 `first_pid/body_text/contents_raw`，作者、动态属性、互动数、状态、来源、原始片段与缺失原因。首楼不混作普通一级回复。 |
| `research_post` | 首楼之后的一级回复，保留 `pid/tid/floor`、正文、作者和该次响应计数。 |
| `research_comment` | 楼中楼，保存 `cid/tid/parent_post_pid`、回复目标用户信息、作者和互动字段。 |
| `research_user` | 跨内容来源观察到的用户身份字段与首次/最近观察时间。 |
| `research_user_forum_snapshot` | 用户在某贴吧的观察时点快照，包括吧内等级、头衔、角色、来源、状态和 `observed_at`。 |
| `research_media_asset` | 按内容 owner 和片段位置保存图片/视频等媒体元数据，不下载媒体二进制。 |
| `research_content_observation` | 每个完整正文/媒体观测的来源和时间水位。 |
| `research_run` / `research_checkpoint` | run 与页级恢复、失败和列表分页状态。 |
| `research_task_scope` | 每个详情任务当前代际、父级代际、恢复参数和状态，是详情重入队及当前质量摘要的依据。 |

`research_content_observation` 使用 `(owner_type, owner_id)` 标识内容：`thread_first_post` 的 `owner_id` 是 `tid`，媒体表对应 `owner_type = thread`；`post` 对应 `pid`；`comment` 对应 `cid`。主题列表响应不能推进正文水位。完整 `get_posts` 首楼、普通一级回复和独立 `get_comments` 响应会在同一数据库事务中更新正文、接受新水位并替换整组媒体。最新空媒体集合也会推进水位并清掉旧片段，较旧响应不能再恢复旧媒体。

研究记录带 `source_api`、`observed_at`、`source_payload_json` 和 `missing_reasons_json`。API 数值字段若在解析对象中为 `0`，但没有 protobuf 字段 presence 证据，规范化列写 `NULL`，原始解析值留在 source payload，缺失原因注明 `zero_presence_unverified_in_parsed_object`。`get_user_forum_info` 返回的 JSON 明确字段按其接口值保存。数据时间是观察时间，不代表历史发帖时状态。

## 回复层级和来源边界

```text
thread(tid)
├── 首楼：research_thread.first_pid / body_text / contents_raw
├── post(pid, tid)：首楼之外的一级回复
└── comment(cid, tid, parent_post_pid)：楼中楼
```

`get_posts` 返回的附带 comments 有数量限制，不能代替 `get_comments(tid, pid, page)` 独立分页。`reply_to_id` 指向用户，不是被回复评论的 ID；aiotieba 用 `0` 表示无法解析的目标，研究规范将标准化目标设为 `NULL` 并记录缺失原因。首楼楼中楼报告数放在 `research_thread.source_payload_json["first_post"]`，不与主题帖 `reply_count` 混用。

作者 IP 字段只在经有限样本检查为非 IP 格式地域文本时保存为 `ip_location_text`，同时标明来源字段路径和响应观察时间。IPv4/IPv6 字面值不会存进研究 payload；评论作者数据模型没有相同字段时保持缺失。地域字段描述一次响应，不代表用户的永久位置。

## 登录等待与用户吧内快照

`fetch_user_forum_info` 默认关闭。启用后，`get_user_forum_info` 用配置的账号文件发起有界的附加查询；无效凭据会让 run 进入认证暂停状态。当前流程等待账号文件变化，校验通过后恢复请求。

`python -m src.research login --browser` 打开一个临时、非持久的浏览器上下文，用户在百度官方页面手动扫码。程序只检查该上下文的百度域 BDUSS，不读取已有浏览器 profile，也不保存完整 `storage_state`。隐藏输入方式也可通过 CLI 保存 BDUSS 或 Cookie header；文件只保留 BDUSS，原子写入并设置本机权限。不要把 Cookie/BDUSS 写入聊天、命令参数或 Git。

2026-11-14 的一次有界登录后资料查询返回 `available`，请求用户与响应用户匹配，吧内等级、等级名称和角色名称均非空。它只证明该用户、该贴吧和该时点的字段路径可用，不能回填为旧帖历史状态或代表全体用户覆盖率。

## CSV 批次和验证

`python -m src.research export` 从本机数据库导出七个 CSV：`threads`、`posts`、`comments`、`users`、`user_forum_snapshots`、`media_assets`、`crawl_quality_report`，并写一个 manifest。每次创建新批次目录，文件名含 `all-runs`、Asia/Shanghai 导出时间和唯一批次 ID；不会覆盖既有批次。实体 CSV 包含数据库的全部 run。传 `--run-id` 只筛选质量报告并为批次提供命名上下文，实体 CSV 仍是全库范围。

导出会保留没有研究扩展行的旧基础记录，并标为 `legacy_base_unverified`；它不会用旧 ORM 的零值补齐研究扩展中的 `NULL`。写入过程中会校验数据库查询数、CSV 行数、唯一键和回复父子关系。PostgreSQL 的实体查询使用一致快照；导出前仍应暂停采集，以免 checkpoint 汇总和实体查询来自不同时间。实体一致性验证通过与整站采集完整是两种不同状态。无 checkpoint 证据时 manifest 标记 `unknown_not_tested`。

导出只产生 CSV 与 manifest，不含 Excel/XLSX。文件保存在本机指定目录，权限为当前用户读写。CSV 中的用户 ID、昵称、回复目标 ID 和原始正文不脱敏。

三次真实 CLI 导出均生成了不同时间戳加 UUID 的新批次，没有覆盖旧文件。最终批次中六类实体 CSV 覆盖全数据库，其行数与 SQL 查询和 manifest 一致；manifest `entity_scope=all_database_rows`、`validation_status=passed`，全 runs `crawl_quality_report` 有 98 行。前两次导出传入 comments run 的 `--run-id`，所以质量报告各有 2 行。精确实体计数见[开发记录](DEVELOPMENT_LOG.md)。

## 已核实与待验收

当前实现回归记录为全仓 236 passed、10 个默认门控用例 skipped；质量 SQL 定向用例 17 passed，专用 PostgreSQL scope 双事务锁竞态两种顺序 2 passed。Ruff、compileall、diff-check 通过；独立 Sol 对质量 SQL 的定向复核未发现新问题。有限真实样本观察到主题回复报告 2 对 1 条唯一普通回复、以及楼中楼报告 51 对 50 条唯一记录，原因未知。三次真实 CLI 导出验证了六类实体 CSV、全 runs 质量报告和 manifest；全吧采集、详情 scopes 完成、超过 10 页运行和备份恢复仍待完成；见[开发记录](DEVELOPMENT_LOG.md)与[字段来源审计](FIELD_SOURCE_AUDIT.md)。

上游项目的代码和文档归属见 [TiebaMeow/TiebaScraper](https://github.com/TiebaMeow/TiebaScraper) 与 [LICENSE](../LICENSE)。
