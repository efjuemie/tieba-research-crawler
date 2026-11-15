# 开发记录

本文记录已执行的建仓、实现和验证，区分公开提交、当前工作树和仍待验收的行为。不会把合成样本或单次 API 响应说成全量采集结果。

V2 功能源码已在提交 `304bf314d561b8c5bab8bb71bd8c1035f92fe10f` 公开，后续只读审计导入修复与最终文档分别记录。备份恢复、真实 API 补存、网页定向回填及十三类 CSV 已验证。下方 2026-11-14 的结果保留当时状态，最新结果见 2026-11-15 各节。

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

## V1 当时的验收待办（历史记录）

- 备份恢复尚未验证；这不影响已完成的 CSV/manifest 验收。
- 在完整 detail scope drain、超过 10 页、完整回复对账、媒体范围和 IDE GUI 停止得到验证前，不宣称全吧完整采集或 IDE 端到端验收。
- Root 提供线上导出最终证据后更新文档；文档/IDE 变更另行审查后再提交。

## 发布纪律

- GitHub 公开仓库只放代码、非敏感配置模板及不含真实用户数据的文档/测试。
- PostgreSQL 数据、备份、CSV、日志、参考截图、BDUSS/Cookie 和 `config.toml` 留在本机。
- VS Code 共享配置虽已跟踪，也不能包含数据库密码或账号凭据。提交前逐项核对 `git status`、暂存文件清单、忽略规则和 staged diff；不要使用 `git add -A`。

## 2026-11-15：V2 Stage 0收尾审计

- 从 `origin/main` 拉取后仍位于 `2aebedffb81862ce7447e7b9c2f1a58c3989f663`；当前检查没有修改产品源码、测试或公开配置。
- 在 Python 3.12.16、aiotieba 4.8.2、tiebameow 0.5.0 的本机环境运行全仓 pytest：236 passed、10 skipped、15 warnings；Ruff 0 issues。
- 为原有本地数据库留存权限受限的备份。在线验收库恢复至新隔离库后，非系统表列、逐表行数与 run 状态汇总一致；旧数据源保持只读。备份文件及详细结果留在本机忽略目录。
- Stage 0字段审计区分“解析类未映射”与“接口不支持”：本地protobuf descriptor列出了评论作者 `ip_address`、`user_growth.level_id`、`tieba_uid`、`bawu_type` wire候选，而 aiotieba 4.8.2 的 `UserInfo_c` 解析类未映射这些字段。一次低频 `get_comments` 请求正常返回，但安全scope匹配失败，因此本次字段值为 `NOT_TESTED`，不能据此认定字段未返回或接口不支持。
- 用户提供的DOM与nested-floor JSON摘要支持后续核验候选字段，但没有将真实标识、用户名、帖子/回复正文或字段原值写入本记录。root通过用户手动完成验证的专用本地浏览器会话做了只读UI核验并触发展开控件；这只证明当前页面可读和展开操作可用。JSON字段与请求上下文仍需在当前会话中live capture核对，深链和楼中楼数据库身份关联尚未验证。
- 匿名网页探测曾收到HTTP 403人工验证拦截并立即停止。未尝试绕过、重放签名请求或读取用户已有浏览器Profile。
- 审计还确认：`get_user_info`结果尚未作为用户观察持久化，昵称空值fallback与稀疏upsert存在字段丢失风险，`get_bawu_info`尚未接入研究管线；当前threads CSV没有最终排序约束，模型/CSV没有主题帖及回复URL列。详细字段矩阵和本机备份证据仅在本地审核材料中。

本节更新了前文“备份恢复尚未验证”的状态：该项已在Stage 0后续完成。Stage 0审计结束，后续按批准的分阶段计划推进；当前记录不表示全吧采集、回复完整性或网页补采已经通过验收。

## 2026-11-15：V2 分阶段开发与有界实测（历史状态；V3 历史和 V5 当前批次见下节）

- Stage 1 `c1dc8e1747b98370f7e98ae8a52a3a778e1e71c3` 增加已有 run 的 `drain-pending`、资料缓存、稀疏身份保留及成长等级无损增列；Stage 2 `e45ea856a0430ae6d47bbb44946cd3b3ad2a44a9` 增加三类主题链接、主题 CSV 全文件按创建时间倒序和显式完成主题导出范围。合并树回归为 294 passed、10 skipped；Stage 1 单独 CI 的两个跨阶段 mock 签名测试在 Stage 2 组合后通过，没有重写提交历史。
- Stage 3A `42f2370df8183776f1619fb53d3397a8abcb9cbf` 提交只读 Network／DOM 抽取、隔离浏览器、独立身份与缺失原因、手工登录留页及取消清理。94 项定向测试和独立 Sol 审查通过；该提交的 CI、构建检查均成功。真实主键、正文、截图、JSON、账号和 Profile 没有进入提交。
- 一次真实待办排空使用 4 次 API 请求，取得首楼、2 条一级回复、9 条楼中楼；主题详情结束，两个评论范围完成，一个首楼评论范围因分页身份／空页证据不足保持不完整。其余 run 待办未冒充已完成。
- 三次受控当前资料请求验证 profile、论坛资料、吧务名单，响应身份匹配且缓存可复用；仅写入隔离库当前用户／论坛快照，不回写历史内容属性。
- 独立评论接口的一次原始 protobuf 对照通过同 TID／父 PID／CID／作者校验，7 条响应没有非空属地、成长等级、另一套 UID 或具体职务值。另一条正常帖子详情请求的 9 条附带评论却提供 7 条属地和 9 条正成长等级，明确了“API 已取到但研究映射忽略”的补存路径；不能将单个评论接口的空值泛化成所有 API 都不支持。
- 专用手工登录会话已能由程序自动读取。自然展开的 3 条评论与数据库组合身份匹配；未重放用户请求、未读取个人浏览器默认 Profile。只读新浏览器检查中首楼与一级回复身份通过；评论展开发现真实 `lzl-wrapper` 层级与固定子节点假设不符，正在按实采结构修正，并审查自然 `page_pc` JSON 的强身份读取。
- 普通 CREATE 连续 12 页、精品 1 页的独立深度库保存 374 个去重主题索引，383 条页面观察，两个流交集 9；非置顶创建时间页内及跨页倒序、未发现重复页。14 次 SDK HTTP 计数后预算阻止下一请求，索引详情全部 pending。八个 CSV 与一个 manifest 通过导出器验证。这是列表深度证明，不是详情、全吧覆盖或 GUI 端到端证明。
- 上述深度样本首两请求间隔 0.327 秒，定位到整数 FID 冷缓存触发内部名称解析；已将解析移到单独受预算的 `get_fname` 调用，再以名称请求列表。7 项新增测试及独立保留 SDK 调用链的复核通过，缓存命中不会增加解析请求。

PostgreSQL 租约竞争、过期接管、身份混批原子拒绝和 API 预览两种到达顺序已通过隔离验证。一次真实 API 生产持久化使既有 9 条评论成长等级从 0 条有值到 9 条、属地到 7 条，完整内容哈希保持不变；新批次十三 CSV／manifest 经 SQL 和文件复核通过。真实网页回填和最终综合交付仍以本机 `test-data/next-phase-validation-report.md` 及后续新批次审计为准。未通过的现场案例保留 NOT_TESTED／INCONCLUSIVE，不以单元测试代替线上成功。

- 审计导入修复前的源码回归为 488 passed、10 skipped，Ruff 与差异检查通过；18 条警告包含依赖弃用提示和 5 条既有 AsyncMock 未 await 提示。新增网页接口的两处接入阻塞已修复：来源／FID 证明合同与事务合并贯通，正常第一页未出现目标父楼不再污染后续页面。独立 Sol 复审 138 项测试通过，并以内存 SQLite 验证明确收藏零、评论属地及点赞零经提取、任务租约和生产合并器成功写入。
- V2 其余源码、配置与合成测试已作为 `304bf314d561b8c5bab8bb71bd8c1035f92fe10f` 推送到同一仓库 `main`；两项 GitHub Actions 检查均成功（run `43686395195`、`43686395193`）。该提交未包含真实数据、私有报告、登录资料或导出批次。
- 从 API 阶段 CSV 选取 3 条不同主题链接，由专用已登录浏览器低频导航；3/3 页面落点及自然 `page_pc` 响应主题 ID 匹配，导航起始最小间隔为 5.001 秒，未遇登录或访问拦截。检查不更新数据库或全库 URL 验证状态。浏览器关闭后保留专用 Profile，后续可复用登录状态。
- 正式网页回填限定 5 个已有目标，复用 0.2 RPS 预算：主题 1 个、评论 3 个完成，一级回复 1 个部分完成；后者只写入有依据的本吧头衔，具体职务与吧务状态仍缺证据。共新增 14 条网页字段观察，样本评论属地 7/9→9/9；完整正文、作者与父子身份、媒体、行观察时间及内容水位哈希均保持不变。全部规范字段更新来自生产任务租约与事务合并流程，没有手工 SQL 填值。
- 14 条网页观察逐条与规范记录组合身份核对，全部匹配；13 条 applied、1 条 confirmed，含 9 条明确数值零证据。只读审计的现场运行发现两处相对导入错误，真实 SQLite 会话回归先复现后修复，18 项定向测试通过。审计重跑身份通过、状态正常，九张相关表全列全行前后哈希相同，未创建任务或改写数据库。
- 网页回填后生成全新 `all` 与 `completed-threads` 两批，各十三 CSV 和 manifest；行数、主键、长整数、主题最终排序、规范 URL、深链空值和私有权限校验通过。全库内容为 89 条主题、4 条一级回复、59 条楼中楼；用户 83、吧内快照 131、媒体 32，字段观察共 63、补采任务与尝试各 5、失败 0。严格完成主题筛选当前内容实体和媒体均为 0，因为没有主题同时满足全部最新详情和已知子 scope 完成证据；用户、快照和审计仍保持文档规定的范围。数据未被删除，也没有把待办标为完整。
- 审计导入修复提交为 `3ff496798e2d82233c74a1d5b7a56bc7fb5156a7`，已推送 `main`。修复后全仓回归 489 passed、10 skipped、18 warnings，Ruff 和差异检查通过；独立 Sol 对该补丁及真实 SQLite 快照回归复核通过。该提交两项 GitHub Actions 均成功（run `43688101346`、`43688101300`）。

## 2026-11-15：V3 最终导出与字段核验（历史批次；V5 见下节）

- V3 当时全库 CSV：threads 89、posts 2,173、comments 77、users 1,619、user_forum_snapshots 2,089、media_assets 542。正文非空数依次为 87/89（85 条 API 核实、2 条旧列表 fallback 未核实；2 条纯媒体行正文为空）、2,150/2,173、74/77；IP 属地非空数为 80、1,441、23，正成长等级数为 86、2,095、27。没有内容行导出等级 `0`；protobuf scalar 的零在无 presence 证据时仍作未知，不用当前 profile 回填历史内容字段。
- 媒体表包括 226 条图片引用、316 条表情引用，没有真实视频样本。内容 CSV 的 `media_urls` 非空行数为 threads 38、posts 117、comments 0；三类 CSV 与媒体行按 owner 类型、owner ID、TID、位置及 URL 对照通过。每行 JSON 数组只保留图片／视频引用并按 position 排序去重；媒体表仍单独导出。权威首楼当前空观察不再被旧 alias 媒体复活。无 OCR、URL 文件名推断，也不把视频封面缩略图当作正文视频 URL；真实视频目前只有合成支持证据。
- V3 定向截图中 3 条一级回复均显示并保留等级 10、成长图标 URL 与 IP。一个嵌套评论的成长图标通过深层 `feed_head.main_data` 路径提取；该字段任务 planned／eligible／collected／completed 各 1，missing 0。`global_growth_icon_url` 在 threads/posts/comments 的定向样本数为 1/3/1，不代表批量覆盖；评论 `reply_to_display_name` 只取明确显示名称。未使用 OCR。
- raw `Post` 节点不声明 IP/location；作者地区候选属于响应的 user list。一个精确 API 对照中解析等级为默认零、属地为空；同目标原生 `page_pc` 深层响应含成长徽章 URL，但没有证明数字等级或 IP。另有 3 条一级回复截图显示等级 10、图标 URL 和 IP；这些是不同的页面证据。采集按内容与作者身份绑定。缺少原始字段 presence 时不将等级零视为真实零，也不从当前资料补历史属性。
- 后页语义修复后，仅在 `get_posts` 详情分页响应中存在唯一明确 floor 1 Post 时采纳 canonical 首楼 PID，避免把 `raw.thread.pid`（页头 PID）误当主题首楼。60 个详情响应页已保存、0 失败，canonical 主题仍为 89。5/6 个主题详情任务在本次有界分页验证中取得终止证据，但没有推进对应 checkpoint；另一次原生 CLI 验证完成同一主题的两个一级回复页面 checkpoint，其评论子 scope 为 3 complete、7 incomplete、22 pending。因此这不是完整 run。长正文单样本 API 10,686 字符与 DOM 10,649 字符空白归一后相等，不能外推至所有帖子。IDE 图形界面停止行为为 `NOT_TESTED`。
- 主 CSV 仅保留已批准字段：保留主题浏览量；不提供不赞成数、转发数、具体吧务职务／头衔或楼层／评论深链。成长图标与 `reply_to_display_name` 按字段来源和内容身份记录。原数据库受保护，验证仅针对隔离恢复库。
- 最终全仓回归 557 passed、10 skipped、18 warnings；Ruff 与差异检查通过。该回归、导出一致性和定向 UI 样本均不代表全站覆盖。

## 2026-11-15：V5 当前字段交付与边界

- 当前全库 CSV 有 89 个主题、2,173 条一级回复、77 条楼中楼、1,619 个用户、2,097 条吧内快照和 542 条媒体记录。正文非空数仍为 87/89（85 条 API 核实、2 条旧列表 fallback 未核实；2 条纯媒体主题正文为空）、2,150/2,173、74/77；字段补采没有改动正文。
- 正成长数字／成长图标 URL／数字或图标至少一项覆盖：主题为 86／4／89，一级回复为 2,095／81／2,173，楼中楼为 37／36／72（共 77 条）。IP 地点非空分别为主题 80、一级回复 1,441、楼中楼 76。所有 797 个原始缺失字段目标均已尝试（9 个主题、734 条一级回复、54 条楼中楼，含 10 条 legacy 目标）；未遇到认证门。
- 5 条仍缺成长数字和图标的楼中楼均通过父楼和作者标题身份验证，页面只显示头像，没有成长徽章；没有填造数值或 URL。一个其他既有缺失目标通过当前已登录 `WebFallbackProvider.collect` 做了只读验证：一页加 5 次自然展开共 6 次受控动作，收集到身份通过的成长图标候选，无用户卡片或认证门，且没有数据库写入。这一真实页面结果支持把默认展开预算提高到 10；单目标仍限一个页面和 90 秒，允许本机显式调低，配置上限为 120 秒。
- 732 条缺失一级回复 IP 的来源字段都记录了 `field_sources_json.ip_location_text.web_check`。7 条缺失主题 IP 经检查仍为空；2 个主题响应 HTTP 200 但返回错误 4，缺少可靠作者信息，不能称为已删除；1 条评论的接口没有提供 IP。
- 浏览器执行 167 次导航和 5 次自然展开，另有 1 次导航及 5 次展开用于核对剩余 5 条评论的成长徽章。10 条 legacy 评论以 6 次 API 请求刷新，ID、父楼、作者、FID 和正文均与原记录相同；研究评论计数从 67 增至 77，CSV 仍为 77 行。生产 enqueue／lease／reconcile 流程合并网页值；此前被 source-anchor URL 拒绝的 41 个目标改用规范来源主题 URL 后全部重试通过，owner PID/CID 证据未丢失。
- 媒体 URL 与内容 owner、TID 及片段顺序逐项对照通过。17 张受保护表中的内容、ID、父子关系、媒体、正文标记、checkpoint 和 scope 哈希未改变。真实数据只保存在受 Git 忽略的本机数据和私有交付物中，没有加入公开仓库。
- V5 主入口异步／信号收尾经受控信号测试和独立审阅；没有实际 VS Code/PyCharm GUI 停止或 SIGKILL 验收。此前全仓回归 639 passed、10 skipped、18 warnings 及 Ruff／compileall／diff 检查通过，发生在配置默认值调整之前。将网页默认改为每目标最多 10 次展开、90 秒后，配置、浏览器、任务和 CLI 定向回归为 99 passed、1 条依赖弃用警告；Ruff、compileall 和 diff 检查通过。没有把历史全仓结果说成变更后的全仓验证。
