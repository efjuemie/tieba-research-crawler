# 字段来源与验证范围

审计日期：2026-11-15（V1/V2/V3 为历史记录；V5 为当前批次）

代码范围：当前工作树、`aiotieba==4.8.2`、`tiebameow==0.5.0`。本地参考截图只用于 UI 线索；公开仓库不包含截图、真实帖子正文、用户标识、原始属地值或登录资料。

## 证据等级

- **源码**：在已安装库或本项目代码中核对过属性与转换路径。
- **合成**：用合成对象验证项目 mapper、数据库写入或导出，不代表贴吧实际响应。
- **有限线上样本**：已执行并在此记录样本数和结果，不公开原始 ID、字段值或正文。
- **未验证**：字段语义、跨页完整性或更大样本覆盖仍未知。

`observed_at` 表示本次接口响应/观察时间，不能代替内容创建时间，也不能描述旧帖发表时的用户资料。

## 主题帖、首楼和计数

| 字段 | 来源路径 | 当前保存位置 | 证据与限制 |
|---|---|---|---|
| `tid/fid/fname/title/create_time/last_time` | `get_threads` 的 `ThreadDTO`；`get_posts` raw `Posts.thread: Thread_p` 提供首楼部分字段 | 基础表 + `research_thread`，记录 `create_time_raw/last_time_raw/source_api/observed_at` | 源码与合成映射已核对。`create_time` 是主题创建时间，`last_time` 是最后回复时间，二者不可互换。 |
| `first_pid/body_text/contents_raw` | `get_posts` raw `Posts.thread`，并与返回的 floor 1 `Post` 配对 | `research_thread.first_pid/body_text/contents_raw`；`research_content_observation` 保存 `thread_first_post` owner 水位 | floor 1 不作为普通 `research_post` 行。列表新记录不会清掉详情正文；旧详情仅在内容水位被接受时补写详情独有字段。当前批次 87/89 正文非空，其中 85 条 API 核实、2 条旧列表 fallback 未核实，另有 2 条纯媒体主题正文为空。 |
| `reply_count/agree_count/disagree_count/view_count/share_count` | `get_threads` 与 `get_posts` raw thread 的计数属性 | `research_thread` 规范列和 source payload | 计数是响应时点的值。接口报告数与真实回复总数/页面展示口径的差异尚未完成全量对账。 |
| `collect_count` | `get_posts` raw `Posts.thread.collect_num`；`get_threads` 不提供该列 | `research_thread.collect_count`；raw 解析值保留在 `source_payload_json["counts"]["collect_count"]` | 合成映射和 PostgreSQL 水位回归已覆盖非零与零值。没有字段 presence 证据的 raw `0` 规范列写 `NULL`，缺失原因标记 `zero_presence_unverified_in_parsed_object`。没有据网页数字补值。 |
| 主题状态和作者动态属性 | `get_threads` 列表字段；`get_posts` raw thread / author 字段 | `research_thread` 保存支持的 `thread_type`、状态、作者、互动和属地列 | 只更新响应来源支持的字段；共享字段使用更新时点，详情独有字段由内容观测水位控制。分享帖自身内容缺失时不把分享来源正文冒充首楼正文。 |
| `share_origin/vote_info` | aiotieba raw `Thread_p` 声明 | 当前研究表未定义对应规范列 | raw 类型字段存在不等于本项目已经映射、持久化或验证该数据；目前不用于补造首楼正文。 |

`tiebameow` 的 `get_posts_dto` 转换只返回 `PostsDTO.objs/page/forum`，不会保留 raw `Posts.thread`。项目通过 `ResearchClient` 的 `ResearchPage` 同次保留 raw 与 DTO，不需再发一条请求取得 `collect_num` 或首楼属性。

分页报告数与主题/父楼的回复报告数是不同来源。`aiotieba==4.8.2` 的 `Page_t` 和 `Page_c` 有 `total_count`，但上游 DTO 转换没有保留该值；项目 `ResearchClient` 在 `get_threads`、`get_comments` 转换后从 raw page 复制它。`get_posts` 的 `Page_p` 没有 `total_count`，其 DTO 默认 `0` 不能当作接口报告值；相关页 checkpoint 标记 `not_provided_by_aiotieba_posts_page`。数值为 `0` 时仍按字段 presence 规则区分可验证报告与未验证值。

## 一级回复与楼中楼

| 范围 | 来源路径 | 当前保存位置 | 边界 |
|---|---|---|---|
| 一级回复 | `get_posts` raw `Posts.objs[]` / `Post`，含 `pid/tid/fid/floor/create_time/contents/author/counts` | `research_post`；媒体片段写入 `research_media_asset` | `research_post` 保存首楼以外的楼层。首楼 ID、正文和评论报告数留在 `research_thread`。当前 CSV 2,150/2,173 行正文非空。 |
| 楼中楼 | 独立 `get_comments(tid, pid, page)` 的 raw `Comment` | `research_comment`，保留 `cid/tid/parent_post_pid` 和 `reply_to_user_id/reply_to_id_raw` | `get_posts` 带回的有限附带 comments 不代表完整楼中楼分页，也不会替代独立 `get_comments` 扫描。真实多页完整率未验证。 |
| 回复目标 | `Comment.reply_to_id` | 非零解析值原样保留；`0` 写入 `reply_to_id_raw`，规范目标为 `NULL` 并记录 `aiotieba_zero_sentinel` | 该字段是目标用户 ID，不是被回复 comment ID。解析器的 `0` 是 sentinel，不能据此断言 API 明确返回数值零。 |
| 首楼楼中楼父楼报告数 | `get_posts` 匹配到的 floor 1 raw `Post.reply_num` | `research_thread.source_payload_json["first_post"]`，包含 raw 值、规范值、来源和观察时间 | 不与 `ResearchThread.reply_count` 混用。没有 presence 证据的 parsed `0` 规范值为 `NULL`。质量报告优先使用该评论 scope 当前代最近有可信报告数的完成 checkpoint；没有时才回退此父楼值。 |
| 评论作者等级/属地 | 独立 `get_comments` 的 `UserInfo_c`；`get_posts` 预览的 `UserInfo_p` | 同次响应中可验证的预览作者属性保存为 `api_raw` 字段观察；按评论完整身份补规范字段 | 预览不代表全文或分页完成，不创建评论壳、不推进内容水位；先有观察或先有完整详情均按同 CID／父楼／作者匹配。具体属地时点仍是 API 观察，不冒充发表当年位置。 |

楼层、点赞/反对数、作者标识和内容片段均保留来源与响应观察时间。父子关系在导出前由 SQL 检查；回复数与已采集唯一 ID 数分别统计。

## 用户、吧内快照与属地

| 字段组 | 来源路径 | 当前保存位置 | 证据与限制 |
|---|---|---|---|
| 用户 ID、用户名、昵称、portrait、`tieba_uid` | `get_threads/get_posts/get_comments` 作者对象 | `research_user` 最新身份行与每条内容的作者 ID | `user_id` 与 `tieba_uid` 是不同标识。用户名、昵称、portrait 会变化。 |
| 吧内 `forum_level/forum_level_name/role_name` | 启用登录后的 `get_user_forum_info`；内容接口作者对象也可能提供 `forum_level` | `research_user_forum_snapshot`，按用户、贴吧、来源和观察时间记录 | `get_user_forum_info` 的映射明确将 `is_bawu`、`icons` 留空并记录为接口未提供；内容作者对象单独提供这些字段时才映射。`role_type` 或徽章 ID 不从图标猜测。 |
| 作者 `is_bawu/icons` | `get_threads/get_posts/get_comments` 返回的内容作者对象（仅在该对象暴露时） | 对应内容行及由内容接口生成的 `research_user_forum_snapshot` | 不来自 `get_user_forum_info`；不同内容接口的作者对象字段并不完全相同，缺失时保持空值并记录原因。 |
| 帖子作者 `ip_location_text` | `get_posts` 的 `PostUserDTO.ip` 或 raw thread author `.ip`；raw `Post` 节点本身不声明 IP/location | `research_post` / `research_thread` 的属地列，另保存来源路径和观察时间 | 10 条历史有界作者样本字段非空 10/10；后续精确 API 对照曾见解析对象等级默认 `0`、属地空值，而定向 UI 样本明确显示属地。只把非 IP 字面值当属地文本写入；该内容作者值不由当前 profile 倒填，且不代表历史位置。 |
| 评论作者 `ip_location_text/glevel` | `UserInfo_c` 未声明；同帖预览 `UserInfo_p` 与精确网页响应可能提供 | 只有明确属性及组合身份通过才补；`verified_content_api` 与网页来源分开保留 | 不按头像／截图／昵称推断，不将附带评论误认作完整分页。后续接口缺少属性不擦除已验证值，冲突留证据。 |

用户在百度官方页面手动登录后，进行过一次有界 `get_user_forum_info` 检查：状态 `available`，请求和响应用户匹配，`forum_level`、`level_name`、`role_name` 均非空。只验证这一当前响应，不代表所有用户都可查询，也不回填旧帖发表时的身份。原始账号值未写入仓库或本文件。

### 有界真实内容样本

- 一个小主题的 `get_posts` 请求读取一页并以终止状态完成；raw/DTO 含首楼和一条普通一级回复两个楼层 ID，数据库分别保存首楼和一条 `research_post`，checkpoint 为 `completed`。该次同时生成 4 条用户吧内快照。主题 `reply_num` 报告为 2，而保存的唯一普通一级回复为 1；差异原因和字段口径未知。
- 一条楼中楼扫描跨两页返回 30+20 个唯一评论，共 50 条且均已持久化。两页各自的 raw、DTO 和 checkpoint `total_count` 均为 51；质量报告现显示 51 对 50、`count_mismatch=true`。未确定差异原因。第二页独立 DTO ID 指纹未保留，因此不能声称已对全部 ID 做独立逐项验证；两页 raw/DTO 的数量一致。该结果只代表此 scope。

## 媒体、checkpoint 与质量报告

- 详情响应中的内容片段映射到 `research_media_asset`：`get_posts` 的首楼（需匹配到 floor 1 `Post`）和普通一级回复，以及独立 `get_comments` 返回的评论；`get_threads` 列表响应不会直接写媒体行。已映射的片段保留 owner 类型、ID、位置、片段类型、URL/hash/尺寸、来源及观察时间；不下载媒体文件。实际批次的媒体引用归属已与三类内容 CSV 逐行按 owner 类型、owner ID 和 TID 对照通过；没有真实视频样本。
- `research_content_observation(owner_type, owner_id)` 只由完整内容响应推进。thread 首楼 owner 为 `thread_first_post/tid`，普通 post 与独立 comment 的 owner 分别为 `post/pid`、`comment/cid`。空媒体集合也有水位，避免较旧页面恢复已删除的媒体引用。
- `research_run` 记录每次研究运行；`research_checkpoint` 按 run/task/page 保存页码、分页状态、游标、报告数、错误类型与 scope 状态。`crawl_quality_report` 对详情 scope 汇总报告数、成功/失败页、唯一记录数、差异和原因。楼中楼 scope 优先采用当前代最近一个报告数可信的完成 checkpoint；无可信 checkpoint 时，首楼回退 `research_thread.source_payload_json["first_post"]["subreply_count_reported"]`，普通回复楼回退 `research_post.subreply_count_reported`。评论质量计数不会回退到主题总回复数；一级回复对账使用主题回复报告数和保存的唯一一级回复数。列表流的分页状态仍由 `thread_list` checkpoint 保存。
- manifest 的实体导出校验与采集完整性是分开的结果。没有 checkpoint 时，质量证据为 `unknown_not_tested`；存在 checkpoint 也不证明平台上的所有数据都已抓取。

V1 历史回归为全仓 236 passed、10 skipped；质量 SQL 定向 17 passed、专用 PostgreSQL scope 锁竞态 2 passed。三次历史导出未覆盖旧批次，当时全 runs 质量报告为 98 行。V2 已验证隔离恢复与无损迁移，新增批次及字段证据见下节；导出一致始终不代表平台数据完整。

## 截图仅提供 UI 线索

七张本机截图显示了排序、等级、角色文字/图标、媒体、属地、计数和折叠评论等页面元素。截图没有作为公开文件发布。页面控件本身不能证明接口字段、protobuf presence、计数口径或历史状态。

## V1 当时未验证项（后续 V2/V3 历史证据及 V5 当前证据见下文）

- 超过两页的真实 CREATE 跨页排序、服务端页数变化与全吧终止条件。
- 超过 10 页真实运行、真实一级回复/楼中楼完整分页和数量对账。
- 多类型真实媒体片段、分享帖和投票内容的 raw 到数据库完整性。
- 详情队列完成与详情 checkpoint 恢复、IDE GUI 停止、进程崩溃后的现场恢复，以及备份恢复。
- 当时尚未对照 `ip` 地域文本与网页 UI 地区标签；V3 历史对照及 V5 覆盖限制见下文。

## V2 历史证据与字段状态（由 V3/V5 后续批次更新）

本节记录 V2 当时证据；V3 是后续历史批次，当前字段／导出结果见末尾 V5 小节。不能把源码实现、合成测试、只读实采和正式入库四种证据混为一谈。所有真实值、精确标识和现场材料只在本机报告中保存。

| 字段／行为 | V2 证据 | 状态与限制 |
|---|---|---|
| 规范 `thread_url` | 三类内容 CSV 从严格合法 TID 构造可信 HTTPS 地址，长整数与非法输入有测试；另选 3 条不同导出链接，浏览器落点和自然响应 TID 均匹配 | 该次样本 3/3 通过；导出仍默认 `not_live_verified`，不把样本成功外推到全部链接 |
| 楼层／评论深链 | 尚无可靠实际落点证明 | 当时保持 `NULL`；V3 主内容 CSV 已移除楼层／评论深链字段 |
| 正文和详情待办 | 真实 `drain-pending` 4 次请求保存首楼、2 条一级回复和 9 条楼中楼，未发现新主题 | 一个首楼评论范围缺可靠空页证据，仍 `incomplete`；不声称整个 run 完成 |
| 独立评论接口作者扩展 | 一次原 protobuf 与 SDK 同响应、同 CID／父楼／作者校验通过，共 7 条；属地、成长等级、另一套 UID 和具体职务均无非空值 | `partial`；只限该响应，不能泛化为 endpoint 不支持；scalar 默认零未获得 presence 证明 |
| 帖子详情附带评论作者 | `get_posts` 的 9 条预览通过组合身份校验；有界生产入库使既有 9 条评论成长等级 0→9、属地 0→7 | `api_verified` 包含该次入库及十三 CSV 验证，正文／媒体／内容水位和行观察时间哈希未变。预览仍不证明完整正文或分页结束 |
| 网页评论属地、赞／踩 | 专用登录会话自然 `page_pc`／`nestedFloor` 响应，经数据库 TID／父 PID／CID／UID 校验；3 个评论目标正式回填完成，样本属地 7/9→9/9，并保存赞／踩明确零值 | `web_verified` 包含正式租约／事务入库证据；不能用无 CID 的 DOM 评论绑定，页面显示也不冒充发表当年的属地 |
| 本吧等级与头衔 | 实采 DOM 和 API 已知作者交叉核验，等级来自同 owner 的 `icon_level`、明确“本吧头衔”提示与 `#level_N`，不是图片文件名 | `partial`；同页另一作者没有等级图标时保持缺失，不能借用嵌套评论图标 |
| 全局成长等级图片 | V2 当时页面仅见成长图标，未验证数字 | `needs_evidence` 是历史 V2 结论；V3 定向页面样本核实过数字及图标 URL，V5 全库覆盖见末尾小节 |
| 具体吧务角色 | 有界当前资料、论坛资料、吧务名单共 3 次请求，UID／FID 匹配，样本确认具体职务 | `api_verified` 当前快照；不代表历史发帖时职务，名单未列出不能写确定 False |
| 收藏／赞按钮无数字 | DOM 仅显示操作文案；自然 `page_pc` 明确计数字段另有结构化证据 | 文案仍为 `zero_presence_unverified`；已用 JSON 明确数字补入主题收藏／转发／反对零值，不把文案当 0 |
| 媒体与回复方向 | 原完整 API 内容／媒体水位及富媒体映射保留；用户提及的被回复 UID 与父楼 UID 分开 | V2 历史批次未提供独立 `media_urls` 字段；V3 已增加。视频／精确被回复评论 ID 当时仍未证实 |

## V3 历史批次与定向页面证据（由 V5 当前批次更新）

以下数字和状态仅描述 V3 批次；当前字段覆盖和 V5 尝试结果见本文件末尾。

V3 当时的全库 CSV 批次：threads 89、posts 2,173、comments 77、users 1,619、user_forum_snapshots 2,089、media_assets 542。三类内容正文非空分别为 87、2,150、74；属地非空分别为 80、1,441、23；正成长等级分别为 86、2,095、27。没有一行被规范化为成长等级 `0`；未能证明 presence 的 protobuf 标量零仍表示未知，不从当前 profile 补写历史内容作者字段。

媒体表共 226 条图片引用、316 条表情引用、0 个真实视频样本。三类内容 CSV 的 `media_urls` 非空行数分别为 38、117、0；数组只含按片段位置排序、去重的图片／视频 URL，并按 `(owner_type, owner_id, tid)` 关联。跨三类内容 CSV 对应媒体归属、顺序及 URL 对照通过。媒体表独立保留。不会做 OCR 或 URL 文件名推断，也不把表情、成长图标或视频封面缩略图误列为正文媒体。视频支持目前只有合成测试证据。

目标页面截图中，3 条一级回复均显示并保留数值等级 10、成长图标 URL 和属地。一个嵌套评论的成长图标从深层 `feed_head.main_data` 读取并保留；页面字段任务状态为 planned／eligible／collected／completed 各 1、missing 0。三类内容 CSV 的 `global_growth_icon_url` 分别有 1、3、1 条，仅是定向页面证据，不代表全库覆盖；评论 `reply_to_display_name` 按页面明确显示值提取。未使用 OCR。

API 对照发现 raw `Post` 节点没有 `ip_address` 或 `location` 字段；帖子地区候选位于 `user_list` 作者对象。已存目标的有效 API 对照中解析等级为默认零、属地为空；同目标的原生 `page_pc` 深层响应含成长徽章 URL，但该证据没有证明其数字等级或 IP。另有 3 条一级回复截图样本显示等级 10、图标 URL 和 IP。网页观察按精确内容及作者身份绑定，并与该 API 样本区分；不借用其他楼层或当前用户 profile。成长值为 `0` 的 presence 无法由 protobuf scalar 证明。

`get_posts` 详情分页响应的 canonical PID 需由明确、唯一的 floor 1 Post 证明，不能将分页返回的 `raw.thread.pid` 当成主题首楼 PID。修正后 60 个详情响应页已保存且 0 失败，canonical 主题仍为 89。5/6 个主题详情 API 任务在本次有界分页验证中取得终止证据，但直接采集没有推进对应 checkpoint；另一次原生 CLI 验证完成同一主题的两个一级回复页面 checkpoint，其子评论 scope 有 3 个完成、7 个未完成、22 个待处理。因此整轮及详情队列均未完成。长正文样本 API 10,686 字符与页面 DOM 10,649 字符在空白归一后相等；这是单个样本，不能保证所有正文。IDE 图形界面停止行为仍为 NOT_TESTED。

字段来源使用响应／页面观察时间。帖子详情中的用户对象也不能据此声称是作者发表当年的属性；独立 profile、论坛资料和吧务名单始终属于当前快照。API 预览中的共享用户信息需保留明确 source path，并与网页的该条内容展示值分别对照。

正常／精品流深度测试实际保存了普通 CREATE 12 页和精品 1 页，共 374 个去重主题索引；全部详情仍待处理。13 页的非置顶创建时间页内和跨页均倒序，未发现重复页。14 次 HTTP 计数中包含一次 SDK 论坛名称解析；首两次间隔 0.327 秒，不能宣称这一旧版本实测的全部 HTTP 都满足 5 秒间隔。该隐式解析路径已纳入后续限速修复和独立测试。

API 能取但未映射、接口此样本空值、页面未显示、图标语义未证实、身份不匹配、分页未完成和当前资料快照分别记录。最终是否写入及每次非空率，以本机新批次的 field observation、field quality 和验收报告为准；本节不表示全吧覆盖。

## V5 当前批次与网页字段尝试

V5 全库 CSV 有 89 个主题、2,173 条一级回复、77 条楼中楼、1,619 个用户、2,097 条吧内快照和 542 条媒体记录。正文非空数为主题 87/89（85 条 API 核实、2 条旧列表 fallback 未核实；另有 2 条纯媒体主题正文为空）、一级回复 2,150/2,173、楼中楼 74/77；此次补采没有修改正文。

| 内容 | 正成长数字 | 成长图标 URL | 数字或图标至少一项 | IP 地点 |
|---|---:|---:|---:|---:|
| 主题 | 86 | 4 | 89/89 | 80 |
| 一级回复 | 2,095 | 81 | 2,173/2,173 | 1,441 |
| 楼中楼 | 37 | 36 | 72/77 | 76 |

所有 797 个原始缺失字段目标均有一次网页尝试：9 个主题、734 条一级回复和 54 条楼中楼，其中包含 10 条 legacy 目标。浏览器执行 167 次导航和 5 次自然展开；另有一次导航及 5 次展开，用于核对剩余 5 条评论是否有成长徽章。没有遇到认证门。5 条缺少成长值的评论均以确切父楼和作者标题身份核对；页面只有头像，没有成长徽章，因此未编造数值或图标 URL。

732 条缺失一级回复 IP 的字段来源均记录了 `field_sources_json.ip_location_text.web_check`。7 条缺失主题 IP 经网页检查仍为空；另有 2 个主题页面返回 HTTP 200 及响应错误 4，无法获得可靠作者信息，不能据此断言主题已删除。1 条评论的接口未提供 IP 字段。

另有一个缺少成长数字的既有评论在手动登录 Profile 的只读验证中通过当前 `WebFallbackProvider.collect` 流程：一页加载后自然展开 5 次，共 6 次受控页面动作，在 90 秒默认任务时限内完成；没有认证门或用户卡片，收集器返回了身份验证通过的成长图标候选。该验证没有数据库写入；它表明将默认展开预算从 1 提高到 10，能覆盖这一已选目标所需的 5 次展开，不代表所有页面都能显示等级。单目标仍只访问一页，超时的配置允许上限为 120 秒。

10 条 legacy 评论通过 6 次 API 请求刷新，评论 ID、父楼、作者、FID 和正文均与原值一致；研究评论计数从 67 增至 77，最终 CSV 仍为 77 条，没有重复行。正式网页值通过原有 enqueue／lease／reconcile 事务流程合并。41 个此前因 source-anchor URL 被拒绝的目标改用规范来源主题 URL 后全部重试通过，具体 owner PID/CID 证据保留。

媒体 `media_urls` 按 owner、TID 和片段顺序与三类内容 CSV 对照通过，独立媒体表仍为 542 行（226 条图片、316 条表情、无真实视频）。17 张受保护表的内容、ID、父子关系、媒体、正文标记、checkpoint 和 scope 哈希均未改变。真实数据只保留在受忽略的本机文件和私有交付物中，没有提交到公开仓库。

V5 结果只覆盖这些既有目标的字段尝试，不证明全站完整。新版主入口的异步／信号收尾有受控测试和独立审阅，但没有实际 VS Code/PyCharm GUI 停止或 SIGKILL 验收；强制杀进程后的现场行为不作保证。
