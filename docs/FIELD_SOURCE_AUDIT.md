# 字段来源与验证范围

审计日期：2026-11-14

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
| `first_pid/body_text/contents_raw` | `get_posts` raw `Posts.thread`，并与返回的 floor 1 `Post` 配对 | `research_thread.first_pid/body_text/contents_raw`；`research_content_observation` 保存 `thread_first_post` owner 水位 | floor 1 不作为普通 `research_post` 行。列表新记录不会清掉详情正文；旧详情仅在内容水位被接受时补写详情独有字段。完整实时样本仍未验证。 |
| `reply_count/agree_count/disagree_count/view_count/share_count` | `get_threads` 与 `get_posts` raw thread 的计数属性 | `research_thread` 规范列和 source payload | 计数是响应时点的值。接口报告数与真实回复总数/页面展示口径的差异尚未完成全量对账。 |
| `collect_count` | `get_posts` raw `Posts.thread.collect_num`；`get_threads` 不提供该列 | `research_thread.collect_count`；raw 解析值保留在 `source_payload_json["counts"]["collect_count"]` | 合成映射和 PostgreSQL 水位回归已覆盖非零与零值。没有字段 presence 证据的 raw `0` 规范列写 `NULL`，缺失原因标记 `zero_presence_unverified_in_parsed_object`。没有据网页数字补值。 |
| 主题状态和作者动态属性 | `get_threads` 列表字段；`get_posts` raw thread / author 字段 | `research_thread` 保存支持的 `thread_type`、状态、作者、互动和属地列 | 只更新响应来源支持的字段；共享字段使用更新时点，详情独有字段由内容观测水位控制。分享帖自身内容缺失时不把分享来源正文冒充首楼正文。 |
| `share_origin/vote_info` | aiotieba raw `Thread_p` 声明 | 当前研究表未定义对应规范列 | raw 类型字段存在不等于本项目已经映射、持久化或验证该数据；目前不用于补造首楼正文。 |

`tiebameow` 的 `get_posts_dto` 转换只返回 `PostsDTO.objs/page/forum`，不会保留 raw `Posts.thread`。项目通过 `ResearchClient` 的 `ResearchPage` 同次保留 raw 与 DTO，不需再发一条请求取得 `collect_num` 或首楼属性。

分页报告数与主题/父楼的回复报告数是不同来源。`aiotieba==4.8.2` 的 `Page_t` 和 `Page_c` 有 `total_count`，但上游 DTO 转换没有保留该值；项目 `ResearchClient` 在 `get_threads`、`get_comments` 转换后从 raw page 复制它。`get_posts` 的 `Page_p` 没有 `total_count`，其 DTO 默认 `0` 不能当作接口报告值；相关页 checkpoint 标记 `not_provided_by_aiotieba_posts_page`。数值为 `0` 时仍按字段 presence 规则区分可验证报告与未验证值。

## 一级回复与楼中楼

| 范围 | 来源路径 | 当前保存位置 | 边界 |
|---|---|---|---|
| 一级回复 | `get_posts` raw `Posts.objs[]` / `Post`，含 `pid/tid/fid/floor/create_time/contents/author/counts` | `research_post`；媒体片段写入 `research_media_asset` | `research_post` 保存首楼以外的楼层。首楼 ID、正文和评论报告数留在 `research_thread`。 |
| 楼中楼 | 独立 `get_comments(tid, pid, page)` 的 raw `Comment` | `research_comment`，保留 `cid/tid/parent_post_pid` 和 `reply_to_user_id/reply_to_id_raw` | `get_posts` 带回的有限附带 comments 不代表完整楼中楼分页，也不会替代独立 `get_comments` 扫描。真实多页完整率未验证。 |
| 回复目标 | `Comment.reply_to_id` | 非零解析值原样保留；`0` 写入 `reply_to_id_raw`，规范目标为 `NULL` 并记录 `aiotieba_zero_sentinel` | 该字段是目标用户 ID，不是被回复 comment ID。解析器的 `0` 是 sentinel，不能据此断言 API 明确返回数值零。 |
| 首楼楼中楼父楼报告数 | `get_posts` 匹配到的 floor 1 raw `Post.reply_num` | `research_thread.source_payload_json["first_post"]`，包含 raw 值、规范值、来源和观察时间 | 不与 `ResearchThread.reply_count` 混用。没有 presence 证据的 parsed `0` 规范值为 `NULL`。质量报告优先使用该评论 scope 当前代最近有可信报告数的完成 checkpoint；没有时才回退此父楼值。 |
| 评论作者等级/属地 | 独立 `get_comments` 的 `UserInfo_c` | `research_comment` 对该接口未提供的 `glevel/ip` 留空并记录缺失原因 | raw 附带 comments 的 `UserInfo_p` 有更丰富字段，但它不是全量来源，当前不会用它填充独立评论记录。 |

楼层、点赞/反对数、作者标识和内容片段均保留来源与响应观察时间。父子关系在导出前由 SQL 检查；回复数与已采集唯一 ID 数分别统计。

## 用户、吧内快照与属地

| 字段组 | 来源路径 | 当前保存位置 | 证据与限制 |
|---|---|---|---|
| 用户 ID、用户名、昵称、portrait、`tieba_uid` | `get_threads/get_posts/get_comments` 作者对象 | `research_user` 最新身份行与每条内容的作者 ID | `user_id` 与 `tieba_uid` 是不同标识。用户名、昵称、portrait 会变化。 |
| 吧内 `forum_level/forum_level_name/role_name` | 启用登录后的 `get_user_forum_info`；内容接口作者对象也可能提供 `forum_level` | `research_user_forum_snapshot`，按用户、贴吧、来源和观察时间记录 | `get_user_forum_info` 的映射明确将 `is_bawu`、`icons` 留空并记录为接口未提供；内容作者对象单独提供这些字段时才映射。`role_type` 或徽章 ID 不从图标猜测。 |
| 作者 `is_bawu/icons` | `get_threads/get_posts/get_comments` 返回的内容作者对象（仅在该对象暴露时） | 对应内容行及由内容接口生成的 `research_user_forum_snapshot` | 不来自 `get_user_forum_info`；不同内容接口的作者对象字段并不完全相同，缺失时保持空值并记录原因。 |
| 帖子作者 `ip_location_text` | `get_posts` 的 `PostUserDTO.ip` 或 raw thread author `.ip` | `research_post` / `research_thread` 的属地列，另保存来源路径和观察时间 | 10 条有界作者样本中，字段非空且为中文地域文本 10/10；IPv4/IPv6 字面值 0/10。只把非 IP 字面值当属地文本写入；原始 `ip` 不写入 source payload。该样本不能证明其他接口、其他用户或历史位置。 |
| 评论作者 `ip_location_text/glevel` | 独立 `get_comments` 的 `UserInfo_c` 未声明这些字段 | 缺失时保持 `NULL` 并写原因 | 不用作者头像、截图或附带 comments 推断。 |

用户在百度官方页面手动登录后，进行过一次有界 `get_user_forum_info` 检查：状态 `available`，请求和响应用户匹配，`forum_level`、`level_name`、`role_name` 均非空。只验证这一当前响应，不代表所有用户都可查询，也不回填旧帖发表时的身份。原始账号值未写入仓库或本文件。

### 有界真实内容样本

- 一个小主题的 `get_posts` 请求读取一页并以终止状态完成；raw/DTO 含首楼和一条普通一级回复两个楼层 ID，数据库分别保存首楼和一条 `research_post`，checkpoint 为 `completed`。该次同时生成 4 条用户吧内快照。主题 `reply_num` 报告为 2，而保存的唯一普通一级回复为 1；差异原因和字段口径未知。
- 一条楼中楼扫描跨两页返回 30+20 个唯一评论，共 50 条且均已持久化。两页各自的 raw、DTO 和 checkpoint `total_count` 均为 51；质量报告现显示 51 对 50、`count_mismatch=true`。未确定差异原因。第二页独立 DTO ID 指纹未保留，因此不能声称已对全部 ID 做独立逐项验证；两页 raw/DTO 的数量一致。该结果只代表此 scope。

## 媒体、checkpoint 与质量报告

- 详情响应中的内容片段映射到 `research_media_asset`：`get_posts` 的首楼（需匹配到 floor 1 `Post`）和普通一级回复，以及独立 `get_comments` 返回的评论；`get_threads` 列表响应不会直接写媒体行。已映射的片段保留 owner 类型、ID、位置、片段类型、URL/hash/尺寸、来源及观察时间；不下载媒体文件。图片字段在库源码中有声明；真实媒体样本的完整性尚未现场验证，不能据此声称媒体覆盖完整。视频稳定 ID 仍未确认。
- `research_content_observation(owner_type, owner_id)` 只由完整内容响应推进。thread 首楼 owner 为 `thread_first_post/tid`，普通 post 与独立 comment 的 owner 分别为 `post/pid`、`comment/cid`。空媒体集合也有水位，避免较旧页面恢复已删除的媒体引用。
- `research_run` 记录每次研究运行；`research_checkpoint` 按 run/task/page 保存页码、分页状态、游标、报告数、错误类型与 scope 状态。`crawl_quality_report` 对详情 scope 汇总报告数、成功/失败页、唯一记录数、差异和原因。楼中楼 scope 优先采用当前代最近一个报告数可信的完成 checkpoint；无可信 checkpoint 时，首楼回退 `research_thread.source_payload_json["first_post"]["subreply_count_reported"]`，普通回复楼回退 `research_post.subreply_count_reported`。评论质量计数不会回退到主题总回复数；一级回复对账使用主题回复报告数和保存的唯一一级回复数。列表流的分页状态仍由 `thread_list` checkpoint 保存。
- manifest 的实体导出校验与采集完整性是分开的结果。没有 checkpoint 时，质量证据为 `unknown_not_tested`；存在 checkpoint 也不证明平台上的所有数据都已抓取。

当前实现回归为全仓 236 passed、10 个默认门控用例 skipped；质量 SQL 定向用例 17 passed；专用 PostgreSQL scope 双事务锁竞态两种顺序 2 passed。Ruff、compileall、diff-check 通过。三次真实 CLI 导出各生成唯一批次且未覆盖旧文件；最终批次六类实体 CSV 的全数据库行数与 SQL/manifest 一致，`entity_scope=all_database_rows`、`validation_status=passed`，全 runs 质量报告为 98 行。备份恢复尚未验证。导出校验不能证明平台数据完整。

## 截图仅提供 UI 线索

七张本机截图显示了排序、等级、角色文字/图标、媒体、属地、计数和折叠评论等页面元素。截图没有作为公开文件发布。页面控件本身不能证明接口字段、protobuf presence、计数口径或历史状态。

## 当前未验证项

- 超过两页的真实 CREATE 跨页排序、服务端页数变化与全吧终止条件。
- 超过 10 页真实运行、真实一级回复/楼中楼完整分页和数量对账。
- 多类型真实媒体片段、分享帖和投票内容的 raw 到数据库完整性。
- 详情队列完成与详情 checkpoint 恢复、IDE GUI 停止、进程崩溃后的现场恢复，以及备份恢复。
- `ip` 地域文本与具体网页 UI 地区标签的一一映射；当前结论只限于上述 10 条接口字段样本。
