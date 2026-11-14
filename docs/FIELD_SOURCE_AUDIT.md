# 字段来源审计（静态源码初版）

审计日期：2026-11-14

审计范围：已安装的 `aiotieba==4.8.2`、`tiebameow==0.5.0`，以及 TiebaScraper 当前工作区源码。

## 证据边界

本文件只记录源码中声明的字段、转换路径和当前 ORM 列。没有发起贴吧 API 请求，也没有检查真实 API 响应样本；所有响应证据栏均据此标注。截图只说明网页能显示相应控件或文本，不作为 API 字段证据。未在本文复制或公开截图中的帖文正文、用户标识或图片内容。

状态含义：

- **A**：源码有 API/DTO 到 ORM 的字段映射；不代表已经以真实响应验证字段非空或语义。
- **B**：源码中有 API/DTO 字段，但当前 ORM 没有对应持久化列，或只有未规范化的内容片段。
- **C**：可由另一个源码接口请求取得，需额外请求；真实请求结果未验证。
- **D**：确认原始 API 有字段而当前解析层丢弃。当前初审未找到可满足目标字段的确定 D 证据。
- **E**：网页可见，当前 API/DTO 路径或映射语义未证实。
- **F**：当前公开 API/DTO/ORM 代码路径没有该字段；这只表示当前代码不可用，不证明百度服务端绝不返回。

## 主题帖（`thread`，含首楼）

| field_name | scope | API endpoint | aiotieba path | tiebameow DTO path | ORM column | response evidence (non-sensitive) | availability | source_time_semantics | change_required | risk |
|---|---|---|---|---|---|---|---|---|---|---|
| `tid`, `fid`, `fname` | thread | `get_threads` | `Client.get_threads` | `ThreadsDTO.objs[].tid/fid/fname` | `Thread.tid/fid`; `Forum.fid/fname` | 仅源码字段声明；无线上响应样本 | A | `create_time` 与标识分开，见下行 | 复用基础列 | `fname` 存在 `forum` 表，不在 `thread` 行 |
| `first_pid` | thread / first post | `get_posts`（也见 `get_threads`） | `Posts.thread.pid` | `ThreadDTO.pid`；`get_posts_dto` 返回的 `PostsDTO` 不暴露 raw `.thread` | 无 | aiotieba `Posts` 定义 `thread: Thread_p`，tiebameow DTO 转换结果只含 `objs/page/forum`；无真实响应样本 | B | 帖子创建时点需与 `tid` 分开 | 从同次 `get_posts` raw response 读取首楼 ID 并保存 | worker 过滤 `floor == 1` 的回复，首楼当前没有独立 `post` 行 |
| `title`, `body_text`, `contents_raw` | thread / first post | `get_posts` raw thread；`get_threads` DTO | `Posts.thread.title/contents/text` | `ThreadDTO.title/contents/text`；`PostsDTO` 丢弃 raw `.thread` | `Thread.title/text/contents`（仅列表 DTO 转换路径） | raw `Thread_p` 暴露首楼 `title/contents/text`；列表 DTO 也有内容路径；无真实响应样本 | B | 属于主题帖首楼内容；是否完整未在线验证 | 将 raw `Posts.thread` 纳入项目转换层并确认内容完整性 | 分享帖 `is_share=true` 时 raw 首楼 `contents` 为空，内容位于 `share_origin` |
| `create_time`, `last_time`, `scraped_at` | thread | `get_threads` | `Thread.create_time/last_time` | `ThreadDTO.create_time/last_time` | `Thread.create_time/last_time/scrape_time` | 源码定义 timezone-aware datetime，并映射到 ORM | A | `create_time` 是主题帖创建时间；`last_time` 是最后回复时间；`scrape_time` 是采集时间 | 补充字段来源与采集时间说明；当前未保留原始 API 时间戳表示 | 排序研究必须使用 `create_time`，不能用 `last_time` 替代 |
| `author_user_id`, `author_user_name`, `author_nickname`, `portrait` | thread author | `get_threads` | `Thread.author` | `ThreadDTO.author.user_id/user_name/nick_name_new/portrait` | `Thread.author_id`; `User.user_id/user_name/nick_name/portrait` | 只有类型与映射代码；无用户记录样本 | A | 作者资料是采集时可见资料；昵称和 portrait 会变化 | 保留原始 ID；为变动属性加观察时间 | `user_id` 与 `tieba_uid` 不同，不能互换 |
| `forum_level`, `global_growth_level` | thread author | `get_threads` | `Thread.author.level/glevel` | `ThreadUserDTO.level/glevel` | 仅 `Thread.author_level` 保存 `level` | DTO 声明两字段；ORM 映射只写 `author_level=level` | B | 消息对象上的值是该次响应值，不是旧帖发布时的历史快照保证 | 扩展表分别保存 `level` 与 `glevel` | 不能仅凭 UI `Lv.x` 或 SVG `#level_4` 判定两字段语义 |
| `role_type`, `role_name`, `role_badge_id/url` | thread author | `get_threads`; 用户资料见下表 | `ThreadUserDTO.is_bawu/icons` | `ThreadDTO.author.is_bawu/icons` | 无 | DTO 有布尔值和字符串列表；未验证其图标映射 | B/E | 页面角色与徽章可能随时间变化 | 保存原始 `is_bawu/icons`；角色名称/徽章映射待 API 或 DOM 证据 | 布尔“吧务”不能替代具体身份名称或图标语义 |
| `reply_count`, `agree_count`, `disagree_count`, `view_count`, `share_count` | thread | `get_threads` | `Thread` | `ThreadDTO.reply_num/agree_num/disagree_num/view_num/share_num` | 仅 `Thread.reply_num` 保存 | DTO 有计数字段；ORM 只映射 `reply_num` | B | 计数是采集时的动态值；reply 总数不等于实际一级楼层数或楼中楼数 | 扩展列保存其余原值；另做数量口径对照 | API 计数与页面显示语义尚未做真实样例核对 |
| `collect_count` | thread | `get_posts` raw thread | `Posts.thread.collect_num` | `get_posts_dto` 转换后无 thread-level 字段 | 无 | aiotieba `Thread_p` 声明并从 `thread_proto.collect_num` 赋值；DTO/ORM 未保存；无真实响应样本 | B | 计数是该次帖子详情响应值 | 在项目转换层从 raw `.thread.collect_num` 保存 | 不从网页数字补造；应记录采集时点 |
| `thread_type`, `is_top`, `is_good`, `is_livepost`, `is_hide`, `is_share` | thread | `get_threads`; `get_posts` raw thread | `Thread`; `Posts.thread.type/is_share` | 同名 `ThreadDTO` 中有部分状态；`PostsDTO` 丢弃 raw `.thread` | 无 | DTO/raw 源码声明字段；ORM 未保存 | B | 状态为采集时观察值 | 扩展列保留原始状态与 raw share fields | 当前 `ThreadsTaskHandler` 过滤 `is_livepost`，会缩小目标总体；分享帖结构不同 |
| `share_origin`, `vote_info` | thread / shared or poll content | `get_posts` raw thread | `Posts.thread.share_origin/vote_info` | `PostsDTO` 不暴露 raw `.thread` | 无 | raw `Thread_p` 声明字段；未检查真实样本 | B | 内容和状态取自帖子详情响应时点 | 扩展保存结构化内容与缺失原因 | 分享帖自己的 `contents` 为空时，不能误写原帖内容为分享者正文 |

## 一级回复（`post`）

| field_name | scope | API endpoint | aiotieba path | tiebameow DTO path | ORM column | response evidence (non-sensitive) | availability | source_time_semantics | change_required | risk |
|---|---|---|---|---|---|---|---|---|---|---|
| `pid`, `tid`, `fid`, `fname`, `floor`, `create_time` | post | `get_posts` | `Client.get_posts` | `PostDTO.pid/tid/fid/fname/floor/create_time` | `Post.pid/tid/fid/floor/create_time`; `Forum.fname` | 仅 DTO 和 ORM 声明/映射；无真实页响应 | A | `create_time` 为该楼层创建时间 | 复用基础列 | `pid` 与 `tid` 是不同层级 ID |
| `text`, `contents_raw` | post | `get_posts` | `Post.contents` | `PostDTO.text/contents` | `Post.text/contents` | DTO 纯文本由内容片段派生，ORM 保存文本和 JSON 片段 | A | 回复自身创建时间见 `create_time` | 核验片段类型序列化与真实内容完整性 | 原始片段结构含 @、图片等，不应只导出纯文本 |
| `author_user_id`, `user_name`, `nickname`, `portrait` | post author | `get_posts` | `Post.author` | `PostDTO.author.*` | `Post.author_id`; `User.*` | 基础身份列由 `User.from_dto` 写入 | A | 作者档案是采集时资料 | 身份保留；变动信息宜加观察时点 | 昵称不是稳定唯一标识 |
| `forum_level`, `global_growth_level`, `ip` | post author | `get_posts` | `Post.author.level/glevel/ip` | `PostUserDTO.level/glevel/ip` | 仅 `Post.author_level` 保存 `level` | DTO 有字段；ORM 未保存 `glevel`、`ip` | B | 均为响应时点属性 | 扩展保存原值并标明采集时间 | DTO 字段名 `ip` 与网页显示的属地文案是否同义未验证 |
| `is_thread_author`, `agree_count`, `disagree_count`, `subreply_count_reported` | post | `get_posts` | `Post` | `PostDTO.is_thread_author/agree_num/disagree_num/reply_num` | 仅 `Post.reply_num` 保存 | DTO 有字段；ORM 丢失其余字段 | B | 计数与标志为采集时响应值 | 扩展列；保存报告数与实际评论数分别统计 | `reply_num` 不代表已采到的 comment 数 |
| `post_url` | post | 可按 `tid/pid` 构造；未见 DTO 字段 | 无 | 无 | 无 | 只由标识可生成；未核查页面 URL 规则 | E | 不适用 | 后续统一生成并验证 | 不能写成 API 返回字段 |

## 二级回复（`comment`）

| field_name | scope | API endpoint | aiotieba path | tiebameow DTO path | ORM column | response evidence (non-sensitive) | availability | source_time_semantics | change_required | risk |
|---|---|---|---|---|---|---|---|---|---|---|
| `cid`, `tid`, `pid`, `fid`, `fname`, `create_time`, `text`, `contents_raw`, `author_user_id` | comment | `get_comments`; `get_posts` 可附带有限评论 | `Client.get_comments`; `Posts.objs[].comments` | `CommentDTO.cid/tid/pid/fid/fname/create_time/text/contents/author_id` | `Comment.cid/tid/pid/fid/create_time/text/contents/author_id`; `Forum.fname` | DTO 有分页信息与字段声明，ORM 映射保存；无真实响应样本 | A | `create_time` 是楼中楼回复创建时间 | 复用基础列并以独立 `get_comments` 分页作为全量来源 | 附带 `comments` 数量受 `comment_rn` 限制，不等于全量 |
| `reply_to_user_id` / `reply_to_id` | comment target | `get_comments`; `get_posts` 附带 comments | `Comment.reply_to_id` | `CommentDTO.reply_to_id` | `Comment.reply_to_id` | aiotieba parser 默认设为 `0`，仅在内容首片段为 `回复 ` 且 protobuf 第二片段带非零 uid 时解析；tiebameow 把数值写入 ORM；无真实样本 | B | 非零值是回复创建时解析出的目标用户；`0` 是 parser sentinel，不是已证实目标 | 原样保留非零值；`0` 转为 NULL 并记录 `missing_reason='aiotieba_zero_sentinel'` | 不可当作被回复 comment ID；缺失值不可按楼层作者补造 |
| `floor`, `is_thread_author`, `agree_count`, `disagree_count` | comment | `get_comments` | `Comment` | `CommentDTO.floor/is_thread_author/agree_num/disagree_num` | 无对应列 | DTO 有声明；当前 ORM 未写入 | B | 楼层与互动字段属于响应时点 | 扩展列 | 评论互动数与帖子回复数不可混用 |
| `author_user_name`, `nickname`, `portrait`, `forum_level` | comment author | `get_comments` | `Comment.author` | `CommentDTO.author.user_name/nick_name_new/portrait/level` | `User.*`; `Comment.author_level` | 基础身份与 `level` 映射存在 | A | 作者资料/等级为响应时点属性 | 保留原始用户 ID 和动态观察时间 | `CommentUserDTO` 没有 `glevel`/`ip` 字段声明 |
| `global_growth_level`, `ip_location_text` | comment author | `get_posts` 附带 comments；独立 `get_comments` | raw 附带 `Comment_p.user: UserInfo_p`；独立接口的 `Comment.user: UserInfo_c` | `CommentUserDTO` 无 `glevel/ip` | 无 | raw 附带路径 `UserInfo_p` 声明这些字段；独立分页 `UserInfo_c` 不声明；无真实响应样本 | D | raw 附带作者属性是响应时点值 | 若保留附带值，解析层需保留；独立分页无此字段时保持 NULL | 附带 comments 不是全量来源；不能替代独立 `get_comments` 分页 |

## 用户资料与吧内动态快照

| field_name | scope | API endpoint | aiotieba path | tiebameow DTO path | ORM column | response evidence (non-sensitive) | availability | source_time_semantics | change_required | risk |
|---|---|---|---|---|---|---|---|---|---|---|
| `user_id`, `user_name`, `nickname`, `portrait` | user | `get_threads/get_posts/get_comments` 作者信息 | `UserInfo_t` / 作者对象 | `BaseUserDTO.user_id/user_name/nick_name_new/portrait` | `User.user_id/user_name/nick_name/portrait` | DTO 与 `User.from_dto` 映射代码可见；未检查真实用户数据 | A | `User` 是当前覆盖式身份表，无首次/最近观察时间 | 增加首次/最近观察时间或独立快照 | 当前同一用户昵称变化会覆盖旧值 |
| `tieba_uid`, `global_growth_level` | user | `anyid2user_info` / user info | `UserInfo` | `UserInfoDTO.tieba_uid/glevel` | 无 | DTO 声明且 `anyid2user_info_dto` 可调用；无结果样本 | B/C | 当前查询时的资料，不代表发帖时历史属性 | 增加扩展列或用户快照 | `tieba_uid` 与 `user_id` 必须分别保留 |
| `forum_level`, `forum_level_name`, `role_name` | user/forum snapshot | `get_user_forum_info` | `UserForumInfo.level/level_name/role_name` | 当前无对应 DTO；客户端公开 aiotieba 返回类型 | 无 | 源码注释说明该接口强制要求有效 BDUSS；未做登录请求 | C | 仅能表示观察时点，不能回填历史帖子创建时状态 | 用户已选择稍后通过官方登录补采；由后续模块接入并保存 `observed_at/source` | 当前未读取 BDUSS；登录凭据需由用户在本机安全配置 |
| `role_type` / `bawu_type` | user/forum snapshot | 未确认 | 安装版 `UserForumInfo` 无 `bawu_type` 属性 | 无 | 无 | 已检查的接口 DTO 只有 `role_name`，没有 `bawu_type` | E/F | 角色需带观察时点 | 不映射到未证实字段；待 API/页面结构证据 | 不能把 `is_bawu` 布尔值当作角色枚举 |
| `role_badge_id`, `role_badge_url` | user/content appearance | 未确认 | 作者对象 `icons` 是 `list[str]` | `ThreadUserDTO/PostUserDTO/CommentUserDTO.icons` | 无 | DTO 声明 `icons`；未验证其中值与页面徽章的对应关系 | E | 徽章/装饰随观察时点变化 | 保留原始 icon 值后再做有证据映射 | 截图中的 `#level_4` SVG 与等级/角色没有已证实关联 |
| `ip_location_text`, `ip_location_source`, `ip_location_observed_at` | author / post | `get_posts` 作者信息；user info | `PostUserDTO.ip`; `UserInfoDTO.ip` | `PostDTO.author.ip` / `UserInfoDTO.ip` | 无 | 源码只有名为 `ip` 的值，网页截图显示属地标签；无 API 原始响应 | E | 应记录帖子/资料来源和采集时间 | 核实 `ip` 是否为页面地区标签；评论作者另行处理 | 该字段若可得是展示属地，不是实际网络 IP 地址 |

## 媒体与完整性记录

| field_name | scope | API endpoint | aiotieba path | tiebameow DTO path | ORM column | response evidence (non-sensitive) | availability | source_time_semantics | change_required | risk |
|---|---|---|---|---|---|---|---|---|---|---|
| `contents_raw` / image fragment (`src`, `big_src`, `origin_src`, `hash`, dimensions) | thread/post/comment media | `get_threads/get_posts/get_comments` | `Contents_t` / `FragImage` | `contents` 中的图片片段 | `Thread/Post/Comment.contents` JSONB | aiotieba 源码声明图片 URL/hash/尺寸；ORM 保存片段字段；无线上样本/序列化 round-trip 测试 | A/B | 与所属内容同时采集 | 为每个媒体片段建立规范化记录并核对落库 | HTML `img.src` 只作页面线索；不下载媒体二进制 |
| `video_id`, video URL/cover/duration/dimensions | thread/post/comment media | 内容接口；稳定 ID 未确认 | `FragVideo` 暴露 `src/cover_src/duration/width/height/view_num` | DTO 内容片段（实际转换/持久化需用合成对象验证） | 仅 `contents` JSONB；无 `media_asset` | 类字段结构可见；未检查真实响应 | B/E | 媒体与所属内容采集时点相关 | 保存原始 URL 与片段；稳定 ID 不可解析时保持 NULL | 不从 URL 任意截取并命名为官方 video ID |
| `media_asset` (`owner_type/id/position/source_api/observed_at`) | media relation | 内容接口 | contents fragments | Thread/Post/Comment DTO `contents` | 无独立表 | 目标契约，不是上游现有模型 | B | 记录本次观察时间 | 项目扩展层新增关联表 | 一条内容可能包含多张图/多个资源，不能只存首个 |
| `page.total_page/total_count/has_more`, page checkpoint | scan quality | 三个列表接口 | `Page_t` | `PageInfoDTO` | 无逐页记录表；另有 `pending_thread_scan`、`pending_comment_scan` | DTO 提供页信息；本地仅有两类 pending ORM，不是页快照 | B | 表示请求当时分页状态 | 在项目层持久化成功/失败页与检查点 | 空页或页码位移不能仅凭 DTO 自动宣告全量完成 |
| `reported_reply_num`, actual unique `pid/cid` counts, `count_mismatch` | crawl quality | posts/comments | `Post.reply_num`, comment IDs | `PostDTO.reply_num`, `CommentDTO.cid` | 有回复数及实体行；无质量报告列/表 | 字段和 ID 路径可见；未执行真实数量对账 | B | 报告数和实际采集数要分别保留采集时点 | 增加逐 `(tid,pid)` 的扫描状态与数量审计 | 差异不能自动解释为漏采或完整 |

## 截图核对（仅 UI 证据）

- 图 1 的“最新 / 发布”是排序目标的页面证据。`ThreadSortType.CREATE` 可经 `get_threads` 参数传递，但当前 `ThreadsTaskHandler` 调用没有传 `sort`；跨页排序尚未在线验证。
- 图 2 的 `Lv.x`、徽章和 `#level_4` SVG sprite 是不同 UI 结构；源码 DTO 的 `level`/`glevel` 有独立字段，但它们到界面文字/图标的映射仍未验证。
- 图 3 可见角色文字与图像徽章；网页资源不证明 API 有结构化 `role_type` 或角色徽章 ID。
- 图 4 可见图片资源 URL；API 源码声明图片片段 URL/hash，但真实响应与当前 ORM JSONB 的完整性尚未验证。
- 图 5 可见属地样式文本；DTO 的 `ip` 字段与该标签是否等价未验证，也不代表要采集真实 IP 地址。
- 图 6 可见回复/点赞/收藏等数字；源码 raw `Posts.thread` 声明 `collect_num`，但 `get_posts_dto` 当前转换丢弃该字段；UI 与 API 的计数语义仍未做真实样例核对。
- 图 7 显示折叠楼中楼与展开入口；当前 worker 通过 `get_posts(..., comment_rn=10)` 附带有限评论，并有独立 `get_comments` 接口。真实多页样本与全量性未测。

## 当前实现事实与扩展建议

- `src/scraper/worker.py` 的线程扫描没有设置 `ThreadSortType.CREATE`，并过滤 `is_livepost`；回复页请求以 `comment_rn=10` 附带评论，再通过 pending comment task 进入独立扫描。这些是当前源码事实，不是现场数据结论。
- 不修改 `.venv/site-packages` 中的 `tiebameow` / `aiotieba`。建议在本项目新增 `src/research` 扩展模型/查询层，用一对一扩展表保存 DTO 已有但基础 ORM 缺失的互动计数、状态与作者属性；另建 `user_forum_snapshot`、`media_asset`、分页/质量检查点表。扩展表保留源字段名、原始 ID、采集时间和缺失原因。
- user/forum 的 level 和 role 是当前时点资料。`get_user_forum_info` 的源码要求 BDUSS；用户已选择稍后通过官方登录补采。本审计没有读取登录凭据，也未调用该接口；后续由负责模块实现本机安全配置和实测。
- 暂未识别真实回复/图片/视频响应，因此字段可得性、跨页完整性、实际非空率和 UI 语义映射均为未验证。
