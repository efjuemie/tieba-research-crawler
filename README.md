# tieba-research-crawler

本项目从 [TiebaMeow/TiebaScraper](https://github.com/TiebaMeow/TiebaScraper) 延伸，使用 `aiotieba` 访问贴吧接口，并保留上游历史与 [MIT 许可](LICENSE)中的版权声明。项目面向本机研究采集，保存原始用户 ID、昵称和帖子内容，不做匿名化。

> **当前状态（2026-11-15）**：最新全库批次含 89 个主题、2,173 条一级回复、77 条楼中楼、1,619 个用户、2,089 条吧内快照和 542 条媒体记录。60 个详情响应页已保存且无失败，canonical 主题数仍为 89；详情子范围仍有未完成项，不能称整轮或全吧采集完成。字段覆盖与样本边界见下方和[字段来源审计](docs/FIELD_SOURCE_AUDIT.md)。

## 最新字段范围

按用户 2026-11-15 的补充要求，主要数据输出采用以下范围；数据库旧列及历史审计保留兼容，不删除原有记录：

- 吧内任职只判断 `is_bawu`，不要求具体职务名称。缺少可靠肯定／否定证据时保持未知。
- 主题互动保留评论数、点赞数、收藏数，以及接口已提供的浏览量；一级与二级评论互动仅保留点赞数。不再为不赞成量、转发数或具体职务额外补采。
- 回复对象优先使用明确用户 ID；无法确定具体被回复评论时，保存“回复 XXX”中的显示对象，不按昵称猜 ID。
- 只提供所属主题的 `thread_url`，不提供楼层或评论深链。
- 成长等级数字不明时保存对应 `global_growth_icon_url`，与本吧等级和其他徽章分开。
- 图片／视频引用除单独的 `media_assets.csv`，也在三类内容 CSV 的 `media_urls` 中按对应内容归集。该列为 JSON 链接数组，按媒体片段顺序去重，不混入表情或成长徽章。
- protobuf 标量等级的解析值 `0` 缺少字段 presence 证明时按未知处理；不当作真实等级零，也不从用户当前资料倒填历史内容字段。图标和等级只按可确认的内容作者 UI 证据采集，不用 OCR 或 URL 文件名推断。
- 评论中的 `reply_to_display_name` 保存页面明确显示的回复对象名称；没有充分证据时不猜测用户 ID。

正文通过详情接口获取；列表索引中尚未执行详情的记录不标为正文完整。完整性以原始富文本片段、正文观察水位及相应详情 scope 证据为准，单个样本成功不代表全库完成。

## 采集范围和隐私

- 主题、首楼、一级回复、楼中楼、作者资料、媒体引用和页级检查点保存在配置的本机 PostgreSQL。
- 帖子和用户 ID、昵称及正文按接口原值保存。CSV 也会保留这些内容；只应在本机查看和备份。
- 公开 GitHub 仓库用于源码、非敏感配置模板和文档。不要提交数据库、CSV、日志、截图、Cookie、BDUSS、`config.toml` 或 `.env`。
- 可选登录只在启用吧内资料补充时使用，默认关闭。登录值只保存在本机忽略的凭据文件；不要把凭据贴进聊天、Issue、命令参数或 IDE 配置。
- 字段来源、样本边界和未证实语义见[字段来源审计](docs/FIELD_SOURCE_AUDIT.md)。架构说明见[数据与运行架构](docs/ARCHITECTURE.md)。

## 本机环境

需要 Python 3.12 或更新版本、[uv](https://docs.astral.sh/uv/) 和一套可连接的 PostgreSQL。项目不要求购买 API、云数据库或云服务器；程序直接连接本机 PostgreSQL。默认 `consumer.transport = "none"`、`cache.backend = "memory"`，不需要 Docker、Redis、TimescaleDB 或额外云服务。访问贴吧需要本机网络能够连接其公开接口。

### 安装 PostgreSQL 并创建数据库

从 [PostgreSQL 官方下载页](https://www.postgresql.org/download/)选择适合操作系统的原生安装方式，启动本机 PostgreSQL 服务。之后在终端创建项目数据库；将用户名替换为本机已有的数据库角色：

```sh
createdb -W -h localhost -p 5432 -U YOUR_POSTGRES_USER tieba_data
```

`-W` 会在终端提示数据库密码。不要把密码写到命令行或提交到仓库。

### 安装项目

```sh
git clone https://github.com/efjuemie/tieba-research-crawler.git
cd tieba-research-crawler
uv sync
cp config.example.toml config.toml
```

Windows PowerShell 复制配置：

```powershell
Copy-Item config.example.toml config.toml
```

编辑本机 `config.toml`：

- 在 `[tieba]` 的 `forums` 中填要采集的贴吧名，格式不要带末尾的“吧”。
- 在 `[database]` 中填本机 PostgreSQL 的主机、端口、用户名、密码和 `db_name`。把模板里的示例账号密码换成本机设置。
- 保持 `partition_enabled = false`、`consumer.transport = "none"` 和 `cache.backend = "memory"`，即可使用标准 PostgreSQL 单机运行。
- `[research] enabled` 默认为 `true`；`fetch_user_forum_info` 默认为 `false`。需要用户吧内快照时再按“登录与用户快照”一节启用。

首次启动时，程序会创建缺失的基础表和研究扩展表。`create_all` 不会替已有表执行通用的增列迁移。对已有数据库升级前先做本机备份。

## 开始采集

从项目根目录运行历史回溯：

```sh
uv run python main.py --mode backfill
```

每次不带 `--resume` 都会在数据库建立一个新的研究 run。回溯按主题创建时间从新到旧读取普通主题流和精华流，每条流从第 1 页开始。`[tieba] max_backfill_pages = 0` 表示不设页数上限；设置为正数时，该数限制本次运行每条流读取的页数。

运行 `hybrid` 时会同时启动周期首页监控和一次历史回溯。周期监控按回复时间读取首页；回溯仍使用创建时间排序，并从普通流和精华流各自第 1 页开始。两种列表各有独立调度，不共用页码。

```sh
uv run python main.py --mode hybrid
```

调度器逐页等待工作器确认后再投递下一页，并在贴吧间轮转。线程接口连续返回后续页时，`max_backfill_pages = 0` 会继续翻页，直到接口确认结束或当前流进入暂停/失败状态。

### 请求频率

默认 `[rate_limit] global_rps = 0.2`，约每 5 秒最多发出一次研究客户端请求。所有通过本程序 `ResearchClient` 的请求共用这一个预算，包括帖子、回复、评论和可选用户资料查询。`threads_rps`、`posts_rps`、`comments_rps` 还分别限制对应接口；调整单接口值不会提高全局上限。该默认值只是项目本机的限速配置，不代表贴吧官方承诺的安全频率。

### 恢复已有 run

进程启动时会显示数据库中的 `run_id`。同一论坛范围、同一运行模式要继续原 run 时，执行：

```sh
uv run python main.py --mode backfill --resume
```

如需精确选择 run：

```sh
uv run python main.py --mode backfill --resume --run-id YOUR_RUN_ID
```

`--run-id` 必须和 `--resume` 一起使用。`--resume` 选择当前论坛范围下最新的可恢复 run；没有匹配的 run 时程序会报错，不会自动另开新 run。回溯列表在重启后会重放最近一个已完成的非终止页一次，以应对顶部内容变化；内容行按 ID 幂等更新。检查点记录列表、帖子详情和评论页状态，导出的质量报告会显示未完成、暂停和计数差异。

本机主入口的一次真实演练验证了列表 checkpoint 的 SIGINT/同 run 恢复：已完成的第 1 页会在重启时重放，run 状态保留为 `interrupted`。该结果不验证详情 scope 完成、IDE GUI 停止或长页端到端行为。进程退出码 0 不等于 run 已正常完成；请查看数据库 run 状态与 checkpoint，不要强制结束进程或删除 checkpoint。

## 登录与用户快照

吧内等级、头衔和角色快照来自 `get_user_forum_info`，反映查询时的资料，不代表发帖时的历史状态。默认 `fetch_user_forum_info = false`，不会读账号文件或打开登录页面。

需要采集时，在 `config.toml` 设置：

```toml
[research]
enabled = true
fetch_user_forum_info = true
account_file = "secrets/baidu-account.json"
```

若使用浏览器扫码，安装可选依赖并启动独立登录窗口：

```sh
uv sync --group login
uv run playwright install chromium
uv run python -m src.research login --browser --account-file secrets/baidu-account.json
```

在打开的百度官方页面由用户手动扫码。程序使用临时浏览器上下文，不读取本机既有浏览器个人资料。若采集程序因未登录或凭据失效暂停，它会等待本机账号文件更新；登录命令成功保存后，采集程序会验证认证并继续。

不安装浏览器也可以用隐藏输入提示保存 BDUSS 或 Cookie header：

```sh
uv run python -m src.research login --account-file secrets/baidu-account.json
```

凭据不会通过命令参数传入，也不会写进日志；默认账号文件权限为仅当前用户可读。不要把 Cookie 或 BDUSS 发给任何人。

## 只处理待办和定向补采

以下命令在本机 Python 环境运行。`YOUR_RUN_ID`、`YOUR_TID`、`YOUR_PID`、`YOUR_CID` 请替换为本地数据库中的真实标识，不要使用昵称代替 ID。

停止发现新主题、只排空已有 run 的详情待办：

```sh
uv run python main.py drain-pending --run-id YOUR_RUN_ID --limit 10 --max-seconds 120
uv run python main.py drain-pending --run-id YOUR_RUN_ID --tid YOUR_TID --limit 1 --max-seconds 120
```

`--limit` 限制初始选中的 scope，父任务产生的子任务仍可继续处理；`--max-seconds` 包含排队和处理时间，超时会保留待办。它不扫描论坛列表、不创建新 run。普通历史回溯仍使用 `backfill`；同 run 列表恢复仍使用前述 `--resume --run-id`。零回复主题也需要取得首楼详情。API 缺少分页身份或终止证据时记录不完整，不能把空解析对象当作成功空页。

网页补采先检查已有 API 详情 scope，再读取指定字段。先检查计划：

```sh
uv run python -m src.research field-backfill --run-id YOUR_RUN_ID --target-type comment --tid YOUR_TID --cid YOUR_CID --field ip_location_text --field agree_count --limit 1 --dry-run
```

`--dry-run` 不创建补采任务、不打开浏览器、不写字段观察。API 未完成的目标为 `pending_api_detail`，应先用 `drain-pending`。正式执行需安装可选浏览器依赖，并指定项目专用资料目录：

```sh
uv sync --group login
uv run playwright install chromium
uv run python -m src.research field-backfill --run-id YOUR_RUN_ID --target-type comment --tid YOUR_TID --cid YOUR_CID --field ip_location_text --field agree_count --limit 1 --max-seconds 300 --browser-profile secrets/tieba-web-profile --headed --wait-for-login
```

需要官方登录或安全验证时，程序保存任务、释放数据库事务，保留可见页面；在该窗口扫码或完成验证，确认能看到帖子后回到终端按回车，同一目标至多恢复一次。等待计入 `--max-seconds`。HTTP 429 会停止，不作为扫码重试。无可取消标准输入的 IDE 控制台请改用 IDE 的 Terminal。不要选择个人 Chrome 默认 Profile，也不要在聊天或命令参数中粘贴 Cookie。

只读网页审计不更新规范字段或创建补采任务，报告写入本机忽略目录：

```sh
uv run python -m src.research web-audit --run-id YOUR_RUN_ID --target-type post --tid YOUR_TID --pid YOUR_PID --field global_growth_icon_url --field ip_location_text --limit 1 --browser-profile secrets/tieba-web-profile --headed --wait-for-login --max-seconds 300
```

`post` 的 `--pid` 是目标楼层；`comment` 的 `--cid` 是目标评论，`--pid` 可进一步限定父楼。筛选条件同时生效，空结果不会退回全库。默认补采最多 5 个目标、120 秒；审计默认 1 个、最多 5 个。字段值缺失与身份未验证分别报告，正确身份但无数字的等级图标不会被猜成等级。

补充当前用户资料、本吧头衔和吧务名单快照：

```sh
uv run python -m src.research user-backfill --run-id YOUR_RUN_ID --tid YOUR_TID --limit 5 --max-seconds 120 --profile --forum --bawu
```

该命令按 UID／FID 去重，使用本地账号文件；论坛查询需先完成上一节的账号登录。结果进入当前用户及吧内快照，不倒填成历史发帖时的属性。名单中未找到某用户不等于已确认不是吧务。

API 与浏览器受控导航／展开使用同一请求预算；单个浏览器任务串行执行。页面自动加载的子资源另行计数，`global_rps` 不是全部网页资源 HTTP 请求数的逐个上限。网页只补已有目标的字段，不替代论坛 API 回溯。

## 导出本机 CSV

先暂停采集，再运行导出，避免导出期间数据库内容和质量检查点继续变化：

```sh
uv run python -m src.research export
```

默认写入 `exports/` 下一个新的批次目录。每次导出都会生成唯一目录与文件名，文件名前缀含 `all-runs`、导出时间（Asia/Shanghai）和唯一批次 ID；已有批次不会覆盖。七个标准 UTF-8 with BOM CSV 保留：

- `threads`
- `posts`
- `comments`
- `users`
- `user_forum_snapshots`
- `media_assets`
- `crawl_quality_report`

另有一个 manifest，记录文件名、记录数和验证状态，不包含帖子内容、用户 ID 或登录凭据。文件权限为仅当前用户可读写。实体 CSV 导出数据库中的**全部 run**，也会保留没有研究扩展记录的旧基础行，并标记其来源未经研究字段核实。研究扩展中为 `NULL` 的字段保持 `NULL`，不会用旧 ORM 的零值补齐。

可以用 `--output` 指定本机输出根目录：

```sh
uv run python -m src.research export --output exports
```

`--run-id` 筛选质量及相关运行审计，并作为批次命名上下文；默认六类实体 CSV 仍导出数据库全部 run。批次名称和 manifest 会标明范围，不会把这次导出误称为单 run 实体数据。

```sh
uv run python -m src.research export --run-id YOUR_RUN_ID
```

CSV 保留原始 ID、昵称和正文。实体行数、唯一 ID、回复父子关系和 CSV 行数会在写入后校验；manifest 的实体验证通过不代表平台数据已经完整。没有 checkpoint 时，质量证据状态为 `unknown_not_tested`。

V2 另输出 scope、字段来源、补采任务／尝试、字段观察、失败及字段质量审计 CSV；这些是本地审计材料，不是新增分析样本。API 字段观察与网页观察分开统计；无法归属 run 的 API 观察明确标为未归属。API／网页值冲突保留证据，不静默覆盖已有值。

```sh
uv run python -m src.research export --entity-scope completed-threads --output exports
```

`completed-threads` 选择最新详情及已知子评论 scope 有完成证明的主题，属于跨 run 的完整主题筛选，不是单 run 导出。用户和吧内快照仍为全库，媒体跟随所选内容，排除理由见 scope 审计。默认 `all` 不过滤未完成记录。

`threads` 全文件按 `create_time DESC NULLS LAST, tid DESC` 排序；`posts` 按主题、创建时间、PID，`comments` 按主题、父 PID、创建时间、CID，后两者时间升序且空值最后。三类内容均有合法 TID 构造的完整 `https://tieba.baidu.com/p/{tid}`，主内容 CSV 不导出楼层／评论深链。规范主题链接的 `not_live_verified` 不承诺所有旧帖仍可访问。

CSV 原样保留研究文本。用 Excel 查看时请通过“从文本／CSV”导入，把 ID 列设为文本，避免大整数被截断；以 `=,+,-,@` 开头的原始文本也可能被 Excel 当作公式。不要用 Excel 自动格式后的内容覆盖原始 CSV。

早期 V1/V2 批次数字是当时快照，已由后续全库导出更新。当前批次中正文非空数为主题 87/89（85 条 API 已核实、2 条旧列表 fallback 未核实；另有 2 条纯媒体主题正文为空）、一级回复 2,150/2,173、楼中楼 74/77。属地非空数分别为 80、1,441、23；正成长等级数分别为 86、2,095、27。没有导出等级值 `0`；这不代表缺失行已确认真实为零。媒体 URL 数组非空行数为主题 38、一级回复 117、楼中楼 0；独立媒体表 542 行含 226 条图片引用和 316 条表情引用，没有真实视频样本。成长图标 URL 在三类内容 CSV 中分别为主题 1 条、一级回复 3 条、楼中楼 1 条；这些是定向页面证据，不能外推为批量覆盖。批次和行数校验通过不代表平台采集完整。

## VS Code 与 PyCharm

打开项目根目录 `tieba-research-crawler`，选择项目 `.venv` 中的 Python 解释器。VS Code 的共享启动配置、PyCharm 运行配置、数据库连接设置和本机恢复步骤见[本机 IDE 运行指南](docs/IDE_SETUP.md)。该文档说明如何选择回溯、恢复或导出配置；公共 IDE 配置中不要存密码或账号凭据。

## 本机备份与恢复

以下命令把自定义格式的 PostgreSQL 备份放入被 Git 忽略的 `data/` 目录。将用户名和数据库名替换为 `config.toml` 中的值：

```sh
mkdir -p data/backups
pg_dump --format=custom --password --host=localhost --port=5432 --username=YOUR_POSTGRES_USER --dbname=tieba_data --file=data/backups/tieba_data.dump
```

先创建空的恢复目标，再执行恢复：

```sh
createdb --password --host=localhost --port=5432 --username=YOUR_POSTGRES_USER tieba_data_restore
pg_restore --password --host=localhost --port=5432 --username=YOUR_POSTGRES_USER --dbname=tieba_data_restore data/backups/tieba_data.dump
```

命令选项见 PostgreSQL 官方手册：[createdb](https://www.postgresql.org/docs/current/app-createdb.html)、[pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html)、[pg_restore](https://www.postgresql.org/docs/current/app-pgrestore.html)。不要把数据库备份或 CSV 上传到 GitHub。

## 公开仓库的安全检查

`origin` 指向本项目公开仓库，`upstream` 指向 TiebaScraper 上游仓库。提交前先检查文件清单、暂存区与忽略规则：

```sh
git status --short
git diff --cached --name-only
git ls-files
git check-ignore -v config.toml .env data/backups/tieba_data.dump exports/example/threads.csv logs/example.log references/example.png secrets/baidu-account.json
git diff --cached --check
```

忽略规则不会移除已经跟踪的文件，也不能阻止 `git add -f`。只暂存经过确认的路径，不要用 `git add -A`：

```sh
git add README.md docs/ARCHITECTURE.md docs/DEVELOPMENT_LOG.md docs/FIELD_SOURCE_AUDIT.md
git diff --cached --name-only
git diff --cached --check
```

VS Code 的共享 `.vscode/launch.json` 已由仓库跟踪；不要在其中加入数据库密码或登录资料。用户私有 IDE 配置、数据、数据库、导出、截图和凭据留在本机。

## 项目记录

- [架构与数据边界](docs/ARCHITECTURE.md)
- [字段来源与验证范围](docs/FIELD_SOURCE_AUDIT.md)
- [开发阶段记录](docs/DEVELOPMENT_LOG.md)
- [本机 IDE 运行指南](docs/IDE_SETUP.md)
- 上游项目：[TiebaMeow/TiebaScraper](https://github.com/TiebaMeow/TiebaScraper)
