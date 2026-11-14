# tieba-research-crawler

本项目从 [TiebaMeow/TiebaScraper](https://github.com/TiebaMeow/TiebaScraper) 延伸，使用 `aiotieba` 访问贴吧接口，并保留上游历史与 [MIT 许可](LICENSE)中的版权声明。项目面向本机研究采集，保存原始用户 ID、昵称和帖子内容，不做匿名化。

> **当前状态（2026-11-14）**：研究扩展已推送，合成回归、专用 PostgreSQL scope 竞态和独立审查通过。有限真实主题/楼中楼样本、主入口列表 checkpoint 的 SIGINT/同 run 恢复，以及七类 CSV/manifest 的三次真实导出已验证；这不代表完整详情队列或全吧采集完成。超过 10 页运行和备份恢复仍待验收。

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

## 导出本机 CSV

先暂停采集，再运行导出，避免导出期间数据库内容和质量检查点继续变化：

```sh
uv run python -m src.research export
```

默认写入 `exports/` 下一个新的批次目录。每次导出都会生成唯一目录与文件名，文件名前缀含 `all-runs`、导出时间（Asia/Shanghai）和唯一批次 ID；已有批次不会覆盖。目录内包含七个 UTF-8 with BOM CSV：

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

`--run-id` 只筛选 `crawl_quality_report` 中的 checkpoint，并作为批次命名上下文；其余六类实体 CSV 仍导出数据库全部 run。批次名称和 manifest 会标明全库范围，不会把这次导出误称为单 run 数据。

```sh
uv run python -m src.research export --run-id YOUR_RUN_ID
```

CSV 保留原始 ID、昵称和正文。实体行数、唯一 ID、回复父子关系和 CSV 行数会在写入后校验；manifest 的实体验证通过不代表平台数据已经完整。没有 checkpoint 时，质量证据状态为 `unknown_not_tested`。

已完成三次真实数据库 CLI 导出。三个批次均生成不同的时间戳加 UUID 目录，旧批次保留且未被覆盖。最终批次的六类实体 CSV 覆盖全数据库，行数与 SQL 查询及 manifest 一致：`threads=89`、`posts=2`、`comments=50`、`users=80`、`user_forum_snapshots=119`、`media_assets=31`；manifest 的 `entity_scope=all_database_rows`、`validation_status=passed`，`crawl_quality_report` 覆盖全部 runs 共 98 行。前两次命令带有 comments run 的 `--run-id`，其质量报告各有 2 行。备份恢复尚未验证。

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
