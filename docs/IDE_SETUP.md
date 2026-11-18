# 本机 IDE 运行指南

本文说明如何在本机 VS Code 或 PyCharm 中运行回溯采集、恢复任务和导出 CSV。程序直接连接 `config.toml` 里配置的 PostgreSQL；无需 Docker、Redis 或远程服务。统一“发布”排序和重复页保护已实现，自动化回归及 SDK 第 71–75 页列表写入验收通过；该有限验收不代表详情、网页补采或 2021 年以来的完整覆盖。历史 SIGINT/恢复结果及验收范围见[稳定性验收摘要](PREFLIGHT_STABILITY_SUMMARY.md)。

## 准备项目和 PostgreSQL

1. 安装 Python 3.12 或更高版本、[uv](https://docs.astral.sh/uv/) 和本机 PostgreSQL。PostgreSQL 可从[官方下载安装页](https://www.postgresql.org/download/)选择适合操作系统的安装方式。
2. 在终端进入项目根目录，也就是包含 `main.py`、`pyproject.toml` 和 `config.example.toml` 的目录；不要把它的上一级目录当作工作目录。
3. 创建虚拟环境并安装项目依赖：

   ```sh
   uv sync
   ```

   `uv sync` 会使用项目的 Python 版本要求创建 `.venv`。macOS/Linux 解释器通常是 `.venv/bin/python`；Windows 通常是 `.venv\Scripts\python.exe`。

4. 复制本机配置并编辑它：

   ```sh
   cp config.example.toml config.toml
   ```

   Windows PowerShell：

   ```powershell
   Copy-Item config.example.toml config.toml
   ```

   在 `config.toml` 的 `[tieba]` 设置目标贴吧，在 `[database]` 填写实际 PostgreSQL 的 `host`、`port`、`username`、`password` 和 `db_name`。可使用本机任意可连接的 PostgreSQL 地址和端口，不必采用 `localhost:5432`。数据库服务和目标数据库需先由本机管理员准备好。不要把本机配置文件加入提交。

   默认的 `[consumer] transport = "none"` 和 `[cache] backend = "memory"` 不使用 Redis。基础回溯不要求安装 Redis 或启用 TimescaleDB。

5. 若要保存查询时的用户吧内等级和角色快照，在 `[research]` 中保持 `enabled = true`，设置 `fetch_user_forum_info = true`。登录资料保存在默认的 `secrets/baidu-account.json`，也可在 `account_file` 指向另一个本机路径。此路径只供本机使用。

   网页补采需要 Chromium。在项目根目录执行：

   ```sh
   uv sync
   uv run playwright install chromium
   ```

   首次用浏览器二维码登录时，再运行：

   ```sh
   uv run python -m src.research login --browser
   ```

   在新打开的百度官方页面完成登录。该流程不会读取已有浏览器个人资料。需要登录的采集进程会等本机账号文件更新并验证通过后继续。如果不安装浏览器依赖，也可运行 `uv run python -m src.research login`，在隐藏输入提示中输入 BDUSS 或 Cookie header；不要把凭据放进命令参数、日志或聊天。

## 选择 IDE 解释器和工作目录

### VS Code

1. 用 VS Code 打开 `TiebaScraper` 项目根目录。
2. 运行 **Python: Select Interpreter**，选择本项目 `.venv` 中的 Python 3.12+ 解释器。
3. 在 Run and Debug 面板选择 `TiebaScraper: Backfill`、`TiebaScraper: Resume backfill` 或 `TiebaScraper: Export all runs`，然后按 F5。项目跟踪的 `.vscode/launch.json` 保留两个通用示例，并包含这三项研究配置；[vscode-launch.example.json](vscode-launch.example.json) 是三项配置的独立参考副本。
4. 这些公共配置使用 `${workspaceFolder}`，没有用户专属路径、数据库密码、账号凭据或固定 `run_id`。`.vscode/launch.json` 是仓库跟踪文件，对它的修改会出现在 Git diff 中；不要把密码或登录资料写进 IDE 配置。数据库连接信息保存在本机忽略的 `config.toml`；BDUSS/Cookie 只保存在配置指定的本机账号文件（默认 `secrets/baidu-account.json`）。

### PyCharm

1. 用 PyCharm 打开 `TiebaScraper` 项目根目录。
2. 在项目 Python Interpreter 设置中选择已有解释器：macOS/Linux 为 `.venv/bin/python`，Windows 为 `.venv\Scripts\python.exe`。也可让 PyCharm 根据现有 `.venv` 添加项目解释器。
3. 打开 **Run | Edit Configurations**，新建 **Python** 配置。`Script path` 选择项目根目录的 `main.py`，`Parameters` 填入 `--mode backfill`，`Working directory` 选择 `TiebaScraper` 项目根目录，`Python interpreter` 选项目 `.venv`。
4. 导出配置选择 **Module name**，模块名填 `src.research`，参数填 `export`；工作目录和解释器保持项目根目录与 `.venv`。
5. 恢复配置可复制采集配置后，将参数改为 `--mode backfill --resume`；要选定某个批次时，使用 `--mode backfill --resume --run-id <run_id>`。

PyCharm 的配置名称和字段以所安装版本为准；本项目没有在 PyCharm 图形界面中实际创建或运行配置。可参考 [JetBrains Python Run/Debug configuration 文档](https://www.jetbrains.com/help/pycharm/run-debug-configuration-python.html)。

## 开始或恢复回溯

运行科研采集前，确认 `[research] enabled = true`。若采集范围包含吧内用户快照，也要按上一节配置 `fetch_user_forum_info = true` 并完成登录。

从项目根目录开始一轮新回溯：

```sh
uv run python main.py --mode backfill
```

每次不带 `--resume` 都会创建新的研究 run。采集范围由当前配置中的论坛列表决定。

回溯使用统一的“发布”排序列表，不再单独扫描精华主题；每条主题的已有 `is_good` 字段记录精华标记。主题创建时间乱序会记警告并继续；暂停条件为同一 run、同一列表流连续 3 个新页面中，每页至少 90% 的唯一非置顶 TID 已在该流先前页面出现。恢复时重读的列表页和置顶 TID 不计入此判断。该重复规则不用于 `periodic` 的 REPLY 首页监控。自动化回归及 SDK 第 71–75 页列表写入验收通过；本次列表验收未运行详情或网页补采。

恢复当前论坛范围下最近一个可恢复的 run：

```sh
uv run python main.py --mode backfill --resume
```

要精确恢复某个 run，使用启动日志中显示的 `run_id`：

```sh
uv run python main.py --mode backfill --resume --run-id <run_id>
```

`--run-id` 必须与 `--resume` 同用；所选 run 还必须匹配当前配置的论坛范围。若没有匹配的未完成 run，程序会报错，不会静默创建新 run。开始新 run 时去掉两个恢复参数。

### 停止后继续

在终端按 **Ctrl+C** 请求程序协作停止。每个已完成页面会保存数据和 checkpoint；正在请求或尚未完成写入的页面不会被当成已完成。回溯恢复会重放最近成功的非终止列表页一次；详情任务则从各自的 checkpoint 继续。新策略排除这种重放页对三页重复判定的影响。终端恢复机制不代表 IDE 图形 Stop 或强制结束行为已验证。

旧版列表策略的一次真实主入口演练中，普通 CREATE 流第 1 页保存 checkpoint 后收到 SIGINT；同一 run 恢复时重放该页并继续到第 2 页，再次停止。数据库中的 run 状态为 `interrupted`。该结果只验证旧版列表 checkpoint 的恢复路径，不代表新分页策略、详情 scope 或整轮采集已完成；进程 exit code 不能代替数据库 run 状态。

如果 IDE 的强制停止、任务管理器结束进程或断电直接杀掉进程，清理代码可能不会运行。不要因此删除 checkpoint 或另起一个新 run；重新启动时使用相同论坛配置并加 `--resume`。若本轮已完成，程序不会把已完成 run 当作未完成 run 恢复。

## 导出 CSV

在项目根目录执行：

```sh
uv run python -m src.research export
```

默认在 `exports/` 下新建独立批次目录，导出完整本地数据库中的全部 run，保留七类标准 UTF-8 with BOM CSV：`threads`、`posts`、`comments`、`users`、`user_forum_snapshots`、`media_assets`、`crawl_quality_report`，以及 V2 运行／字段审计，目前共十三类 CSV 和一个批次 manifest。每次导出创建唯一目录，不覆盖已有批次。

可用 `--output` 指定另一个本机导出根目录：

```sh
uv run python -m src.research export --output exports
```

如果追加 `--run-id`，它筛选运行质量及相关审计，并作为批次命名上下文；默认其他六类实体 CSV 仍包含全数据库所有 run：

```sh
uv run python -m src.research export --run-id <run_id>
```

命令会打印实际生成的批次目录。质量报告汇总采集 checkpoint，不是“所有平台记录均已抓取”的证明。CSV 和账号文件均只保存在本机。

三次真实 CLI 导出、98 行全 runs 质量报告和旧实体数是 V1 历史结果。V2 已验证隔离备份恢复及十三 CSV 新批次；当前批次计数和字段覆盖见[开发记录](DEVELOPMENT_LOG.md)，以最新全库导出为准。

## 定向网页任务（V2 引入；V5 当前设置）

沿用项目 `.venv` 和项目根目录作为 working directory。PyCharm 的 `drain-pending` 使用 Script path `main.py`，其余研究子命令使用 Module name `src.research`；Parameters 只填子命令及其参数；VS Code 可在集成 Terminal 运行完整命令。需要扫码后按回车时使用有标准输入的 Terminal，避免无可取消输入的调试控制台。

```sh
uv run python main.py drain-pending --run-id YOUR_RUN_ID --tid YOUR_TID --limit 1 --max-seconds 120
uv run python -m src.research field-backfill --run-id YOUR_RUN_ID --target-type comment --tid YOUR_TID --cid YOUR_CID --field ip_location_text --limit 1 --dry-run
uv run python -m src.research web-audit --run-id YOUR_RUN_ID --target-type post --tid YOUR_TID --pid YOUR_PID --field global_growth_icon_url --browser-profile secrets/tieba-web-profile --headed --wait-for-login --max-seconds 300
uv run python -m src.research export --entity-scope completed-threads
```

网页任务默认每个目标只访问一个直接定位页面，最多自然展开十次，单目标超时 90 秒（允许配置上限 120 秒）。`[web_fallback]` 中显式配置较低的 `max_expansions` 或 `timeout_seconds` 会保留低值。API 默认 0.5 次请求机会/秒，WEB 浏览器独立使用 0.2 次/秒；API 遇到 429/220034 或连续三次异常空响应会冷却 10 秒后按次数上限继续。每个持久网页目标最多尝试三次，失败或部分完成后的等待为 3 秒、5 秒，耗尽后仍保留数据库记录。网页端登录／验证需按提示人工确认，浏览器端遇到 429 不自动恢复。执行前运行 `uv sync` 和 `uv run playwright install chromium`；正式 `field-backfill` 在去掉 `--dry-run` 后也需传专用 `--browser-profile`；需要手工验证时加 `--headed --wait-for-login`。登录在当前官方页面完成，不复制 Cookie。等待计入命令运行时限。

`drain-pending` 不发现新主题。`field-backfill` 通常等待 API 详情范围完成；有当前 run／generation、精确身份、水位和类型化完整内容标记的目标可在 scope 完成前补采元数据。列表来源主题仅在 TID/FID、作者 UID 和首楼证据明确时开放作者元数据补采；未经验证的 legacy 记录仍不可跳过 API 详情。当前用户资料可使用 `user-backfill --profile --forum --bawu` 单独补充，不能倒填为历史发表属性。完整条件、参数和数据范围见 [README](../README.md) 与[架构说明](ARCHITECTURE.md)。

导出默认仍为全库，`--run-id` 不是单 run 实体过滤器；`completed-threads` 是最新 API scope 完成证明筛选。主题 CSV 按创建时间倒序，三类内容均带规范主题链接和 `media_urls` JSON URL 数组；`media_assets.csv` 仍独立输出。主内容 CSV 不含楼层／评论深链。每批时间戳与 UUID 唯一；用 Excel 导入时把 ID 列设为文本，保留原 CSV。

### 帮助检查

这些帮助命令只打印用法，不启动采集、登录或导出：

```sh
uv run python main.py --help
uv run python -m src.research --help
uv run python -m src.research login --help
uv run python -m src.research export --help
```

命令行选项也可参考 [VS Code Python 调试文档](https://code.visualstudio.com/docs/python/debugging)；该文档说明如何选择项目解释器并配置 `launch.json`。

本指南编写时已用项目 `.venv` 验证 `main.py --help`、研究 CLI 的总帮助、`login --help` 和 `export --help`；也验证了启动示例是有效 JSON，并确认常规采集模式中缺少 `--resume` 时 `--run-id` 会在启动应用前被拒绝；`drain-pending --run-id` 则是合法用法。V5 交付批次有 89 个主题、2,173 条一级回复、77 条楼中楼、2,097 条吧内快照和 542 条媒体记录；成长数字或图标覆盖为 89/89、2,173/2,173、72/77，IP 地点非空为 80、1,441、76。字段尝试与未解决边界见[字段审计](FIELD_SOURCE_AUDIT.md)。V1/V2/V3 的导出数字是历史快照。VS Code/PyCharm 图形界面停止及强制结束行为尚无端到端验收。
