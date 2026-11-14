# 本机 IDE 运行指南

本文说明如何在本机 VS Code 或 PyCharm 中运行回溯采集、恢复任务和导出 CSV。程序直接连接 `config.toml` 里配置的 PostgreSQL；无需 Docker、Redis 或远程服务。本机 Python 主入口已做有限真实回溯与 SIGINT/恢复验证，但没有实际操作过 VS Code 或 PyCharm 的图形界面。

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

   首次用浏览器二维码登录时，在项目根目录执行：

   ```sh
   uv sync --group login
   uv run playwright install chromium
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

优先在终端按 **Ctrl+C**，或使用 IDE 的正常停止操作，让 Python 执行收尾。每个完成的页面会保存数据和 checkpoint；正在请求或尚未完成写入的页面不会被当成已完成。下次用 `--resume` 重放尚未完成的工作，页面写入可以安全重试。

本机主入口的一次真实演练中，普通 CREATE 流第 1 页保存 checkpoint 后收到 SIGINT；同一 run 恢复时重放该页并继续到第 2 页，再次停止。数据库中的 run 状态为 `interrupted`。该结果只验证列表 checkpoint 的恢复路径，不代表详情 scope 已清空或整轮采集已完成；进程 exit code 不能代替数据库 run 状态。

如果 IDE 的强制停止、任务管理器结束进程或断电直接杀掉进程，清理代码可能不会运行。不要因此删除 checkpoint 或另起一个新 run；重新启动时使用相同论坛配置并加 `--resume`。若本轮已完成，程序不会把已完成 run 当作未完成 run 恢复。

## 导出 CSV

在项目根目录执行：

```sh
uv run python -m src.research export
```

默认在 `exports/` 下新建独立批次目录，导出完整本地数据库中的全部 run，包含七个 UTF-8 with BOM CSV：`threads`、`posts`、`comments`、`users`、`user_forum_snapshots`、`media_assets`、`crawl_quality_report`，以及批次 manifest。每次导出都会创建唯一目录，不覆盖已有批次。

可用 `--output` 指定另一个本机导出根目录：

```sh
uv run python -m src.research export --output exports
```

如果追加 `--run-id`，它只筛选质量报告中的 checkpoint，并作为批次名称的上下文；其他六个内容 CSV 仍包含数据库所有 run 的记录：

```sh
uv run python -m src.research export --run-id <run_id>
```

命令会打印实际生成的批次目录。质量报告汇总采集 checkpoint，不是“所有平台记录均已抓取”的证明。CSV 和账号文件均只保存在本机。

已有三次真实 CLI 导出成功：每次都创建不同时间戳加 UUID 的新批次目录，旧批次未覆盖。最终批次的六类实体 CSV 覆盖全数据库，行数与 SQL 查询和 manifest 一致；manifest `entity_scope=all_database_rows`、`validation_status=passed`，全 runs 质量报告有 98 行。前两次命令传入 comments run 的 `--run-id`，所以质量 CSV 各有 2 行。备份恢复尚未验证。

## 验证命令行参数

这些帮助命令只打印用法，不启动采集、登录或导出：

```sh
uv run python main.py --help
uv run python -m src.research --help
uv run python -m src.research login --help
uv run python -m src.research export --help
```

命令行选项也可参考 [VS Code Python 调试文档](https://code.visualstudio.com/docs/python/debugging)；该文档说明如何选择项目解释器并配置 `launch.json`。

本指南编写时已用项目 `.venv` 验证 `main.py --help`、研究 CLI 的总帮助、`login --help` 和 `export --help`；也验证了启动示例是有效 JSON，并确认缺少 `--resume` 时 `--run-id` 会在启动应用前被拒绝。本机 Python 主入口已连接真实 PostgreSQL 并做有限真实采集和列表恢复演练；独立的小范围详情检查也验证了一页帖子详情与两页楼中楼数据落库。VS Code/PyCharm GUI 尚未操作。三次真实导出已校验七类 CSV/manifest；备份恢复仍待验证。
