# Tieba Research Crawler

Tieba Research Crawler 是基于 [TiebaMeow/TiebaScraper](https://github.com/TiebaMeow/TiebaScraper) 开发的贴吧研究采集工具，使用 Python、`aiotieba` 和 PostgreSQL。项目保留上游署名，采用 MIT 许可证，见 [LICENSE](LICENSE)。

程序优先通过贴吧 API 获取主题、回复、楼中楼和作者信息；网页采集只用于补充已保存记录中可定向核实的字段。数据和导出文件保存在本机，不做匿名化。请勿将数据库、CSV、日志、Cookie、BDUSS 或本机配置文件提交到公开仓库。

## 功能与数据范围

- `backfill` 默认使用统一的“发布”排序列表；可显式选择实验性的 API REPLY 历史发现列表。两种列表分别保存运行与分页检查点，不会互相续接。
- `periodic` 按回复时间监控首页，使用 REPLY 排序；`hybrid` 同时运行周期监控与一次历史回溯。
- 详情任务分页读取首楼、一级回复和楼中楼，并将每轮采集（run）、列表页和详情任务的分页检查点保存在 PostgreSQL。设定 `max_backfill_pages = 0` 表示不设页数上限；实际能读取的范围仍取决于服务端响应和任务状态，不代表某段历史必定完整。
- 网页补采只处理 API 已保存、目标身份可核实的记录。无法访问或缺少字段证据时会保留缺失状态，不根据昵称、页面顺序或相似文本猜测身份。
- 用户吧内等级、头衔和角色快照来自查询时的资料，不代表用户发帖时的历史状态。
- 列表中出现主题，不代表正文和媒体内容已经采集完成；详情任务状态会单独记录。

数据模型、字段来源和缺失边界见[架构说明](docs/ARCHITECTURE.md)与[字段来源审计](docs/FIELD_SOURCE_AUDIT.md)。

### 回溯列表规则

默认 `backfill` 使用 CREATE/rn=30；显式 `--backfill-sort reply` 使用 API REPLY/rn=100，且要求 `[research] enabled = true`。REPLY 列表只发现主题并交给现有详情队列；它不代表详情、楼中楼或网页补采已经完成。该选项为实验性入口，不能据服务端报告的页数或条数声称已发现全站全部历史主题。

两种回溯列表都按 TID 去重；仅当同一 run、同一列表流连续 3 个新页面中，每页至少 90% 的唯一非置顶 TID 都已在该流先前页面出现时，才暂停该流并保留第三页证据。REPLY 恢复重放允许同页集合变化，按 TID 合并新旧观察，重放不改变重复计数；单个终止页若自身已有至少 90% 重复，或非空终止页仅含置顶主题，也不作为可信终点。服务端页数和数量元数据不用于 REPLY 的页号边界。置顶 TID 不计入重复率。该规则只用于 `backfill`；`periodic` 的 REPLY 首页监控不使用此判定。SDK CREATE 的自动化回归与第 71–75 页列表写入验收见[稳定性验收摘要](docs/PREFLIGHT_STABILITY_SUMMARY.md)，它不证明详情、网页补采或任何年份的全量覆盖。

截至 2026-11-16，API REPLY/rn=100 连续列表探针覆盖自然页 1–1004，共观察到 99,290 条列表项；按 TID 去重为 92,263 条，重复 7,027 条（7.077%，不计 5 条恢复重放记录）。唯一索引记录的创建时间覆盖率为 100%，范围为 2010-04-11 至 2026-11-16。索引导出的 92,263 行均与相同 TID 的完整日志观察逐行一致，链接格式核验通过。p1002 开始出现高重叠，p1003–p1004 重复同一页集合；p1004 保存后触发保护并以 incomplete 暂停，未请求 p1005。服务端报告的 3,309 页不作为可达范围或完成证明。探针的 `INDEX_ONLY` 仅为主题索引，不代表正文、一级回复或楼中楼已采集；本次连续实测和边界说明见[历史主题发现可行性报告](docs/PC_HISTORICAL_DISCOVERY_FEASIBILITY.md)。

## 环境与安装

需要 Python 3.12 或更新版本、[uv](https://docs.astral.sh/uv/) 和可连接的 PostgreSQL。默认使用本机 PostgreSQL、内存缓存和本地任务队列，不需要 Docker、Redis 或 TimescaleDB。

从 PostgreSQL [官方下载页](https://www.postgresql.org/download/)安装并启动服务后，创建数据库：

```sh
createdb -W -h localhost -p 5432 -U YOUR_POSTGRES_USER tieba_data
```

在终端输入数据库密码。请将示例用户名换成本机已有的数据库角色。

克隆项目并安装依赖：

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

- 在 `[tieba]` 的 `forums` 中填写目标贴吧名，不加末尾的“吧”。
- 在 `[database]` 中设置 PostgreSQL 的主机、端口、用户名、密码和数据库名。
- 初次运行可保留 `[research]` 中的默认设置。用户吧内资料查询默认关闭。

程序首次启动时会创建缺少的表。它不会为现有表自动执行通用的增列迁移；升级已有数据库前请先备份。

网页补采和浏览器扫码需要安装 Chromium：

```sh
uv run playwright install chromium
```

## 采集与恢复

在项目根目录开始一次历史回溯：

```sh
uv run python main.py --mode backfill
```

每次不带 `--resume` 的回溯都会创建新的采集轮次（run）。周期首页监控和混合模式分别为：

```sh
uv run python main.py --mode periodic
uv run python main.py --mode hybrid
```

未指定排序时，`backfill` 使用 CREATE/rn=30，行为保持原样。可选择隔离的 API REPLY 主题发现列表：

```sh
uv run python main.py --mode backfill --backfill-sort reply
```

该模式固定使用普通论坛列表、rn=100 和已验证的 API raw/DTO 身份检查；它要求 `[research] enabled = true`。其运行标记为独立的 `reply_backfill`，因此恢复时必须保留排序选项：

```sh
uv run python main.py --mode backfill --backfill-sort reply --resume
uv run python main.py --mode backfill --backfill-sort reply --resume --run-id YOUR_REPLY_RUN_ID
```

`max_backfill_pages` 同样限制每轮列表页数，默认 `0` 表示不设页数上限。REPLY 模式不以服务端 `total_page` 或 `total_count` 判断可达页数或全量结束；连续三页各自至少 90% 的非置顶 TID 已见时，会在保存第三页后以 incomplete 暂停。单页终止信号若与高重复相符，或该非空页没有任何非置顶主题，也会按不可信终点暂停。以上停止状态用于保留证据和防止循环，不代表全站覆盖已完成。详情、字段补采、CSV 映射及周期 REPLY 首页仍走原有逻辑。

在终端按 Ctrl+C 请求程序协作停止。IDE 图形界面的 Stop 和强制结束行为尚未现场验证，不能据此保证程序完成收尾或排空待办。中断后在相同论坛范围和模式下恢复原采集轮次：

```sh
uv run python main.py --mode backfill --resume
uv run python main.py --mode backfill --resume --run-id YOUR_RUN_ID
```

启动日志会显示 `run_id`。指定 `--run-id` 时必须同时使用 `--resume`。强制结束进程不会继续处理待办，未完成的分页任务需要恢复；不要删除检查点或另开一轮采集。

使用相同的 `--resume --run-id` 恢复时，程序继续该 run 的 API 工作和持久化网页补采任务。网页队列优先尝试尚未执行的目标，再取到期重试；默认每个目标最多尝试 3 次，首次失败等待 30 秒，第二次失败等待 60 秒。已标记为 `failed_final` 的任务不会自动重置。

只排空已有 run 的详情待办，不继续发现主题：

```sh
uv run python main.py drain-pending --run-id YOUR_RUN_ID --limit 10 --max-seconds 120
```

该命令不会扫描论坛列表或创建新 run。详情任务可能继续产生子任务；超时或中断后可针对同一 run 再次处理。更多本机运行方式见[IDE 指南](docs/IDE_SETUP.md)。

列表恢复须使用原来的 run 和来源契约：REPLY 回溯要继续带 `--backfill-sort reply`，不能用默认 CREATE 恢复；`drain-pending` 使用指定 run 已持久化的来源和范围，不切换列表类型。

### 请求频率

默认 `[rate_limit] global_rps = 0.2`，约每 5 秒允许一个受控请求动作。API 请求与浏览器受控导航、展开动作共用这项预算；网页自动加载的图片等子资源不会逐个计入。修改 `config.toml` 中的限速值后，需正常停止并重启程序才会生效，运行中不会热更新。

## 登录与定向网页补采

基础 API 回溯默认不需要登录。需要采集用户吧内快照时，在 `config.toml` 的 `[research]` 中开启：

```toml
[research]
enabled = true
fetch_user_forum_info = true
account_file = "secrets/baidu-account.json"
```

浏览器扫码登录：

```sh
uv run python -m src.research login --browser --account-file secrets/baidu-account.json
```

也可运行 `uv run python -m src.research login`，在隐藏输入提示中保存凭据。程序不会把凭据写入日志或命令参数；凭据文件只保存在本机。

定向网页补采可先用 `--dry-run` 查看符合条件的目标。以下例子只检查一个已保存的楼中楼记录：

```sh
uv run python -m src.research field-backfill --run-id YOUR_RUN_ID --target-type comment --tid YOUR_TID --cid YOUR_CID --field ip_location_text --limit 1 --dry-run
```

正式补采需去掉 `--dry-run`，并用 `--browser-profile` 指定项目专用资料目录：

```sh
uv run python -m src.research field-backfill --run-id YOUR_RUN_ID --target-type comment --tid YOUR_TID --cid YOUR_CID --field ip_location_text --limit 1 --max-seconds 300 --browser-profile data/web-fallback-profile
```

如需在可见浏览器中手动登录或完成验证，在有标准输入的终端运行，并同时加入 `--headed --wait-for-login`：

```sh
uv run python -m src.research field-backfill --run-id YOUR_RUN_ID --target-type comment --tid YOUR_TID --cid YOUR_CID --field ip_location_text --limit 1 --max-seconds 300 --browser-profile data/web-fallback-profile --headed --wait-for-login
```

网页补采不替代 API 回溯，也不会为身份或字段缺少证据的记录补造数据。

## 导出与媒体

导出本机数据库为 CSV：

```sh
uv run python -m src.research export
```

每次导出都会在 `exports/` 下创建新批次目录，不覆盖已有批次。内容表包括主题、一级回复和楼中楼；另有用户、用户贴吧快照、媒体引用及采集质量和字段审计材料。默认实体 CSV 包含数据库中的全部 run。传入 `--run-id` 会筛选运行质量报告并标记批次，但不会把实体 CSV 限定到该 run。

```sh
uv run python -m src.research export --run-id YOUR_RUN_ID
uv run python -m src.research export --entity-scope completed-threads
uv run python -m src.research export --output exports
```

`completed-threads` 按最新详情任务的完成记录筛选主题，范围仍可能跨越多个 run，也不表示贴吧内容完整。CSV 中的 `media_urls` 和 `media_assets` 保存媒体引用与元数据，不下载图片或视频文件。导出保留原始用户 ID、昵称和正文，请在本机妥善保存。

## 命令帮助与项目文档

查看当前安装版本支持的命令和参数：

```sh
uv run python main.py --help
uv run python -m src.research --help
uv run python -m src.research field-backfill --help
uv run python -m src.research export --help
```

- [架构与数据边界](docs/ARCHITECTURE.md)
- [字段来源与验证范围](docs/FIELD_SOURCE_AUDIT.md)
- [历史主题发现可行性与阶段性结果](docs/PC_HISTORICAL_DISCOVERY_FEASIBILITY.md)
- [开发记录](docs/DEVELOPMENT_LOG.md)
- [本机 IDE 运行指南](docs/IDE_SETUP.md)
- 上游项目：[TiebaMeow/TiebaScraper](https://github.com/TiebaMeow/TiebaScraper)
