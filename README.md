# 事故诊断包断点续传服务

值班工程师接收事故诊断包的上传网页：网络断开可续传，交接时只能下载校验通过的完整文件。

## 功能

- 浏览器创建任务时提交 **文件大小 + SHA-256**，按 **1 MiB** 分块上传
- 重开页面可查看各任务 **已收/缺失分块** 并补齐（断点续传）
- 相同分块同内容重传 **幂等去重**；异内容 **拒绝并保留原块**（409）
- 提交完成时服务端核对 **分块齐全、总大小、整体 SHA-256**，通过才发布下载
- 缺块可补传；摘要不符 **拒绝发布** 并提示重新创建任务
- 多进程并发上传/提交、磁盘写失败、进程重启均不会发布残缺文件或产生两份成品
- 已发布的包重启后仍可下载
- **容量预留与录取控制**：数据卷有应用层上限，创建任务按 `2 × 文件大小` 预留峰值空间，容量不足返回 **507** 拒绝且不留下任务；网页实时显示剩余可接纳容量，发布后按保留文件结算

## 快速开始

### Docker Compose

```bash
# 可选：覆盖默认容量上限（支持字节数或 k/m/g/t 后缀，0/unlimited 为不限制）
STORAGE_LIMIT=1GiB docker compose up --build
# 打开 http://localhost:8000
```

容器内以 `--workers 2` 运行两个服务进程，共享挂载卷中的 SQLite 与数据目录。
容量上限通过环境变量 `STORAGE_LIMIT` 配置（compose 默认 `512MiB`），两个进程共享同一套容量账本。

### 本地开发

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
STORAGE_LIMIT=512MiB DATA_DIR=data .venv/bin/uvicorn app.main:app --port 8000
# 打开 http://localhost:8000
```

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/capacity` | 容量账本：`limit_bytes` / `reserved_bytes`（总承诺占用）/ `available_bytes`（剩余可接纳）/ `disk_used_bytes` / `over_limit` / 各状态任务与孤儿文件明细 |
| POST | `/api/tasks` | 创建任务 `{filename, size, sha256}`，返回 `task_id`、分块参数与 `reserved_bytes`；峰值预留超上限返回 **507**（不写任何任务/文件） |
| GET | `/api/tasks` | 任务列表（状态、已收分块数、预留/结算字节） |
| GET | `/api/tasks/{id}` | 任务详情：`received_chunks` / `missing_chunks` / `state` |
| PUT | `/api/tasks/{id}/chunks/{i}` | 上传第 i 块（1 MiB，最后一块按实际大小）。同内容重传返回 `deduplicated:true`；异内容 409 |
| POST | `/api/tasks/{id}/finalize` | 核对分块+大小+摘要，通过则发布；缺块返回 409+`missing_chunks`；摘要不符标记 `failed` |
| GET | `/api/tasks/{id}/download` | 下载已发布的包（未发布返回 409） |

任务状态机：`uploading` → `published`（终态）/ `failed`（终态，摘要不符）。

## 容量预留设计

- **峰值预留**：创建任务即承诺 `2 × size` 字节——`size` 给分块文件，另 `size` 给 finalize 时的整包拼装临时成品（二者在磁盘上同时存在），上传期的分块临时文件也包含在该承诺内。空文件承诺 0 字节。
- **原子录取**：容量检查与任务行插入在同一个 `BEGIN IMMEDIATE` 事务中完成，SQLite 在两个 worker 进程间串行化写事务，因此并发创建不可能双双通过上限检查；拒绝时事务回滚，不留任务、不留文件，返回 507。
- **发布结算**：成品经 `os.replace` 原子改名并通过校验后，**先删除分块文件并确认消失，再**把 `2×size` 承诺替换为成品的实际占用（`reserved_bytes=0, retained_bytes=size`）。若分块删除失败，承诺保留，绝不在文件仍占盘时虚释放；下次启动对账会重试清理。
- **摘要失败结算**：`failed` 任务的分块作为证据保留，承诺替换为分块实际字节数，继续计入占用。
- **启动对账（reconcile）**：每次进程启动（两个 worker 各自执行，由 IMMEDIATE 事务串行化）按磁盘重建账本——
  - 旧版本任务（无预留列）回填 `2×size` 峰值承诺，**旧任务继续可用**；
  - 已发布但崩溃残留的分块目录被清理并按成品重新结算；
  - 任何不属于在册任务的残留文件记入 `orphan_reservations` 并继续计费，文件真正消失后才释放（**不会虚释放导致后续任务误获准**）；
  - SQLite 的 WAL/SHM、per-task flock 等服务自身文件不计入孤儿。
- **上限调低**：已存在的 `uploading` 任务保留原承诺、可继续续传与发布，已发布包仍可下载；当总承诺超过新上限时 `over_limit=true`、`available_bytes=0`，暂停接纳超额新任务，待结算后占用回落到上限以内即恢复录取。
- `GET /api/capacity` 的 `reserved_bytes` 是账本口径的总承诺占用（上传任务预留 + 终态任务保留 + 孤儿），`disk_used_bytes` 是数据卷内全部常规文件的实际字节，两者在启动对账后收敛，便于人工核对。

## 一致性设计

- **分块幂等**：`(task_id, chunk_index)` 主键。先写临时文件 → INSERT 抢主键 → 赢者原子改名落盘；撞主键则比对内容哈希，相同去重、不同 409 保留原块。
- **发布原子性**：成品先在临时目录完整拼装并 fsync，**校验通过后**才 `os.replace` 原子改名到 published 路径；任何时刻 published 路径要么不存在、要么是完整成品。
- **多进程互斥**：finalize 用 per-task `flock` 串行化；SQLite WAL + busy_timeout 处理并发写。两个进程同时提交，只会有一个完成校验-改名-置态，另一个读到已发布直接返回。容量录取则用 `BEGIN IMMEDIATE` 事务串行化，保证总承诺不超过上限。
- **崩溃恢复**：状态全在 SQLite（synchronous=FULL）与磁盘。重启后已收分块保留；DB 有记录但文件丢失的块（INSERT 后、改名前崩溃）在 finalize 时被发现并允许补传。容量账本在启动时按磁盘对账（见上）。
- **磁盘写失败**：改名失败即回滚 DB 记录并返回 500，任务保持 `uploading`，不会产生半成品；容量只在文件确认删除后才释放。

## 测试

```bash
.venv/bin/python -m pytest tests/ -v
```

- `tests/test_app.py`（14 项）：完整流程、续传、幂等去重、异内容拒绝、缺块补传、摘要拒绝、磁盘失败注入（chunk 上传 / finalize 两条路径）、重启恢复、空文件、参数校验。
- `tests/test_quota.py`（15 项）：上限解析、录取拒绝且不留痕、容量接口、发布/失败结算、写失败不虚释放、重启对账与孤儿台账、旧库迁移回填、上限调低后旧任务完成/新任务暂停、并发录取不越上限、空文件零预留、删块失败保留承诺、锁文件不计孤儿。
- `tests/test_live.py`（5 项）：两个 uvicorn 进程并发上传/并发提交同一任务、kill -9 后重启续传、已发布包重启后可下载、跨进程同块并发去重，以及跨进程容量录取竞争（总承诺不越上限）、崩溃残留计费与降 cap 后的完整生命周期。

## 数据布局（`DATA_DIR`）

```
uploader.db        SQLite（WAL）：任务/分块 + 容量账本（reserved/retained + 孤儿台账）
chunks/<task>/     已收分块
published/<task>   校验通过后的成品（原子改名出现）
tmp/               临时文件（启动时清理超过 1 小时的残留；其余残留计入孤儿容量）
locks/             per-task flock 文件
```
