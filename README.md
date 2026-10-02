# dependency-cache-layer-python

面向上游数据源的**缓存访问层**：所有业务读取收敛到统一入口，上游以**本地可注入的模拟实现**
（`cachelayer.upstream.MockUpstream`）替代，整条链路不依赖任何外部服务，可在本地复现与验证。

## 要解决的问题

- 每次读取都直打上游：上游变慢 / 抖动时延迟被放大，重复读取增加压力；
- 缓存与上游真实数据之间缺少明确的一致性约定。

本层提供：新鲜度分级、陈旧回落 + 后台刷新、按依赖精准失效、并发一致性、
缺失 / 故障区分、有界容量淘汰、可观测、优雅停止。

## 快速开始

```bash
uv sync
uv run python demo.py     # 本地端到端复现全部典型场景
uv run pytest             # 47 个自动化测试
uv run python main.py     # 启动 HTTP 服务（默认 127.0.0.1:8000）
```

## 新鲜度语义（阈值边界确定）

设 `age = now - written_at`（单调时钟）：

| 条件 | 状态 | 行为 |
| --- | --- | --- |
| `age <= ttl` | `fresh` | 直接返回缓存 |
| `ttl < age <= ttl + stale_window` | `stale` | **先完整返回上一次结果**，并触发一次后台刷新（single-flight） |
| `age > ttl + stale_window` | `expired` | **同步等待刷新完成**；失败则保留旧值并打降级标记 |
| 负缓存 `age <= missing_ttl` | `negative_fresh` | 视为“上游明确不存在”，短 TTL 内不再回源 |
| 负缓存 `age > missing_ttl` | — | 重新回源；负缓存**没有陈旧回落** |

边界点归属“更新的那个状态”：`age == ttl` 仍算新鲜；
`age == ttl + stale_window` 仍算可容忍陈旧；严格大于才算过期。
后台刷新成功后以**新的不可变快照**整体替换，下一次读取立即看到完整新版本。

`CacheResult.status` 显式给出六种状态：`fresh / stale / expired / negative_fresh /
degraded / miss`；`from_upstream` 表示本次数据是否由回源得到；
`degrades` 是可区分的降级原因元组（刷新失败保留旧值时非空）。

## 缺失与故障的区分

- 上游**明确不存在**（`UpstreamNotFoundError`）→ 加载器返回 `Loaded(missing=True)`，
  按较短的 `missing_ttl` 做负缓存；
- 上游**故障 / 超时 / 格式错误**（`UpstreamUnavailableError` / `UpstreamFormatError`）
  **绝不缓存为缺失**：无旧值时原样抛出（HTTP 502，携带 `timeout / unavailable / format`
  原因）；有旧值且已过期时，旧值原样保留并以 `status=degraded` + `degrades` 标记返回，
  **失败永远不会被当成有效新数据**。

## 按依赖精准失效

加载器在 `Loaded.dependencies` 中声明结果依赖的上游实体：

```python
Loaded(value=feed, dependencies=["feed:42", "user:7", "user:9"])
```

实体变化时调用 `cache.invalidate_dependency("user:7")`，
只有真正依赖 `user:7` 的条目被清除，无关条目（含无依赖条目）不受影响。
条目被覆盖 / 淘汰时双向依赖索引同步更新。

## 并发一致性

- 缓存条目是**不可变快照**（`CacheEntry`），读侧锁内只取引用；写侧锁内整体替换
  （值 + LRU 序 + 依赖索引）。任何时刻读到的都是**某次完整写入的版本**，
  不存在新旧混合 / 半写结果。
- 并发**缺失合并**（single-flight）：同一 key 的并发冷读只回源一次，等待者共享结果 / 错误。
- 并发**陈旧读取**只触发一次后台刷新；并发**过期读取**共享同一次同步刷新。

## 有界容量与淘汰

- `capacity` 硬上界，且不得超过 `max_capacity`（默认 10000），构造期校验；
- 容量满时：优先回收**已过期 / 负缓存**条目，否则按 **LRU**（最近最少访问）淘汰；
- 长期运行内存有界；被淘汰条目再次读取正常回源。

## 可观测与优雅停止

`cache.metrics()` 提供：`hits / misses / negative_hits / stale_served /
background_refreshes / synchronous_refreshes / refresh_failures / degrades /
invalidations / evictions / in_flight_refreshes`。

- 后台刷新池大小 `refresh_concurrency` 可配；超出部分排队，可 `cancel_pending()`；
- `cache.shutdown()`：拒绝新任务、取消排队、在途任务自身原子写完或失败回落，
  最多等待 `shutdown_timeout`；worker 为 daemon 线程，**超时也不阻塞进程退出**，
  且缓存中不会留下半个条目。FastAPI lifespan 关闭时自动调用。

## 配置项（`cachelayer.models.CacheConfig`，时间单位均为秒）

| 配置 | 默认 | 含义 |
| --- | --- | --- |
| `ttl` | 30 | 新鲜存活时间 |
| `stale_window` | 30 | 新鲜后可容忍陈旧的窗口（0 = 严格过期，立即同步刷新） |
| `missing_ttl` | 5 | 明确缺失的负缓存时间，必须为正 |
| `capacity` | 1024 | 条目数上界 |
| `max_capacity` | 10000 | capacity 允许配置的硬上限 |
| `refresh_concurrency` | 4 | 后台刷新并发上限 |
| `refresh_timeout` | 10 | 过期读取等待单次刷新的上限 |
| `shutdown_timeout` | 5 | 关闭时等待在途刷新原子收尾的上限（超时不阻塞退出） |

## 代码结构

```
cachelayer/
  clock.py    可注入时间源（SystemClock / FakeClock）
  errors.py   缺失 / 故障 / 超时 / 格式错误（可区分）
  models.py   状态枚举、Loaded、CacheResult、CacheEntry、CacheConfig
  metrics.py  线程安全计数
  deps.py     依赖双向索引（精准失效）
  refresh.py  有界 / 可去重 / 可取消 / 可优雅停止的执行器
  cache.py    缓存层主体
  upstream.py 本地模拟上游（故障注入、门闩、调用计数）
services.py   UserService / FeedService（读取路径全部收敛于此）
api.py        FastAPI：状态与降级标记透传、管理 / 指标接口
demo.py       端到端复现脚本
tests/        阈值边界 / 后台刷新失败 / 精准失效 / 并发 /
              缺失与故障 / 容量淘汰 / 可观测停止 / HTTP
```

## HTTP 示例

```bash
curl localhost:8000/users/u1
# {"key":"user:u1","value":{...},"status":"fresh","age":0.0,
#  "ttl":30.0,"stale_window":30.0,"degrades":[],"from_upstream":true}

curl -X POST localhost:8000/admin/notify/user/u1   # 上游实体变化 -> 精准失效
curl localhost:8000/metrics
```
