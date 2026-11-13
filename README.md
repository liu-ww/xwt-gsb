# Session Mill

离线事件流会话窗口物化器，完整协议见 `docs/SPEC.md`。实现只使用 Python 3.11+ 标准库与 SQLite。

## 特性

- 严格校验六个事件字段、NFC 字符串、控制字符、整数与 `open/ping/close` 类型。
- `event_id` 全局唯一；`arrival_seq` 全局唯一并永久绑定事件内容。
- 批内和库内精确重复均幂等；冲突内容拒绝整批。
- 按 `(event_time, arrival_seq)` 排序物化，30 秒间隔合并，迟到桥接事件自动合并旧会话。
- `close` 立即结束会话，后续事件开启新会话；迟到 close 也可拆分既有会话。
- 水位线只前进，事件必须落在 `[watermark-60, watermark]` 内。
- 确定性 changelog：先按 ID 输出 `retract`，再按 ID 输出 `upsert`，offset 单调递增，revision 按会话历史严格递增。
- 事件、会话、水位线与 changelog 在同一个 `BEGIN IMMEDIATE` 事务中提交；三个故障阶段均完整回滚。
- SQLite `user_version` 为 2；更高版本直接拒绝。版本 0/1 数据库会原地升级并重建缺失会话。
- 同一引擎线程安全，多连接写入由 SQLite 写锁串行化。

## CLI

数据库可用 `--db PATH`、`SESSIONMILL_DB` 环境变量指定；也支持把数据库路径放在命令前或最后一个参数。

```bash
python -m sessionmill --db data.sqlite ingest events.json watermarks.json
python -m sessionmill --db data.sqlite sessions
python -m sessionmill --db data.sqlite changes 0
```

`EVENTS` 为 JSON 数组（也接受 JSONL），`WATERMARKS` 为 JSON 对象；参数可用 `-` 从标准输入读取，也可直接传入内联 JSON。成功时输出单行、键排序的 JSON；业务错误退出码为 2，且不输出 traceback。

## Python API

```python
from sessionmill import SessionEngine

engine = SessionEngine("data.sqlite")
engine.ingest(events, {"tenant-a": 120})
engine.sessions()
engine.changes(since=0)
engine.close()
```

`ingest` 返回 `{"accepted": 新事件数, "sessions": 会话总数}`。`sessions()` 的每项恰含
`session_id,tenant,user,start,end,event_count,closed,revision`；`changes()` 的每项包含
`offset,action,retract/upsert,以及同样的会话字段`。

### 故障注入

```python
engine.ingest(events, watermarks, fault="after_events")
engine.ingest(events, watermarks, fault="after_sessions")
engine.ingest(events, watermarks, fault="before_commit")
```

任一阶段抛错都会回滚本批全部写入。

## 测试

```bash
python -m unittest discover -s tests -v
```

测试覆盖乱序、桥接、拆分、关闭、边界水位线、永久去重、冲突整批拒绝、
revision/offset、故障回滚、并发重投递、schema 版本、v0 升级与 CLI 行为。
详细验证说明见 `docs/VERIFICATION.md`。
