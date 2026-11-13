# Time Weave

双时态维表与事实点时关联引擎。`TemporalStore` 仅依赖 Python 标准库与 SQLite，
按 `docs/SPEC.md` 实现业务时间与系统时间双时态语义。

## 特性

- 严格 RFC3339 秒级带时区时间解析，统一规范化为 UTC 后比较。
- 业务时间与系统时间均为左闭右开区间，`null` 上界表示正无穷。
- 更正以更高 `revision` 关闭旧系统区间并写入新版本，禁止篡改历史、
  系统区间重叠、空区间及同 `revision` 冲突。
- 同一 `entity` 的业务区间仅在共享的系统时间切片上禁止重叠。
- 事实 `fact_id` 永久绑定内容：相同重投递幂等，不同内容报冲突。
- `join(as_of)` 按半开区间选择唯一可见维度；无匹配返回 `dimension=null`
  与 `unmatched=true`；`as_of` 早于 `observed_at` 的事实不可见。
- `apply` 整批原子，支持 `after_dimensions`、`after_facts`、`before_commit`
  故障注入并回滚；并发写入借助 `BEGIN IMMEDIATE` 保证仅一个成功。
- 数据库 schema 版本固定为 2，未来版本或损坏结构直接拒绝打开。

## 库用法

```python
from timeweave import TemporalStore

store = TemporalStore("data.sqlite")
store.apply(dimensions=[...], facts=[...])
rows = store.join("2024-06-01T00:00:00Z")
store.close()
```

## CLI

```bash
python -m timeweave apply data.sqlite --input batch.json [--fault before_commit]
python -m timeweave join  data.sqlite --as-of 2024-06-01T00:00:00Z
```

`batch.json` 形如 `{"dimensions": [...], "facts": [...]}`。业务错误以退出码 2
结束且只输出错误信息，不打印 traceback。

## 测试

```bash
python -m unittest discover -s tests -v
```
