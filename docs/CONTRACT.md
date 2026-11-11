# Reconcile v2：确定性协议

## 1. 业务与公开接口

Python 3.11+，仅标准库。时间为 UTC 整数秒，金额为有符号整数分，禁止浮点金额。`from reconcile.store import Ledger, Conflict, Fenced, Invalid` 必须有效，后三个异常均继承 ValueError。

`Ledger(path)` 打开/创建 SQLite 数据库并自动完成必要迁移。不得靠全局内存保证重开后的正确性。

- `claim(partition, owner) -> int`：事务化接管一个采集分区，返回递增的 epoch。首次为1，每次调用（即使 owner 相同）都加1。已有 checkpoint 不变。
- `ingest(partition, owner, epoch, rows, fault=None) -> dict`：原子摄入一个批次，返回 `checkpoint`, `generation`, `applied`, `recomputed` 四个整数。`applied` 为新消费的传输位置数，`recomputed` 为实际重新计算的不同订单键数。
- `view() -> dict`：返回 `groups`, `pending`, `checkpoints`, `generation`，格式见下文。
- `export(path, fault=None) -> int`：恢复并追加 NDJSON 变更流，返回本次新追加的完整帧数。相同数据库可在不同目标路径独立导出。
- `close()`：释放连接。

各方法可被不同进程、不同 Ledger 实例并发调用。SQLite 等锁可等待（建议不少于10秒），不得把正常的并发竞争变成不完整提交。

`fault` 为测试用 callable，若非 None，在规定位置以 `fault(stage)` 调用。它可能抛异常，也可能直接终止进程：
`after_history`（新历史行写入后）、`after_projection`（投影与变更帧写入后）、`before_commit`（摄入事务提交前）、`after_append`（导出新帧已 flush+fsync、导出事务提交前）。正常无新传输位置的重放无需触发摄入故障点。故障后不得依赖调用 close 才恢复。

## 2. 输入、幂等、排序与冲突

partition、owner、event_id、key、customer、segment 均为非空字符串。所有整数均拒绝 bool。epoch 为正整数。每个 row 必须且只能包含：

```json
{"seq":1,"event_id":"source-event-1","entity":"order","key":"o1","rev":1,"value":{"customer":"c1","at":0,"gross_cents":100}}
```

- seq、rev 为正整数；entity 仅 `order` 或 `plan`。允许 value=null 表示墓碑。
- order 的非空 value 必须且只能含 `customer`, `at`, `gross_cents`。
- plan 的非空 value 必须且只能含 `customer`, `start`, `end`, `segment`, `discount_bps`, `priority`。start 为整数，end 为大于 start 的整数或 null（正无穷）；discount_bps 为0至10000的整数，priority 为有符号整数。
- rows 是列表，列表内 seq 必须连续递增，禁止自动排序。空列表合法，但仍检查租约。
- 每个分区 checkpoint 初始为0。允许批次从已提交位置开始重放，但每个旧位置的全部内容必须与原记录相同；新位置从 checkpoint+1 连续增长。空洞、无效结构为 Invalid；改写已提交位置为 Conflict。
- 事件身份是全库的 event_id：相同 event_id 的 entity/key/rev/value 必须相同，换分区或换 seq 可以重投递并消费新位置。冲突必须拒绝整个批次。
- `(entity,key,rev)` 的 value 也具有全库不可变性：同版本不同内容即 Conflict，包括旧版本，不允许以“已经过时”为由忽略冲突。
- JSON 对象字段顺序不构成区别；数组顺序构成区别（输入协议中的业务值不使用数组）。所有输入验证与冲突检测均不得留下部分数据。
- 每个实体键的最大 rev 是当前获胜版本，与到达时间、分区、event_id、业务时间无关。最大版本墓碑删除实体；更高的非空版本可恢复它。
- 同一批次最终以所有记录摄入后的获胜版本投影，不暴露中间修订。旧版本和语义重复版本不应触发无关投影。
- 租约检查和写入必须在同一事务中。owner 或 epoch 与当前 claim 不符即 Fenced，包括旧位置重放和空列表。参数格式错误用 Invalid。异常优先级无需固定，但不能改变数据库和导出文件。

## 3. 双时态投影与增量边界

订单使用当前最大修订版本；方案也使用每个方案键的当前最大修订版本。按订单 `customer` 与 `at` 在方案的 `[start,end)` 区间内匹配。在所有匹配方案中，依次选 priority 最大、start 最大、key 按 Python Unicode 字符串字典序最小者。有效区间属于业务时间；rev 属于修订次序，不可混为一谈。

无匹配方案的未删除订单进入 pending（按订单 key 排序），不进入 groups。匹配订单归入 `(day=at//86400, segment)`，包括负时间的向下取整日期。每笔订单先算 `gross_cents*(10000-discount_bps)/10000`，以“最接近整数、正好一半取偶数”舍入，再汇总为 net_cents，负数必须同样正确。

groups 按 (day,segment) 排序，每项精确包含 `day`, `segment`, `count`, `gross_cents`, `net_cents`；count 计订单数，组无订单时消失，即使金额为0但有订单时仍保留。checkpoints 是分区到 checkpoint 的对象（包括新 claim 的0）。generation 初始0，只有本次事务最终 groups 或 pending 与事务前不同才加1；只有 checkpoint 或订单身份变化而最终报表不变时不加1。

必须增量维护持久化订单投影，不能在每次 ingest/view 中回放完整历史或全量重算当前订单。一个批次允许重算的订单集合仅包括：
1. 获胜 order 版本实际改变的键；
2. 获胜 plan 版本实际改变时，其批次前和批次后有效区间覆盖的、同客户的批次前或批次后订单键。
本批次中间版本的区间不需要处理。允许以这个候选集合为上界多算，但不能触碰无关客户/时间的订单。`recomputed` 应真实反映实际重算的不同订单键数，迁移时不受该限制。须建立支持按客户/时间定位订单和匹配方案的索引。view 可以遍历聚合组和 pending，但不得重新执行订单归属计算。

view 示例（对象键顺序不限）：
```json
{"groups":[{"day":0,"segment":"basic","count":1,"gross_cents":100,"net_cents":90}],"pending":[],"checkpoints":{"p":2},"generation":1}
```

## 4. 持久化变更流和导出恢复

每个 generation 恰好持久化一个不可变帧，与历史、checkpoint、投影在同一 SQLite 事务提交。帧包含 `generation`, `previous`, `changes`, `pending_add`, `pending_remove`, `digest` 六个字段：
- changes 是相对前一报表的非零组差值，按 (day,segment) 排序；每项含 day/segment/count/gross_cents/net_cents，后三项是有符号增量。三项增量均为0的组不出现。
- pending_add/pending_remove 是排序后的订单键集合差。
- previous 是前帧 digest，首帧为64个字符 `0`。
- digest = SHA256(canonical(body).encode('utf-8')).hexdigest()，body 恰好是前五个字段。
- canonical = `json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)`。
- 导出行是 canonical(完整帧)+`\n` 的 UTF-8字节，禁止 BOM。空数据库导出为空文件。

export 可以在摄入和其他导出进程并发时调用，必须相对于某个已提交的 generation 前缀表现为一个串行操作，不得看到未提交帧。对于同一数据库和同一文件的并发导出，不得重复、交叉写入。不同数据库禁止共同使用一个目标文件，调用者负责此约束。

导出时验证目标文件的所有完整行：必须逐字节等于数据库已提交帧的从1开始的连续前缀。缺帧、乱序、伪造、内容被改、完整但非规范JSON的行均抛 Conflict，且不修改文件。只允许修复末尾“不以换行结束的残片”：先验证其前面的完整行，再截去残片；即使残片恰好是完整JSON但缺换行也视为残片。随后追加尚未导出的帧并 flush+fsync。

after_append 故障之后已落盘的有效行必须被下一次 export 识别为已有前缀，不能重复。数据库不得因为崩溃而丢失可重导出的帧。export 不产生新的业务 generation。不要使用只在进程内有效的锁。

## 5. v1 原地迁移

`PRAGMA user_version=1` 的数据库仅含以下业务表（旧索引可存在）：

```sql
CREATE TABLE partitions(partition TEXT PRIMARY KEY, owner TEXT NOT NULL, epoch INTEGER NOT NULL, checkpoint INTEGER NOT NULL);
CREATE TABLE history(partition TEXT NOT NULL, seq INTEGER NOT NULL, event_id TEXT NOT NULL,
 entity TEXT NOT NULL, entity_key TEXT NOT NULL, rev INTEGER NOT NULL, value_json TEXT NOT NULL,
 PRIMARY KEY(partition,seq));
```

history 保存完整已消费记录；value_json 是 value 的 JSON（墓碑为 `null`）。迁移必须按上述事件身份/版本规则重建最终投影，保留分区 owner/epoch/checkpoint 及完整历史。要求历史位置从1到 checkpoint 连续且无额外位置，所有内容合法；跨分区读取顺序不能影响结果。若重建的 groups 或 pending 非空，从空视图生成 generation=1 的初始帧，否则为0。只有全部成功才设置 user_version=2，失败须保持原v1数据库的表和数据，无部分迁移。再次打开v2不得重建/增加generation。v0空文件创建v2；遇到其他非零版本抛 Invalid，不擅自覆盖。

## 6. 工程交付

保留 `python -m reconcile --db PATH claim PARTITION OWNER`、`ingest PARTITION OWNER EPOCH JSON_FILE`、`view`、`export OUTPUT`。成功输出一行JSON；业务异常以JSON错误对象输出并返回非零退出码，不打印 traceback。现有测试必须继续通过，新增测试覆盖真实的跨步骤问题。可改内部表结构但不能破坏v1兼容和公开协议。仅使用SQLite和标准库，不引入网络、Docker或服务依赖。
