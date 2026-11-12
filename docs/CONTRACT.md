# CDC Forge v2数据工程协议

## 1.输入批次

每个批次目录恰有`manifest.json`、`schema.json`和`events.jsonl`。所有文本必须是严格UTF-8、NFC且不得含控制字符或孤立代理项；JSON对象拒绝重复键、非有限数字、布尔值冒充整数以及额外/缺失字段。对象使用键排序、无多余空白、UTF-8直写的规范JSON。`manifest.json`和`schema.json`不得带换行；`events.jsonl`每行是一个规范JSON对象且文件必须以单个换行结尾。

Manifest字段恰为`format,batch_id,source,previous_batch,schema_version,schema_sha256,events_sha256,event_count,watermark`。`format`为1；标识符为非空NFC字符串；`previous_batch`为字符串或null；版本、数量和watermark为整数。两个SHA-256字段是对应文件原始字节的小写十六进制哈希，`event_count`等于非空事件行数。

Schema字段恰为`version,fields`。字段项恰为`name,type,nullable,default`；名称匹配`[a-z][a-z0-9_]{0,31}`且唯一，类型仅为`string,int,float,bool`。default必须为null或匹配类型，float必须有限。首个版本必须为1；升级只能逐版本追加字段，已有字段顺序、类型、可空性和默认值不可变化。新增字段必须可空或提供非null默认值。相同版本的schema字节必须完全一致。

事件字段恰为`op,pk,event_time,source_seq,schema_version,data`。`op`为`upsert/delete`；`pk`为非空NFC字符串；时间和序号为整数且`source_seq>0`。upsert的data字段必须恰好等于该版本全部schema字段并逐项通过类型检查；delete的data必须为null。

## 2.批次链、水位线与去重

每个source维护独立head与watermark。首批`previous_batch=null`，后续必须精确指向当前head；watermark不得回退。事件必须满足`watermark-86400<=event_time<=watermark`，边界均包含。

批次身份是全局`batch_id`。重复提交相同目录内容是幂等成功，返回原generation且`applied=false`；相同batch_id对应不同内容必须拒绝。每个`(source,source_seq)`永久绑定事件规范字节：跨批次完全相同的重投递是no-op，不同内容是冲突并使整批失败。

## 3.物化规则与模式演进

每个pk保留所有source中的确定性赢家，排序键为`(event_time,source,source_seq)`，较大者获胜；因此输入行顺序不影响结果，旧事件不能复活较新的墓碑。delete保留内部墓碑但不输出当前行。模式升级后，旧行按新增字段default回填；default为null时写null。

`Warehouse.query()`返回当前非删除行，按`_pk`升序。每行字段顺序不作为Python对象语义，但内容必须恰为`_pk,_event_time,_source,_source_seq`与当前schema字段。

## 4.快照与原子性

成功的非幂等批次使全局generation加1。`current/manifest.json`使用规范JSON，字段恰为`generation,schema_version,schema_sha256,row_count,partitions`。数据按赢家event_time的UTC日期写入`current/date=YYYY-MM-DD/part-NNNNN.jsonl`；每个part最多2行，行按`_pk`升序，文件为规范JSONL并以单个换行结尾。partitions按path排序，每项恰为`path,rows,sha256`。当前目录不得残留旧分区或临时文件。

`apply_batch(path,fault=None)`以及`apply_batches(paths,fault=None)`必须保证状态库、批次head、generation与current快照的全有或全无。批量路径按给定顺序组成一个事务，返回每批结果；任一失败不得留下前序批次。fault在每次状态写入后的`after_state`、暂存快照后的`after_stage`、发布前的`before_publish`被调用；异常后关闭重开仍不得出现半提交。多进程/线程并发提交同一批只能有一次`applied=true`。

启动时校验SQLite`PRAGMA user_version=2`及表结构；空目录创建v2，未来版本或结构损坏必须拒绝。若数据库有效而`current`缺失或manifest/part哈希损坏，启动时从数据库确定性重建current，不改变generation。

## 5.CLI与边界

```text
python -m cdcforge --warehouse DIR apply BATCH
python -m cdcforge --warehouse DIR apply-many BATCH...
python -m cdcforge --warehouse DIR query
```

成功向stdout输出单行、键排序JSON；输入/业务错误输出`{"error":"类型","message":"..."}`并退出2且无traceback，内部错误退出1。不得联网、使用第三方依赖、读取任务仓库之外的任意文件或在输出中泄露完整事件数据。

