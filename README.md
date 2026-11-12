# CDC Forge

`cdcforge`是一个仅使用Python标准库与SQLite的离线CDC物化器，实现`docs/CONTRACT.md`定义的v2协议：严格批次校验、schema演进、跨源确定性冲突处理、迟到数据边界、批次与事件幂等、墓碑、分区快照、批量原子性、故障回滚、并发安全与快照自愈。

SQLite状态库（`PRAGMA user_version=2`）是持久化事实源：每个source独立维护head/watermark，schema历史按版本追加，`(source,source_seq)`永久绑定事件规范字节。`current/`是数据库状态的确定性投影，提交时先写入暂存目录再原子切换；启动时校验表结构并从数据库重建缺失或损坏的快照（不改变generation）。

```powershell
python -m unittest discover -s tests -v
python -m cdcforge --warehouse .\warehouse apply .\batch
python -m cdcforge --warehouse .\warehouse apply-many .\batch1 .\batch2
python -m cdcforge --warehouse .\warehouse query
```
