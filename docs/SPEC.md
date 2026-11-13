# Ice Crate快照协议

输入segment是规范JSONL，每行字段恰为`pk,partition,version,op,data`，op为upsert/delete，version为正整数；同一segment内`(partition,pk,version)`唯一。`Compactor.commit(snapshot_id,parent,segments,fault=None)`读取段并创建不可变快照。snapshot_id全局唯一并永久绑定parent及所有输入字节；相同内容幂等，不同内容冲突。首快照parent为null，后续parent必须是当前head，采用CAS避免并发分叉。

每个`(partition,pk)`选择最大version；同version内容不同为冲突，delete为墓碑且不出现在读取结果。输出chunk按partition、pk排序，每个最多3行，以内容SHA-256命名`objects/<hash>.jsonl`，重复内容复用对象。快照manifest记录父、generation、行数及排序后的对象路径和哈希，必须规范JSON。

`read(snapshot_id=None)`支持历史快照；`compact(snapshot_id)`只重写该快照的物理布局，不改变逻辑行、generation或历史manifest。`vacuum(retain,grace_before)`仅删除不被head及最近retain个祖先引用、且mtime早于grace_before的对象，返回确定性删除列表；不得删除仍被任一保留快照引用的共享对象。

manifest、head和对象发布原子；fault阶段`after_objects,before_manifest,before_head`异常不产生可见快照或孤儿对象。损坏对象、哈希不符、父链环或未来catalog版本必须拒绝。CLI错误退出2且无traceback。仅使用标准库，不联网。

