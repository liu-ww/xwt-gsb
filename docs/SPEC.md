# Backfill Loom协议

计划字段恰为`plan_id,partitions,tasks`。partitions是排序且不重复的`YYYY-MM-DD`日期。task字段恰为`name,depends_on,max_attempts,fan_in,command_digest`；名称唯一，依赖形成DAG，max_attempts为正整数，fan_in为`same_partition/all_previous`。plan_id永久绑定规范计划内容。

每个task×partition形成实例。same_partition依赖同分区父任务；all_previous还要求父任务所有更早分区成功。`claim(worker,now,lease_seconds,limit)`按partition、拓扑层、task名确定性选择ready实例，原子授予租约并返回attempt与唯一lease_token。未过期租约不可被其他worker领取；过期后可重领并增加attempt。超过max_attempts变为failed并阻断所有传递下游。

`complete(instance,lease_token,output_digest,now)`仅接受当前租约；首次成功永久绑定output_digest，相同重复完成幂等，不同digest或旧token拒绝。`fail`记录错误类别并按次数重试。计划恢复、进程重启和并发claim不得重复运行有效租约。`status()`给出每种状态计数与按实例排序的明细；根因失败与blocked必须区分。

`extend(plan)`只允许追加晚于现有最大日期的分区，task定义必须逐字节相同；新增实例不得改变旧状态。所有操作事务化，fault阶段`after_lease,before_commit`回滚。SQLite schema版本2，未来版本拒绝。CLI错误退出2且无traceback。调度器不执行真实命令、不联网、仅用标准库。

