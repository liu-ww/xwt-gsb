# Time Weave双时态协议

`TemporalStore(db)`接收维度修订和事实。维度字段恰为`entity,valid_from,valid_to,system_from,system_to,revision,attrs`，区间左闭右开，to可为null表示正无穷；事实字段恰为`fact_id,entity,event_time,observed_at,metrics`。所有时间为带Z的RFC3339秒级字符串，规范化后比较；revision为正整数。

同一entity在任一system_time切片上，业务有效区间不得重叠。更正使用更高revision关闭旧system区间并新增记录；禁止篡改历史、系统区间重叠、空区间和同revision冲突。事实ID永久绑定内容，相同重投递幂等，不同内容冲突。

`join(as_of)`对每条事实选择满足`valid_from<=event_time<valid_to`且`system_from<=as_of<system_to`的唯一维度版本；无匹配时dimension为null并标记`unmatched=true`。输出按fact_id排序，包含原事实、dimension、dimension_revision。as_of早于fact的observed_at时该事实不可见。边界必须严格采用半开区间。

`apply(dimensions,facts,fault=None)`整批原子，fault阶段`after_dimensions,after_facts,before_commit`回滚；并发修订只能一个成功。数据库schema版本2，未来版本及损坏结构拒绝。CLI支持apply/join，业务错误退出2且无traceback。仅用标准库和SQLite，不联网。

