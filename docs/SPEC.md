# Session Mill协议

输入事件字段必须恰为`tenant,user,event_id,event_time,arrival_seq,kind`，kind为`open,ping,close`。字符串为非空NFC且无控制字符，时间与序号为整数，arrival_seq全局唯一并永久绑定事件内容。输入列表可乱序；重复事件内容幂等，冲突内容拒绝整批。

`SessionEngine.ingest(events,watermarks,fault=None)`原子处理一批事件。watermarks是tenant到整数的精确映射，每个tenant水位线不得回退；事件允许迟到60秒，满足`watermark-60<=event_time<=watermark`。每个`(tenant,user)`按event_time、arrival_seq排序：相邻事件间隔不超过30秒属于同一会话；桥接迟到事件必须合并原有会话；close事件立即结束会话，后续事件开启新会话。会话ID为其中最小event_id，输出字段恰为`session_id,tenant,user,start,end,event_count,closed,revision`，按tenant、user、start排序。

每次成功非幂等写入产生确定性changelog：旧会话先按ID排序输出`retract`，新会话再输出`upsert`；revision对每个会话变更严格递增。`changes(since)`返回全局offset大于since的记录。数据库、会话、水位线和changelog同一事务提交；fault阶段`after_events,after_sessions,before_commit`异常必须全回滚。并发重投递只允许一个写入者。SQLite schema版本为2，未来版本拒绝。

CLI支持`ingest EVENTS WATERMARKS`、`sessions`、`changes SINCE`；成功输出单行排序键JSON，业务错误退出2且无traceback。仅用标准库，不联网。

