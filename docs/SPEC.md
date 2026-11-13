# Rule Quarry协议

`QualityRun(root).evaluate(plan,fault=None)`读取规范JSON计划。计划字段恰为`run_id,datasets,rules`；dataset定义CSV路径、主键列与严格schema；rule字段恰为`id,type,dataset,column,params,severity,depends_on`，ID唯一，依赖必须存在且形成DAG。规则按拓扑层、ID顺序执行，依赖失败则输出blocked而非执行。

CSV必须UTF-8、唯一表头、列数一致；空串表示null。类型支持string,int,float,bool，解析严格且float有限。规则类型：not_null、unique、range、regex、foreign_key、aggregate_count。除not_null外，行级规则均忽略null；unique忽略null；foreign_key的null不违规，引用数据集必须存在；range边界由params的inclusive_min/max控制；regex使用fullmatch；aggregate_count支持按列分组的min/max。

每条规则输出`id,status,checked,violations,violation_keys,digest`，status为pass/fail/blocked，主键按字符串排序且去重。severity为error/warn；最终status仅由error失败决定。所有失败行写`quarantine/<dataset>.jsonl`，每行包含原始规范记录和排序后的rule_ids，同一行只出现一次。summary和隔离文件必须确定性、内容寻址并原子发布。

run_id永久绑定计划及所有CSV字节；相同输入幂等，不同输入冲突。fault阶段`after_parse,after_rules,before_publish`回滚。增量运行可复用内容哈希不变数据集的解析缓存，但结果必须等同全量；缓存损坏须重建。禁止eval与任意代码执行、路径逃逸、未来状态版本。CLI错误退出2且无traceback，仅用标准库。
