# Rule Quarry

确定性数据质量评估与隔离报告工具，仅依赖 Python 标准库。

## 用法

    from rulequarry import QualityRun
    result = QualityRun(root).evaluate(plan)

也可通过 CLI 运行（参数错误或处理失败退出码为 2，且不输出 traceback）：

    python -m rulequarry <root> <plan.json>

## 规则类型

- `not_null`：空串（null）即违规。
- `unique`、`range`、`regex`、`foreign_key`：行级规则，除 `not_null` 外均忽略 null。
- `aggregate_count`：按列分组计数，支持 `min` / `max`。

规则依赖通过 `depends_on` 声明为 DAG，按拓扑层、同层 ID 顺序执行；上游规则未通过
（`fail` 或 `blocked`）时下游输出 `blocked` 且不执行。

## 产物（位于 `root` 内）

- `cache/<dataset>.json`：按 CSV 内容哈希复用的解析缓存，损坏自动重建。
- `quarantine/<dataset>.jsonl`：去重隔离记录，含原始记录与排序后的 `rule_ids`。
- `runs/<run_id>/summary-<digest>.json`：内容寻址、确定性的摘要。
- `runs/<run_id>/binding.json`：将 `run_id` 永久绑定到计划与所有 CSV 字节，输入冲突时报错。

所有写入均经临时文件原子发布；`after_parse`、`after_rules`、`before_publish`
故障注入点触发时回滚，不留下半成品。CSV 解析严格（UTF-8、唯一表头、列数一致、
类型严格且 float 有限），相对路径禁止逃逸出 `root`。