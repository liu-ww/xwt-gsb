*** Begin Patch
*** Update File: README.md
@@
 # Backfill Loom
-分区回填DAG的租约、重试与产物提交调度器。当前原型只做静态拓扑排序。
+基于 SQLite 的分区回填 DAG 调度器：计划校验、确定性租约领取、attempt
+限制、成功产物绑定、失败/阻断传播、只追加扩展与重启恢复。
+
+CLI 入口为 `python -m backfillloom`，支持 `load/extend/claim/complete/fail/status`。
*** End Patch