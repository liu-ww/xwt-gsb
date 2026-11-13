# Vault Unpack安全协议

`Extractor(policy).extract(archive,destination,fault=None)`仅接受ZIP。policy字段恰为`max_files,max_file_size,max_total_size,max_ratio,max_depth,allowed_methods`，所有限制为正数，压缩方法仅允许stored/deflated。

归档成员名必须为NFC UTF-8相对POSIX路径：禁止空名、绝对路径、盘符、反斜杠、空段、`.`、`..`、控制字符、尾随点/空格、Windows保留名和超过max_depth。拒绝重复规范名、Unicode规范化碰撞与不区分大小写碰撞。目录必须以`/`结束；拒绝符号链接、硬链接、设备、FIFO、socket、加密成员、未知压缩法、重叠数据区、CRC错误和目录携带数据。

在写盘前验证成员数量、单文件声明/实际大小、总解压大小及每个文件的实际压缩比；任何整数溢出、截断或超限均拒绝。不得依赖可伪造的中心目录大小跳过流式计数。

destination必须不存在；在同一父目录内暂存并原子改名。创建路径时不得跟随父目录符号链接或junction；文件权限固定为0640、目录0750，忽略归档权限与时间。成功写`MANIFEST.json`，记录排序后的`path,size,sha256`，自身不列入manifest。fault阶段`after_member,before_manifest,before_publish`异常后不得留下destination或暂存目录。并发向同一destination只能一个成功，失败不得覆盖现有内容。

CLI成功输出单行排序键JSON；输入/策略错误退出2且无traceback。不得联网或执行归档内容，仅使用Python标准库。

