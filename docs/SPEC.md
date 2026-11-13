# Net Fence策略协议

策略字段恰为`version,default,rules`，version为1，default为allow/deny。规则字段恰为`id,priority,action,protocol,src,dst,ports,principals`；ID唯一，priority为整数，action为allow/deny，protocol为tcp/udp/any。src/dst是非空、排序、无重复的规范CIDR数组，不允许host bits或同数组内可合并/包含网段；IPv4与IPv6可并存但仅同族匹配。ports是排序、互不重叠且不相邻的`[start,end]`闭区间，范围1..65535；protocol=any时ports必须为[[1,65535]]。principals是排序、唯一、非空NFC字符串数组，`*`只能单独出现。

`PolicyCompiler.compile(policy)`严格校验并生成确定性compiled对象，包含原策略规范SHA-256、按`priority降序,id升序`排列的规则和不可篡改校验值。两个同priority、相同protocol交集、主体交集、地址交集和端口交集均非空但action不同的规则是歧义，必须拒绝。完全被更高priority同action规则覆盖的规则不可达，必须拒绝；不同action覆盖不是不可达，因为更高优先级有意义。

`decide(compiled,packet)`先验证compiled校验值。packet字段恰为`principal,src_ip,dst_ip,protocol,port`；IP必须是规范文本，protocol仅tcp/udp，port范围合法。选择全部匹配规则中最高priority，若同priority多个同action则按id最小并返回`decision,rule_id,priority`；无匹配使用default且rule_id/priority为null。不能把IPv4映射IPv6、IPv6 zone ID、前导零IPv4或Unicode混淆地址当作合法输入。

编译结果必须与输入规则顺序无关；不得访问网络、DNS或系统防火墙。CLI错误退出2且无traceback，仅使用Python标准库。

