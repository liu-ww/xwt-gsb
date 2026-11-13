# Net Fence

离线零信任网络策略编译器与判定器，仅使用 Python 标准库，不访问网络、DNS 或系统防火墙。

## 功能

- 严格校验策略与规则字段、CIDR（禁止 host bits、包含或可合并网段）、端口区间、NFC 主体和协议。
- 检测同优先级且存在实际匹配交集、但动作不同的歧义规则。
- 检测被更高优先级同动作规则完全覆盖的不可达规则；不同动作覆盖不属于不可达。
- 编译结果按 `priority` 降序、`id` 升序排列，与输入规则顺序无关。
- 编译结果包含规范 SHA-256 (`spec_sha256`) 与完整性校验值 (`checksum`)，判定前先验证。
- 支持 IPv4/IPv6 同族匹配，拒绝映射地址、zone ID、前导零和非规范地址。
- 无匹配规则时使用默认策略，返回的 `rule_id` 与 `priority` 为 `null`。

## 用法

```python
from netfence import PolicyCompiler

compiler = PolicyCompiler()
compiled = compiler.compile(policy)
result = compiler.decide(compiled, packet)
```

CLI 读取 JSON 策略并输出确定性编译结果；校验或文件错误以退出码 2 结束且不输出 traceback：

```bash
python -m netfence policy.json
```
