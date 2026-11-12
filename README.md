# securehook

离线 Webhook 授权验证器，实现 docs/CONTRACT.md 定义的 SecureHook v2 协议。
公开入口为 securehook.Gateway 和 python -m securehook，仅使用 Python 3.11+
标准库与内置 SQLite，不联网、不依赖第三方包。

## 能力

- 严格 JSON（重复键、非有限数字、非 NFC、控制字符、孤立代理项、字段集合）与
  无填充规范 base64url（重编码逐字节一致）解析。
- HS256 与 RS256 类型绑定验证；RSA 使用严格 RSASSA-PKCS1-v1_5/SHA-256，
  校验完整 DigestInfo、签名长度与填充，拒绝算法与密钥类型混淆。
- 请求方法、规范 origin-form target 与 body SHA-256 绑定。
- 时钟偏差 30 秒、最大寿命 300 秒、密钥激活/退役窗口与租户约束。
- 按 issuer 持久化 bundle epoch，拒绝密钥集回滚，并对 schema 未来版本围栏。
- (issuer,jti) 永久重放防护，以及每个授权域的严格递增 seq。
- erify_many 整批原子：批内 jti 唯一、同域按输入顺序递增，失败不消耗状态；
  全部校验与写入在单个 BEGIN IMMEDIATE 事务内完成，支持 fault 钩子、崩溃回滚
  与多进程/多线程并发。
- 空库创建 schema v2；符合 v1 seen(issuer,jti) 结构的数据库原地事务迁移，
  其他旧结构、损坏结构与未来版本一律拒绝。

## CLI

`	ext
python -m securehook --db DB --keys KEYS verify REQUEST TOKEN --now UNIX_SECONDS
python -m securehook --db DB --keys KEYS verify-many ITEMS --now UNIX_SECONDS
`

成功向 stdout 输出单行排序键 JSON；业务/输入错误输出错误 JSON 并退出 2；
内部错误退出 1，均不输出 traceback。

## 测试

`powershell
python -m unittest discover -s tests -v
`
