# SecureHook v2安全协议

## 1.输入与解析

所有JSON均使用UTF-8。对象存在重复键、非有限数字、非NFC字符串、控制字符、孤立代理项或额外/缺失字段时必须拒绝。布尔值不是整数。Token为`header.payload.signature`，三段必须是无填充、URL安全且重新编码后逐字节相同的base64url；禁止空段。

密钥文件顶层字段必须恰为`version,issuer,audience,epoch,keys`。`version`只能为2，`epoch`为正整数，`issuer/audience`为非空NFC字符串。`kid`在文件内唯一。密钥字段必须恰为：

- HS256：`kid,kty,alg,material,epoch,active_from,retire_at,tenants`；`material`是至少32字节的规范base64url。
- RS256：`kid,kty,alg,n,e,epoch,active_from,retire_at,tenants`；`n/e`为规范base64url无符号大端整数，模数至少768位，指数为奇数且大于2。

`tenants`是非空、无重复、排序后的字符串数组。时间和epoch均为整数。`retire_at`可为null，否则必须大于`active_from`。密钥`alg/kty`固定匹配，不得回退或借用另一类材料。

## 2.Token与请求绑定

Header字段必须恰为`alg,kid,typ`，其中`typ`必须是`SHK2`，算法仅可为`HS256/RS256`。Payload字段必须恰为`iss,sub,aud,tenant,iat,nbf,exp,jti,seq,method,target,body_sha256,key_epoch`。

Token签名输入是原始规范header段、点号、原始规范payload段。HS256使用HMAC-SHA256；RS256使用RSASSA-PKCS1-v1_5与SHA-256，严格校验完整DigestInfo和签名长度，拒绝宽松前后缀匹配。

请求JSON字段必须恰为`method,target,body_b64`。`body_b64`为规范base64url，哈希是原始body字节的十六进制小写SHA-256。Payload的method、target、body_sha256必须逐字节匹配请求。

method仅允许`POST,PUT,PATCH,DELETE`且必须大写。target必须是规范ASCII origin-form：以单个`/`开始，不含fragment、userinfo、反斜杠、控制字符、连续斜杠、空路径段、`.`或`..`段；百分号转义必须大写，禁止编码非保留字符；query使用`&`分隔，名字非空、禁止重复，按编码后的`name=value`字节排序，所有项都必须含`=`。

## 3.身份、时间与轮换

`iss/aud`必须匹配密钥文件。`sub,tenant,jti`为非空NFC字符串且不含控制字符；tenant必须在密钥tenants中。`seq`为正整数。`key_epoch`必须等于密钥epoch且不能大于bundle epoch。

令允许时钟偏差为30秒、最大Token寿命为300秒：`iat-30 <= now`、`nbf-30 <= now`、`now < exp+30`、`nbf >= iat-30`、`exp > nbf`、`exp-iat <= 300`。密钥必须满足`iat >= active_from`；若`retire_at`非空则`iat < retire_at`。

状态库按issuer持久化见过的最大bundle epoch；打开更低epoch的bundle必须拒绝，且失败不得降低状态。未来schema版本必须拒绝。

## 4.重放、序列与事务

已成功接受的`(issuer,jti)`永久占用；任何再次使用都返回Replay，即使请求相同。每个`(issuer,tenant,sub,method,target)`的seq必须严格递增。验证签名、字段、时间、请求绑定、重放和seq更新必须在同一`BEGIN IMMEDIATE`事务内。

`Gateway.verify_many(items,now,fault=None)`原子处理整个非空列表，每项为`{"request":对象或路径,"token":字符串}`：保持返回顺序；批内jti不得重复；同一序列域按输入顺序逐项递增；任一失败整批不消耗jti/seq。`verify`是单项包装。

fault在写入后依次调用`after_replay`和`before_commit`；异常或进程退出后重开数据库不得出现半提交。并发进程/线程验证同一jti只能一项成功。

## 5.持久化与CLI

空库创建schema v2。已存在`PRAGMA user_version=1`且含`seen(issuer TEXT,jti TEXT,PRIMARY KEY(issuer,jti))`的数据库须原地事务迁移，保留seen记录并创建其余状态；其他旧结构、损坏结构及未来版本拒绝。

CLI：

```text
python -m securehook --db DB --keys KEYS verify REQUEST TOKEN --now UNIX_SECONDS
python -m securehook --db DB --keys KEYS verify-many ITEMS --now UNIX_SECONDS
```

成功向stdout输出单行、排序键JSON；业务/输入错误输出`{"error":"类型","message":"..."}`并退出2，不输出traceback；内部错误退出1。不得联网，不得使用第三方依赖，不得记录密钥材料或完整Token。

