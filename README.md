# securehook

离线Webhook授权验证器原型。公开入口为`securehook.Gateway`和`python -m securehook`。

当前实现只覆盖最基础的HS256正常路径，安全协议的完整要求见`docs/CONTRACT.md`。

```powershell
python -m unittest discover -s tests -v
```

