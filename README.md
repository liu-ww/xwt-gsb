# CDC Forge

`cdcforge`是一个仅使用Python标准库与SQLite的离线CDC物化器。当前仓库是故意保留缺陷的起始版本，只覆盖单批次正常路径；完整要求见`docs/CONTRACT.md`。

```powershell
python -m unittest discover -s tests -v
python -m cdcforge --warehouse .\warehouse apply .\batch
```

