# Reconcile

供应商订单与客户方案的增量采集账本。当前代码为旧原型，修复目标见 USER_PROMPT.md 和 docs/CONTRACT.md。

```powershell
python -m unittest discover -s tests -v
python -m reconcile --db sample.sqlite claim feed worker
python -m reconcile --db sample.sqlite ingest feed worker 1 examples/batch.json
python -m reconcile --db sample.sqlite view
```

无需安装依赖。订单时间按UTC秒计，折扣为万分比。`tools/make_v1.py DEST` 可生成包含真实历史修订的旧库样本，用于迁移检查。
