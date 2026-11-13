# Vault Unpack
受限ZIP归档的安全、原子解包器。完整要求见`docs/SPEC.md`。

```powershell
python -m vaultunpack archive.zip destination '{"max_files":10,"max_file_size":1000000,"max_total_size":5000000,"max_ratio":20,"max_depth":8,"allowed_methods":["stored","deflated"]}'
```

策略也可以放在 JSON 文件中并通过第三个位置参数或 `--policy-file` 传入。
