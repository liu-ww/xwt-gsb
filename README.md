# Ice Crate

Local immutable-segment snapshot, compaction, and retention tooling. It uses only
the Python standard library.

Objects live at `objects/<sha256>.jsonl`; metadata lives under `catalog/`.
Snapshots are linearized with parent CAS, support historical reads and
deterministic compaction, and are cleaned by reference-aware vacuum with a
retention window and grace period.

CLI: `python -m icecrate --root ROOT <commit|read|compact|vacuum>`.
Errors exit with code 2 and do not print a traceback.
