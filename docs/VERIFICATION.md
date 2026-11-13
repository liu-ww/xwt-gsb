# Verification Notes

Run the full suite from the repository root:

```bash
python -m unittest discover -s tests -v
```

The suite uses only standard-library `unittest` and writes scratch databases under
`tests/.work/` (removed after each test), so it does not depend on the system temp
directory or network access.

## Coverage matrix

| Requirement | Test |
| --- | --- |
| Out-of-order input / time then arrival ordering | `test_out_of_order_events_use_time_then_arrival_order` |
| 30-second boundary, close boundary, new-session behavior | `test_gap_boundary_and_close_starts_new_session` |
| Late bridge merges old sessions | `test_late_bridge_merges_two_sessions_and_retracts_old_states` |
| Late close splits an existing session | `test_late_close_can_split_an_existing_session` |
| Revision history across retract/reappearance | `test_revision_history_survives_retract_and_reappearance` |
| Permanent idempotent deduplication | `test_exact_duplicates_are_permanently_idempotent` |
| Conflicting identity rejects the whole batch | `test_conflicting_identities_reject_the_entire_batch` |
| Watermark regression, future event, late event, missing tenant | `test_watermark_window_regression_and_missing_mapping` |
| Watermark-only advance and duplicate replay after horizon | `test_watermark_only_advance_and_new_lateness_horizon` |
| Rollback at `after_events` / `after_sessions` / `before_commit` | `test_fault_rollback_at_all_documented_stages` |
| Schema version 2 and future-version rejection | `test_schema_version_is_two_and_future_versions_are_rejected` |
| v0 database upgrade and event retention | `test_version_zero_database_is_upgraded_without_losing_events` |
| Input contract validation | `test_validation_contract` |
| Global changelog offsets and `since` filtering | `test_changes_offsets_since_and_unchanged_other_pairs` |
| Session ordering and exact output fields | `test_sessions_are_sorted_by_tenant_user_start` |
| Persistence across reopen | `test_persistence_survives_reopen` |
| Concurrent redelivery has one effective writer | `test_concurrent_redelivery_has_one_effective_writer` |
| CLI success JSON and traceback-free exit 2 | `test_cli_success_and_business_error_behavior` |

## Manual smoke commands

```bash
python -m sessionmill --db /tmp/smoke.sqlite ingest /tmp/events.json /tmp/watermarks.json
python -m sessionmill --db /tmp/smoke.sqlite sessions
python -m sessionmill --db /tmp/smoke.sqlite changes 0
```

Expected properties: one JSON object/array per command on a single stdout line, sorted
keys, monotonic changelog offsets, and exit code 2 with a one-line stderr error for
business failures.
