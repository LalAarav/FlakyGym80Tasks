# Align QC static review (hard-80 subset)

Response to Align task QC report `align-qc-aeb2de37288e4cf3bae43dda9156e872` (Harbor 0.26.0, engine 0.2.7,
2026-11-19). The report had 0 errors and 1,600 warnings across 160 folders. Every warning occurred identically in
every folder: the 80 `tasks/<id>` folders and the 80 `docs/<id>/edit_history/v0/task` snapshots. The 80 tasks share
one verifier and one hidden suite, so each candidate below is answered once and the answer applies to all of them.

## Changed

| Rule | Finding | Action |
|---|---|---|
| `harbor.root-agent` | `[agent] user` omitted, so the agent ran as root | `user = "agent"` added to `[agent]` in all 80 `tasks/<id>/task.toml`. The image already creates `agent` (uid 1000) and makes `/app` writable (`chmod -R a+rwX /app`). Harbor still installs tmux as root during agent setup. |

## Kept, with reasons

| Rule | Finding | Verdict |
|---|---|---|
| `harbor.unknown-config-field` | top-level `[judge]` | Kept. The delivery spec requires it at the top level (Tencent V3 R-1.30; tb-ci G0 check `F7.judge`). Moving it under `[metadata]` would fail that gate, and `[metadata]` is a closed key set (R-1.20, `F3`). Harbor ignores the block at runtime. |
| `harbor.unknown-config-field` | top-level `[rollout_baseline]` | Kept. Required at the top level by Tencent V3 R-1.28/1.29 (tb-ci `F6.baselines`). It holds only the measured oracle 1.0 and nop 0.0 baselines. |
| Duplicate folders `task` | 80 folders named `task` | Not duplicates. They are the `docs/<id>/edit_history/v0/task` snapshots that the spec requires (V3 R-1.16), and every fingerprint differs. The tasks to score are `tasks/<id>` only. |

## Static candidates refuted

| Rule · location | Code | Why it is not a permissive grading predicate |
|---|---|---|
| `verifier.execution-isolation` · `tests/verify.py:280` | `as_agent()` runs pytest via `setpriv --reuid=1000 --regid=1000 --clear-groups` | This is the intended isolation boundary. Before it runs: `/tests` and `/logs/verifier` are made `root:root 0700`. The submission is copied to `/work`, which is `chown 0:0` and `chmod a-w`. The visible suite is restored from an archive and checked against a SHA-256 manifest. JUnit reports go to random 128-bit names in a `0733` directory that the test user cannot list. Only root writes `reward.txt`/`reward.json`. A run counts only when pytest exits 0 and reports exactly the expected test ids, with nothing missing and nothing extra. Agent code is never imported into the scoring process, which reads only the XML report. |
| `verifier.permissive-match` · `tests/hidden/test_fanout_guarantees.py:540` | `"signature" in seen[0]` | A dict-key presence check on the captured delivery. It is followed by an exact equality on the verified payload: `verified.payload == {"event_id": "e1", "topic": "t", "body": {"total": 7}}`. |
| `verifier.permissive-match` · `tests/hidden/test_jobqueue_guarantees.py:105` | `"/ids/" not in path` | Part of the test fixture (a `HookFs.write_bytes` override). It chooses which write to pause and is not a grading predicate. |
| `verifier.permissive-match` · `tests/hidden/test_journal_guarantees.py:365` | `k.startswith("batch")` | Selects the keys under test. The assertions are exact: `present == []` and `reopen(work).items() == {"base": b"b", "after": b"x"}`. |
| `verifier.permissive-match` · `tests/hidden/test_retry_guarantees.py:342` | `"error" not in box` | A dict-key check that the worker thread raised nothing. It is followed by an exact equality: `sorted(sink.got + returned) == ["a", "b", "c"]`. |
| `verifier.permissive-match` · `tests/hidden/test_signing_guarantees.py:452` | `"value" in b` | A dict-key check on thread results. The assertions are exact: `len(accepted) == 1` and `isinstance(other.get("error"), VerificationError)`. |
| `verifier.permissive-match` · `tests/policy.py:225` | `attr.startswith("__") and attr.endswith("__")` | The anti-cheat AST scanner. Prefix/suffix matching here makes it reject more (any dunder attribute outside an allowlist), so it cannot accept a wrong answer. |

No agent output text is graded by substring, prefix or regex. Reward is binary. It is 1 only if every hidden check
passes and the visible suite passes in all `visible_runs` repeats.

## Still to run (Align `runtime.*` and `acceptance.*`)

The edit changes each task's fingerprint, so oracle and nop evidence must be re-measured for these 80 fingerprints.
Static QC cannot cover image build, oracle, nop, repeatability, adversarial or difficulty runs.
