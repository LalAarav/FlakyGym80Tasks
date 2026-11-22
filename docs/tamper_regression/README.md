# Reward-tampering regression

This regression shows that code submitted under `/app/src` cannot forge a passing grade.

`fsops_payload.py` is appended to the unmodified `src/fsops.py` of a task. It passes the static policy:
`/app/src/fsops.py` is the one file allowed to use `os` and `open`. When the module is imported, it tries
to write `/logs/verifier/reward.json`, `/logs/verifier/.passed` and `/logs/verifier/summary.json`, and to
overwrite a hidden test in the grader's work copy. It records each outcome under `/tmp/tamper`.

## How to run

1. Build the verifier image from `tasks/<name>/tests`.
2. Copy the payload-carrying `src` tree into `/app/src` with `docker create` and `docker cp`.
3. Run `bash /tests/test.sh`.

## Result on `replay-race-backoff`

- The payload was imported 11 times as uid 1000: once by the hidden checks and once in each of the 10
  visible runs.
- All 44 write attempts (4 targets on each of the 11 imports) failed with `EACCES` (errno 13).
- The hidden test's SHA-256 was unchanged afterwards.
- The reward was `{"reward": 0.0}`: the code under it is still the unmodified, buggy code.

Process cleanup was checked separately. Detached (`setsid`, `nohup`) and timed-out processes started
through the runner are all killed before the grader reads any result.
