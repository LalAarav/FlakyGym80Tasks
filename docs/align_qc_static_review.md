# Align QC static review (hard-80 subset)

Response to Align task QC report `align-qc-df02c884285043ed81ad4f1320614e67` (generic profile, Harbor 0.26.0,
engine 0.2.8, snapshot `49e9dd7`). The report had 80 errors (five Axis 1 checks failing on every task) and 720
warnings. Every task shares the same verifier, hidden suite and Dockerfiles, so each fix below was applied
identically to all 80 `tasks/<id>` folders. The `docs/<id>/edit_history/v0/task` snapshots are history and were left
as they were.

## Errors fixed

| Check | Fix |
|---|---|
| `check-lockfiles.sh` | Both Dockerfiles copy a committed `deps/pyproject.toml` and `deps/uv.lock` to `/opt/deps`, run `uv export --locked`, and install with `pip install --require-hashes --no-deps -r /opt/deps/requirements.txt`. `uv` comes from `ghcr.io/astral-sh/uv:0.12.5` pinned by digest. pytest 9.1.1 is unchanged. |
| `check-task-fields.sh` | `[metadata]` now has `author_name`, `author_email`, `author_github`, `author_organization`, `category = "Software"`, `subcategory = "Systems"`, `tags` and `expert_time_estimate_hours`. Each task has a `README.md` with the four required sections and a `## Task Metadata` block that matches `task.toml`. |
| `check-task-package-name.sh` | `[task] name = "terminal-bench/<id>"`. |
| `check-task-absolute-path.sh` | Policy lines name `/app/src/fsops.py`, `/app/src/rpc/conn.py` and `/app/src/clock.py` by absolute path. Banned module names are given as `conftest`, `sitecustomize` and `usercustomize`. The verbatim CI assertion messages, which contained strings like `'topic/0'`, moved unchanged into `/app/ci/failures.log`, and the instruction points there. |
| `check-reward-file-format.sh` | The verifier writes only `/logs/verifier/reward.json` (the `reward.txt` writes in `tests/test.sh` and `tests/verify.py` were removed). |

## Warnings and info fixed

| Rule | Fix |
|---|---|
| `harbor.unknown-config-field` (`judge`, `rollout_baseline`) | Moved under `[metadata.judge]` and `[metadata.rollout_baseline]`. |
| `framework.adoption` | `[metadata.align] framework_version = "0.2.8"`. |
| `verifier.permissive-match` | All 21 substring and prefix predicates in `tests/hidden/` and `tests/policy.py` were rewritten as exact equivalents: key-set comparisons (`{"value"} <= b.keys()`, `box.keys().isdisjoint({"error"})`), path-component checks (`_has_dir(path, "ids")`), and fixed-width slice equality (`k[:5] == "batch"`, `attr[:2] == "__" and attr[-2:] == "__"`). Grading behaviour is unchanged. |

## Warning kept, with reason

| Rule · location | Reason |
|---|---|
| `verifier.execution-isolation` · `tests/verify.py` | Running the agent's code is the point of the verifier, so this boundary cannot be removed. It is already isolated. `/tests` and `/logs/verifier` are made `root:root 0700`. The submission is copied to a root-owned, read-only `/work`. The visible suite is restored from an archive and checked against a SHA-256 manifest. pytest runs via `setpriv --reuid=1000 --regid=1000 --clear-groups`. Reports go to random 128-bit names in a directory the test user cannot list. Only root writes `reward.json`, and a run counts only when pytest exits 0 and reports exactly the expected test ids. |

## Follow-up: report on `51a7e52` (engine 0.2.9)

| Rule | Action |
|---|---|
| `check-canary.sh` · `tests/deps/pyproject.toml` | Canary comment added to both `deps/pyproject.toml` files in every task. `uv lock --check` still passes. |
| `check-dockerfile-digest-platform.sh` (warning) | No change needed. The checker could not reach the registry. `python:3.12-slim@sha256:aa943ce4…` is a multi-platform OCI index (linux/amd64, linux/arm64 and others), and so is the pinned `ghcr.io/astral-sh/uv:0.12.5` digest. |

## Follow-up: reward publication (engine 0.2.9, report on `e4f15fe`)

| Rule | Action |
|---|---|
| `check-reward-file-format.sh` ("cannot establish that grading finished before reward.json was published") | `tests/test.sh` no longer writes any reward. It only runs `exec python3 /tests/verify.py`. In `verify.py` all grading moved into `grade()`, which returns its result, and the single writer `publish(grade())` runs once at the end. It writes `summary.json` first, then stages the reward and atomically renames it to `/logs/verifier/reward.json`. If grading crashes, no reward is published, so Harbor reports a verifier error instead of a false 0. |

## Follow-up: report on `e4f15fe` (engine 0.2.9, 0 errors)

| Rule | Action |
|---|---|
| `check-reward-file-format.sh` (`test.sh:6`, `:9`, `verify.py:261`) | Already fixed by the single-writer change above (commit `18aabad`). The report was run on the commit before it. |
| `verifier.execution-isolation` · `tests/verify.py` `as_agent()` | Hardened the runner as the report's fix suggests. Code under test runs via `setpriv --reuid=1000 --regid=1000 --clear-groups --no-new-privs`. It gets a fixed minimal environment (nothing inherited from the grader), no stdin/stdout/stderr pipe to the grader, no core dumps and a 512 MiB file-size cap, in its own process group. When a run ends or times out, the process group is killed, then every remaining live uid 1000 process (including ones that called `setsid`) is killed and reaped before results are read. The run log is read with a bounded tail. Checked in Docker: detached and timed-out processes are all gone afterwards, and the reference solution still scores 1.0 while the unmodified and empty submissions score 0.0. The boundary itself stays, since running the agent's code is how the task is graded. |

## Follow-up: report on `4c5f6fc` (`verify.py:267`, 44 folders)

| Rule | Action |
|---|---|
| `check-reward-file-format.sh` ("cannot establish that grading finished before reward.json was published") | The timing check (`align_qc/reward_timing.py`) follows `tests/test.sh` step by step. Any step it cannot model before the reward write makes the result "unknown", and Python that runs shutil, tar and setpriv subprocesses cannot be modelled. Publication therefore moved into shell, where the order is provable. `test.sh` runs the whole grader as `python3 -m pytest /tests/grade_run.py`, which is one complete grading phase. Afterwards it writes `{"reward": 1.0}` or `{"reward": 0.0}` with `printf`, from the outcome `verify.record()` left in root-only `/logs/verifier` (`.passed`, `.graded`). No Python file writes `reward.json` any more. If grading did not finish, `test.sh` exits 1 without writing a reward, so the run is a verifier error and not a false 0. |
| `verifier.execution-isolation` (2 per folder) | Kept. This rule (`align_framework/engine.py`) flags any subprocess call in a verifier `.py` file and any `pytest` or `exec` in a verifier `.sh` file. A verifier that runs the agent's tests always matches it. The isolation measures are described above. |

## Task folder names (Axis 4 `task_name` criterion)

The content rubric (`tb41_checks/task-implementation.toml`, `task_name`) requires a descriptive lowercase kebab-case
slug of at most 3 words, so the `ftg-<hash>` folders were renamed. Each name puts a planted root cause in a module CI
reports as flaky (two words) next to a second root cause (one word). For example, `replay-race-backoff` is the replay
check-then-add race in request signing plus the uncapped decorrelated retry backoff. All 80 bug sets differ, and so
do all 80 names. `tasks/<name>` and `docs/<name>` were renamed together, and `[task] name` is
`terminal-bench/<name>`. `instance_id`, `hard80_task_ids.txt`, `docs/analysis.md` and `docs/_qc` keep the original
Flaky Test Gym IDs, and `task_names.tsv` maps each new name to its original ID. Task content and grading are unchanged.

## Not covered by static QC

Every edit above changes the task fingerprint, so oracle and nop evidence must be re-measured for these fingerprints.
