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

## Not covered by static QC

Every edit above changes the task fingerprint, so oracle and nop evidence must be re-measured for these fingerprints.
