# Flaky Test Gym v9: hard 80 subset

80 tasks from the 250-task Flaky Test Gym v9 delivery (AkharaAI_2026-10_001), chosen as the hardest and most
varied. The list is in `hard80_task_ids.txt`.

| Folder | Contents |
|---|---|
| `tasks/<id>/` | Harbor tasks in the delivered form: instruction, environment, reference solution, hidden tests and verifier |
| `docs/<id>/` | Per-task report, failure analysis and edit history |
| `docs/align_qc_static_review.md` | Response to the Align static QC warnings |
| `environment/` | Shared environment notes |

The only change from the original delivery is `user = "agent"` under `[agent]` in each `tasks/<id>/task.toml`,
so the agent runs as a non-root user (uid 1000). Instructions, code, solutions and tests are unchanged.

This repository contains hidden tests and reference solutions. Keep it private.
