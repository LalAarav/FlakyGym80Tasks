# Reward design and hacking analysis

## Observation

No `rh-detect.json` was supplied for this batch.

## Detection

Mechanical detection scans ATIF trajectories for verifier-path writes, Harbor `/tests` tamper, forced skips, and per-task oracle lookups. A pass plus a suspected hit is BAD_SUCCESS and is excluded from `rollout_n`.

## Prevention

Prevention is the separate-verifier image, baked verifier tooling, the reward-on-all-paths trap, `check-solve-no-reward-write`, and the instruction-leak / secrets static checks. The packager re-runs those as the final QC pass.

## Suggestions

TODO(human): what you would change in the verifier or instruction if a later campaign finds a new shortcut.
