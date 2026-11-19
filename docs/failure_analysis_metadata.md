# Failure analysis metadata

## Taxonomy

Failure modes are labeled per trial by `harbor analyze` (`failure_mode`) and, when present, by hint-conditioned reruns.

## Methodology

A trial is valid when it is not a system error and not a confirmed reward hack. Hint rollouts are stored as `rollout_<harness>_hint_<n>/` and never counted in `performance.toml` `rollout_n`.

## Validation pipeline

Authoring QC (static checks + the packaging QC pass) runs before a batch is shared. Difficulty numbers come from `/band`. Reward-hack signals come from `detect_reward_hacking.py`.

## Cumulative label frequency

No analyze-results were supplied. Re-export with `--inputs` to fill this table.

## Distinct-modes-per-task histogram

No analyze-results were supplied. Re-export with `--inputs` to fill this histogram.
