# Corpus 3.2 run 9: Claude Opus 5 on Foundry

This round evaluated the full selected corpus-v3.2 set with explicit
`claude-opus-5` through Microsoft Foundry, the documented 1M context window,
and `high` effort. The design was 20 tasks, three arms, and four replicas: 240
cells. Generation used acceptance feedback only. The ordinary mutant set was
withheld as a post-run disqualification gate, so a surviving mutant was a
final failure without an in-session refutation retry.

## Results

All 240 generation cells reached operational success. Post-run scoring yielded
230 strict successes, six conclusive disqualifications, and four cells that
remained unmeasured after their permitted infrastructure retry.

| arm | strict success | disqualified | infrastructure-unmeasured | list-price cost | mean cost per finalized cell | mean wall time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `agent_only` | 75/80 (93.75%) | 3 | 2 | $37.3875 | $0.46734 | 122.8 s |
| `hybrid_flexible` | 77/80 (96.25%) | 2 | 1 | $38.6501 | $0.48313 | 122.2 s |
| `hybrid_guided` | 78/80 (97.50%) | 1 | 1 | $39.6157 | $0.49520 | 126.4 s |
| **overall** | **230/240 (95.83%)** | **6** | **4** | **$115.6532** | **$0.48189** | **123.8 s** |

The strict-success percentages use all scheduled cells as their denominator.
The four unmeasured cells are infrastructure missingness rather than
experimental failures. The table is descriptive; with four replicas it does
not establish a statistically reliable ranking among the arms.

## Failures and infrastructure recovery

All six conclusive failures occurred on `QP-part-025` because
`QP-part-025-lost-element` survived the disqualification gate. In accordance
with the protocol, these cells were not retried:

- `agent_only`: replicas 1, 2, and 4
- `hybrid_flexible`: replicas 2 and 4
- `hybrid_guided`: replica 2

Six `SM-select-022` ordinary-gate scores were solver-inconclusive in the main
scoring pass. Each was rerun once, sequentially and with exclusive access to
both shared prover slots. Two passed the gate but initially had an
inconclusive strict score; their permitted strict-only retry killed all three
strict mutants. Four ordinary gates remained inconclusive and are retained as
unmeasured: `hybrid_flexible` replica 1, `agent_only` replicas 2 and 4, and
`hybrid_guided` replica 4.

## Cost, time, and provenance

The cost is the cumulative model cost for this round, divided by all finalized
canonical cells in each arm. It includes cells later disqualified or left
unmeasured. Scoring and recovery used no model calls.

Every cell was independently repriced from its raw Foundry token counters at
the recorded Opus 5 list rates: $5/M fresh input, $0.50/M cache reads,
$6.25/M five-minute cache creation, $10/M one-hour cache creation, and $25/M
output. All 240 recomputed prices matched the SDK list-cost estimate within
$1e-9; the largest absolute difference was $2e-16. Azure contract or invoice
adjustments are not present in the telemetry.

Overall wall time had a 93.8-second median, 288.4-second p95, and 691.2-second
maximum. Cell concurrency was three except for one recorded interval at two.
The three participating containers cooperatively limited Boogie to two process
groups on a Linux btrfs-backed lock volume; the shared trace recorded no cap
violation.

The experiment source commit is
`01301524c92bf240f853b7eaa2aa265e4c526e53`. The archived Flow binary SHA-256
is `b4deb2871092b92e806f656d3e4aa503f198cfacf54e21ae3a46f5d6446cdd44`.
The apparatus and coordination provenance was pushed at
`04a9bac5e919609124740c25af009ae9552d55f8` on
`origin/wrwg/inf-opus5-foundry`.

The companion archive is
[`corpus3.2-run9-opus5-foundry.tar.gz`](corpus3.2-run9-opus5-foundry.tar.gz).
It contains the detailed report, per-cell and per-query tables, final mutation
summary and per-mutant verdicts, infrastructure-recovery evidence, schedules,
pricing, audit data, apparatus identities, and an internal `SHA256SUMS`. Raw
transcripts, workspaces, binaries, credentials, and solver scratch files are
excluded. The archive SHA-256 is
`2b99f068c26c592b7b9c66221a71d17941f6f650c0f122a1748a10b20375c4a4`.
