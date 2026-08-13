# Individual contributions (as shipped)

Sebastian Steen and Aidan Percy, MSDS 682.

This is the as-shipped record, written from the commit history on 2026-08-13. It is deliberately separate from the contribution plan in `docs/PROPOSAL_DRAFT.md`, which is a submitted document frozen at proposal time and never edited to match reality. Where the two differ, this file is the accurate one.

The planned split was 50-50, divided at the schema: one person upstream of the Avro contracts, one downstream. That division held. What follows is what each person actually built.

## Aidan Percy — the consumer half (downstream of the contracts)

| Component | Evidence |
|---|---|
| Kafka stack smoke test: throwaway-topic round trip through broker and registry | `f513ede` (H1) |
| Scoring consumer: enrichment, frozen-model scoring, delay-risk production | `c6583d4` (H2), `streaming/consumer.py` |
| Risk banding: decile band label for calibrated probability | `4c77b75` |
| In-stream rotation state machine: schedule-consistent linkage, the serve-time enforcement of the leakage rule's linkage clause | `61a5154` (H3) |
| Rotation parity gate: stream versus batch twin versus mart reference | `90337ae`, plus the batch-twin ordering fix `4705201` |
| Alert artifact and the leakage test suite that demonstrates the boundary | `7bc32ad` (H4), ordering fix `0156762` |
| TAF forecast-substitution study, the project's second headline finding | `2d616b0` (H5) |
| Evaluator timestamp normalization at the fastavro read boundary | `17d06f4` |
| Rulebook section 7 (Makefile commands, runtime extras, handoff pointer) | `a7c9ebc` |

Aidan also built the entire Streamlit dashboard and its Cloud Run deployment in the predecessor 681 project. That work is not part of this submission and its code is slated for the deletion audit, but it is a large share of his commit count in this repository's history and should not be mistaken for streaming work.

## Sebastian Steen — the producer half, the platform, and closeout

| Component | Evidence |
|---|---|
| Shared constants module and its source-pinning tests, the single source for every leakage constant | `streaming/constants.py` |
| Replay export and the seeded replay producer (byte-identical same-seed runs, 151,878 events) | `streaming/producer.py` |
| Outcomes producer at actual-arrival event time | `streaming/outcomes_producer.py` |
| Avro contracts, the three topics, registry registration with BACKWARD compatibility | `streaming/schemas/`, `streaming/admin.py`, `docs/schemas.md` |
| TTL-bounded outcome-join evaluator with nine conservation-tested counters | `streaming/evaluator.py` |
| Makefile, Docker Compose stack, model-artifact release and the two-phase fetch script | `Makefile`, `docker-compose.yml`, `scripts/fetch_artifacts.sh` |
| Drift measurement | `scripts/drift_measurement.py`, `data/drift_report.json` |
| Harmonization control study: decomposed the TAF gap into representation and forecast shares | `scripts/harmonization_control.py`, `data/taf_harmonization.json` |
| Origin-pressure features: trailing-window late-arrival and cancellation counts at the origin | `PressureIndex` in `streaming/consumer.py`, `streaming/test_pressure.py` |
| Project documentation: the streaming rulebook, plan, handoff prompts, data sources, this file | `CLAUDE.md`, `docs/` |
| Dress rehearsal and the three defects it exposed, including the retention root cause | `3116433`, `0cc7efb`, `d3c96a9` |

## Shared

The constants module's contents were agreed jointly and both halves import it; the proposal and the demo were joint work; every handoff was reviewed at the sync points recorded in `docs/HANDOFF_PROMPTS.md`. Both authors can explain any part of the system, which was the point of splitting at the schema rather than by convenience.

## A note on commit counts

`git shortlog -sne` reports roughly 150 commits for Sebastian and 25 for Aidan across the repository's full history. That ratio does not describe the streaming project. It is inflated on one side by documentation, infrastructure, and the 681 lakehouse platform this project was built on, and it undercounts Aidan's streaming work, which landed as five large handoff commits rather than many small ones. Component ownership above is the honest measure.
