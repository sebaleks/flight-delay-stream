> Superseded (2026-08-11) by the submitted proposal at `docs/PROPOSAL_DRAFT.md`. Kept as provenance: its flight_id keying and cloud-primary review path were overridden in reconciliation (`docs/PLAN.md`).

MSDS 682 Final Project Proposal
Project title: Gate-Time Delay Risk — Streaming Flight-Delay Scoring and Alerts

Student name(s): Aidan Percy Sebastian Steen

Project format: Individual

Contribution plan: Split code, presentation, and report work.

1. Problem Summary
Airport operations teams learn about delays after they happen. This project scores every departing flight at scheduled gate time, using only pre-departure information, for an ops/notification audience. My MSDS 681 lakehouse produced a leakage-gated feature mart and a trained XGBoost classifier for arrival delay ≥15 min (held-out ROC-AUC 0.74). That batch model becomes a streaming data product: a scored delay-risk topic, an alert artifact, and a streaming evaluation report. Scope: one week of replayed test-period flights.

2. Planned Data Source and Classification
Data source and official URL: BTS On-Time Performance, https://www.transtats.bts.gov (curated in my 681 lakehouse’s BigQuery feature mart)
Data owner: US DOT Bureau of Transportation Statistics (public domain)
Classification: Batch, with deterministic streaming replay
Why: the data is historical files; a replay producer enters them into Kafka record by record in scheduled-departure order, simulating a realtime feed.
Access and limitations: public, no key or rate limit; a frozen ~1-week sample (pre-departure fields only) plus the model artifact are exported once and committed as replay data — no dependency on my GCP project.
Review path: cloud core path on Confluent Cloud with full reviewer access (cluster, topics, Schema Registry), committed sample data, run commands, expected outputs, and cleanup steps; deterministic replay also serves as a local fallback.
3. Architecture Sketch
Realtime Data Streaming Layer (Confluent Cloud)
replay sample (data/)                        outcome sample (data/)
   │ replay producer (departure order)          │ outcome replay producer
   ▼                                            ▼
flight.departures.v1                     flight.outcomes.v1
(key=flight_id; Avro contract,                  │
 Schema Registry + pydantic validation)         │
   ▼                                            ▼
delay-risk processor (model scoring) ──► outcome evaluator
   │                                     (joins risk×outcome by flight_id)
   ▼                                            ▼
flight.delay_risk.v1 ─► alerts.jsonl     evaluation/streaming_eval.json
                        (high risk)      (PR-AUC, alert precision/recall)
Other Components (batch, upstream)
681 lakehouse (GCS → dbt/BigQuery feature mart → trained XGBoost artifact) → one-time export of sample + model into this repo.

4. Planned Tools and Packages
Python 3.11; confluent-kafka (producers/consumers, Schema Registry Avro serde); Avro (event contracts); pydantic (validation); xgboost (inference only); pandas/pyarrow (sample export); pytest (contract, scoring, join tests); python-dotenv (credentials outside code).

5. Feasibility Risks and Plan
Minimum end-to-end result: replay → validated departure events → scored delay-risk topic → alert file → evaluation JSON, inspected via full Confluent Cloud reviewer access.
Primary risks: free-tier cluster limits; out-of-order/late outcomes in the join; feature-schema drift between mart export and scorer.
Fallback plan: shrink the replay window; TTL-bound join state, count unmatched events; fail fast on schema mismatch against a pinned feature list.
Milestones: (1) export sample, freeze Avro contracts; (2) replay producer + topics; (3) scoring processor; (4) outcome evaluator + report; (5) tests, docs, cleanup.
6. AI Element and Disclosure
Planned AI element: ML classification — the pre-departure delay-risk model inside the stream processor.
Input and output boundary: in: one validated departure event (pre-departure features only); out: delay probability + risk band.
Verification method: streaming PR-AUC and alert precision/recall from the outcome evaluator, compared against the batch held-out baseline; upstream dbt leakage tests guard the features.
Fallback: a historical route/carrier smoothed delay-rate lookup replaces the model when the artifact is unavailable or rejected.
AI use in preparing this proposal: Claude Code (Fable 5) drafted it from my 681 repository documentation and the course template; I reviewed and edited the design and scope and verified all metrics and claims.