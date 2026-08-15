# AI usage

Sebastian Steen and Aidan Percy, MSDS 682. This project's AI element is
disclosed and verified AI-assisted development: Claude (Anthropic) wrote code
inside a boundary the team defined, and nothing it wrote shipped without
passing a deterministic gate the team specified in advance.

## The task AI owned

Implementing the five handoff prompts in `docs/HANDOFF_PROMPTS.md`: the Kafka
smoke test (H1), the scoring consumer with feature enrichment (H2), the
in-stream rotation state machine (H3), the alert artifact and leakage test
suite (H4), and the TAF forecast-substitution study (H5). AI also assisted
with documentation drafts. The architecture, contracts, leakage rules,
constants, producers, evaluator, and every design decision in `CLAUDE.md`
belong to the team, not the AI.

## Representative input and output

Input: the H3 handoff prompt, which specifies the rotation state machine's
exact semantics (duty window, swap-class triggers, NULL rotation for
swap-shaped links) and its pass gate (full-week parity against the batch
mart). Output: commits `61a5154` and `90337ae`, `streaming/rotation.py` and
its tests. Every handoff prompt and its gate are in
`docs/HANDOFF_PROMPTS.md`; the commits above and in `docs/CONTRIBUTIONS.md`
are the corresponding outputs.

## What we accepted and rejected

Accepted: code that passed its gate, after review at the sync points recorded
in `docs/HANDOFF_PROMPTS.md`. Rejected or corrected along the way: a weather
lookup that divided microsecond timestamps as if they were milliseconds
(caught by the golden-vector test before merge); alert files whose line order
differed between runs (rejected for breaking byte-identity, fixed with a
canonical sort); a presentation slide describing a record field that does not
exist in the schema (caught in team fact-check). We also kept decisions away
from the AI entirely: the fail-loud choice over a silent fallback score, the
alert threshold, and the tail-keying design were made by the team.

## How we verified the result

Every AI-written component has a gate that does not trust the author:
enrichment matches the batch mart's own feature rows exactly on golden
vectors; the rotation state machine matches both the mart reference and an
independently written batch twin over the full 151,878-event week (the
parity run also exposed a real ordering bug in the twin); the leakage suite
proves no post-departure field reaches the scorer; two full replays produce
byte-identical alert files and evaluation reports; `make test` runs the
59-test suite plus lint. Evaluation numbers come from the conservation-tested
evaluator, not from the AI's claims.

## Limitations and fallback

AI-written code can be plausible and wrong, as the timestamp bug shows, so
correctness rests on the gates, not on reading the diff. The AI worked from
the handoff prompts; anything underspecified there had to be caught at review.
Fallback: the handoff prompts are complete implementation specs, so any
component could have been written by hand against the same gate, as the
producers, evaluator, and constants module were.
