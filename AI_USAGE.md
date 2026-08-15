# AI usage

MSDS 682 final project. The AI element is disclosed and verified AI-assisted
development: AI coding assistants (Claude) helped write code and
documentation throughout the project, used collaboratively, and every
AI-assisted change was human reviewed before it shipped.

## The task AI owned

Drafting implementations of components the team had already specified:
streaming modules, tests, analysis scripts, and documentation drafts. The
architecture, contracts, leakage rules, and every design decision recorded
in `CLAUDE.md` were made by the team.

## Representative input and output

Input: a written specification for a component, including its exact
semantics and a pass gate (for example, full-week parity against the batch
feature mart). Output: the implementation and its tests, landed as reviewed
commits. The specifications are in `docs/HANDOFF_PROMPTS.md`.

## What we accepted and rejected

We accepted code only after it passed its predefined gate and a human
review. Along the way we rejected or corrected AI output, including a
timestamp unit bug caught by a golden-vector test and nondeterministic
output ordering rejected for breaking byte-identity. Design choices stayed
with the team.

## How we verified the result

Deterministic gates that do not trust the author: golden-vector parity
against the batch feature mart, a full-week rotation parity check, a leakage
test suite proving no post-departure field reaches the scorer, byte-identical
double runs, and a 59-test suite with lint (`make test`). Reported numbers
come from the conservation-tested evaluator, not from AI claims.

## Limitations and fallback

AI-written code can be plausible and wrong, so correctness rests on the
gates and review, not on trusting the diff. Fallback: every component had a
complete written spec and gate, so any part could have been written by hand
the same way, and several were.
