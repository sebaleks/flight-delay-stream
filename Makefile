# MSDS 682 streaming demo. Three targets matter:
#   make demo   stack up, contracts registered, week replayed, evaluation printed
#   make eval   the outcome-join evaluation report (headline + all counters)
#   make test   the streaming test suite + lint
# `make demo` recreates the topics first, so every run is a clean, deterministic
# replay. While the scoring consumer is not built yet (handoffs H2/H3), demo
# says so plainly and the evaluation shows every outcome in orphan_outcome.

UV := uv run --extra kafka --extra ml --extra serve --extra ingestion
TOPICS := flight.departures.v1 flight.outcomes.v1 flight.delay_risk.v1

.PHONY: demo eval test up down reset

up:
	docker compose up -d --wait

down:
	docker compose down -v

reset: up
	@for t in $(TOPICS); do $(UV) python -m streaming.admin --recreate $$t; done
	$(UV) python -m streaming.admin

demo: reset
	$(UV) python -m streaming.producer
	$(UV) python -m streaming.outcomes_producer
	@if [ -f streaming/consumer.py ]; then \
		$(UV) python -m streaming.consumer; \
	else \
		echo ""; \
		echo "NOTE: the scoring consumer is not built yet (docs/HANDOFF_PROMPTS.md"; \
		echo "H2/H3), so flight.delay_risk.v1 stays empty and the evaluation below"; \
		echo "reports every outcome as orphan_outcome. That is the expected state,"; \
		echo "not a failure."; \
		echo ""; \
	fi
	$(MAKE) eval

eval:
	$(UV) python -m streaming.evaluator

# terminal consumer over the risk topic: per-flight risk + cascade exposure.
# pass flags through, e.g. make ui ARGS="--origin ORD --min-risk 0.6"
ui:
	$(UV) python -m streaming.ui $(ARGS)

test:
	$(UV) python -m pytest streaming/ -q
	$(UV) ruff check streaming/ scripts/
