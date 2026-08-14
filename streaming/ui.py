"""Terminal consumer over the risk topic: per-flight risk and cascade exposure.

The planned interface for this project (docs/PLAN.md kickoff decision 6: "the
tail UI is a terminal consumer"). Two views, both read from
flight.delay_risk.v1 and nothing else:

  FLIGHT RISK    every scored departure, searchable, sorted by probability.
  CASCADE RISK   which delays would propagate furthest, ranked by this
                 flight's probability weighted by how tight the downstream
                 turnarounds are on the same aircraft that day.

The cascade view is the only surface where the rotation work becomes visible
to a user, and it needs no new model: every input is already on the topic,
because the events are keyed by tail and carry their own turnaround band.

    uv run --extra kafka --extra ml python -m streaming.ui
    uv run --extra kafka --extra ml python -m streaming.ui --origin ORD --min-risk 0.6
    uv run --extra kafka --extra ml python -m streaming.ui --cascade-only --limit 20
    uv run --extra kafka --extra ml python -m streaming.ui --follow

Determinism: every ordering is total, so two runs print identical bytes.
Colour is emitted only to a TTY, so piping to a file stays plain.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict

from streaming import constants as c

OUT_TOPIC = "flight.delay_risk.v1"
BAR_WIDTH = 10
# the order bands are reported in, tightest first: TURNAROUND_BANDS is
# authored no_inbound-first, which is not the order a reader wants
BAND_DISPLAY_ORDER = ("lt_35", "35_60", "60_120", "ge_120")


# ---- cascade scoring -------------------------------------------------------


def downstream_legs(events: list[dict]) -> dict[int, list[dict]]:
    """For each event index, the later legs the same aircraft flies that day.

    Keyed by (tail_number, flight_date) and ordered by event time, which is
    exactly the per-tail ordering the topic's tail keying guarantees. Events
    with no tail cannot be chained and map to an empty list; the caller
    reports them separately rather than scoring them as zero-exposure.
    """
    by_tail: dict[tuple, list[int]] = defaultdict(list)
    for i, e in enumerate(events):
        if e.get("tail_number"):
            by_tail[(e["tail_number"], str(e["flight_date"]))].append(i)

    out: dict[int, list[dict]] = {}
    for idxs in by_tail.values():
        ordered = sorted(idxs, key=lambda i: (events[i]["scored_at_ts_utc"], i))
        for rank, i in enumerate(ordered):
            out[i] = [events[j] for j in ordered[rank + 1 :]]
    return out


def cascade_score(event: dict, downstream: list[dict]) -> tuple[float, dict, int]:
    """(score, legs-by-band, legs whose band is unknown).

    score = P(delay) * sum of tightness weights over downstream legs whose
    turnaround band is known. A swap-shaped downstream leg has no knowable
    turnaround, so it contributes NOTHING to the score and is counted
    separately: imputing a weight there would invent signal the stream does
    not have, and the project counts unknown categories everywhere else.
    """
    by_band: dict[str, int] = defaultdict(int)
    unknown = 0
    weight = 0.0
    for leg in downstream:
        band = leg.get("turnaround_band")
        if band is None or band not in c.CASCADE_TIGHTNESS_WEIGHTS:
            unknown += 1
            continue
        by_band[band] += 1
        weight += c.CASCADE_TIGHTNESS_WEIGHTS[band]
    return float(event["delay_probability"]) * weight, dict(by_band), unknown


def rank_cascade(events: list[dict]) -> tuple[list[dict], int]:
    """Cascade rows, highest exposure first, plus the untailed count.

    MUST be given the full event set, never a filtered one. A chain is the
    aircraft's real itinerary, so dropping its intermediate legs first would
    splice a false one: filter to origin ORD and an ORD-BWI departure would
    appear to be followed by another ORD departure, skipping the leg that
    actually flew the aircraft back. Rows carry their source index so the
    caller can filter what it DISPLAYS without touching what it computed.
    """
    chains = downstream_legs(events)
    rows, no_tail = [], 0
    for i, e in enumerate(events):
        if not e.get("tail_number"):
            no_tail += 1
            continue
        legs = chains.get(i, [])
        score, by_band, unknown = cascade_score(e, legs)
        rows.append({
            "index": i,
            "event": e,
            "score": score,
            "downstream": len(legs),
            "by_band": by_band,
            "unknown": unknown,
            "chain": [f"{leg['origin']}-{leg['dest']}" for leg in legs],
        })
    rows.sort(key=_cascade_sort_key)
    return rows, no_tail


def _cascade_sort_key(row: dict) -> tuple:
    # total order: score, then event time, then identity. Without the full
    # tie-break two runs could disagree on equal-scored rows.
    e = row["event"]
    return (-row["score"], e["scored_at_ts_utc"], e["carrier"], e["flight_number"])


def _risk_sort_key(e: dict) -> tuple:
    return (-float(e["delay_probability"]), e["scored_at_ts_utc"],
            e["carrier"], e["flight_number"])


# ---- filtering -------------------------------------------------------------


def drop_warmup(events: list[dict], include: bool) -> list[dict]:
    """The warm-up day scored from a cold rotation cache, so its rows are not
    comparable; the evaluator excludes them into their own counter. Dropped
    before chaining, which is safe because chains key on (tail, flight_date)
    and so never span the warm-up boundary anyway."""
    if include:
        return events
    return [e for e in events if str(e.get("rotation_state_basis")) != "warmup"]


def apply_filters(events: list[dict], args: argparse.Namespace) -> list[dict]:
    """Display filters only. Never call this before building cascade chains."""
    out = events
    if args.origin:
        want = {o.upper() for o in args.origin}
        out = [e for e in out if e["origin"] in want]
    if args.carrier:
        want = {x.upper() for x in args.carrier}
        out = [e for e in out if e["carrier"] in want]
    if args.tail:
        want = {t.upper() for t in args.tail}
        out = [e for e in out if (e.get("tail_number") or "").upper() in want]
    if args.date:
        out = [e for e in out if str(e["flight_date"]) == args.date]
    if args.min_risk is not None:
        out = [e for e in out if float(e["delay_probability"]) >= args.min_risk]
    return out


# ---- rendering -------------------------------------------------------------


class Style:
    """ANSI only when stdout is a terminal; plain text when piped."""

    def __init__(self, enabled: bool) -> None:
        self.on = enabled

    def _w(self, code: str, s: str) -> str:
        return f"\033[{code}m{s}\033[0m" if self.on else s

    def bold(self, s: str) -> str:
        return self._w("1", s)

    def dim(self, s: str) -> str:
        return self._w("2", s)

    def risk(self, s: str, p: float) -> str:
        if not self.on:
            return s
        return self._w("31" if p >= 0.7 else "33" if p >= 0.5 else "32", s)


def bar(p: float) -> str:
    filled = int(round(p * BAR_WIDTH))
    return "#" * filled + "." * (BAR_WIDTH - filled)


def _hhmm(v: str) -> str:
    s = str(v).zfill(4)
    return f"{s[:2]}:{s[2:]}"


def render_flights(events: list[dict], limit: int, st: Style) -> list[str]:
    lines = [
        st.bold("FLIGHT DELAY RISK"),
        st.dim("  probability that the flight arrives 15+ minutes late, scored at its "
               "scheduled gate time"),
        "",
        st.dim(f"  {'date':<10} {'flight':<9} {'route':<9} {'dep':>5} {'tail':<8} "
               f"{'p':>6}  {'':<10} {'band':<9} {'rotation':<11} {'late':>5} {'canc':>5}"),
    ]
    for e in sorted(events, key=_risk_sort_key)[:limit]:
        p = float(e["delay_probability"])
        lines.append(
            f"  {str(e['flight_date']):<10} "
            f"{e['carrier'] + e['flight_number']:<9} "
            f"{e['origin'] + '-' + e['dest']:<9} "
            f"{_hhmm(e['crs_dep_time']):>5} "
            f"{(e.get('tail_number') or 'NOTAIL'):<8} "
            f"{st.risk(f'{p:.3f}', p):>6}  {bar(p):<10} "
            f"{e['risk_band']:<9} "
            f"{str(e['rotation_state_basis']):<11} "
            f"{(e.get('pressure_late_arrivals') or 0):>5} "
            f"{(e.get('pressure_cancellations') or 0):>5}"
        )
    return lines


def render_cascade(rows: list[dict], no_tail: int, limit: int, st: Style) -> list[str]:
    weights = ", ".join(f"{b} {c.CASCADE_TIGHTNESS_WEIGHTS[b]}" for b in BAND_DISPLAY_ORDER)
    lines = [
        st.bold("CASCADE RISK"),
        st.dim("  expected downstream exposure = P(delay) x sum of tightness weights over"),
        st.dim("  the later legs this aircraft flies today. Tighter turnarounds absorb"),
        st.dim(f"  less, so they weigh more: {weights}."),
        st.dim("  Weights are a stated judgment, not a measured quantity. Legs whose"),
        st.dim("  linkage is swap-shaped have no knowable turnaround and are counted"),
        st.dim("  under 'unk' rather than given a weight."),
        "",
        st.dim(f"  {'date':<10} {'flight':<9} {'route':<9} {'tail':<8} {'p':>6} "
               f"{'legs':>5} {'unk':>4} {'exposure':>9}  downstream"),
    ]
    for r in rows[:limit]:
        e = r["event"]
        p = float(e["delay_probability"])
        chain = " > ".join(r["chain"][:4]) + (" ..." if len(r["chain"]) > 4 else "")
        lines.append(
            f"  {str(e['flight_date']):<10} "
            f"{e['carrier'] + e['flight_number']:<9} "
            f"{e['origin'] + '-' + e['dest']:<9} "
            f"{(e.get('tail_number') or ''):<8} "
            f"{st.risk(f'{p:.3f}', p):>6} "
            f"{r['downstream']:>5} {r['unknown']:>4} {r['score']:>9.3f}  "
            f"{st.dim(chain)}"
        )
    if no_tail:
        lines += ["", st.dim(f"  {no_tail:,} events in the week have no tail number, so "
                             "no rotation chain exists to walk; excluded from this "
                             "ranking regardless of filters.")]
    return lines


def render_header(events: list[dict], shown: list[dict], st: Style) -> list[str]:
    dates = sorted({str(e["flight_date"]) for e in shown}) or \
        sorted({str(e["flight_date"]) for e in events})
    alerts = sum(1 for e in shown if e["alert"])
    model = next((e["model_run_id"] for e in events), "unknown")
    span = f"{dates[0]} to {dates[-1]}" if dates else "no events"
    warmup = sum(1 for e in events if str(e.get("rotation_state_basis")) == "warmup")
    lines = [
        st.bold("Gate-Time Delay Risk"),
        st.dim(f"  {len(shown):,} of {len(events):,} scored departures, {span}, "
               f"model {model}"),
        st.dim(f"  {alerts:,} at or above the {c.ALERT_THRESHOLD} alert threshold"),
    ]
    if warmup and not any(str(e.get("rotation_state_basis")) == "warmup" for e in shown):
        lines.append(st.dim(f"  {warmup:,} warm-up-day rows hidden (cold rotation state); "
                            "--include-warmup to show them"))
    return [*lines, ""]


# ---- follow mode -----------------------------------------------------------


def follow(args: argparse.Namespace, st: Style) -> None:
    """Live tail: one line per risk record as it arrives."""
    from confluent_kafka import Consumer, KafkaError
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    from streaming.admin import bootstrap, registry_url

    sr = SchemaRegistryClient({"url": registry_url()})
    deser = AvroDeserializer(sr)
    consumer = Consumer({
        "bootstrap.servers": bootstrap(),
        "group.id": "delay-risk-ui",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([OUT_TOPIC])
    print(st.bold(f"tailing {OUT_TOPIC} (ctrl-c to stop)"))
    ctx = SerializationContext(OUT_TOPIC, MessageField.VALUE)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise SystemExit(f"consumer error: {msg.error()}")
            e = deser(msg.value(), ctx)
            if not apply_filters(drop_warmup([e], args.include_warmup), args):
                continue
            p = float(e["delay_probability"])
            print(f"  {str(e['flight_date'])} {e['carrier'] + e['flight_number']:<9} "
                  f"{e['origin']}-{e['dest']:<4} {_hhmm(e['crs_dep_time'])} "
                  f"{(e.get('tail_number') or 'NOTAIL'):<8} "
                  f"{st.risk(f'{p:.3f}', p)} {bar(p)} {e['risk_band']}")
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        consumer.close()


# ---- entry point -----------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--origin", action="append", help="filter by origin airport, repeatable")
    ap.add_argument("--carrier", action="append", help="filter by carrier, repeatable")
    ap.add_argument("--tail", action="append", help="filter by tail number, repeatable")
    ap.add_argument("--date", help="filter to one flight date, YYYY-MM-DD")
    ap.add_argument("--min-risk", type=float, help="only flights at or above this probability")
    ap.add_argument("--include-warmup", action="store_true",
                    help="include the warm-up day, whose rotation state was still cold")
    ap.add_argument("--limit", type=int, default=25, help="rows per view (default 25)")
    ap.add_argument("--flights-only", action="store_true")
    ap.add_argument("--cascade-only", action="store_true")
    ap.add_argument("--follow", action="store_true", help="live tail instead of a summary")
    ap.add_argument("--no-color", action="store_true")
    args = ap.parse_args()

    st = Style(sys.stdout.isatty() and not args.no_color)
    if args.follow:
        follow(args, st)
        return

    from streaming.evaluator import _consume_all  # the proven batch reader

    events = _consume_all(OUT_TOPIC)
    if not events:
        raise SystemExit(
            f"no events on {OUT_TOPIC}. Run `make demo` first, which recreates the "
            "topics and scores the replay week."
        )
    base = drop_warmup(events, args.include_warmup)
    shown = apply_filters(base, args)

    out = render_header(events, shown, st)
    if not shown:
        out.append(st.dim("  no flights match those filters."))
    else:
        if not args.cascade_only:
            out += render_flights(shown, args.limit, st) + [""]
        if not args.flights_only:
            # chains over the FULL set, then keep only the rows on display:
            # filtering first would splice itineraries that never happened
            rows, no_tail = rank_cascade(base)
            keep = {id(e) for e in shown}
            rows = [r for r in rows if id(r["event"]) in keep]
            out += render_cascade(rows, no_tail, args.limit, st)
    print("\n".join(out))


if __name__ == "__main__":
    main()
