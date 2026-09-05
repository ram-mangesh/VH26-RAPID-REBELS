# Judge-Facing Demo Guide (5 minutes)

This is the exact script to convince the judges **this is not "just a UI"** — it is
an adaptive distributed data pipeline with a scored, traceable decision function,
dynamic worker scaling, idempotent at-least-once delivery, durable persistence,
and a measurable cost advantage over a naive design.

---

## The one question every judge mentally asks
> "If I throttle your dashboard's animations, what intelligence actually remains?"

**Your one-line answer:** the intelligence is a decision function
`ProcessingDecision = f(priority, queueSize, latency, workerLoad, dataSize, processingCost)`
that runs for **every event** — and the dashboard lets you *read that decision live*.

---

## 0. Setup (before judges arrive — 20s)

```bash
cd intelligent-pipeline
python server.py 8080        # pipeline + dashboard on :8080
```
Keep the browser open on `http://localhost:8080/` at baseline.

---

## 1. The "why" — Decision Trace (60s)

> "Every event is scored, not guessed."

**Do:** Point at the **Decision Trace** panel (green ADMIT rows).

- Each row = one live routing decision with the scored formula:
  `[ADMIT] payment CRITICAL · score=17.05 stream(n=1) · prio=0 lat=0 load=16.33 sat=0.11 size=0.61`
- **Critical events stream one-at-a-time and are NEVER shed.**
- Say: "The weight of the formula is set in `pipeline.py` — priority×3.0 ·
  latency×1.5 · load×1.0 · saturation×2.0 · size×0.5. A payment gets the highest
  urgency and is routed to a dedicated streaming worker instantly."

---

## 2. A 20× spike — watch the pipeline *decide*, not just *render* (90s)

**Do:** Press **Spike (20K/min)**.

- Watch **mode flip to SPIKE** and the overload light go amber.
- Point to the **Event Type Breakdown**: payment/order show **"never shed"**;
  click/log show **shed counts climbing**.
- Point to the **Decision Trace** now showing RED `[SHED]` rows with the exact
  reason: `prob_shed=0.15 load=0.02 spike=1`.
- Say: "We shed low-value logs/clicks first — they're the elastic buffer. We
  *never* shed a payment. That's not a dashboard animation; that's the backpressure
  policy running per-event (`backpressure.py`)."

**Numeric punchline:**
> "Critical P95 stayed around **~0.3s** at 20K/min while a baseline that can't shed
> would sit at **3.3s** — we measured it (benchmark_results.json)."

---

## 3. Fault injection — idempotent retry, *provably* (60s)

**Do:** In the **Fault Injection** panel, press **"⚡ Kill Worker (1-shot crash)"**.

- **Faults Injected** increments to 1.
- **Retries Performed** increments to 1.
- Say: "We just simulated a worker crashing mid-payment. The pipeline retried
  exactly once — no data lost."
- Then the kicker: point to **Events Persisted (SQLite)** and **Duplicates Blocked**.
- Say: "Every event is durably written to a SQLite sink keyed by `event_id`
  (PRIMARY KEY). The retried payment was written **exactly once** — the storage
  layer blocked the duplicate. That is at-least-once execution with exactly-once
  side effects, visible right here."

---

## 4. A/B — adaptive vs naive, measured not claimed (90s)

**Do:** Press **"▶ Run A/B"** and let it run ~25s.

- When it completes, point to the two columns:
  - **Adaptive: Critical P95 ≈ 390ms, shed 365**
  - **Naive: Critical P95 ≈ 6000ms, shed 0**
- Show the **23× / 15.8×** speedup number.
- Say: "Same spike, same 8 workers, identical events. The naive pipeline has no
  shedding, so it treats a log exactly like a payment — the queue grows, everyone
  waits. The adaptive pipeline sheds non-critical work to protect critical revenue
  events. This is what the problem statement asked for: priority-aware behavior
  under overload, and we can *measure* the difference."

---

## 5. Self-healing scale (optional, 30s)

- Leave it at spike; the worker count climbs under load, then drops back down.
- Say: "The `_scaling_loop` adds and removes workers based on queue depth within
  safe bounds — so the pipeline adapts capacity too, not just routing."

---

## Suggested closing line
> "It's not a dashboard with a pipeline behind it — it's a pipeline with a
> dashboard in front of it. The dashboard only exists to make the intelligence
> legible. Every number up there comes from an event that actually traversed
> priority queues, got scored, batched or shed, retried if it crashed, and was
> durably persisted — exactly once."

---

## Files that back every claim
| Claim | Evidence |
|---|---|
| Scored decision function | `pipeline.py` → `ScoredDecisionEngine.score_components()` |
| Critical never shed | `backpressure.py` (CRITICAL → always `return True`) |
| Idempotent retry | `pipeline.py` → `FaultInjector` + `_process_event` retry loop |
| Exactly-once persistence | `sink.py` → SQLite PRIMARY KEY + `upsert` blocks dupes |
| Measured adaptive vs naive | `benchmark_results.json`, `ab_compare.py` |
| Auto demo (baseline→spike→recover) | `DataPipeline.start_auto_demo()` |