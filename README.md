# Intelligent Data Pipeline for Optimized Data Processing

> UCET I 2026 HACK-o-THON — Pixels to Possibilities  
> Domain: Application Building Pipelines/Processing

A data pipeline that survives a 20x traffic spike by getting smarter about what to process now, what to batch, what to defer, and what to drop — without losing critical data.

## Quick Start

```bash
pip install aiohttp
python server.py
# Open http://localhost:8080 in your browser
```

## Architecture

```
[Event Simulator] ──> [Ingestion/Router] ──> [Priority Queues]
                                                    │
                              ┌─────────────────────┼─────────────────────┐
                              │                     │                     │
                         CRITICAL queue         HIGH queue            LOW queue
                         (workers x4)          (workers x2)       (batch proc x2)
                              │                     │                     │
                              └─────────────────────┼─────────────────────┘
                                                    │
                                              [Metrics Collector]
                                                    │
                                              [WebSocket] ──> [Live Dashboard]
```

### Components

| File | Purpose |
|------|---------|
| `simulator.py` | Generates events (payment, order, inventory, click, log) at adjustable rates |
| `pipeline.py` | Core pipeline: priority queues, adaptive router, workers, batch processors |
| `backpressure.py` | Shedding policy — critical events NEVER dropped |
| `metrics.py` | Latency (per-tier P50/P95/P99), throughput, shed counts |
| `server.py` | aiohttp server: REST API + WebSocket + static dashboard |
| `dashboard/` | Real-time HTML/JS/CSS dashboard |
| `benchmark.py` | Compares adaptive vs naive pipeline under baseline and spike |

## How It Works

### Priority Tiers
- **CRITICAL** (orders, payments): Always processed immediately, stream mode, NEVER shed
- **HIGH** (inventory updates): Processed with priority, batched under extreme spike
- **LOW** (logs, clicks): Micro-batched normally, aggressively batched + shed under spike

### Adaptive Behavior
- **Normal load (1K/min)**: All events processed with low latency. Low-priority events micro-batched (25 events, 200ms window).
- **Spike load (20K/min)**: Low-priority events aggressively batched (50 events, 500ms window). Non-critical events probabilistically shed to protect critical throughput.

### Shedding Policy
- Critical events: NEVER dropped. Backpressure applied upstream if queue nears capacity.
- High events: Shed when queue >85% full, with increasing probability.
- Low events: Shed when queue >60% full, with increasing probability.
- All shed decisions are logged and visible on the dashboard.

## Controls

| Button | Action |
|--------|--------|
| **Baseline (1K/min)** | Set simulator to 1,000 events/minute |
| **Spike (20K/min)** | Set simulator to 20,000 events/minute (20x spike) |
| **Stop/Start** | Pause/resume the pipeline |
| **Reset** | Clear all metrics and restart |

## Running the Benchmark

```bash
python benchmark.py
```

This runs both adaptive and naive pipelines under baseline (1K/min) and spike (20K/min) load, producing a comparison report with per-tier latency and throughput.

## Tech Stack

- **Python 3.10+** with asyncio
- **aiohttp** for HTTP server + WebSocket
- **Vanilla JS** dashboard (no framework dependencies)

## Deliverables

1. Working prototype (this repo)
2. Architecture diagram (see Architecture section above)
3. Benchmark report (`benchmark_results.json` after running `benchmark.py`)
4. Live dashboard at http://localhost:8080
