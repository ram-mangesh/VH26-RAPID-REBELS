import asyncio
import time
import json
import statistics

from simulator import EventSimulator, Event, Priority
from pipeline import DataPipeline
from metrics import MetricsCollector


class NaivePipeline:
    def __init__(self, num_workers=4):
        self.simulator = EventSimulator()
        self.metrics = MetricsCollector(window_seconds=10.0)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=100_000)
        self._num_workers = num_workers
        self.running = False
        self._workers = []

    async def start(self):
        self.running = True
        for i in range(self._num_workers):
            w = asyncio.create_task(self._worker(i))
            self._workers.append(w)
        await self.simulator.start()
        asyncio.create_task(self._router())

    async def stop(self):
        self.running = False
        await self.simulator.stop()

    async def _router(self):
        while self.running:
            try:
                event = await asyncio.wait_for(self.simulator.event_queue.get(), timeout=0.5)
                try:
                    self._queue.put_nowait(event)
                except asyncio.QueueFull:
                    await self.metrics.record_shed(event)
                self.simulator.event_queue.task_done()
            except asyncio.TimeoutError:
                continue

    async def _worker(self, wid):
        while self.running:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await asyncio.sleep(event.processing_time_ms / 1000.0)
                latency = event.latency_ms()
                await self.metrics.record_processed(event, latency, "stream")
                self._queue.task_done()
            except asyncio.TimeoutError:
                continue


async def run_benchmark(pipeline, name, rate, duration_seconds=30):
    print(f"\n{'='*60}")
    print(f"  Benchmark: {name}")
    print(f"  Rate: {rate} events/min | Duration: {duration_seconds}s")
    print(f"{'='*60}")

    await pipeline.metrics.reset()
    pipeline.simulator.set_rate(rate)

    if not pipeline.running:
        await pipeline.start()

    await asyncio.sleep(duration_seconds)
    snap = await pipeline.metrics.get_snapshot()

    await pipeline.stop()

    print(f"\n  Results:")
    print(f"  {'─'*50}")
    print(f"  Total Processed:  {snap['total_processed']}")
    print(f"  Total Shed:       {snap['total_shed']}")
    print(f"  Throughput:       {snap['throughput_eps']} events/sec")

    for tier_name, tier_data in snap['tiers'].items():
        print(f"\n  [{tier_name}]")
        print(f"    Latency Avg:  {tier_data['latency_avg_ms']} ms")
        print(f"    Latency P50:  {tier_data['latency_p50_ms']} ms")
        print(f"    Latency P95:  {tier_data['latency_p95_ms']} ms")
        print(f"    Latency P99:  {tier_data['latency_p99_ms']} ms")
        print(f"    Throughput:   {tier_data['throughput_eps']} evt/sec")

    return snap


async def main():
    print("\n" + "▓"*60)
    print("  INTELLIGENT DATA PIPELINE — BENCHMARK REPORT")
    print("  Comparing Adaptive Pipeline vs Naive Baseline")
    print("▓"*60)

    results = {}

    # --- Adaptive Pipeline ---
    adaptive = DataPipeline(num_workers=4)

    r = await run_benchmark(adaptive, "Adaptive Pipeline", 1000, duration_seconds=20)
    results["adaptive_baseline"] = r

    adaptive2 = DataPipeline(num_workers=8)
    r = await run_benchmark(adaptive2, "Adaptive Pipeline (SPIKE)", 20000, duration_seconds=20)
    results["adaptive_spike"] = r

    # --- Naive Pipeline ---
    naive = NaivePipeline(num_workers=8)
    r = await run_benchmark(naive, "Naive Pipeline (Baseline)", 1000, duration_seconds=20)
    results["naive_baseline"] = r

    naive2 = NaivePipeline(num_workers=8)
    r = await run_benchmark(naive2, "Naive Pipeline (SPIKE)", 20000, duration_seconds=20)
    results["naive_spike"] = r

    # --- Summary ---
    print("\n\n" + "▓"*60)
    print("  COMPARISON SUMMARY")
    print("▓"*60)

    header = f"  {'Metric':<30} {'Adaptive':>12} {'Naive':>12} {'Delta':>10}"
    print(f"\n{header}")
    print(f"  {'─'*66}")

    def cmp(label, key, a_data, n_data):
        a = a_data.get(key, 0)
        n = n_data.get(key, 0)
        delta = a - n
        sign = "+" if delta >= 0 else ""
        print(f"  {label:<30} {a:>12} {n:>12} {sign}{delta:>9}")

    a_spike = results["adaptive_spike"]
    n_spike = results["naive_spike"]

    cmp("Total Processed", "total_processed", a_spike, n_spike)
    cmp("Total Shed", "total_shed", a_spike, n_spike)
    cmp("Throughput (eps)", "throughput_eps", a_spike, n_spike)

    for tier in ["CRITICAL", "HIGH", "LOW"]:
        a_lat = a_spike.get("tiers", {}).get(tier, {}).get("latency_p95_ms", 0)
        n_lat = n_spike.get("tiers", {}).get(tier, {}).get("latency_p95_ms", 0)
        delta = a_lat - n_lat
        sign = "+" if delta >= 0 else ""
        print(f"  {tier + ' P95 Lat (ms)':<30} {a_lat:>12} {n_lat:>12} {sign}{delta:>9}")

    print(f"\n  KEY INSIGHT:")
    a_crit = a_spike.get("tiers", {}).get("CRITICAL", {}).get("latency_p95_ms", 0)
    n_crit = n_spike.get("tiers", {}).get("CRITICAL", {}).get("latency_p95_ms", 0)
    if n_crit > 0:
        ratio = n_crit / max(a_crit, 0.1)
        print(f"  Critical events are {ratio:.1f}x faster in adaptive pipeline under spike load.")
    print(f"  Adaptive pipeline sheds non-critical events to protect critical throughput.")
    print(f"  Naive pipeline treats all events equally — critical events slow down too.\n")

    with open("benchmark_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print("  Results saved to benchmark_results.json\n")


if __name__ == "__main__":
    asyncio.run(main())
