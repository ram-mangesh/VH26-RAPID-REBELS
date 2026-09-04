import asyncio
import time
from collections import defaultdict, deque


class MetricsCollector:
    def __init__(self, window_seconds: float = 5.0):
        self.window = window_seconds
        self._lock = asyncio.Lock()

        self.total_processed = 0
        self.total_shed = 0
        self.total_batched = 0
        self.total_streamed = 0
        self.total_retried = 0
        self.total_duplicates_detected = 0

        self._latencies = defaultdict(deque)
        self._throughput_samples = defaultdict(deque)
        self._all_samples = deque()
        self._shed_count = defaultdict(int)
        self._batch_count = defaultdict(int)

        self._report_counts = {"processed": defaultdict(int), "shed": defaultdict(int)}
        self._processing_times = defaultdict(list)

        self._last_report = time.monotonic()
        self._recent_throughput = 0.0

    async def record_processed(self, event, latency_ms: float, mode: str, retried=False):
        async with self._lock:
            tier = event.priority.name
            etype = event.event_type
            now = time.monotonic()
            self._latencies[tier].append((now, latency_ms))
            self._throughput_samples[tier].append(now)
            self._all_samples.append(now)
            self.total_processed += 1
            self._report_counts["processed"][etype] += 1
            self._processing_times[etype].append(latency_ms)
            if len(self._processing_times[etype]) > 500:
                self._processing_times[etype] = self._processing_times[etype][-500:]
            if mode == "batch":
                self.total_batched += 1
                self._batch_count[tier] += 1
            else:
                self.total_streamed += 1
            if retried:
                self.total_retried += 1

    async def record_shed(self, event):
        async with self._lock:
            tier = event.priority.name
            etype = event.event_type
            self.total_shed += 1
            self._shed_count[tier] += 1
            self._report_counts["shed"][etype] += 1

    async def record_duplicate(self):
        async with self._lock:
            self.total_duplicates_detected += 1

    def _cleanup_window(self, deq: deque):
        cutoff = time.monotonic() - self.window
        while deq and deq[0][0] < cutoff:
            deq.popleft()

    def _cleanup_throughput(self, deq: deque):
        cutoff = time.monotonic() - self.window
        while deq and deq[0] < cutoff:
            deq.popleft()

    async def get_event_type_breakdown(self) -> dict:
        async with self._lock:
            processed = dict(self._report_counts["processed"])
            shed = dict(self._report_counts["shed"])
            total_proc = max(sum(processed.values()), 1)
            breakdown = {}
            for etype in ["payment", "order", "inventory_update", "user_click", "app_log"]:
                p = processed.get(etype, 0)
                s = shed.get(etype, 0)
                lats = self._processing_times.get(etype, [])
                breakdown[etype] = {
                    "processed": p,
                    "shed": s,
                    "pct_processed": round(100 * p / total_proc, 2),
                    "latency_avg_ms": round(sum(lats) / len(lats), 2) if lats else 0,
                }
            return breakdown

    async def get_snapshot(self) -> dict:
        async with self._lock:
            now = time.monotonic()
            snapshot = {
                "total_processed": self.total_processed,
                "total_shed": self.total_shed,
                "total_batched": self.total_batched,
                "total_streamed": self.total_streamed,
                "total_retried": self.total_retried,
                "total_duplicates_detected": self.total_duplicates_detected,
                "tiers": {},
                "throughput_eps": 0.0,
                "timestamp": now,
            }

            total_eps = 0
            self._cleanup_throughput(self._all_samples)
            recent_total = len(self._all_samples) / max(self.window, 0.01)
            self._recent_throughput = recent_total
            for tier in ["CRITICAL", "HIGH", "LOW"]:
                self._cleanup_window(self._latencies[tier])
                self._cleanup_throughput(self._throughput_samples[tier])

                lats = [lat for _, lat in self._latencies[tier]]
                eps = len(self._throughput_samples[tier]) / max(self.window, 0.01)
                total_eps += eps

                tier_data = {
                    "latency_avg_ms": round(sum(lats) / len(lats), 2) if lats else 0,
                    "latency_p50_ms": round(self._percentile(lats, 0.5), 2) if lats else 0,
                    "latency_p95_ms": round(self._percentile(lats, 0.95), 2) if lats else 0,
                    "latency_p99_ms": round(self._percentile(lats, 0.99), 2) if lats else 0,
                    "throughput_eps": round(eps, 1),
                    "shed_count": self._shed_count.get(tier, 0),
                    "batch_count": self._batch_count.get(tier, 0),
                    "sample_count": len(lats),
                }
                snapshot["tiers"][tier] = tier_data

            snapshot["throughput_eps"] = round(total_eps, 1)
            snapshot["event_type_breakdown"] = await self._get_breakdown_locked()
            return snapshot

    async def _get_breakdown_locked(self) -> dict:
        processed = dict(self._report_counts["processed"])
        shed = dict(self._report_counts["shed"])
        total_proc = max(sum(processed.values()), 1)
        breakdown = {}
        for etype in ["payment", "order", "inventory_update", "user_click", "app_log"]:
            p = processed.get(etype, 0)
            s = shed.get(etype, 0)
            lats = self._processing_times.get(etype, [])
            breakdown[etype] = {
                "processed": p,
                "shed": s,
                "pct_processed": round(100 * p / total_proc, 2),
                "latency_avg_ms": round(sum(lats) / len(lats), 2) if lats else 0,
            }
        return breakdown

    @staticmethod
    def _percentile(data: list, p: float) -> float:
        if not data:
            return 0
        sorted_data = sorted(data)
        k = (len(sorted_data) - 1) * p
        f = int(k)
        c = f + 1
        if c >= len(sorted_data):
            return sorted_data[-1]
        d = k - f
        return sorted_data[f] + d * (sorted_data[c] - sorted_data[f])

    async def reset(self):
        async with self._lock:
            self.total_processed = 0
            self.total_shed = 0
            self.total_batched = 0
            self.total_streamed = 0
            self.total_retried = 0
            self.total_duplicates_detected = 0
            self._latencies.clear()
            self._throughput_samples.clear()
            self._all_samples.clear()
            self._shed_count.clear()
            self._batch_count.clear()
            self._report_counts["processed"].clear()
            self._report_counts["shed"].clear()
            self._processing_times.clear()
            self._recent_throughput = 0.0
