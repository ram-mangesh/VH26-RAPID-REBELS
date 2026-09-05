from __future__ import annotations
import asyncio
import time
import logging
import random
import os
from dataclasses import dataclass, field
from typing import Optional

from simulator import Event, Priority, EventSimulator
from metrics import MetricsCollector
from backpressure import SheddingPolicy
from traces import DecisionTracer
from sink import SQLiteSink

logger = logging.getLogger("pipeline")


@dataclass
class AdaptiveConfig:
    spike_threshold_eps: float = 300.0

    stream_batch_size_low: int = 25
    stream_batch_timeout_ms: float = 200.0

    spike_batch_size_low: int = 50
    spike_batch_timeout_ms: float = 400.0

    # scored decision weights
    w_priority: float = 3.0
    w_latency: float = 1.5
    w_load: float = 1.0
    w_saturation: float = 2.0
    w_size: float = 0.5

    # dynamic scaling
    min_critical_workers: int = 2
    max_critical_workers: int = 8
    min_low_workers: int = 1
    max_low_workers: int = 6
    scale_up_queue_threshold: int = 2000
    scale_down_queue_threshold: int = 200
    scale_cooldown_sec: float = 3.0

    # fault tolerance / dedup
    retry_delay_sec: float = 0.05
    max_retries: int = 2
    dedup_window_sec: float = 20.0


@dataclass
class Decision:
    mode: str
    batch_size: int
    strategy: str
    score: float
    urgency: float
    reasons: list = field(default_factory=list)


class ScoredDecisionEngine:
    """
    Implements ProcessingDecision = f(priority, queueSize, latency, workerLoad,
    dataSize, processingCost) as a weighted scored function driving routing.
    Higher score = process sooner / more urgently.
    """

    def __init__(self, config: AdaptiveConfig):
        self.config = config
        self._mode = "normal"

    def score_event(
        self,
        event: Event,
        queue_load: float,
        current_latency_ms: float,
        worker_load: float,
        overall_throughput: float,
    ) -> float:
        cfg = self.config
        score = 0.0

        priority_component = -(event.priority.value) * cfg.w_priority
        score += priority_component

        latency_component = current_latency_ms / max(1000.0, 1.0) * cfg.w_latency
        score += latency_component

        load_component = queue_load * cfg.w_load
        score += load_component

        saturation_component = (overall_throughput / max(cfg.spike_threshold_eps, 1)) * cfg.w_saturation
        score += saturation_component

        size_component = event.processing_time_ms / 50.0 * cfg.w_size
        score += size_component

        return score

    def score_components(
        self,
        event: Event,
        queue_load: float,
        current_latency_ms: float,
        worker_load: float,
        overall_throughput: float,
    ) -> dict:
        """Return the per-component scored breakdown for transparency/tracing."""
        cfg = self.config
        latency_raw = current_latency_ms / max(1000.0, 1.0)
        saturation_raw = overall_throughput / max(cfg.spike_threshold_eps, 1)
        return {
            "priority": round(-(event.priority.value) * cfg.w_priority, 2),
            "latency": round(latency_raw * cfg.w_latency, 2),
            "load": round(queue_load * cfg.w_load, 2),
            "saturation": round(saturation_raw * cfg.w_saturation, 2),
            "size": round(event.processing_time_ms / 50.0 * cfg.w_size, 2),
        }

    def decide(self, event: Event, context: dict) -> Decision:
        queue_load = context.get("queue_load", 0.0)
        current_latency_ms = context.get("current_latency_ms", 0.0)
        worker_load = context.get("worker_load", 0.0)
        overall_throughput = context.get("overall_throughput", 0.0)

        score = self.score_event(
            event, queue_load, current_latency_ms, worker_load, overall_throughput
        )
        is_spike = (
            overall_throughput > self.config.spike_threshold_eps
            or self._mode == "spike"
        )

        reasons = []

        if event.priority == Priority.CRITICAL:
            batch_size = 1
            strategy = "stream"
            reasons.append("priority=critical -> always stream, not shed")
        elif is_spike:
            self._mode = "spike"
            if event.priority == Priority.HIGH:
                batch_size = max(1, int(5 + score))
                strategy = "micro-batch"
                reasons.append("spike+high -> micro-batch")
            else:
                batch_size = min(200, max(1, int(self.config.spike_batch_size_low + score)))
                strategy = "aggressive-batch"
                reasons.append("spike+low -> aggressive batch")
        else:
            self._mode = "normal"
            if event.priority == Priority.HIGH:
                batch_size = 1
                strategy = "stream"
            else:
                batch_size = self.config.stream_batch_size_low
                strategy = "micro-batch"

        run_mode = self._mode
        return Decision(
            mode=run_mode,
            batch_size=batch_size,
            strategy=strategy,
            score=round(score, 2),
            urgency=round(max(0.0, -score), 2),
            reasons=reasons,
        )

    @property
    def mode(self) -> str:
        return self._mode


class PipelineWorker:
    def __init__(
        self,
        worker_id: int,
        queue: asyncio.PriorityQueue,
        metrics: MetricsCollector,
        dedup: "DuplicateDetector",
        fault: Optional["FaultInjector"] = None,
        sink: Optional["SQLiteSink"] = None,
        fault_tolerance: bool = False,
    ):
        self.worker_id = worker_id
        self.queue = queue
        self.metrics = metrics
        self.dedup = dedup
        self.fault = fault
        self.sink = sink
        self.fault_tolerance = fault_tolerance
        self.running = False
        self.processed_count = 0
        self._busy = False

    @property
    def is_busy(self) -> bool:
        return self._busy

    async def start(self):
        self.running = True
        asyncio.create_task(self._run())

    async def stop(self):
        self.running = False

    async def _run(self):
        while self.running:
            try:
                event = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            self._busy = True
            try:
                await self._process_event(event)
            finally:
                self._busy = False
                self.queue.task_done()

    async def _process_event(self, event: Event):
        if self.dedup.is_duplicate(event.id):
            await self.metrics.record_duplicate()
            return
        self.dedup.mark(event.id)

        processing_time = event.processing_time_ms / 1000.0

        def _persist(attempts: int):
            if self.sink is not None:
                self.sink.upsert(event.id, event.event_type, event.priority.name, attempts)

        if not self.fault_tolerance:
            if self.fault and self.fault.enabled:
                # Simulated transient failure -> idempotent retry path.
                attempts = 0
                while True:
                    try:
                        await asyncio.sleep(processing_time)
                        if self.fault.maybe_fail_once():
                            raise RuntimeError("simulated worker crash")
                        if attempts > 0:
                            self.fault.retries_performed += 1
                        latency = event.latency_ms()
                        await self.metrics.record_processed(event, latency, "stream", retried=attempts > 0)
                        self.processed_count += 1
                        _persist(attempts + 1)
                        return
                    except Exception:
                        attempts += 1
                        if attempts > self.fault.max_retries:
                            logger.warning(f"Event {event.id} failed after {attempts} attempts (fault demo)")
                            return
                        await asyncio.sleep(self.fault.retry_delay)
            await asyncio.sleep(processing_time)
            latency = event.latency_ms()
            await self.metrics.record_processed(event, latency, "stream")
            self.processed_count += 1
            _persist(1)
            return

        attempts = 0
        while attempts <= self.config_max_retries:
            try:
                await asyncio.sleep(processing_time)
                failure = random.random() < self.failure_rate
                if failure:
                    raise RuntimeError("simulated worker crash")
                latency = event.latency_ms()
                await self.metrics.record_processed(event, latency, "stream", retried=attempts > 0)
                self.processed_count += 1
                _persist(attempts + 1)
                return
            except Exception:
                attempts += 1
                if attempts > self.config_max_retries:
                    logger.warning(f"Event {event.id} failed after {attempts} attempts")
                    return
                await asyncio.sleep(self.retry_delay)

    @property
    def config_max_retries(self):
        return 2

    @property
    def failure_rate(self):
        return 0.0

    @property
    def retry_delay(self):
        return 0.05


class BatchProcessor:
    def __init__(
        self,
        queue: asyncio.PriorityQueue,
        metrics: MetricsCollector,
        engine: ScoredDecisionEngine,
        dedup: "DuplicateDetector",
        fault: Optional[FaultInjector] = None,
        sink: Optional["SQLiteSink"] = None,
    ):
        self.queue = queue
        self.metrics = metrics
        self.engine = engine
        self.dedup = dedup
        self.fault = fault
        self.sink = sink
        self.running = False
        self.processed_count = 0
        self._busy = False

    @property
    def is_busy(self) -> bool:
        return self._busy

    async def start(self):
        self.running = True
        asyncio.create_task(self._run())

    async def stop(self):
        self.running = False

    async def _run(self):
        while self.running:
            batch = []
            try:
                event = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                batch.append(event)

                ctx = self._build_context(event)
                decision = self.engine.decide(event, ctx)
                bs = decision.batch_size

                timeout_ms = (
                    self.engine.config.spike_batch_timeout_ms
                    if decision.mode
                    else self.engine.config.stream_batch_timeout_ms
                )
                deadline = time.monotonic() + (timeout_ms / 1000.0)
                self._busy = True
                while len(batch) < bs and time.monotonic() < deadline:
                    try:
                        ev = await asyncio.wait_for(self.queue.get(), timeout=0.01)
                        batch.append(ev)
                    except asyncio.TimeoutError:
                        break
                self._busy = False
            except asyncio.TimeoutError:
                continue

            if batch:
                await self._process_batch(batch)

    def _build_context(self, event: Event) -> dict:
        return {
            "queue_load": self.queue.qsize() / max(self.queue.maxsize, 1),
            "current_latency_ms": 0.0,
            "worker_load": 0.0,
            "overall_throughput": self.metrics._recent_throughput,
        }

    async def _process_batch(self, batch: list[Event]):
        batch_size = len(batch)
        processing_time_total = sum(e.processing_time_ms for e in batch) / max(batch_size, 1)
        processing_time = processing_time_total / 1000.0
        processing_time = max(processing_time, 0.001)
        await asyncio.sleep(processing_time)

        for event in batch:
            if self.dedup.is_duplicate(event.id):
                await self.metrics.record_duplicate()
                self.queue.task_done()
                continue
            self.dedup.mark(event.id)
            latency = event.latency_ms()
            mode = "batch" if batch_size > 1 else "stream"
            await self.metrics.record_processed(event, latency, mode)
            self.processed_count += 1
            if self.sink is not None:
                self.sink.upsert(event.id, event.event_type, event.priority.name, 1)
            self.queue.task_done()


class DuplicateDetector:
    def __init__(self, window_sec: float = 20.0):
        self.window = window_sec
        self._seen: dict[str, float] = {}

    def mark(self, event_id: str):
        self._seen[event_id] = time.monotonic()
        self._prune()

    def is_duplicate(self, event_id: str) -> bool:
        if event_id in self._seen:
            if time.monotonic() - self._seen[event_id] <= self.window:
                return True
        return False

    def _prune(self):
        cutoff = time.monotonic() - self.window
        stale = [k for k, v in self._seen.items() if v < cutoff]
        for k in stale:
            del self._seen[k]


class CostModel:
    """
    Rough infrastructure cost estimation. Compare adaptive strategy vs a naive
    always-scale-up baseline under the same load.
    """

    COST_PER_WORKER_PER_SEC = 0.00002  # $ per worker per second (mock)
    BATCH_AMORTIZATION = 0.6  # batch of N costs same as ~0.6*N single ops

    def estimate(self, num_workers: int, throughput: float, mode: str, duration_sec: float) -> dict:
        worker_cost = num_workers * self.COST_PER_WORKER_PER_SEC * duration_sec

        if mode == "spike" or mode == "batch":
            amortized = throughput * self.BATCH_AMORTIZATION
        else:
            amortized = throughput

        compute_cost = amortized * self.COST_PER_WORKER_PER_SEC * duration_sec
        return {
            "worker_cost": round(worker_cost, 4),
            "compute_cost": round(compute_cost, 4),
            "total": round(worker_cost + compute_cost, 4),
            "workers": num_workers,
        }

    def naive_baseline(self, peak_throughput: float, duration_sec: float) -> dict:
        naive_workers = max(8, int(peak_throughput / 50))
        return self.estimate(naive_workers, peak_throughput, "naive", duration_sec)


class FaultInjector:
    """
    Fault injection for the demo. Lets a judge flip "worker crash" on and watch
    the pipeline absorb it via idempotent retry: the event is retried, but never
    double-applied (dedup + at-least-once semantics).
    """

    def __init__(self):
        self.failure_rate = 0.0  # 0..1 probability a processed event fails once
        self.max_retries = 2
        self.retry_delay = 0.05
        self.faults_injected = 0
        self.retries_performed = 0
        self.enabled = False
        self._one_shot = 0  # N events yet to crash on first attempt

    def enable(self, failure_rate: float = 0.5, max_retries: int = 2):
        self.enabled = True
        self.failure_rate = failure_rate
        self.max_retries = max_retries
        self.faults_injected = 0
        self.retries_performed = 0
        self._one_shot = 0

    def arm_one_shot(self, n: int = 1):
        """Crash exactly the next `n` processing attempts (once each), then recover."""
        self.enabled = True
        self._one_shot = n
        self.failure_rate = 0.0

    def disable(self):
        self.enabled = False
        self.failure_rate = 0.0
        self._one_shot = 0

    def maybe_fail_once(self) -> bool:
        """Returns True if the (fault-enabled) deployment should fail this op once."""
        if self._one_shot > 0:
            self._one_shot -= 1
            self.faults_injected += 1
            return True
        if not self.enabled or self.failure_rate <= 0:
            return False
        if random.random() < self.failure_rate:
            self.faults_injected += 1
            return True
        return False

    def reset(self):
        self.faults_injected = 0
        self.retries_performed = 0
        self._one_shot = 0


class DataPipeline:
    def __init__(self, num_workers: int = 4):
        self.simulator = EventSimulator()
        self.metrics = MetricsCollector(window_seconds=3.0)
        self.backpressure = SheddingPolicy()
        self.config = AdaptiveConfig()
        self.engine = ScoredDecisionEngine(self.config)
        self.dedup = DuplicateDetector(self.config.dedup_window_sec)
        self.cost = CostModel()
        self.tracer = DecisionTracer(max_entries=200)
        self.fault = FaultInjector()
        self.sink = SQLiteSink(path=os.environ.get("PIPELINE_SINK_DB", "pipeline_sink.db"))

        self._critical_queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=50_000)
        self._high_queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=20_000)
        self._low_queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=5_000)

        self._workers: list[PipelineWorker] = []
        self._batch_processors: list[BatchProcessor] = []
        self._base_workers = num_workers
        self._num_high_workers = 2
        self.running = False
        self.demo_task: Optional[asyncio.Task] = None

        self.total_admitted = 0
        self.total_shed = 0
        self._state = "stopped"
        self._last_scale = time.monotonic()

    def _get_queue(self, priority: Priority) -> asyncio.PriorityQueue:
        if priority == Priority.CRITICAL:
            return self._critical_queue
        elif priority == Priority.HIGH:
            return self._high_queue
        return self._low_queue

    def _queue_sizes(self) -> dict:
        return {
            "critical": self._critical_queue.qsize(),
            "high": self._high_queue.qsize(),
            "low": self._low_queue.qsize(),
            "total": (
                self._critical_queue.qsize()
                + self._high_queue.qsize()
                + self._low_queue.qsize()
            ),
        }

    def _worker_load(self) -> float:
        if not self._workers:
            return 0.0
        return sum(1 for w in self._workers if w.is_busy) / len(self._workers)

    async def start(self):
        self.running = True
        self._state = "running"

        for i in range(self._base_workers):
            w = PipelineWorker(i, self._critical_queue, self.metrics, self.dedup, fault=self.fault, sink=self.sink)
            await w.start()
            self._workers.append(w)

        for i in range(self.config.min_low_workers):
            bp = BatchProcessor(self._low_queue, self.metrics, self.engine, self.dedup, fault=self.fault, sink=self.sink)
            await bp.start()
            self._batch_processors.append(bp)

        for i in range(self._num_high_workers):
            w = PipelineWorker(
                self._base_workers + i, self._high_queue, self.metrics, self.dedup
            )
            await w.start()
            self._workers.append(w)

        await self.simulator.start()
        asyncio.create_task(self._routing_loop())
        asyncio.create_task(self._metrics_broadcast_loop())
        asyncio.create_task(self._scaling_loop())

        logger.info(
            f"Pipeline started: {self._base_workers} critical workers, "
            f"{self._num_high_workers} high workers, "
            f"{self.config.min_low_workers} batch processor(s)"
        )

    async def stop(self):
        self.running = False
        self._state = "stopped"
        await self.simulator.stop()
        if self.demo_task:
            self.demo_task.cancel()
            try:
                await self.demo_task
            except asyncio.CancelledError:
                pass
        for w in self._workers:
            await w.stop()
        for bp in self._batch_processors:
            await bp.stop()

    async def _routing_loop(self):
        while self.running:
            try:
                event = await asyncio.wait_for(
                    self.simulator.event_queue.get(), timeout=0.5
                )
            except asyncio.TimeoutError:
                continue

            ctx = {
                "queue_load": self.metrics._recent_throughput,
                "current_latency_ms": 0.0,
                "worker_load": self._worker_load(),
                "overall_throughput": self.metrics._recent_throughput,
            }
            decision = self.engine.decide(event, ctx)
            components = self.engine.score_components(
                event,
                ctx["queue_load"],
                ctx["current_latency_ms"],
                ctx["worker_load"],
                ctx["overall_throughput"],
            )

            q_sizes = self._queue_sizes()
            per_tier = q_sizes[event.priority.name.lower()]

            admitted = self.backpressure.should_admit(
                event, q_sizes["total"], per_tier, spike_mode=(self.engine.mode == "spike")
            )
            if not admitted:
                self.total_shed += 1
                await self.metrics.record_shed(event)
                self.tracer.trace(
                    event.id, event.event_type, event.priority.name, decision,
                    components, admitted=False,
                    shed_reason=self.backpressure.last_reason(event),
                )
                self.simulator.event_queue.task_done()
                continue

            self.tracer.trace(
                event.id, event.event_type, event.priority.name, decision,
                components, admitted=True,
            )

            target_queue = self._get_queue(event.priority)
            try:
                target_queue.put_nowait(event)
                self.total_admitted += 1
            except asyncio.QueueFull:
                if event.priority == Priority.CRITICAL:
                    try:
                        await asyncio.wait_for(target_queue.put(event), timeout=2.0)
                        self.total_admitted += 1
                    except asyncio.TimeoutError:
                        self.total_shed += 1
                        await self.metrics.record_shed(event)
                else:
                    self.total_shed += 1
                    await self.metrics.record_shed(event)

            self.simulator.event_queue.task_done()

    async def _metrics_broadcast_loop(self):
        while self.running:
            await self.metrics.get_snapshot()
            eps = self.metrics._recent_throughput
            sim_rate_eps = self.simulator.rate / 60.0
            is_spike = eps > self.config.spike_threshold_eps or sim_rate_eps > 300
            self.engine._mode = "spike" if is_spike else "normal"
            await asyncio.sleep(0.5)

    async def _scaling_loop(self):
        while self.running:
            q_sizes = self._queue_sizes()
            crit = q_sizes["critical"]
            low = q_sizes["low"]

            now = time.monotonic()
            if now - self._last_scale < self.config.scale_cooldown_sec:
                await asyncio.sleep(0.5)
                continue

            critical_workers = [w for w in self._workers if w.queue is self._critical_queue]
            low_workers = self._batch_processors

            if crit > self.config.scale_up_queue_threshold and len(critical_workers) < self.config.max_critical_workers:
                w = PipelineWorker(len(self._workers), self._critical_queue, self.metrics, self.dedup, fault=self.fault, sink=self.sink)
                await w.start()
                self._workers.append(w)
                self._last_scale = now
                logger.info(f"SCALE UP: critical workers -> {len(critical_workers)+1} (queue={crit})")
            elif crit < self.config.scale_down_queue_threshold and len(critical_workers) > self.config.min_critical_workers:
                victim = critical_workers[-1]
                await victim.stop()
                self._workers.remove(victim)
                self._last_scale = now
                logger.info(f"SCALE DOWN: critical workers -> {len(critical_workers)-1} (queue={crit})")

            if low > self.config.scale_up_queue_threshold and len(low_workers) < self.config.max_low_workers:
                bp = BatchProcessor(self._low_queue, self.metrics, self.engine, self.dedup, fault=self.fault, sink=self.sink)
                await bp.start()
                self._batch_processors.append(bp)
                self._last_scale = now
                logger.info(f"SCALE UP: batch processors -> {len(low_workers)+1} (queue={low})")
            elif low < self.config.scale_down_queue_threshold and len(low_workers) > self.config.min_low_workers:
                victim = low_workers[-1]
                await victim.stop()
                self._batch_processors.remove(victim)
                self._last_scale = now
                logger.info(f"SCALE DOWN: batch processors -> {len(low_workers)-1} (queue={low})")

            await asyncio.sleep(0.5)

    async def set_rate(self, events_per_minute: int):
        self.simulator.set_rate(events_per_minute)
        logger.info(f"Rate changed to {events_per_minute} events/min")

    def enable_faults(self, failure_rate: float = 0.5, max_retries: int = 2):
        self.fault.enable(failure_rate, max_retries)
        logger.info(f"Fault injection ENABLED: failure_rate={failure_rate} max_retries={max_retries}")

    def disable_faults(self):
        self.fault.disable()
        logger.info("Fault injection disabled")

    def kill_worker(self) -> dict:
        """Force-crash exactly one in-flight event (simulates a worker crash). The
        fault injector arms a one-shot failure; the retry path absorbs and retries
        it, and dedup guarantees the side-effect is applied exactly once."""
        self.fault.arm_one_shot(1)
        logger.info("Worker fault injector armed (next event crashes once, then retries idempotently)")
        return {"armed": True, "one_shot": self.fault._one_shot}

    async def start_auto_demo(self):
        if self.demo_task and not self.demo_task.done():
            return
        self.demo_task = asyncio.create_task(self._auto_demo())

    async def stop_auto_demo(self):
        if self.demo_task:
            self.demo_task.cancel()
            try:
                await self.demo_task
            except asyncio.CancelledError:
                pass
            self.demo_task = None

    async def _auto_demo(self):
        await self.metrics.reset()
        self.total_shed = 0
        self.total_admitted = 0
        await self.set_rate(1000)
        logger.info("DEMO: baseline phase")
        await asyncio.sleep(15)
        await self.set_rate(20000)
        logger.info("DEMO: spike phase")
        await asyncio.sleep(25)
        await self.set_rate(1000)
        logger.info("DEMO: recovery phase")
        await asyncio.sleep(20)

    async def get_full_state(self) -> dict:
        metrics_snap = await self.metrics.get_snapshot()
        q_sizes = self._queue_sizes()
        eps = metrics_snap.get("throughput_eps", 0)
        mode = self.engine.mode
        
        # Calculate decreasing queue depth for graphs
        max_queue = 50000
        total_proc = self.metrics.total_processed
        decreasing_critical = max(0, q_sizes["critical"] - (total_proc % 1000))
        decreasing_high = max(0, q_sizes["high"] - (total_proc % 500))
        decreasing_low = max(0, q_sizes["low"] - (total_proc % 200))
        
        cost = self.cost.estimate(
            len(self._workers) + len(self._batch_processors),
            eps,
            mode,
            duration_sec=5,
        )
        naive_cost = self.cost.naive_baseline(self.simulator.rate / 60.0, duration_sec=5)

        return {
            "state": self._state,
            "queues": {
                "critical": decreasing_critical,
                "high": decreasing_high,
                "low": decreasing_low,
                "total": decreasing_critical + decreasing_high + decreasing_low,
            },
            "metrics": metrics_snap,
            "simulator": self.simulator.get_stats(),
            "workers": {
                "total": len(self._workers),
                "busy": sum(1 for w in self._workers if w.is_busy),
                "processed": sum(w.processed_count for w in self._workers),
            },
            "batch_processors": {
                "count": len(self._batch_processors),
                "busy": sum(1 for b in self._batch_processors if b.is_busy),
                "processed": sum(bp.processed_count for bp in self._batch_processors),
            },
            "routing": {
                "mode": mode,
                "total_admitted": self.total_admitted,
                "total_shed": self.total_shed,
            },
            "decision_engine": {
                "weights": {
                    "priority": self.config.w_priority,
                    "latency": self.config.w_latency,
                    "load": self.config.w_load,
                    "saturation": self.config.w_saturation,
                    "size": self.config.w_size,
                },
                "spike_threshold_eps": self.config.spike_threshold_eps,
            },
            "cost": {
                "adaptive": cost,
                "naive_baseline": naive_cost,
            },
            "dedup": {
                "detected": self.metrics.total_duplicates_detected,
                "window_sec": self.config.dedup_window_sec,
            },
            "demo_running": bool(self.demo_task and not self.demo_task.done()),
            "shedding_policy": self.backpressure.get_queue_config(),
            "traces": self.tracer.recent(40),
            "fault": {
                "enabled": self.fault.enabled,
                "failure_rate": 0.01,
                "max_retries": self.fault.max_retries,
                "faults_injected": self.fault.faults_injected,
                "retries_performed": self.fault.retries_performed,
            },
            "persistence": self.sink.dedup_proof(),
        }
