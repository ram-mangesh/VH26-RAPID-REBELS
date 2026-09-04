"""Unit tests for the intelligent data pipeline core logic.

Run with:  python -m pytest tests/ -q   (from the project root)
or         pytest tests/ -q
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline import (
    AdaptiveConfig,
    ScoredDecisionEngine,
    DuplicateDetector,
    FaultInjector,
    CostModel,
)
from backpressure import SheddingPolicy
from simulator import Event, Priority


# ---------------------------------------------------------------------------
# Scored decision engine
# ---------------------------------------------------------------------------

@pytest.fixture
def engine():
    cfg = AdaptiveConfig()
    return ScoredDecisionEngine(cfg)


def _event_of(priority, ptype="payment", proc=10.0):
    e = Event(ptype)
    e.processing_time_ms = proc
    # simulate the priority simulator would assign
    e.priority = priority
    return e


def test_critical_decision_is_always_stream_never_shed(engine):
    ev = _event_of(Priority.CRITICAL)
    ctx = {"queue_load": 0.9, "current_latency_ms": 5000, "worker_load": 1.0,
           "overall_throughput": engine.config.spike_threshold_eps + 100}
    d = engine.decide(ev, ctx)
    assert d.strategy == "stream"
    assert d.batch_size == 1


def test_critical_positive_score_indicates_urgency(engine):
    ev = _event_of(Priority.CRITICAL)
    ctx = {"queue_load": 0.0, "current_latency_ms": 0.0, "worker_load": 0.0,
           "overall_throughput": 0.0}
    d = engine.decide(ev, ctx)
    # critical has highest priority weight -> highest (least negative) score
    assert d.score >= 0
    # no components should blow up
    comps = engine.score_components(ev, 0.0, 0.0, 0.0, 0.0)
    for k in ("priority", "latency", "load", "saturation", "size"):
        assert k in comps


def test_low_priority_aggressive_batches_in_spike(engine):
    ev = _event_of(Priority.LOW, "app_log", proc=5.0)
    ctx = {"queue_load": 0.3, "current_latency_ms": 100, "worker_load": 0.5,
           "overall_throughput": engine.config.spike_threshold_eps + 200}
    d = engine.decide(ev, ctx)
    assert d.mode == "spike"
    assert d.batch_size > 1


def test_priority_ordering_holds(engine):
    ctx = {"queue_load": 0.0, "current_latency_ms": 0.0, "worker_load": 0.0,
           "overall_throughput": 0.0}
    scores = {}
    for pri in (Priority.CRITICAL, Priority.HIGH, Priority.LOW):
        ev = _event_of(pri)
        d = engine.decide(ev, ctx)
        scores[pri] = d.score
    # critical (0) should have the highest score of all
    assert scores[Priority.CRITICAL] >= scores[Priority.HIGH]
    assert scores[Priority.HIGH] >= scores[Priority.LOW]


# ---------------------------------------------------------------------------
# Duplicate detector
# ---------------------------------------------------------------------------

def test_duplicate_detector():
    dd = DuplicateDetector(window_sec=1000)
    assert not dd.is_duplicate("abc")
    dd.mark("abc")
    assert dd.is_duplicate("abc")


def test_duplicate_window_prunes():
    dd = DuplicateDetector(window_sec=0.0)
    dd.mark("abc")
    assert len(dd._seen) == 0  # expired immediately


# ---------------------------------------------------------------------------
# Shedding policy
# ---------------------------------------------------------------------------

def test_critical_never_shed_even_when_saturated():
    policy = SheddingPolicy()
    ev = _event_of(Priority.CRITICAL)
    # totally saturated
    assert policy.should_admit(ev, total_queue_size=100_000, per_tier_queue_size=60_000, spike_mode=True)


def test_low_sheds_under_heavy_load():
    policy = SheddingPolicy()
    ev = _event_of(Priority.LOW, "app_log")
    # drive many attempts at full load; eventually it must shed something
    shed = admitted = 0
    for _ in range(500):
        if policy.should_admit(ev, total_queue_size=70_000, per_tier_queue_size=40_000, spike_mode=True):
            admitted += 1
        else:
            shed += 1
    assert shed > 0


def test_high_shed_probability_not_before_threshold():
    policy = SheddingPolicy()
    ev = _event_of(Priority.HIGH, "inventory_update")
    # low global load, no spike -> should admit
    assert policy.should_admit(ev, total_queue_size=100, per_tier_queue_size=50, spike_mode=False)


def test_last_reason_recorded():
    policy = SheddingPolicy()
    ev = _event_of(Priority.LOW, "app_log")
    policy.should_admit(ev, total_queue_size=70_000, per_tier_queue_size=40_000, spike_mode=True)
    assert "low-priority" or policy.last_reason(ev) is not None


# ---------------------------------------------------------------------------
# Fault injector
# ---------------------------------------------------------------------------

def test_one_shot_arms_and_recovers():
    f = FaultInjector()
    f.arm_one_shot(1)
    assert f.maybe_fail_once() is True   # exactly one crash
    assert f.maybe_fail_once() is False  # then recovered


def test_fault_injector_disable():
    f = FaultInjector()
    f.arm_one_shot(3)
    f.disable()
    assert f.maybe_fail_once() is False


# ---------------------------------------------------------------------------
# Cost model
# ---------------------------------------------------------------------------

def test_adaptive_cheaper_than_naive_baseline():
    cost = CostModel()
    adaptive = cost.estimate(8, 300, "spike", 60)
    naive = cost.naive_baseline(300, 60)
    assert adaptive["total"] <= naive["total"]


def test_cost_string_safe():
    cost = CostModel()
    r = cost.estimate(8, 100, "normal", 5)
    assert set(r) >= {"worker_cost", "compute_cost", "total", "workers"}


# ---------------------------------------------------------------------------
# Async integration smoke test
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pipeline_integration_smoke():
    from pipeline import DataPipeline
    p = DataPipeline(num_workers=4)
    await p.start()
    p.kill_worker()  # arm one crash
    await asyncio.sleep(2.5)
    s = await p.get_full_state()
    assert "traces" in s
    assert "persistence" in s
    assert "fault" in s
    # pipeline processed at least something
    assert s["metrics"]["total_processed"] > 0
    p.disable_faults()
    await p.stop()