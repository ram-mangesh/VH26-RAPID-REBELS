import asyncio
import logging
import random
from enum import IntEnum

from simulator import Event, Priority

logger = logging.getLogger("backpressure")


class SheddingPolicy:
    def __init__(
        self,
        critical_max_queue: int = 50_000,
        high_max_queue: int = 20_000,
        low_max_queue: int = 5_000,
        low_shed_start: float = 0.6,
        high_shed_start: float = 0.85,
        critical_shed_start: float = 0.95,
        global_shed_start: float = 0.5,
    ):
        self.critical_max = critical_max_queue
        self.high_max = high_max_queue
        self.low_max = low_max_queue
        self.low_shed_threshold = low_shed_start
        self.high_shed_threshold = high_shed_start
        self.critical_shed_threshold = critical_shed_start
        self.global_shed_start = global_shed_start
        self.global_capacity = critical_max_queue + high_max_queue + low_max_queue
        self._last_reason: dict = {}

    def _get_max(self, priority: Priority) -> int:
        if priority == Priority.CRITICAL:
            return self.critical_max
        elif priority == Priority.HIGH:
            return self.high_max
        return self.low_max

    def _get_shed_threshold(self, priority: Priority) -> float:
        if priority == Priority.CRITICAL:
            return self.critical_shed_threshold
        elif priority == Priority.HIGH:
            return self.high_shed_threshold
        return self.low_shed_threshold

    def last_reason(self, event: Event) -> str:
        """Human-readable reason for the most recent shed decision for an event."""
        return self._last_reason.get(event.id, "low-priority elastic buffer under load")

    def _shed(self, event: Event, reason: str):
        self._last_reason[event.id] = reason
        logger.info(f"SHED {event.event_type} ({event.priority.name}) {reason}")
        if len(self._last_reason) > 512:
            self._last_reason.pop(next(iter(self._last_reason)))

    def should_admit(self, event: Event, total_queue_size: int, per_tier_queue_size: int, spike_mode: bool = False) -> bool:
        # Global system pressure drives shedding: when the whole pipeline is
        # backed up past a threshold, non-critical events start to shed.
        global_load = total_queue_size / max(self.global_capacity, 1)
        max_q = self._get_max(event.priority)
        threshold = self._get_shed_threshold(event.priority)
        local_load = per_tier_queue_size / max(max_q, 1)

        # Critical events are NEVER shed — hard constraint. Apply backpressure upstream.
        if event.priority == Priority.CRITICAL:
            if local_load >= self.critical_shed_threshold or global_load >= 0.9:
                logger.warning(
                    f"CRITICAL pipeline saturated (global={global_load:.2f}, "
                    f"critical={per_tier_queue_size}/{max_q}). Critical events are "
                    f"NEVER shed — backpressure applied upstream."
                )
            return True

        load = max(global_load, local_load)

        if load >= 1.0 or per_tier_queue_size >= max_q:
            self._shed(event, f"queue_full={per_tier_queue_size}/{max_q} global={global_load:.2f}")
            return False

        # During a spike, low-priority events act as the elastic buffer: they
        # are progressively sampled/dropped to protect critical throughput.
        # A base shed probability ensures graceful degradation is visible even
        # before queues back up, ramping with load.
        effective_threshold = threshold
        if spike_mode and event.priority == Priority.LOW:
            effective_threshold = min(effective_threshold, 0.25)
            base_shed_prob = 0.15
        elif spike_mode and event.priority == Priority.HIGH:
            effective_threshold = min(effective_threshold, self.global_shed_start)
            base_shed_prob = 0.0
        else:
            base_shed_prob = 0.0

        if spike_mode and event.priority in (Priority.LOW, Priority.HIGH):
            ramp = min(1.0, (load - effective_threshold) / max((1.0 - effective_threshold), 0.05))
            ramp = max(0.0, ramp)
            prob = max(base_shed_prob, ramp)
            prob = min(prob, 1.0)
            if random.random() < prob:
                self._shed(event, f"prob_shed={prob:.2f} load={load:.2f} spike=1")
                return False
            return True

        if load >= effective_threshold:
            shed_probability = min(1.0, (load - effective_threshold) / max((1.0 - effective_threshold), 0.05))
            if random.random() < shed_probability:
                self._shed(event, f"prob_shed={shed_probability:.2f} load={load:.2f} spike={int(spike_mode)}")
                return False

        return True

    def get_queue_config(self) -> dict:
        return {
            "critical_max": self.critical_max,
            "high_max": self.high_max,
            "low_max": self.low_max,
            "thresholds": {
                "critical": self.critical_shed_threshold,
                "high": self.high_shed_threshold,
                "low": self.low_shed_threshold,
                "global": self.global_shed_start,
            },
        }
