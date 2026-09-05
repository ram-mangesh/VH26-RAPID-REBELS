"""Decision tracing for the adaptive pipeline.

Records every routing decision with the exact scored-formula breakdown so a
judge can see *why* the pipeline treated each event the way it did — live.
"""
import time
from collections import deque
from typing import Optional


class DecisionTracer:
    def __init__(self, max_entries: int = 200):
        self.max_entries = max_entries
        self._entries: deque = deque(maxlen=max_entries)

    def trace(
        self,
        event_id: str,
        event_type: str,
        priority: str,
        decision,
        components: dict,
        admitted: bool,
        shed_reason: Optional[str] = None,
    ):
        """Record a routing decision with its scored components."""
        self._entries.append(
            {
                "ts": time.monotonic(),
                "event_id": event_id,
                "event_type": event_type,
                "priority": priority,
                "mode": decision.mode,
                "strategy": decision.strategy,
                "batch_size": decision.batch_size,
                "score": decision.score,
                "urgency": decision.urgency,
                "components": components,
                "admitted": admitted,
                "shed_reason": shed_reason,
            }
        )

    def recent(self, n: int = 30) -> list:
        """Return the n most recent traces (newest first)."""
        return [dict(e) for e in reversed(self._entries)][:n]

    def reset(self):
        self._entries.clear()