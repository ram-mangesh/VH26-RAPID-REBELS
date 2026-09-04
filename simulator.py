import asyncio
import random
import time
import uuid
from enum import IntEnum


class Priority(IntEnum):
    CRITICAL = 0
    HIGH = 1
    LOW = 2


EVENT_TYPES = {
    "payment": {
        "priority": Priority.CRITICAL,
        "weight": 3,
        "processing_time_ms": (20, 80),
    },
    "order": {
        "priority": Priority.CRITICAL,
        "weight": 3,
        "processing_time_ms": (30, 100),
    },
    "inventory_update": {
        "priority": Priority.HIGH,
        "weight": 2,
        "processing_time_ms": (10, 40),
    },
    "user_click": {
        "priority": Priority.LOW,
        "weight": 1,
        "processing_time_ms": (5, 15),
    },
    "app_log": {
        "priority": Priority.LOW,
        "weight": 1,
        "processing_time_ms": (3, 10),
    },
}


class Event:
    __slots__ = (
        "id",
        "event_type",
        "priority",
        "created_at",
        "payload",
        "processing_time_ms",
    )

    def __init__(self, event_type: str):
        cfg = EVENT_TYPES[event_type]
        self.id = uuid.uuid4().hex[:12]
        self.event_type = event_type
        self.priority = cfg["priority"]
        self.created_at = time.monotonic()
        self.payload = {"type": event_type, "id": self.id}
        self.processing_time_ms = random.uniform(*cfg["processing_time_ms"])

    def latency_ms(self) -> float:
        return (time.monotonic() - self.created_at) * 1000

    def to_dict(self):
        return {
            "id": self.id,
            "event_type": self.event_type,
            "priority": self.priority.name,
            "created_at": self.created_at,
        }

    def __lt__(self, other):
        return self.priority < other.priority


class EventSimulator:
    def __init__(self):
        self.rate = 1000  # events per minute
        self.running = False
        self.event_queue: asyncio.Queue = asyncio.Queue(maxsize=100_000)
        self.total_generated = 0
        self._burst_mode = False
        self._event_weights = {
            "payment": 15,
            "order": 20,
            "inventory_update": 15,
            "user_click": 30,
            "app_log": 20,
        }
        self._types = list(self._event_weights.keys())
        self._weights = list(self._event_weights.values())

    def set_rate(self, events_per_minute: int):
        self.rate = max(1, events_per_minute)

    def set_burst_mode(self, enabled: bool):
        self._burst_mode = enabled

    async def start(self):
        self.running = True
        asyncio.create_task(self._generate_loop())

    async def stop(self):
        self.running = False

    async def _generate_loop(self):
        while self.running:
            current_rate = self.rate
            events_per_sec = current_rate / 60.0

            batch_size = max(1, int(events_per_sec * 0.05))
            batch_size = min(batch_size, 500)

            for _ in range(batch_size):
                if not self.running:
                    return
                event_type = random.choices(self._types, weights=self._weights, k=1)[0]
                event = Event(event_type)
                try:
                    self.event_queue.put_nowait(event)
                    self.total_generated += 1
                except asyncio.QueueFull:
                    pass

            # Sleep long enough to yield `batch_size` events at `events_per_sec`
            await asyncio.sleep(batch_size / max(events_per_sec, 0.1))

    def get_stats(self):
        return {
            "rate": self.rate,
            "total_generated": self.total_generated,
            "queue_size": self.event_queue.qsize(),
            "burst_mode": self._burst_mode,
        }
