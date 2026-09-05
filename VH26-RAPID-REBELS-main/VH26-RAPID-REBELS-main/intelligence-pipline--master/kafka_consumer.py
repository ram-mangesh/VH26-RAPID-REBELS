"""
Kafka Consumer for Intelligence Pipeline
Consumes real events from the 'events' topic and feeds them into the adaptive pipeline
"""
import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

from kafka import KafkaConsumer
from pipeline import DataPipeline, Event, Priority, MetricsCollector, DecisionTracer

logger = logging.getLogger("kafka-consumer")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
TOPIC_EVENTS = os.getenv("KAFKA_TOPIC_EVENTS", "events")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "intelligence-pipeline")


# Map real event types to pipeline priorities
EVENT_TYPE_MAP = {
    "payment": {"priority": Priority.CRITICAL, "weight": 3, "processing_time_ms": (20, 80)},
    "order": {"priority": Priority.CRITICAL, "weight": 3, "processing_time_ms": (30, 100)},
    "log": {"priority": Priority.LOW, "weight": 1, "processing_time_ms": (3, 10)},
    "click": {"priority": Priority.LOW, "weight": 1, "processing_time_ms": (5, 15)},
}


class KafkaEventAdapter:
    """Adapt real Kafka events to pipeline Event format"""
    
    def __init__(self):
        self.total_adapted = 0
        self.errors = 0
    
    def adapt(self, raw_event: dict) -> Optional[Event]:
        """Convert Kafka event to pipeline Event"""
        try:
            event_type = raw_event.get("type", "").lower()
            if event_type not in EVENT_TYPE_MAP:
                logger.warning(f"Unknown event type: {event_type}")
                return None
            
            config = EVENT_TYPE_MAP[event_type]
            payload = raw_event.get("payload", {})
            
            # Create pipeline event
            event = Event.__new__(Event)
            event.id = raw_event.get("event_id", f"evt_{int(time.time()*1000)}")
            event.event_type = event_type
            event.priority = config["priority"]
            event.created_at = time.monotonic()
            event.payload = {"type": event_type, "id": event.id, **payload}
            event.processing_time_ms = config["processing_time_ms"][0]  # Use min for simplicity
            
            self.total_adapted += 1
            return event
            
        except Exception as e:
            logger.error(f"Failed to adapt event: {e}")
            self.errors += 1
            return None


class RealEventPipeline(DataPipeline):
    """Extended pipeline that consumes from real Kafka topic"""
    
    def __init__(self, num_workers: int = 8):
        super().__init__(num_workers)
        
        # Disable internal simulator
        self.simulator.running = False
        
        # Add Kafka consumer
        self.adapter = KafkaEventAdapter()
        self.kafka_consumer: Optional[KafkaConsumer] = None
        self.consumer_task: Optional[asyncio.Task] = None
        
        # Override metrics to track Kafka events
        self._kafka_metrics = {
            "consumed": 0,
            "adapted": 0,
            "errors": 0,
        }
    
    def _create_kafka_consumer(self):
        """Create Kafka consumer"""
        try:
            consumer = KafkaConsumer(
                TOPIC_EVENTS,
                bootstrap_servers=KAFKA_BROKERS.split(","),
                group_id=CONSUMER_GROUP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=-1,
                max_poll_records=500,
                max_poll_interval_ms=300000,
                session_timeout_ms=10000,
                heartbeat_interval_ms=3000,
                value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
            )
            logger.info(f"Kafka consumer connected to {KAFKA_BROKERS}, topic: {TOPIC_EVENTS}")
            return consumer
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            return None
    
    async def start(self):
        """Start pipeline + Kafka consumer"""
        await super().start()
        
        # Start Kafka consumer task
        self.consumer_task = asyncio.create_task(self._kafka_consumer_loop())
        logger.info("RealEventPipeline started with Kafka consumer")
    
    async def stop(self):
        """Stop pipeline + Kafka consumer"""
        await super().stop()
        
        if self.kafka_consumer:
            try:
                self.kafka_consumer.wakeup()
            except Exception:
                pass
        
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass
        
        if self.kafka_consumer:
            self.kafka_consumer.close()
        
        logger.info("RealEventPipeline stopped")
    
    async def _kafka_consumer_loop(self):
        """Main loop consuming from Kafka and feeding pipeline"""
        while self.running:
            if not self.kafka_consumer:
                self.kafka_consumer = self._create_kafka_consumer()
                if not self.kafka_consumer:
                    await asyncio.sleep(5)
                    continue
            
            try:
                records = self.kafka_consumer.poll(timeout_ms=5000)
            except (TypeError, RuntimeError) as e:
                if "wakeup" in str(e).lower():
                    logger.info("Kafka consumer woken up, stopping...")
                continue
            except Exception as e:
                logger.error(f"Kafka poll error: {e}")
                self.kafka_consumer = None
                await asyncio.sleep(5)
                continue
            
            for partition_records in records.values():
                for msg in partition_records:
                    try:
                        raw_event = json.loads(msg.value)
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decode error: {e}")
                        self._kafka_metrics["errors"] += 1
                        continue
                    
                    self._kafka_metrics["consumed"] += 1
                    
                    # Adapt to pipeline event
                    event = self.adapter.adapt(raw_event)
                    if not event:
                        continue
                    
                    self._kafka_metrics["adapted"] += 1
                    
                    # Route through pipeline's decision engine
                    ctx = {
                        "queue_load": self._queue_sizes()["total"] / 75000.0,
                        "current_latency_ms": 0.0,
                        "worker_load": self._worker_load(),
                        "overall_throughput": self.metrics._recent_throughput,
                    }
                    
                    decision = self.engine.decide(event, ctx)
                    components = self.engine.score_components(event, ctx["queue_load"], ctx["current_latency_ms"], ctx["worker_load"], ctx["overall_throughput"])
                    
                    q_sizes = self._queue_sizes()
                    per_tier = q_sizes[event.priority.name.lower()]
                    
                    from pipeline import SheddingPolicy
                    backpressure = SheddingPolicy()
                    admitted = backpressure.should_admit(event, q_sizes["total"], per_tier, spike_mode=(self.engine.mode == "spike"))
                    
                    if not admitted:
                        self.total_shed += 1
                        await self.metrics.record_shed(event)
                        self.tracer.trace(event.id, event.event_type, event.priority.name, decision, components, admitted=False, shed_reason=backpressure.last_reason(event))
                        continue
                    
                    self.tracer.trace(event.id, event.event_type, event.priority.name, decision, components, admitted=True)
                    
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
            
            await asyncio.sleep(0.01)
    
    async def get_full_state(self) -> dict:
        state = await super().get_full_state()
        
        # Add Kafka metrics
        state["kafka"] = {
            "topic": TOPIC_EVENTS,
            "consumed": self._kafka_metrics["consumed"],
            "adapted": self._kafka_metrics["adapted"],
            "errors": self._kafka_metrics["errors"],
            "adapter_total": self.adapter.total_adapted,
            "adapter_errors": self.adapter.errors,
        }
        
        # Override simulator stats with real data
        state["simulator"] = {
            "rate": "Kafka (real events)",
            "total_generated": self._kafka_metrics["consumed"],
            "queue_size": self.adapter.total_adapted,
            "source": "real_event_simulator",
        }
        
        return state


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    
    pipeline = RealEventPipeline(num_workers=8)
    
    try:
        await pipeline.start()
        
        # Keep running
        while True:
            await asyncio.sleep(10)
            state = await pipeline.get_full_state()
            logger.info(f"State: queues={state['queues']}, "
                       f"throughput={state['metrics'].get('throughput_eps', 0):.1f} eps, "
                       f"kafka={state['kafka']}")
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await pipeline.stop()


if __name__ == "__main__":
    asyncio.run(main())