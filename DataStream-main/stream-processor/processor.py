"""
Real-time stream processor: Kafka → ClickHouse

Implements tumbling windows manually in Python using kafka-python:
  - 1-minute window  → orders_per_minute table
  - 5-minute window  → revenue_by_region + top_products tables
  - Raw flush every 5s / 100 msgs → orders table
  - Late data tolerance: 10 seconds
  - Bad messages → dead_letter_queue table
"""

import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer
import clickhouse_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BROKERS   = os.getenv("KAFKA_BROKERS",     "localhost:9092")
TOPIC_EVENTS    = os.getenv("KAFKA_TOPIC_EVENTS", "events")
CH_HOST         = os.getenv("CLICKHOUSE_HOST",    "localhost")
CH_PORT         = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_DB           = os.getenv("CLICKHOUSE_DB",      "ecommerce")
CH_USER         = os.getenv("CLICKHOUSE_USER",    "default")
CH_PASS         = os.getenv("CLICKHOUSE_PASSWORD", "")

LATE_TOLERANCE_S = 10
WINDOW_1M_S      = 60
WINDOW_5M_S      = 300


# ─── Connections ──────────────────────────────────────────────────────────────

def connect_clickhouse():
    while True:
        try:
            client = clickhouse_connect.get_client(
                host=CH_HOST, port=CH_PORT, database=CH_DB,
                username=CH_USER, password=CH_PASS,
            )
            client.command("SELECT 1")
            log.info("Connected to ClickHouse at %s:%d", CH_HOST, CH_PORT)
            return client
        except Exception as e:
            log.warning("ClickHouse not ready: %s — retry in 5s", e)
            time.sleep(5)


def connect_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_EVENTS,
                bootstrap_servers=KAFKA_BROKERS.split(","),
                group_id="python-stream-processor",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                max_poll_records=50000,
                max_poll_interval_ms=30000,
                session_timeout_ms=10000,
                heartbeat_interval_ms=3000,
                fetch_max_bytes=104857600,
                fetch_min_bytes=1,
                value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
            )
            log.info("Kafka consumer connected to %s", KAFKA_BROKERS)
            return consumer
        except Exception as e:
            log.warning("Kafka not ready: %s — retry in 5s", e)
            time.sleep(5)


# ─── Tumbling Window ──────────────────────────────────────────────────────────

class TumblingWindow:
    def __init__(self, size_s: int):
        self.size_s = size_s
        self._buckets: dict[int, list] = defaultdict(list)

    def add(self, event_time_ms: int, event: dict):
        bucket_ts = (event_time_ms // 1000 // self.size_s) * self.size_s
        current_bucket = (int(time.time()) // self.size_s) * self.size_s
        if bucket_ts < current_bucket - self.size_s - LATE_TOLERANCE_S:
            return
        self._buckets[bucket_ts].append(event)

    def closed_buckets(self):
        cutoff = (int(time.time()) // self.size_s) * self.size_s - LATE_TOLERANCE_S
        results = []
        for ts in sorted(self._buckets):
            if ts < cutoff:
                results.append((ts, self._buckets.pop(ts)))
        return results


# ─── Event Extraction ─────────────────────────────────────────────────────────

def extract_order_fields(payload: dict) -> dict | None:
    """Extract order fields from payload for order events."""
    required = ("order_id", "product", "category", "quantity", "price", "total_amount", "region", "status")
    if not all(k in payload for k in required):
        return None
    return {
        "id": payload["order_id"],
        "customer_id": payload.get("customer_id", ""),
        "product": payload["product"],
        "category": payload["category"],
        "quantity": int(payload["quantity"]),
        "price": float(payload["price"]),
        "total_amount": float(payload["total_amount"]),
        "region": payload["region"],
        "status": payload["status"],
    }


def extract_payment_fields(payload: dict) -> dict | None:
    """Extract payment fields from payload for payment events."""
    required = ("transaction_id", "customer_id", "amount", "currency", "method", "status", "gateway", "region")
    if not all(k in payload for k in required):
        return None
    return {
        "id": payload["transaction_id"],
        "customer_id": payload["customer_id"],
        "product": "N/A",
        "category": "payment",
        "quantity": 1,
        "price": float(payload["amount"]),
        "total_amount": float(payload["amount"]),
        "region": payload["region"],
        "status": payload["status"],
    }


def extract_click_fields(payload: dict) -> dict | None:
    """Extract click/log fields from payload for log/click events."""
    required = ("session_id", "customer_id", "page", "action", "region")
    if not all(k in payload for k in required):
        return None
    return {
        "id": payload["session_id"],
        "customer_id": payload["customer_id"],
        "product": payload.get("product_id", ""),
        "category": payload["action"],
        "quantity": 1,
        "price": 0.0,
        "total_amount": 0.0,
        "region": payload["region"],
        "status": "click",
    }


def extract_event(event: dict) -> dict | None:
    """Extract unified order-like record from any event type."""
    event_type = event.get("type", "")
    payload = event.get("payload", {})
    
    if event_type == "order":
        extracted = extract_order_fields(payload)
    elif event_type == "payment":
        extracted = extract_payment_fields(payload)
    elif event_type == "log":
        extracted = extract_click_fields(payload)
    else:
        return None
    
    if extracted:
        ts = event.get("timestamp", time.time() * 1000)
        if isinstance(ts, str):
            try:
                event_time_ms = int(datetime.fromisoformat(ts.replace('Z', '+00:00')).timestamp() * 1000)
            except:
                event_time_ms = int(time.time() * 1000)
        else:
            event_time_ms = int(ts)
        extracted["event_time"] = event_time_ms
    return extracted


# ─── Aggregations ─────────────────────────────────────────────────────────────

def agg_orders_per_minute(events: list) -> tuple:
    total   = len(events)
    revenue = sum(float(e.get("total_amount", 0)) for e in events)
    done    = sum(1 for e in events if e.get("status") == "completed" or e.get("status") == "success" or e.get("status") == "click")
    failed  = sum(1 for e in events if e.get("status") == "failed" or e.get("status") == "declined")
    avg     = round(revenue / total, 2) if total else 0.0
    return total, round(revenue, 2), done, failed, avg


def agg_by_region(events: list) -> dict:
    acc = defaultdict(lambda: {"count": 0, "revenue": 0.0})
    for e in events:
        r = e.get("region", "Unknown")
        acc[r]["count"]   += 1
        acc[r]["revenue"] += float(e.get("total_amount", 0))
    return acc


def agg_by_product(events: list) -> dict:
    acc = defaultdict(lambda: {"qty": 0, "revenue": 0.0, "count": 0, "category": ""})
    for e in events:
        p = e.get("product", "Unknown")
        acc[p]["qty"]      += int(e.get("quantity", 1))
        acc[p]["revenue"]  += float(e.get("total_amount", 0))
        acc[p]["count"]    += 1
        acc[p]["category"]  = e.get("category", "")
    return acc


# ─── ClickHouse writers ───────────────────────────────────────────────────────

def write_raw_batch(ch, batch: list):
    ch.insert("orders", batch, column_names=[
        "id", "customer_id", "product", "category", "quantity",
        "price", "total_amount", "region", "status", "timestamp", "event_time",
    ])
    log.info("Flushed %d raw events", len(batch))


def write_1m_window(ch, ts: int, events: list):
    if not events:
        return
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    total, revenue, done, failed, avg = agg_orders_per_minute(events)
    ch.insert("orders_per_minute",
              [[dt, total, revenue, done, failed, avg]],
              column_names=["minute", "order_count", "total_revenue",
                            "completed_count", "failed_count", "avg_order_value"])
    log.info("1-min window %s: %d events $%.2f", dt.strftime("%H:%M"), total, revenue)


def write_5m_window(ch, ts: int, events: list):
    if not events:
        return
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)

    for region, d in agg_by_region(events).items():
        avg = round(d["revenue"] / d["count"], 2) if d["count"] else 0
        ch.insert("revenue_by_region",
                  [[dt, region, d["count"], round(d["revenue"], 2), avg]],
                  column_names=["window_start", "region", "order_count",
                                "total_revenue", "avg_order_value"])

    for product, d in agg_by_product(events).items():
        ch.insert("top_products",
                  [[dt, product, d["category"], d["qty"], round(d["revenue"], 2), d["count"]]],
                  column_names=["window_start", "product", "category",
                                "quantity_sold", "total_revenue", "order_count"])

    log.info("5-min window %s: %d events", dt.strftime("%H:%M"), len(events))


def write_dlq(ch, raw: str, reason: str):
    try:
        ch.insert("dead_letter_queue",
                  [[raw[:2000], reason]],
                  column_names=["raw_message", "error_reason"])
    except Exception as e:
        log.error("DLQ write failed: %s", e)


def write_total_event_count(ch, count: int):
    try:
        dt = datetime.now(timezone.utc)
        ch.insert("total_events",
                  [[dt, count]],
                  column_names=["timestamp", "total_count"])
    except Exception as e:
        log.error("Total event count write failed: %s", e)


last_realtime_write = 0


def write_realtime_counter(ch, accepted: int, rejected: int):
    global last_realtime_write
    now = time.time()
    if now - last_realtime_write < 2:
        return
    last_realtime_write = now
    try:
        dt = datetime.now(timezone.utc)
        accepted_in_window = accepted - (getattr(write_realtime_counter, "prev_accepted", 0) or 0)
        rejected_in_window = rejected - (getattr(write_realtime_counter, "prev_rejected", 0) or 0)
        setattr(write_realtime_counter, "prev_accepted", accepted)
        setattr(write_realtime_counter, "prev_rejected", rejected)
        if accepted_in_window < 0:
            accepted_in_window = 0
        if rejected_in_window < 0:
            rejected_in_window = 0
        success_pct = round(100 * accepted_in_window / max(accepted_in_window + rejected_in_window, 1), 2)
        ch.insert("realtime_orders_per_minute",
                  [[dt, accepted_in_window, rejected_in_window, success_pct]],
                  column_names=["timestamp", "accepted", "rejected", "success_pct"])
        log.info("Realtime: %d accepted, %d rejected", accepted_in_window, rejected_in_window)
    except Exception as e:
        log.error("Realtime counter write failed: %s", e)


# ─── Main loop ────────────────────────────────────────────────────────────────

def main():
    ch       = connect_clickhouse()
    consumer = connect_kafka_consumer()

    win_1m = TumblingWindow(WINDOW_1M_S)
    win_5m = TumblingWindow(WINDOW_5M_S)

    raw_batch: list   = []
    last_flush: float = time.time()
    consumed_count = 0
    accepted_count = 0
    last_log = time.time()
    window_events: list = []
    last_window_flush: float = time.time()

    log.info("Stream processor running — topic: %s", TOPIC_EVENTS)

    while True:
        try:
            records = consumer.poll(timeout_ms=5000)
        except Exception as e:
            log.error("Kafka poll error: %s", e)
            consumer = connect_kafka_consumer()
            continue

        for partition_records in records.values():
            for msg in partition_records:
                consumed_count += 1
                raw = msg.value

                try:
                    event = json.loads(raw)
                except json.JSONDecodeError as e:
                    write_dlq(ch, raw, f"json_parse: {e}")
                    continue

                extracted = extract_event(event)
                if not extracted:
                    write_dlq(ch, raw, "unknown_event_type_or_missing_fields")
                    continue
                
                accepted_count += 1
                event_time_ms = extracted["event_time"]
                window_events.append(extracted)

                win_1m.add(event_time_ms, extracted)
                win_5m.add(event_time_ms, extracted)

                ts_dt = datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc)
                raw_batch.append([
                    extracted["id"],
                    extracted.get("customer_id", ""),
                    extracted["product"],
                    extracted["category"],
                    int(extracted["quantity"]),
                    float(extracted["price"]),
                    float(extracted["total_amount"]),
                    extracted["region"],
                    extracted["status"],
                    ts_dt,
                    event_time_ms,
                ])

                if len(raw_batch) >= 500:
                    try:
                        write_raw_batch(ch, raw_batch)
                        raw_batch = []
                        last_flush = now
                    except Exception as e:
                        log.error("Raw batch write failed: %s", e)

        now = time.time()
        if raw_batch and (len(raw_batch) >= 100 or now - last_flush >= 0.5):
            try:
                write_raw_batch(ch, raw_batch)
                raw_batch = []
                last_flush = now
            except Exception as e:
                log.error("Raw batch write failed: %s", e)

        rejected_count = consumed_count - accepted_count
        write_realtime_counter(ch, accepted_count, rejected_count)

        for ts, events in win_1m.closed_buckets():
            try:
                write_1m_window(ch, ts, events)
            except Exception as e:
                log.error("1m window write failed: %s", e)

        now = time.time()
        if now - last_log >= 30:
            log.info("LOOP: consumed=%d accepted=%d", consumed_count, accepted_count)
            try:
                write_total_event_count(ch, accepted_count)
            except:
                pass
            last_log = now

        # Close 5-min windows (same as 1-min but every 5 minutes)
        for ts, events in win_5m.closed_buckets():
            try:
                write_5m_window(ch, ts, events)
            except Exception as e:
                log.error("5m window write failed: %s", e)


if __name__ == "__main__":
    main()