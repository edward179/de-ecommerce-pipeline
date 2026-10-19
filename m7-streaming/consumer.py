"""Read order events from Kafka and write them to the data lake as Parquet micro-batches."""
import argparse
import json
import os
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaException
from pyarrow import fs

TOPIC = "olist.order_events"
BOOTSTRAP = "127.0.0.1:19092"            # EXTERNAL listener, for clients running in WSL
GROUP_ID = "lake-writer"
LAKE_PREFIX = "events/order_events"

SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("event_type", pa.string()),
    ("event_ts", pa.timestamp("ms", tz="UTC")),
    ("order_id", pa.string()),
    ("customer_unique_id", pa.string()),
    ("product_id", pa.string()),
    ("amount", pa.float64()),            # cast to decimal in dbt, like the batch money columns
    ("payment_type", pa.string()),
    ("schema_version", pa.int16()),
    # Kafka position of the event, for debugging and lineage
    ("_kafka_partition", pa.int32()),
    ("_kafka_offset", pa.int64()),
])


def lake_filesystem():
    # Credentials come from LAKE_* environment variables (.env)
    return fs.S3FileSystem(
        access_key=os.environ["LAKE_ACCESS_KEY"],
        secret_key=os.environ["LAKE_SECRET_KEY"],
        endpoint_override=os.environ["LAKE_ENDPOINT"],
        scheme="http",
        region="us-east-1",
    )


def to_record(msg):
    event = json.loads(msg.value())
    event["event_ts"] = datetime.fromisoformat(event["event_ts"])
    event["_kafka_partition"] = msg.partition()
    event["_kafka_offset"] = msg.offset()
    return event


class LakeWriter:
    """Buffers events and writes one Parquet file per event day on each flush."""

    def __init__(self, consumer, filesystem, bucket, max_events, max_seconds):
        self.consumer = consumer
        self.fs = filesystem
        self.bucket = bucket
        self.max_events = max_events
        self.max_seconds = max_seconds
        self.buffer = []
        self.last_flush = time.monotonic()
        self.files = 0
        self.events = 0

    def due(self):
        return len(self.buffer) >= self.max_events or (
            self.buffer and time.monotonic() - self.last_flush >= self.max_seconds)

    def flush(self):
        """Write the buffer to the lake, then commit offsets (at-least-once)."""
        if self.buffer:
            by_day = defaultdict(list)
            for record in self.buffer:
                by_day[record["event_ts"].strftime("%Y-%m-%d")].append(record)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            for dt, rows in by_day.items():
                # Random suffix: two consumers of the group may flush in the same second
                path = f"{self.bucket}/{LAKE_PREFIX}/dt={dt}/batch-{stamp}-{uuid.uuid4().hex[:8]}.parquet"
                pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), path, filesystem=self.fs)
                print(f"Wrote {len(rows)} events to s3://{path}")
                self.files += 1
            self.events += len(self.buffer)
            # Commit only after all files are written: a crash before this line means
            # the events are read again (duplicates, removed by the dlt merge), never lost
            self.consumer.commit(asynchronous=False)
            self.buffer.clear()
        self.last_flush = time.monotonic()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-events", type=int, default=5000, help="flush when the buffer has N events")
    parser.add_argument("--max-seconds", type=float, default=60, help="flush at least every N seconds")
    parser.add_argument("--minutes", type=float, default=0, help="stop after N minutes (0 = until Ctrl+C)")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "client.id": "olist-lake-writer",
        "enable.auto.commit": False,         # commit by hand, after the file is in the lake
        "auto.offset.reset": "earliest",     # a new group starts from the oldest event in the topic
    })
    writer = LakeWriter(consumer, lake_filesystem(), os.environ["LAKE_BUCKET"],
                        args.max_events, args.max_seconds)

    def on_assign(_consumer, partitions):
        print("Assigned partitions:", [p.partition for p in partitions])

    def on_revoke(_consumer, partitions):
        # Before partitions move to another consumer, save and commit what was read from them
        print("Revoked partitions:", [p.partition for p in partitions])
        writer.flush()

    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    end = time.monotonic() + args.minutes * 60 if args.minutes else None
    skipped = 0
    try:
        while end is None or time.monotonic() < end:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    raise KafkaException(msg.error())
                try:
                    writer.buffer.append(to_record(msg))
                except (ValueError, KeyError) as exc:    # bad JSON or missing field
                    skipped += 1
                    print(f"Skipped p{msg.partition()} offset {msg.offset()}: {exc}")
            if writer.due():
                writer.flush()
    except KeyboardInterrupt:
        print("Stopped by user")
    finally:
        writer.flush()
        consumer.close()                     # leave the group now, partitions move without waiting
    print(f"Done: {writer.events} events in {writer.files} files, skipped {skipped}")


if __name__ == "__main__":
    main()
