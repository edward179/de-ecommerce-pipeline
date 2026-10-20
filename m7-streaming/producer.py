"""Send fake order lifecycle events with real Olist IDs to Kafka."""
import argparse
import json
import random
import time
from collections import Counter
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Producer
from faker import Faker

TOPIC = "olist.order_events"
BOOTSTRAP = "127.0.0.1:19092"            # EXTERNAL listener, for clients running in WSL
SCHEMA_VERSION = 1
NEXT_STEP = {
    "order_created": "order_payment_approved",
    "order_payment_approved": "order_shipped",
    "order_shipped": "order_delivered",
}
CANCELLABLE = {"order_created", "order_payment_approved"}   # cannot cancel after shipping
FINAL = {"order_delivered", "order_canceled"}
PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card"]   # values used in Olist
NEW_ORDER_RATE = 0.25    # 1 new order per 4 events keeps the number of open orders stable
CANCEL_RATE = 0.05

fake = Faker()


def load_ids(sample_size):
    """Sample real customers and products (with their average price) from the source DB."""
    # Connection settings come from PG* environment variables (.env)
    with psycopg.connect() as conn:
        customers = [r[0] for r in conn.execute(
            "select customer_unique_id from (select distinct customer_unique_id from olist.customers) c "
            "order by random() limit %s", (sample_size,))]
        products = conn.execute(
            "select product_id, round(avg(price), 2) from olist.order_items "
            "group by product_id order by random() limit %s", (sample_size,)).fetchall()
    return customers, products


def make_event(event_type, order):
    return {
        "event_id": fake.uuid4(),                    # dedup key for the lake and dlt merge
        "event_type": event_type,
        "event_ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "order_id": order["order_id"],
        "customer_unique_id": order["customer_unique_id"],
        "product_id": order["product_id"],
        "amount": order["amount"],
        "payment_type": order["payment_type"],
        "schema_version": SCHEMA_VERSION,
    }


def next_event(open_orders, customers, products):
    """Either open a new order or move a random open order to its next step."""
    if not open_orders or random.random() < NEW_ORDER_RATE:
        product_id, price = random.choice(products)
        order = {
            "order_id": fake.uuid4().replace("-", ""),   # 32 hex chars, same format as Olist
            "customer_unique_id": random.choice(customers),
            "product_id": product_id,
            "amount": float(price),
            "payment_type": random.choice(PAYMENT_TYPES),
            "status": "order_created",
        }
        open_orders[order["order_id"]] = order
    else:
        order = open_orders[random.choice(list(open_orders))]
        if order["status"] in CANCELLABLE and random.random() < CANCEL_RATE:
            order["status"] = "order_canceled"
        else:
            order["status"] = NEXT_STEP[order["status"]]
        if order["status"] in FINAL:
            del open_orders[order["order_id"]]
    return make_event(order["status"], order)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=float, default=2, help="stop after N minutes")
    parser.add_argument("--rate", type=float, default=5, help="events per second")
    parser.add_argument("--sample-size", type=int, default=1000)
    args = parser.parse_args()

    customers, products = load_ids(args.sample_size)
    print(f"Loaded {len(customers)} customers, {len(products)} products")

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "olist-event-producer",
        "acks": "all",                 # wait until the broker has stored the message
        "enable.idempotence": True,    # retries never write the same message twice
        "linger.ms": 100,              # small batching window
    })
    sent, failed = Counter(), Counter()

    def on_delivery(err, msg):
        if err:
            failed[str(err)] += 1

    open_orders = {}
    end = time.monotonic() + args.minutes * 60
    try:
        while time.monotonic() < end:
            event = next_event(open_orders, customers, products)
            # Key = order_id, so all events of one order go to the same partition, in order
            producer.produce(TOPIC, key=event["order_id"], value=json.dumps(event).encode(),
                             on_delivery=on_delivery)
            sent[event["event_type"]] += 1
            producer.poll(0)           # run delivery callbacks
            time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        print("Stopped by user")
    finally:
        remaining = producer.flush(10)
    print(f"Sent: {dict(sent)} | total {sum(sent.values())}")
    print(f"Failed: {dict(failed) or 0} | not delivered after flush: {remaining} | open orders: {len(open_orders)}")


if __name__ == "__main__":
    main()
