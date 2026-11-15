"""
dispatcher.py  –  RabbitMQ topic-exchange dispatcher for the food-order stream.

Usage:
    python dispatcher.py

The broker URL is read from the environment variable AMQP_URL.
Example:
    export AMQP_URL=amqp://guest:guest@localhost/
    python dispatcher.py

Never hard-code the broker URL here.
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import pika

EXCHANGE = "rapido-dispatch"

CITIES = ["bangalore", "chennai"]
ZONES = {
    "bangalore": ["koramangala", "indiranagar", "whitefield", "jayanagar"],
    "chennai": ["tnagar", "adyar", "velachery", "mylapore"],
}
TYPES = ["ride", "food"]

# Every 10th message is "bad" (missing zone) to test D3 NACK behaviour.
_msg_counter = 0


def build_message(bad: bool) -> tuple[str, dict]:
    city = random.choice(CITIES)
    zone = random.choice(ZONES[city])
    order_type = random.choice(TYPES)
    routing_key = f"{city}.{zone}.{order_type}"
    payload: dict = {
        "id": str(uuid.uuid4()),
        "city": city,
        "type": order_type,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    if bad:
        # deliberately omit 'zone' so the driver NACKs it
        routing_key = f"{city}.{zone}.{order_type}"  # still route it normally
    else:
        payload["zone"] = zone
    return routing_key, payload


def main():
    global _msg_counter
    amqp_url = os.environ.get("AMQP_URL", "amqp://guest:guest@localhost/")
    params = pika.URLParameters(amqp_url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    print(f"[dispatcher] connected – publishing to exchange '{EXCHANGE}'")
    try:
        while True:
            _msg_counter += 1
            bad = (_msg_counter % 10 == 0)
            routing_key, payload = build_message(bad=bad)
            body = json.dumps(payload)
            channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=routing_key,
                body=body,
                properties=pika.BasicProperties(delivery_mode=2),
            )
            tag = "[BAD – no zone]" if bad else ""
            print(f"[dispatcher] #{_msg_counter:04d} {tag}  key={routing_key}  id={payload['id'][:8]}")
            time.sleep(2)
    except KeyboardInterrupt:
        print("[dispatcher] stopping")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
