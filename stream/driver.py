"""
driver.py  –  RabbitMQ topic-exchange consumer (driver).

Usage:
    python driver.py NAME PATTERN

    NAME     – driver name, used as queue name (e.g. Ravi)
    PATTERN  – binding pattern  (e.g. 'bangalore.koramangala.*')

The broker URL is read from the environment variable AMQP_URL.
Example:
    export AMQP_URL=amqp://guest:guest@localhost/
    python driver.py Ravi 'bangalore.koramangala.*'

ACK/NACK behaviour (D3):
    - Messages missing the 'zone' field are rejected with basic_nack(requeue=False).
    - All other messages are acknowledged with basic_ack.

Never hard-code the broker URL here.
"""

import json
import os
import sys

import pika

EXCHANGE = "rapido-dispatch"


def main():
    if len(sys.argv) != 3:
        print("Usage: python driver.py NAME PATTERN")
        sys.exit(1)

    name = sys.argv[1]
    pattern = sys.argv[2]

    amqp_url = os.environ.get("AMQP_URL", "amqp://guest:guest@localhost/")
    params = pika.URLParameters(amqp_url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

    # Exclusive queue per driver instance
    result = channel.queue_declare(queue=name, exclusive=False, durable=False)
    queue_name = result.method.queue

    channel.queue_bind(exchange=EXCHANGE, queue=queue_name, routing_key=pattern)
    channel.basic_qos(prefetch_count=1)

    print(f"[{name}] listening on queue '{queue_name}' with pattern '{pattern}'")

    def callback(ch, method, properties, body):
        try:
            msg = json.loads(body)
        except json.JSONDecodeError:
            print(f"[{name}] malformed JSON – NACK (discard)")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        if "zone" not in msg:
            print(f"[{name}] NACK (missing zone) – id={msg.get('id', '?')[:8]}  key={method.routing_key}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        else:
            print(
                f"[{name}] ACK  id={msg['id'][:8]}  key={method.routing_key}"
                f"  city={msg['city']}  zone={msg['zone']}  type={msg['type']}"
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=queue_name, on_message_callback=callback)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print(f"[{name}] stopping")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
