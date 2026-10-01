import json
import os
import time

import pika
import requests

import store
from queues import MAX_RETRIES, QUEUE, declare_topology, get_connection

# Env vars so you can change behaviour for demos without editing code
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
MODEL = os.getenv("MODEL", "llama3.2:1b")
DEMO_DELAY = float(os.getenv("DEMO_DELAY", "0"))   # slow processing so you can kill it


def call_ai(text):
    r = requests.post(
        OLLAMA_URL,
        json={"model": MODEL, "prompt": text, "stream": False},
        timeout=60,
    )
    r.raise_for_status()               # 503, 429 etc. raise an exception
    return r.json()["response"]


def on_message(ch, method, props, body):
    # Malformed message: retrying can never help, so dead-letter immediately
    try:
        msg = json.loads(body)
        msg_id, text = msg["id"], msg["text"]
    except (ValueError, KeyError) as e:
        print(f"[POISON] unparseable message: {e}")
        ch.basic_nack(method.delivery_tag, requeue=False)
        return

    retries = (props.headers or {}).get("x-retries", 0)
    print(f"[RECV] id={msg_id} attempt={retries + 1}/{MAX_RETRIES}")

    try:
        if DEMO_DELAY:
            time.sleep(DEMO_DELAY)     # kill the consumer (Ctrl+C) during this
        result = call_ai(text)
        store.set_result(msg_id, "completed", result=result)
        ch.basic_ack(method.delivery_tag)          # ack ONLY after success
        print(f"[DONE] id={msg_id}")

    except Exception as e:
        print(f"[FAIL] id={msg_id}: {e}")
        if retries + 1 >= MAX_RETRIES:
            store.set_result(msg_id, "error", error=str(e))
            ch.basic_nack(method.delivery_tag, requeue=False)   # -> DLX -> DLQ
            print(f"[DLQ]  id={msg_id} dead-lettered")
        else:
            # Republish with incremented retry count, then ack the original
            ch.basic_publish(
                exchange="",
                routing_key=QUEUE,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                    headers={"x-retries": retries + 1},
                ),
            )
            ch.basic_ack(method.delivery_tag)


def main():
    conn = get_connection()
    ch = conn.channel()
    declare_topology(ch)
    ch.basic_qos(prefetch_count=1)     # one unacked message at a time
    ch.basic_consume(queue=QUEUE, on_message_callback=on_message, auto_ack=False)
    print("Consumer waiting for messages. Ctrl+C to stop.")
    ch.start_consuming()


if __name__ == "__main__":
    main()