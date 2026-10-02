import pika

RABBIT_HOST = "localhost"
QUEUE = "ai_task_queue"
DLX = "dlx_exchange"
DLQ = "ai_task_dlq"
DLQ_KEY = "dead"
MAX_RETRIES = 3


def get_connection():
    return pika.BlockingConnection(
        pika.ConnectionParameters(RABBIT_HOST, heartbeat=600)
    )


def declare_topology(ch):
    # Dead letter side
    ch.exchange_declare(exchange=DLX, exchange_type="direct", durable=True)
    ch.queue_declare(queue=DLQ, durable=True)
    ch.queue_bind(queue=DLQ, exchange=DLX, routing_key=DLQ_KEY)

    ch.queue_declare(
        queue=QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX,
            "x-dead-letter-routing-key": DLQ_KEY,
        },
    )