import json
import uuid
from datetime import datetime, timezone

import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import store
from queues import QUEUE, declare_topology, get_connection

app = FastAPI()


class ProcessRequest(BaseModel):
    text: str


@app.post("/process")
def process(req: ProcessRequest):
    request_id = str(uuid.uuid4())
    store.set_result(request_id, "processing")

    msg = {
        "id": request_id,
        "text": req.text,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # pika connections aren't thread-safe, so use one per request (fine for a lab)
    conn = get_connection()
    ch = conn.channel()
    declare_topology(ch)
    ch.basic_publish(
        exchange="",
        routing_key=QUEUE,
        body=json.dumps(msg),
        properties=pika.BasicProperties(
            delivery_mode=2,                 # persistent
            content_type="application/json",
            headers={"x-retries": 0},
        ),
    )
    conn.close()
    return {"id": request_id}


@app.get("/result/{id}")
def get_result(id: str):
    r = store.get_result(id)
    if r is None:
        raise HTTPException(status_code=404, detail="unknown id")
    return r