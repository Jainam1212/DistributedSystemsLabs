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
    uuId = str(uuid.uuid4())
    store.set_result(uuId, "processing")

    queueMsg = {
        "id": uuId,
        "text": req.text,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    conn = get_connection()
    ch = conn.channel()
    declare_topology(ch)
    ch.basic_publish(
        exchange="",
        routing_key=QUEUE,
        body=json.dumps(queueMsg),
        properties=pika.BasicProperties(
            delivery_mode=2,                
            content_type="application/json",
            headers={"x-retries": 0},
        ),
    )
    conn.close()
    return {"id": uuId}


@app.get("/result/{id}")
def getResult(id: str):
    r = store.get_result(id)
    if r is None:
        raise HTTPException(status_code=404, detail="unknown id")
    return r