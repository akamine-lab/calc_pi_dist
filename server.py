import os, time, json, uuid, asyncio
from typing import Optional, Any, Dict, Set
from collections import deque

from fastapi import FastAPI, HTTPException, Body, WebSocket, WebSocketDisconnect, status
from fastapi.responses import Response, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from contextlib import asynccontextmanager

import redis

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
ROOT_PATH = os.environ.get("ROOT_PATH", "/os")  # nginxのprefixに合わせる

QUEUE_KEY = "pi:queue"          # List: job_id
INFLIGHT_KEY = "pi:inflight"    # ZSET: score=deadline(unix sec), member=job_id
PAYLOAD_KEY_PREFIX = "pi:payload:"  # String: JSON payload
RESULT_KEY_PREFIX = "pi:result:"    # String: JSON result

LEASE_SEC = 10
REQUEUE_PERIOD_SEC = 1  # 回収ループの周期



# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self.recent_results: deque = deque(maxlen=50)  # Keep last 50 results

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)

    async def broadcast(self, message: dict):
        """Broadcast message to all connected clients"""
        disconnected = set()
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.add(connection)
        # Remove disconnected connections
        for connection in disconnected:
            self.active_connections.discard(connection)

    def add_result(self, job_id: str, result: dict):
        """Add result to recent results list"""
        self.recent_results.append({
            "job_id": job_id,
            "result": result,
            "timestamp": int(time.time())
        })

manager = ConnectionManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup 相当
    task = asyncio.create_task(requeue_loop())
    try:
        yield
    finally:
        # shutdown 相当
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
app = FastAPI(
    lifespan=lifespan,
    root_path=ROOT_PATH
)

# Mount static files
static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

class JobOut(BaseModel):
    job_id: str
    payload: Dict[str, Any]
    lease_sec: int

class ResultIn(BaseModel):
    job_id: str
    result: Dict[str, Any]

def payload_key(job_id: str) -> str:
    return f"{PAYLOAD_KEY_PREFIX}{job_id}"

def result_key(job_id: str) -> str:
    return f"{RESULT_KEY_PREFIX}{job_id}"

def get_queue_state() -> dict:
    """Get current queue state"""
    queue_length = r.llen(QUEUE_KEY)
    inflight_count = r.zcard(INFLIGHT_KEY)
    return {
        "queue_length": queue_length,
        "inflight_count": inflight_count
    }

async def broadcast_queue_update():
    """Broadcast queue state update to all connected clients"""
    state = get_queue_state()
    await manager.broadcast({
        "type": "queue_update",
        "queue_length": state["queue_length"],
        "inflight_count": state["inflight_count"]
    })

async def broadcast_job_update():
    """Broadcast job details update to all connected clients"""
    queue_jobs = []
    inflight_jobs = []
    
    # Get queue job IDs
    queue_job_ids = r.lrange(QUEUE_KEY, 0, -1)
    for job_id in queue_job_ids:
        payload_json = r.get(payload_key(job_id))
        if payload_json:
            try:
                payload = json.loads(payload_json)
                queue_jobs.append({"job_id": job_id, "payload": payload})
            except json.JSONDecodeError:
                pass
    
    # Get inflight job IDs
    inflight_job_ids = r.zrange(INFLIGHT_KEY, 0, -1)
    for job_id in inflight_job_ids:
        payload_json = r.get(payload_key(job_id))
        if payload_json:
            try:
                payload = json.loads(payload_json)
                inflight_jobs.append({"job_id": job_id, "payload": payload})
            except json.JSONDecodeError:
                pass
    
    await manager.broadcast({
        "type": "job_update",
        "queue_jobs": queue_jobs,
        "inflight_jobs": inflight_jobs
    })

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Send initial state
        state = get_queue_state()
        await websocket.send_json({
            "type": "queue_update",
            "queue_length": state["queue_length"],
            "inflight_count": state["inflight_count"]
        })
        # Send initial job details
        queue_jobs = []
        inflight_jobs = []
        
        queue_job_ids = r.lrange(QUEUE_KEY, 0, -1)
        for job_id in queue_job_ids:
            payload_json = r.get(payload_key(job_id))
            if payload_json:
                try:
                    payload = json.loads(payload_json)
                    queue_jobs.append({"job_id": job_id, "payload": payload})
                except json.JSONDecodeError:
                    pass
        
        inflight_job_ids = r.zrange(INFLIGHT_KEY, 0, -1)
        for job_id in inflight_job_ids:
            payload_json = r.get(payload_key(job_id))
            if payload_json:
                try:
                    payload = json.loads(payload_json)
                    inflight_jobs.append({"job_id": job_id, "payload": payload})
                except json.JSONDecodeError:
                    pass
        
        await websocket.send_json({
            "type": "job_update",
            "queue_jobs": queue_jobs,
            "inflight_jobs": inflight_jobs
        })
        # Send recent results
        for result in manager.recent_results:
            await websocket.send_json({
                "type": "result",
                **result
            })
        # Keep connection alive
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.post("/seed", status_code=204)
async def seed(n: int = 5):
    """
    ダミーjobを n 件投入。
    payload は JSON で保存。
    """
    pipe = r.pipeline()
    for i in range(n):
        job_id = str(uuid.uuid4())
        payload = {"type": "dummy", "i": i, "msg": "hello"}
        pipe.set(payload_key(job_id), json.dumps(payload))
        pipe.lpush(QUEUE_KEY, job_id)
    pipe.execute()
    await broadcast_queue_update()
    await broadcast_job_update()
    return Response(status_code=204)  # 204はボディなし :contentReference[oaicite:2]{index=2}

@app.post("/enqueue", status_code=204)
async def enqueue(payload: dict = Body(...)):
    job_id = str(uuid.uuid4())
    r.set(payload_key(job_id), json.dumps(payload))
    r.lpush(QUEUE_KEY, job_id)
    await broadcast_queue_update()
    await broadcast_job_update()
    return Response(status_code=204)

@app.get("/job", response_model=Optional[JobOut])
async def get_job():
    """
    1件取り出して in-flight に登録(期限=now+10s)して返す。
    キューが空なら 204。
    """
    job_id = r.rpop(QUEUE_KEY)
    if job_id is None:
        return Response(status_code=204)  # 204はボディなし :contentReference[oaicite:3]{index=3}

    payload_json = r.get(payload_key(job_id))
    if payload_json is None:
        # 不整合（payloadが無い）: とりあえず捨てる or エラー
        raise HTTPException(500, "payload missing")

    deadline = int(time.time()) + LEASE_SEC
    # in-flight登録
    r.zadd(INFLIGHT_KEY, {job_id: deadline})
    payload = json.loads(payload_json)
    
    await broadcast_queue_update()
    await broadcast_job_update()

    return JobOut(job_id=job_id, payload=payload, lease_sec=LEASE_SEC)

@app.post("/result", status_code=204)
async def post_result(x: ResultIn):
    """
    結果を保存し、in-flight から削除。
    （冪等：すでに結果がある場合は上書きしない例）
    """
    # すでに結果があるなら何もしない（重複報告対策）
    if r.exists(result_key(x.job_id)):
        r.zrem(INFLIGHT_KEY, x.job_id)
        await broadcast_queue_update()
        await broadcast_job_update()
        return Response(status_code=204)

    print(f"result: {x.result}")
    pipe = r.pipeline()
    pipe.set(result_key(x.job_id), json.dumps(x.result))
    pipe.zrem(INFLIGHT_KEY, x.job_id)
    pipe.execute()
    
    # Add to recent results and broadcast
    manager.add_result(x.job_id, x.result)
    await manager.broadcast({
        "type": "result",
        "job_id": x.job_id,
        "result": x.result,
        "timestamp": int(time.time())
    })
    await broadcast_queue_update()
    await broadcast_job_update()
    
    return Response(status_code=204)

@app.get("/result/{job_id}")
def get_result(job_id: str):
    v = r.get(result_key(job_id))
    if v is None:
        raise HTTPException(404, "no result")
    return json.loads(v)

@app.get("/queue/status")
def get_queue_status():
    """Get current queue state and recent results"""
    state = get_queue_state()
    return {
        "queue_length": state["queue_length"],
        "inflight_count": state["inflight_count"],
        "recent_results": list(manager.recent_results)
    }

@app.get("/queue/jobs")
def get_queue_jobs():
    """Get current queue and inflight job details"""
    queue_jobs = []
    inflight_jobs = []
    
    # Get queue job IDs
    queue_job_ids = r.lrange(QUEUE_KEY, 0, -1)
    for job_id in queue_job_ids:
        payload_json = r.get(payload_key(job_id))
        if payload_json:
            try:
                payload = json.loads(payload_json)
                queue_jobs.append({"job_id": job_id, "payload": payload})
            except json.JSONDecodeError:
                pass
    
    # Get inflight job IDs
    inflight_job_ids = r.zrange(INFLIGHT_KEY, 0, -1)
    for job_id in inflight_job_ids:
        payload_json = r.get(payload_key(job_id))
        if payload_json:
            try:
                payload = json.loads(payload_json)
                inflight_jobs.append({"job_id": job_id, "payload": payload})
            except json.JSONDecodeError:
                pass
    
    return {
        "queue_jobs": queue_jobs,
        "inflight_jobs": inflight_jobs
    }

@app.post("/queue/clear", status_code=204)
async def clear_queue():
    """Clear all queue data: queue, inflight, payloads, and results"""
    # Get all job IDs from queue and inflight
    queue_job_ids = r.lrange(QUEUE_KEY, 0, -1)
    inflight_job_ids = r.zrange(INFLIGHT_KEY, 0, -1)
    all_job_ids = set(queue_job_ids + inflight_job_ids)
    
    # Delete all payloads and results
    pipe = r.pipeline()
    for job_id in all_job_ids:
        pipe.delete(payload_key(job_id))
        pipe.delete(result_key(job_id))
    pipe.execute()
    
    # Clear queue and inflight
    r.delete(QUEUE_KEY)
    r.delete(INFLIGHT_KEY)
    
    # Clear recent results in memory
    manager.recent_results.clear()
    
    # Broadcast updates
    await broadcast_queue_update()
    await broadcast_job_update()
    
    return Response(status_code=204)

@app.get("/ping", response_class=PlainTextResponse)
def ping():
    return "pong"

@app.get("/healthz")
def healthz():
    try:
        # Redis 疎通確認（最小・高速）
        r.ping()
        return {
            "status": "ok",
            "redis": "ok",
            "time": int(time.time())
        }
    except Exception as e:
        return Response(
            content='{"status":"ng","redis":"ng"}',
            media_type="application/json",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

async def requeue_loop():
    """
    in-flight の期限切れを queue に戻す。
    ※ ZRANGEBYSCORE はdeprecated扱いなので、ZRANGE BYSCORE を使うのが推奨。:contentReference[oaicite:4]{index=4}
    redis-py では zrange(..., byscore=True, ...) で呼べる。
    """
    while True:
        now = int(time.time())

        # 期限切れを最大100件ずつ回収
        expired = r.zrange(INFLIGHT_KEY, "-inf", now, byscore=True, start=0, num=100)
        if expired:
            pipe = r.pipeline()
            for job_id in expired:
                pipe.zrem(INFLIGHT_KEY, job_id)
                pipe.lpush(QUEUE_KEY, job_id)
            pipe.execute()
            await broadcast_job_update()

        await asyncio.sleep(REQUEUE_PERIOD_SEC)
