import asyncio
import logging
import os
import random
import socket
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import redis as redis_lib
from celery.result import AsyncResult
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field
from starlette.routing import Match

from .celery_app import TASKS, celery_app, redis_url
from .logging_setup import setup_logging

setup_logging(os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("demo-app")
access = logging.getLogger("access")

SHARED_DIR = Path(os.environ.get("SHARED_DIR", "/mnt/shared"))
STORAGE_TIMEOUT_S = float(os.environ.get("STORAGE_TIMEOUT_S", "3"))
HOSTNAME = socket.gethostname()
QUIET_ROUTES = {"/healthz", "/readyz", "/metrics"}

# ---- Prometheus metrics (one process per pod: plain client registry is correct)
REQUESTS = Counter("app_http_requests_total", "HTTP requests", ["method", "route", "status"])
LATENCY = Histogram(
    "app_http_request_duration_seconds", "HTTP request latency", ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
TASKS_SUBMITTED = Counter("app_tasks_submitted_total", "Celery tasks submitted", ["task", "queue"])
TASK_SUBMIT_ERRORS = Counter("app_task_submit_errors_total", "Celery task submit failures", ["task"])
DEP_UP = Gauge("app_dependency_up", "1 if dependency answered the last readiness check", ["dependency"])
STORAGE_OK = Gauge("app_storage_check_success", "1 if the last /mnt/shared write+read+delete passed")
STORAGE_SECONDS = Gauge("app_storage_check_duration_seconds", "Duration of the last storage check")

# ---- Dependencies
pool = ConnectionPool(
    conninfo="",  # libpq reads PGHOST, PGUSER, PGPASSWORD, PGDATABASE from the environment
    min_size=1, max_size=int(os.environ.get("DB_POOL_MAX", "5")),
    timeout=5, open=False, kwargs={"connect_timeout": 2},
)
redis_client = redis_lib.Redis.from_url(redis_url(0), socket_timeout=2, socket_connect_timeout=2)
storage_lock = asyncio.Lock()
_schema_ready = False


def ensure_schema() -> None:
    global _schema_ready
    if _schema_ready:
        return
    with pool.connection(timeout=3) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS items ("
            " id BIGSERIAL PRIMARY KEY,"
            " name TEXT NOT NULL,"
            " created_at TIMESTAMPTZ NOT NULL DEFAULT now())"
        )
    _schema_ready = True


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool.open(wait=False)
    try:
        ensure_schema()
    except Exception as exc:  # DB may still be starting: readiness will show it
        log.warning("schema not ready at startup", extra={"fields": {"error": str(exc)}})
    log.info("startup complete", extra={"fields": {"host": HOSTNAME}})
    yield
    pool.close()


app = FastAPI(title="observability-lab demo-app", lifespan=lifespan)


def route_template(request: Request) -> str:
    for route in request.app.router.routes:
        match, _ = route.matches(request.scope)
        if match == Match.FULL:
            return route.path
    return "unmatched"


@app.middleware("http")
async def observe(request: Request, call_next):
    start = time.perf_counter()
    route = route_template(request)
    request_id = request.headers.get("x-request-id", uuid.uuid4().hex[:16])
    status = 500
    try:
        q = request.query_params
        delay_ms = min(max(int(q.get("chaos_delay_ms", "0") or 0), 0), 30000)
        if delay_ms:
            await asyncio.sleep(delay_ms / 1000)
        forced = q.get("chaos_status")
        rate = float(q.get("chaos_error_rate", "0") or 0)
        if forced or (rate > 0 and random.random() < rate):
            code = min(max(int(forced or 500), 100), 599)
            response = JSONResponse({"error": "chaos", "status": code}, status_code=code)
        else:
            response = await call_next(request)
        status = response.status_code
        response.headers["x-request-id"] = request_id
        return response
    finally:
        elapsed = time.perf_counter() - start
        REQUESTS.labels(request.method, route, str(status)).inc()
        LATENCY.labels(request.method, route).observe(elapsed)
        if route not in QUIET_ROUTES:
            access.info("request", extra={"fields": {
                "method": request.method, "route": route, "path": request.url.path,
                "status": status, "duration_ms": round(elapsed * 1000, 1),
                "request_id": request_id, "host": HOSTNAME,
            }})


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/readyz")
def readyz():
    checks = {}
    try:
        with pool.connection(timeout=2) as conn:
            conn.execute("SELECT 1")
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = f"error: {exc.__class__.__name__}"
    try:
        redis_client.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"error: {exc.__class__.__name__}"
    for dep, result in checks.items():
        DEP_UP.labels(dep).set(1 if result == "ok" else 0)
    ok = all(v == "ok" for v in checks.values())
    return JSONResponse({"ready": ok, "checks": checks}, status_code=200 if ok else 503)


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


class ItemIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)


@app.get("/api/items")
def list_items(limit: int = 20):
    ensure_schema()
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT id, name, created_at FROM items ORDER BY id DESC LIMIT %s", (min(limit, 200),)
        ).fetchall()
    return [{"id": r[0], "name": r[1], "created_at": r[2].isoformat()} for r in rows]


@app.post("/api/items", status_code=201)
def create_item(item: ItemIn):
    ensure_schema()
    with pool.connection() as conn:
        row = conn.execute(
            "INSERT INTO items (name) VALUES (%s) RETURNING id, created_at", (item.name,)
        ).fetchone()
    return {"id": row[0], "name": item.name, "created_at": row[1].isoformat()}


class TaskIn(BaseModel):
    duration_s: float = Field(default=1.0, ge=0, le=300)
    fail: bool = False
    fail_rate: float = Field(default=0.0, ge=0, le=1)


def submit(kind: str, body: TaskIn):
    task_name, queue = TASKS[kind]
    try:
        result = celery_app.send_task(task_name, kwargs=body.model_dump(), queue=queue)
    except Exception as exc:
        TASK_SUBMIT_ERRORS.labels(task_name).inc()
        log.error("task submit failed", extra={"fields": {"task": task_name, "error": str(exc)}})
        raise HTTPException(status_code=503, detail="broker unavailable")
    TASKS_SUBMITTED.labels(task_name, queue).inc()
    return JSONResponse({"task_id": result.id, "task": task_name, "queue": queue}, status_code=202)


@app.post("/api/tasks/check")
def task_check(body: TaskIn = TaskIn()):
    return submit("check", body)


@app.post("/api/tasks/collect")
def task_collect(body: TaskIn = TaskIn()):
    return submit("collect", body)


@app.post("/api/tasks/low")
def task_low(body: TaskIn = TaskIn()):
    return submit("low", body)


@app.get("/api/tasks/{task_id}")
def task_status(task_id: str):
    res = AsyncResult(task_id, app=celery_app)
    out = {"task_id": task_id, "state": res.state}
    if res.ready():
        out["result"] = str(res.result)
    return out


def _storage_probe() -> None:
    probe_dir = SHARED_DIR / ".probe"
    probe_dir.mkdir(parents=True, exist_ok=True)
    path = probe_dir / f"{HOSTNAME}.txt"
    token = uuid.uuid4().hex
    path.write_text(token)
    if path.read_text() != token:
        raise IOError("read-back mismatch")
    path.unlink()


@app.get("/api/storage/status")
async def storage_status():
    if storage_lock.locked():
        STORAGE_OK.set(0)
        return JSONResponse({"ok": False, "error": "previous check still running (storage hung?)"},
                            status_code=503)
    async with storage_lock:
        start = time.perf_counter()
        try:
            await asyncio.wait_for(asyncio.to_thread(_storage_probe), timeout=STORAGE_TIMEOUT_S)
            ok, error = True, None
        except asyncio.TimeoutError:
            ok, error = False, f"timeout after {STORAGE_TIMEOUT_S}s"
        except Exception as exc:
            ok, error = False, f"{exc.__class__.__name__}: {exc}"
        elapsed = time.perf_counter() - start
        STORAGE_OK.set(1 if ok else 0)
        STORAGE_SECONDS.set(elapsed)
        if not ok:
            log.error("storage check failed", extra={"fields": {"error": error, "path": str(SHARED_DIR)}})
        return JSONResponse({"ok": ok, "path": str(SHARED_DIR), "duration_ms": round(elapsed * 1000, 1),
                             "error": error}, status_code=200 if ok else 503)
