"""Single-process Celery monitor: task outcomes and runtimes from events,
queue depth / reserved from Redis, worker availability from heartbeats."""
import logging
import os
import threading
import time

import redis as redis_lib
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from .celery_app import TASKS, celery_app, redis_url
from .logging_setup import setup_logging

setup_logging(os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("celery-monitor")

TASK_QUEUE = {name: queue for name, queue in TASKS.values()}   # task name -> queue (static routing)
QUEUES = sorted(set(TASK_QUEUE.values()))
HEARTBEAT_TIMEOUT_S = float(os.environ.get("HEARTBEAT_TIMEOUT_S", "15"))
FORGET_AFTER_S = 600
OUTCOMES = {"task-succeeded": "succeeded", "task-failed": "failed", "task-retried": "retried"}

TASKS_TOTAL = Counter("celery_lab_tasks_total", "Task attempts by outcome", ["task", "queue", "outcome"])
DURATION = Histogram("celery_lab_task_duration_seconds", "Runtime of succeeded task attempts",
                     ["task", "queue"], buckets=(0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 20, 30, 60, 120, 300))
QUEUE_DEPTH = Gauge("celery_lab_queue_depth", "Messages waiting in the Redis list", ["queue"])
RESERVED = Gauge("celery_lab_tasks_reserved", "Messages prefetched by workers, not started (Redis 'unacked')")
EXECUTING = Gauge("celery_lab_tasks_executing", "Tasks started and not yet finished (from events)", ["queue"])
WORKER_UP = Gauge("celery_lab_worker_up", "1 if the worker heartbeat is fresh", ["worker", "queue"])
ACTIVE_WORKERS = Gauge("celery_lab_active_workers", "Worker instances with fresh heartbeat consuming the queue", ["queue"])
LAST_EVENT = Gauge("celery_lab_monitor_last_event_timestamp_seconds", "Unix time of the last Celery event received")
ERRORS = Counter("celery_lab_monitor_errors_total", "Monitor loop errors", ["loop"])

# Pre-create known series: dashboards show a real 0, not "No data"
for task_name, queue in TASK_QUEUE.items():
    for outcome in OUTCOMES.values():
        TASKS_TOTAL.labels(task_name, queue, outcome)
    DURATION.labels(task_name, queue)   # born at 0, so rate() sees the first observations
for q in QUEUES:
    ACTIVE_WORKERS.labels(q).set(0)
    EXECUTING.labels(q).set(0)

state = celery_app.events.State()
workers = {}           # hostname -> {"last": ts, "queues": set(), "exported": set()}
lock = threading.Lock()
rds = redis_lib.Redis.from_url(redis_url(0), socket_timeout=3, socket_connect_timeout=3)


def on_event(event: dict) -> None:
    LAST_EVENT.set(time.time())
    etype = event.get("type", "")
    with lock:
        state.event(event)
        if etype.startswith("worker-"):
            w = workers.setdefault(event.get("hostname"), {"last": 0.0, "queues": set(), "exported": set()})
            w["last"] = 0.0 if etype == "worker-offline" else time.time()
            return
        outcome = OUTCOMES.get(etype)
        if not outcome:
            return
        task = state.tasks.get(event.get("uuid"))
        name = task.name if task is not None and task.name else "unknown"
    queue = TASK_QUEUE.get(name, "unknown")
    TASKS_TOTAL.labels(name, queue, outcome).inc()
    if outcome == "succeeded" and event.get("runtime") is not None:
        DURATION.labels(name, queue).observe(float(event["runtime"]))


def events_loop() -> None:
    while True:
        try:
            with celery_app.connection() as conn:
                receiver = celery_app.events.Receiver(conn, handlers={"*": on_event})
                log.info("listening for celery events")
                receiver.capture(limit=None, timeout=None, wakeup=True)
        except Exception as exc:
            ERRORS.labels("events").inc()
            log.warning("event receiver error", extra={"fields": {"error": str(exc)}})
            time.sleep(5)


def update_worker_gauges() -> None:
    now = time.time()
    active = {q: 0 for q in QUEUES}
    with lock:
        for host in list(workers):
            w = workers[host]
            fresh = (now - w["last"]) < HEARTBEAT_TIMEOUT_S
            queues = w["queues"] or {"unknown"}
            current = {(host, q) for q in queues}
            for old in w["exported"] - current:
                WORKER_UP.remove(*old)
            if now - w["last"] > FORGET_AFTER_S:
                for labels in current:
                    WORKER_UP.remove(*labels)
                del workers[host]
                continue
            for q in queues:
                WORKER_UP.labels(host, q).set(1 if fresh else 0)
                if fresh and q in active:
                    active[q] += 1
            w["exported"] = current
        executing = {q: 0 for q in QUEUES}
        for t in list(state.tasks.values()):
            if t.state == "STARTED" and TASK_QUEUE.get(t.name) in executing:
                executing[TASK_QUEUE[t.name]] += 1
    for q in QUEUES:
        ACTIVE_WORKERS.labels(q).set(active[q])
        EXECUTING.labels(q).set(executing[q])


def poll_loop() -> None:
    last_inspect = 0.0
    while True:
        try:
            for q in QUEUES:
                QUEUE_DEPTH.labels(q).set(rds.llen(q))
            RESERVED.set(rds.hlen("unacked"))
            if time.time() - last_inspect > 30:
                replies = celery_app.control.inspect(timeout=2).active_queues() or {}
                with lock:
                    for host, qs in replies.items():
                        w = workers.setdefault(host, {"last": time.time(), "queues": set(), "exported": set()})
                        w["queues"] = {x["name"] for x in qs}
                last_inspect = time.time()
            update_worker_gauges()
        except Exception as exc:
            ERRORS.labels("poll").inc()
            log.warning("poll error", extra={"fields": {"error": str(exc)}})
        time.sleep(5)


def main() -> None:
    port = int(os.environ.get("METRICS_PORT", "9808"))
    start_http_server(port)
    threading.Thread(target=events_loop, daemon=True).start()
    threading.Thread(target=poll_loop, daemon=True).start()
    log.info("celery monitor started", extra={"fields": {"port": port, "queues": QUEUES}})
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
