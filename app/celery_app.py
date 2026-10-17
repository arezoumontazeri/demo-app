import os
from urllib.parse import quote

from celery import Celery


def redis_url(db: int) -> str:
    password = os.environ.get("REDIS_PASSWORD", "")
    host = os.environ.get("REDIS_HOST", "redis")
    auth = f":{quote(password, safe='')}@" if password else ""
    return f"redis://{auth}{host}:6379/{db}"


celery_app = Celery("lab", broker=redis_url(0), backend=redis_url(1))
celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,            # result keys expire after 1 h -> real "expired keys" in Row 5
    task_track_started=True,
    broker_connection_retry_on_startup=True,
    worker_send_task_events=True,   # events for the monitor (same as celery -E)
    worker_prefetch_multiplier=1,   # each process reserves 1 message -> queue depth stays meaningful
)

# endpoint name -> (registered task name, queue)
TASKS = {
    "check": ("lab.health_check", "checks"),
    "collect": ("lab.collect_data", "data_collection"),
    "low": ("lab.low_priority_job", "low"),
}
