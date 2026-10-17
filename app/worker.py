import logging
import os
import random
import time

from celery import signals

from .celery_app import celery_app
from .logging_setup import setup_logging

log = logging.getLogger("worker")


@signals.setup_logging.connect
def _configure_logging(**_kwargs):
    # Connecting to this signal stops Celery from installing its own (non-JSON) handlers
    setup_logging(os.environ.get("LOG_LEVEL", "INFO"))


def _should_fail(fail: bool, fail_rate: float) -> bool:
    return fail or (fail_rate > 0 and random.random() < fail_rate)


@celery_app.task(name="lab.health_check", bind=True)
def health_check(self, duration_s: float = 0.5, fail: bool = False, fail_rate: float = 0.0):
    time.sleep(duration_s)
    if _should_fail(fail, fail_rate):
        log.warning("health check failed", extra={"fields": {"task_id": self.request.id}})
        raise RuntimeError("health check failed (simulated)")
    return {"status": "healthy"}


@celery_app.task(name="lab.low_priority_job", bind=True)
def low_priority_job(self, duration_s: float = 5.0, fail: bool = False, fail_rate: float = 0.0):
    time.sleep(duration_s)
    if _should_fail(fail, fail_rate):
        log.warning("low priority job failed", extra={"fields": {"task_id": self.request.id}})
        raise RuntimeError("background job failed (simulated)")
    return {"processed": True}


@celery_app.task(name="lab.collect_data", bind=True, max_retries=3)
def collect_data(self, duration_s: float = 2.0, fail: bool = False, fail_rate: float = 0.0):
    time.sleep(duration_s)
    if _should_fail(fail, fail_rate):
        log.warning("collect attempt failed, will retry", extra={"fields": {
            "task_id": self.request.id, "retries": self.request.retries}})
        # After max_retries, Celery re-raises this exception and the task FAILS
        raise self.retry(exc=RuntimeError("upstream unavailable (simulated)"), countdown=2)
    return {"records": random.randint(10, 100)}
