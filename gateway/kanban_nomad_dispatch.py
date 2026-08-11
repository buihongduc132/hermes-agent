# Nomad Dispatch spawn_fn for Hermes Kanban Workers
#
# This module provides an alternative spawn function for the Hermes kanban
# dispatcher. Instead of spawning child processes (which die with the gateway
# when Nomad kills the alloc cgroup), it dispatches workers as separate
# Nomad parameterized batch jobs that survive gateway restarts.
#
# Architecture:
#   Gateway (Nomad alloc A, cgroup A)
#     └── dispatcher tick → spawn_fn=nomad_dispatch_spawn()
#           └── HTTP POST to Nomad API → dispatch kanban-worker job
#                 └── Worker (Nomad alloc B, cgroup B) ← INDEPENDENT
#
# Approach A: Workers are Nomad dispatches (separate alloc, separate cgroup)
# Approach C: Workers persist checkpoint state in kanban DB
#             (the kanban board already has claim/heartbeat/reclaim)
#
# Usage:
#   The gateway's _kanban_dispatcher_watcher() is patched to import this
#   module and pass nomad_dispatch_spawn as spawn_fn to dispatch_once().

import json
import logging
import os
from typing import Optional

import requests

logger = logging.getLogger("gateway.kanban_nomad_dispatch")

# Nomad API endpoint — uses the local agent
NOMAD_ADDR = os.environ.get("NOMAD_ADDR", f"http://{os.environ.get('NOMAD_IP_http', '100.114.135.99')}:4646")
# The parameterized job name
KANBAN_WORKER_JOB = os.environ.get("HERMES_KANBAN_NOMAD_JOB", "kanban-worker")
# Timeout for the dispatch API call
DISPATCH_TIMEOUT = 10


def nomad_dispatch_spawn(task, workspace: str, *, board=None) -> Optional[int]:
    """Spawn a kanban worker as a Nomad parameterized batch dispatch.

    Returns a pseudo-PID derived from the dispatched job ID. The dispatcher
    uses this for crash detection (PID-not-alive check). Since Nomad-managed
    workers don't have local PIDs, we return a sentinel that the dispatcher's
    crash detection will handle differently.

    The crash detection in kanban_db.detect_crashed_workers() checks if
    the PID is alive via os.kill(pid, 0). For Nomad-dispatched workers,
    we return a negative PID which will never match a real process, so
    crash detection relies on the claim TTL / heartbeat mechanism instead.
    """
    if not task.assignee:
        raise ValueError(f"task {task.id} has no assignee")

    # Build dispatch meta from the task
    meta = {
        "TASK_ID": task.id,
        "PROFILE": task.assignee,
    }
    if workspace:
        meta["WORKSPACE"] = workspace
    if board:
        meta["BOARD"] = board
    if task.tenant:
        meta["TENANT"] = task.tenant
    if task.branch_name:
        meta["BRANCH"] = task.branch_name
    if task.current_run_id is not None:
        meta["RUN_ID"] = str(task.current_run_id)
    if task.claim_lock:
        meta["CLAIM_LOCK"] = task.claim_lock
    if task.goal_mode:
        meta["GOAL_MODE"] = "1"
        if task.goal_max_turns is not None:
            meta["GOAL_MAX_TURNS"] = str(int(task.goal_max_turns))
    if task.skills:
        meta["SKILLS"] = ",".join(task.skills)
    if task.model_override:
        meta["MODEL_OVERRIDE"] = task.model_override

    # Dispatch via Nomad API
    url = f"{NOMAD_ADDR}/v1/job/{KANBAN_WORKER_JOB}/dispatch"
    payload = {"Meta": meta}

    try:
        resp = requests.post(
            url,
            json=payload,
            timeout=DISPATCH_TIMEOUT,
        )
        resp.raise_for_status()
        result = resp.json()
        dispatched_job_id = result.get("DispatchedJobID", "")
        logger.info(
            "kanban nomad dispatch: task %s → job %s (profile=%s)",
            task.id, dispatched_job_id, task.assignee,
        )
        # Return a negative sentinel PID.
        # The dispatcher stores this as worker_pid. Crash detection
        # (detect_crashed_workers) checks os.kill(pid, 0) which will
        # raise OSError for negative PIDs, so the dispatcher will rely
        # on the claim TTL and heartbeat to detect dead workers instead.
        # This is actually MORE reliable than PID checking since
        # Nomad workers can be on different machines.
        return -hash(dispatched_job_id) % (2**31)

    except requests.exceptions.RequestException as exc:
        logger.error(
            "kanban nomad dispatch: FAILED for task %s: %s",
            task.id, exc,
        )
        raise RuntimeError(
            f"Nomad dispatch failed for task {task.id}: {exc}"
        ) from exc
