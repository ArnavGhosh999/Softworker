"""
Durable long-running AI agent — reference implementation.

Architecture (see accompanying diagram):
  UI -> Agent Runtime/Planner -> Workflow Orchestrator -> Queue/Workers -> Tool Registry
                                        |                        |
                                   State Store              Approval System
                                        |                        |
                                  Memory Layer          Logging/Audit System

This file simulates the orchestrator, state store, queue/worker system,
tool registry, approval system, and audit log using SQLite so that state
genuinely survives a process restart (the core durability guarantee).

Run:  python agent.py
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
from enum import Enum
from typing import Any, Callable, Optional

DB_PATH = "agent_state.db"


# --------------------------------------------------------------------------
# State store — persistent, durable source of truth for every run and step.
# --------------------------------------------------------------------------

class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    WAITING_APPROVAL = "waiting_approval"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    WAITING_APPROVAL = "waiting_approval"
    PAUSED = "paused"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class StateStore:
    """
    Durable persistence for runs, steps, and the audit log.
    Backed by SQLite so state survives process crashes/restarts —
    this is what makes pause/resume and crash-recovery possible.
    """

    def __init__(self, db_path: str = DB_PATH):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self):
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                task TEXT NOT NULL,
                status TEXT NOT NULL,
                plan TEXT NOT NULL,
                current_step INTEGER NOT NULL DEFAULT 0,
                output TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS steps (
                run_id TEXT NOT NULL,
                step_index INTEGER NOT NULL,
                step_id TEXT NOT NULL,
                name TEXT NOT NULL,
                tool TEXT NOT NULL,
                args TEXT NOT NULL,
                requires_approval INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                result TEXT,
                error TEXT,
                idempotency_key TEXT NOT NULL,
                PRIMARY KEY (run_id, step_index)
            );

            CREATE TABLE IF NOT EXISTS approvals (
                approval_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                step_index INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                requested_at TEXT NOT NULL,
                resolved_at TEXT,
                resolver TEXT,
                note TEXT
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                ts TEXT NOT NULL,
                event TEXT NOT NULL,
                detail TEXT
            );

            CREATE TABLE IF NOT EXISTS memory (
                run_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (run_id, key)
            );
            """
        )
        self.conn.commit()

    # ---- audit -----------------------------------------------------
    def log(self, run_id: str, event: str, detail: Any = None):
        self.conn.execute(
            "INSERT INTO audit_log (run_id, ts, event, detail) VALUES (?, ?, ?, ?)",
            (run_id, datetime.now(UTC).isoformat(), event, json.dumps(detail)),
        )
        self.conn.commit()

    def get_audit_trail(self, run_id: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT ts, event, detail FROM audit_log WHERE run_id=? ORDER BY id", (run_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    # ---- runs --------------------------------------------------------
    def create_run(self, run_id: str, task: str, plan: list[dict]):
        now = datetime.now(UTC).isoformat()
        self.conn.execute(
            "INSERT INTO runs (run_id, task, status, plan, current_step, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, 0, ?, ?)",
            (run_id, task, RunStatus.PENDING, json.dumps(plan), now, now),
        )
        for i, step in enumerate(plan):
            idem_key = f"{run_id}:{i}"
            self.conn.execute(
                "INSERT INTO steps (run_id, step_index, step_id, name, tool, args, "
                "requires_approval, status, attempts, max_attempts, idempotency_key) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)",
                (
                    run_id, i, str(uuid.uuid4()), step["name"], step["tool"],
                    json.dumps(step.get("args", {})),
                    int(step.get("requires_approval", False)),
                    StepStatus.PENDING,
                    step.get("max_attempts", 3),
                    idem_key,
                ),
            )
        self.conn.commit()
        self.log(run_id, "run_created", {"task": task, "num_steps": len(plan)})

    def get_run(self, run_id: str) -> dict:
        row = self.conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        return dict(row) if row else None

    def update_run_status(self, run_id: str, status: RunStatus, output: Any = None):
        self.conn.execute(
            "UPDATE runs SET status=?, output=?, updated_at=? WHERE run_id=?",
            (status, json.dumps(output) if output is not None else None,
             datetime.now(UTC).isoformat(), run_id),
        )
        self.conn.commit()
        self.log(run_id, "run_status_changed", {"status": status})

    def advance_step(self, run_id: str):
        self.conn.execute(
            "UPDATE runs SET current_step = current_step + 1, updated_at=? WHERE run_id=?",
            (datetime.now(UTC).isoformat(), run_id),
        )
        self.conn.commit()

    # ---- steps ---------------------------------------------------------
    def get_step(self, run_id: str, step_index: int) -> dict:
        row = self.conn.execute(
            "SELECT * FROM steps WHERE run_id=? AND step_index=?", (run_id, step_index)
        ).fetchone()
        return dict(row) if row else None

    def get_steps(self, run_id: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM steps WHERE run_id=? ORDER BY step_index", (run_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    def set_step_status(self, run_id: str, step_index: int, status: StepStatus,
                         result: Any = None, error: str = None, inc_attempts: bool = False):
        if inc_attempts:
            self.conn.execute(
                "UPDATE steps SET status=?, result=?, error=?, attempts = attempts + 1 "
                "WHERE run_id=? AND step_index=?",
                (status, json.dumps(result) if result is not None else None, error,
                 run_id, step_index),
            )
        else:
            self.conn.execute(
                "UPDATE steps SET status=?, result=?, error=? WHERE run_id=? AND step_index=?",
                (status, json.dumps(result) if result is not None else None, error,
                 run_id, step_index),
            )
        self.conn.commit()
        self.log(run_id, "step_status_changed", {"step_index": step_index, "status": status})

    # ---- approvals -------------------------------------------------------
    def create_approval(self, run_id: str, step_index: int) -> str:
        approval_id = str(uuid.uuid4())
        self.conn.execute(
            "INSERT INTO approvals (approval_id, run_id, step_index, status, requested_at) "
            "VALUES (?, ?, ?, 'pending', ?)",
            (approval_id, run_id, step_index, datetime.now(UTC).isoformat()),
        )
        self.conn.commit()
        self.log(run_id, "approval_requested", {"step_index": step_index, "approval_id": approval_id})
        return approval_id

    def resolve_approval(self, approval_id: str, approved: bool, resolver: str = "human", note: str = ""):
        row = self.conn.execute(
            "SELECT * FROM approvals WHERE approval_id=?", (approval_id,)
        ).fetchone()
        if not row:
            raise ValueError("No such approval")
        self.conn.execute(
            "UPDATE approvals SET status=?, resolved_at=?, resolver=?, note=? WHERE approval_id=?",
            ("approved" if approved else "rejected", datetime.now(UTC).isoformat(),
             resolver, note, approval_id),
        )
        self.conn.commit()
        self.log(row["run_id"], "approval_resolved",
                  {"approval_id": approval_id, "approved": approved, "resolver": resolver})
        return dict(row)

    def get_pending_approval(self, run_id: str, step_index: int) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT * FROM approvals WHERE run_id=? AND step_index=? AND status='pending' "
            "ORDER BY requested_at DESC LIMIT 1",
            (run_id, step_index),
        ).fetchone()
        return dict(row) if row else None

    # ---- memory ---------------------------------------------------------
    def remember(self, run_id: str, key: str, value: Any):
        self.conn.execute(
            "INSERT INTO memory (run_id, key, value) VALUES (?, ?, ?) "
            "ON CONFLICT(run_id, key) DO UPDATE SET value=excluded.value",
            (run_id, key, json.dumps(value)),
        )
        self.conn.commit()

    def recall(self, run_id: str) -> dict:
        rows = self.conn.execute("SELECT key, value FROM memory WHERE run_id=?", (run_id,)).fetchall()
        return {r["key"]: json.loads(r["value"]) for r in rows}


# --------------------------------------------------------------------------
# Tool registry — the set of callable tools/APIs, each idempotency-aware.
# --------------------------------------------------------------------------

class ToolRegistry:
    def __init__(self):
        self._tools: dict[str, Callable] = {}

    def register(self, name: str):
        def deco(fn):
            self._tools[name] = fn
            return fn
        return deco

    def get(self, name: str) -> Callable:
        if name not in self._tools:
            raise KeyError(f"Unknown tool: {name}")
        return self._tools[name]


registry = ToolRegistry()


# Example tools. Each should be written to be safely retryable
# (idempotent) given the same idempotency_key.
@registry.register("search_web")
def tool_search_web(args: dict, idempotency_key: str) -> dict:
    query = args.get("query", "")
    # Simulate occasional transient failure to exercise the retry path.
    if random.random() < 0.25:
        raise RuntimeError("transient network error")
    return {"query": query, "results": [f"Result about '{query}' #{i}" for i in range(1, 4)]}


@registry.register("draft_summary")
def tool_draft_summary(args: dict, idempotency_key: str) -> dict:
    context = args.get("context", "")
    return {"summary": f"Summary based on: {context[:80]}..."}


@registry.register("send_email")
def tool_send_email(args: dict, idempotency_key: str) -> dict:
    # A real implementation would pass idempotency_key to the email
    # provider so a retried call doesn't send twice.
    return {"status": "sent", "to": args.get("to"), "idempotency_key": idempotency_key}


# --------------------------------------------------------------------------
# Queue / worker system — decouples "decide" from "do", enables retries.
# --------------------------------------------------------------------------

@dataclass
class Job:
    run_id: str
    step_index: int
    not_before: float = field(default_factory=time.time)  # enables backoff


class InMemoryQueue:
    """
    Stand-in for a real queue (SQS, Redis, RabbitMQ, etc). Jobs carry a
    not_before timestamp so retries can honor exponential backoff.
    """
    def __init__(self):
        self._jobs: list[Job] = []

    def enqueue(self, job: Job):
        self._jobs.append(job)

    def pop_ready(self) -> Optional[Job]:
        now = time.time()
        for i, job in enumerate(self._jobs):
            if job.not_before <= now:
                return self._jobs.pop(i)
        return None

    def next_wake_time(self) -> Optional[float]:
        """Earliest not_before among pending jobs, even if not ready yet.
        Lets the driver loop know how long it's safe to sleep/wait
        before a scheduled retry becomes eligible."""
        if not self._jobs:
            return None
        return min(job.not_before for job in self._jobs)

    def __len__(self):
        return len(self._jobs)


class Worker:
    """Pulls jobs off the queue and executes the corresponding step."""

    def __init__(self, store: StateStore, queue: InMemoryQueue, orchestrator: "Orchestrator"):
        self.store = store
        self.queue = queue
        self.orchestrator = orchestrator

    def run_once(self) -> bool:
        job = self.queue.pop_ready()
        if job is None:
            return False
        self._execute(job)
        return True

    def _execute(self, job: Job):
        store, run_id, idx = self.store, job.run_id, job.step_index
        step = store.get_step(run_id, idx)

        store.set_step_status(run_id, idx, StepStatus.RUNNING, inc_attempts=True)
        try:
            tool_fn = registry.get(step["tool"])
            args = json.loads(step["args"])
            result = tool_fn(args, step["idempotency_key"])
        except Exception as exc:
            self._handle_failure(job, step, str(exc))
            return

        store.set_step_status(run_id, idx, StepStatus.SUCCEEDED, result=result)
        store.log(run_id, "tool_call_succeeded", {"step_index": idx, "tool": step["tool"]})
        self.orchestrator.on_step_complete(run_id)

    def _handle_failure(self, job: Job, step: dict, error: str):
        store, run_id, idx = self.store, job.run_id, job.step_index
        attempts = step["attempts"] + 1  # already incremented in DB; mirror here
        max_attempts = step["max_attempts"]
        store.log(run_id, "tool_call_failed", {"step_index": idx, "attempt": attempts, "error": error})

        if attempts < max_attempts:
            backoff = min(2 ** attempts, 30)  # exponential backoff, capped
            store.set_step_status(run_id, idx, StepStatus.PENDING, error=error)
            store.log(run_id, "step_retry_scheduled", {"step_index": idx, "backoff_seconds": backoff})
            self.queue.enqueue(Job(run_id=run_id, step_index=idx, not_before=time.time() + backoff))
        else:
            store.set_step_status(run_id, idx, StepStatus.FAILED, error=error)
            store.update_run_status(run_id, RunStatus.FAILED)
            store.log(run_id, "run_failed", {"step_index": idx, "reason": "max_attempts_exceeded"})


# --------------------------------------------------------------------------
# Workflow orchestrator — drives a run forward step by step; the durable
# execution core. Fully stateless itself: everything it needs is re-read
# from the state store, so it can be killed and restarted at any point.
# --------------------------------------------------------------------------

class Orchestrator:
    def __init__(self, store: StateStore, queue: InMemoryQueue):
        self.store = store
        self.queue = queue

    def start_run(self, run_id: str):
        self.store.update_run_status(run_id, RunStatus.RUNNING)
        self._advance(run_id)

    def resume_run(self, run_id: str):
        """Re-entry point after a pause or a crash: just re-read state and continue."""
        run = self.store.get_run(run_id)
        self.store.log(run_id, "run_resumed", {"current_step": run["current_step"]})
        self.store.update_run_status(run_id, RunStatus.RUNNING)
        self._advance(run_id)

    def on_step_complete(self, run_id: str):
        self.store.advance_step(run_id)
        self._advance(run_id)

    def _advance(self, run_id: str):
        run = self.store.get_run(run_id)
        plan = json.loads(run["plan"])
        idx = run["current_step"]

        if idx >= len(plan):
            self._finalize(run_id)
            return

        step = self.store.get_step(run_id, idx)

        if step["requires_approval"] and step["status"] == StepStatus.PENDING:
            approval_id = self.store.create_approval(run_id, idx)
            self.store.set_step_status(run_id, idx, StepStatus.WAITING_APPROVAL)
            self.store.update_run_status(run_id, RunStatus.WAITING_APPROVAL)
            notify(f"Run {run_id} needs approval for step '{step['name']}' (approval_id={approval_id})")
            return  # halt — resumes only when approve_step() is called

        if step["status"] in (StepStatus.PENDING,):
            self.queue.enqueue(Job(run_id=run_id, step_index=idx))

    def approve_step(self, run_id: str, approval_id: str, approved: bool, resolver: str = "human"):
        self.store.resolve_approval(approval_id, approved, resolver)
        run = self.store.get_run(run_id)
        idx = run["current_step"]

        if approved:
            self.store.set_step_status(run_id, idx, StepStatus.PENDING)
            self.store.update_run_status(run_id, RunStatus.RUNNING)
            self.queue.enqueue(Job(run_id=run_id, step_index=idx))
        else:
            self.store.set_step_status(run_id, idx, StepStatus.SKIPPED)
            self.store.update_run_status(run_id, RunStatus.FAILED)
            self.store.log(run_id, "run_failed", {"reason": "approval_rejected", "step_index": idx})
            notify(f"Run {run_id} was rejected at step {idx} and has stopped.")

    def _finalize(self, run_id: str):
        steps = self.store.get_steps(run_id)
        output = {s["name"]: json.loads(s["result"]) if s["result"] else None for s in steps}
        self.store.update_run_status(run_id, RunStatus.SUCCEEDED, output=output)
        notify(f"Run {run_id} completed successfully.")


# --------------------------------------------------------------------------
# Notification system (stub — swap for email/Slack/webhook in production)
# --------------------------------------------------------------------------

def notify(message: str):
    print(f"[NOTIFY] {message}")


# --------------------------------------------------------------------------
# Agent runtime / planner — turns a task description into a step plan.
# In production this would call an LLM; here it's a simple rule-based
# stand-in so the example is runnable without external API keys.
# --------------------------------------------------------------------------

def plan_task(task: str) -> list[dict]:
    return [
        {"name": "research", "tool": "search_web", "args": {"query": task}, "max_attempts": 3},
        {"name": "summarize", "tool": "draft_summary", "args": {"context": task}, "max_attempts": 2},
        {
            "name": "notify_stakeholder",
            "tool": "send_email",
            "args": {"to": "stakeholder@example.com", "subject": f"Update: {task}"},
            "requires_approval": True,
            "max_attempts": 2,
        },
    ]


# --------------------------------------------------------------------------
# Driver loop — simulates the always-on worker pool processing the queue.
# --------------------------------------------------------------------------

def run_worker_loop(worker: Worker, max_iterations: int = 200, idle_sleep: float = 0.05,
                     max_wait_for_retry: float = 60.0):
    """
    Drives the queue until it's genuinely empty (no jobs at all — not even
    ones scheduled for the future). If a retry is scheduled, sleep until
    it's due rather than giving up early; only bail out if nothing is
    queued or a wait would exceed max_wait_for_retry.
    """
    for _ in range(max_iterations):
        did_work = worker.run_once()
        if did_work:
            continue

        wake_at = worker.queue.next_wake_time()
        if wake_at is None:
            break  # queue is truly empty — nothing left to do

        wait = wake_at - time.time()
        if wait > max_wait_for_retry:
            break  # don't block indefinitely on a far-future retry
        if wait > 0:
            time.sleep(min(wait, idle_sleep + wait))  # sleep until the retry is due
        else:
            time.sleep(idle_sleep)


# --------------------------------------------------------------------------
# Demo
# --------------------------------------------------------------------------

def main():
    store = StateStore(DB_PATH)
    queue = InMemoryQueue()
    orchestrator = Orchestrator(store, queue)
    worker = Worker(store, queue, orchestrator)

    run_id = str(uuid.uuid4())
    task = "Investigate Q3 churn spike and report to stakeholders"
    plan = plan_task(task)

    store.create_run(run_id, task, plan)
    print(f"Created run {run_id} with {len(plan)} steps\n")

    orchestrator.start_run(run_id)
    run_worker_loop(worker)  # processes research + summarize; halts at approval gate

    run = store.get_run(run_id)
    print(f"\nRun status after first pass: {run['status']}")

    if run["status"] == RunStatus.WAITING_APPROVAL:
        pending = store.get_pending_approval(run_id, run["current_step"])
        print(f"Approval pending: {pending['approval_id']}")

        # --- Simulate process restart: a fresh StateStore/Orchestrator/Worker
        # reading the SAME on-disk DB, proving state genuinely persisted. ---
        del store, orchestrator, worker
        store = StateStore(DB_PATH)
        orchestrator = Orchestrator(store, queue)
        worker = Worker(store, queue, orchestrator)

        print("\n--- Simulating human approval after 'restart' ---")
        orchestrator.approve_step(run_id, pending["approval_id"], approved=True, resolver="alice@company.com")
        run_worker_loop(worker)

    run = store.get_run(run_id)
    print(f"\nFinal run status: {run['status']}")
    print(f"Final output: {run['output']}")

    print("\n--- Audit trail ---")
    for entry in store.get_audit_trail(run_id):
        print(f"{entry['ts']}  {entry['event']:<24} {entry['detail']}")


if __name__ == "__main__":
    main()