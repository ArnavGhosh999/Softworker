# Durable Long-Running AI Agent

A reference implementation of a durable execution architecture for AI agents — one that can break a task into steps, call tools, persist its state, pause for human approval, retry failures, and resume from exactly where it left off, even after a process restart.

Built as a single dependency-free Python file using SQLite for persistence, so the durability guarantees are real, not simulated in memory.

## Why this exists

Most "agent" demos run a task start-to-finish in one process and lose everything if that process dies. A production agent needs to survive crashes, wait days for a human approval, retry a flaky API call without double-executing it, and produce an auditable record of exactly what happened. This project is a minimal but complete architecture for that.

## Architecture

```
UI -> Agent Runtime/Planner -> Workflow Orchestrator -> Queue/Workers -> Tool Registry
                                       |                        |
                                  State Store              Approval System
                                       |                        |
                                 Memory Layer          Logging/Audit System
```

| Component | Role |
|---|---|
| **Agent runtime / planner** | Turns a task description into an ordered list of steps, each bound to a tool |
| **Workflow orchestrator** | Drives a run forward step by step; stateless itself, always re-reads from the state store — this is what makes crash recovery possible |
| **Queue / worker system** | Decouples "decide what to do" from "do it"; enables retries with exponential backoff |
| **Tool registry** | Catalog of callable tools/APIs, each invoked with an idempotency key |
| **State store** | SQLite-backed persistence for runs, steps, approvals, and audit events |
| **Approval system** | Halts a run at any step flagged `requires_approval` until a human resolves it |
| **Memory layer** | Per-run key/value store for context that should survive across steps |
| **Logging / audit system** | Append-only event trail for every state transition in a run |
| **Notification system** | Stub hook (prints to console) for alerting a human — swap for email/Slack/webhook |

## What it demonstrates

- **Task decomposition** — `plan_task()` breaks a task into an ordered step plan
- **Tool use** — steps call into a `ToolRegistry` (`search_web`, `draft_summary`, `send_email`)
- **Persistent state** — every run and step lives in `agent_state.db`, not in memory
- **Pause and resume** — the orchestrator halts at approval gates and resumes purely from stored state
- **Retry with backoff** — failed tool calls are retried with exponential backoff up to a max attempt count
- **Human approval** — steps can require sign-off before the run continues
- **Crash recovery** — `main()` tears down and rebuilds the orchestrator/worker mid-run to simulate a real process restart, then resumes from disk
- **Audit trail** — every status change, retry, and approval decision is logged and printed at the end of a run

## Requirements

- Python 3.11+ (uses `datetime.now(UTC)`, added in 3.11)
- No third-party packages — only the standard library (`sqlite3`, `json`, `uuid`, `time`, `random`, `dataclasses`, `datetime`, `enum`)

## Running it

```bash
python agent.py
```

This runs a demo task ("Investigate Q3 churn spike and report to stakeholders") through the full pipeline:

1. Plans the task into 3 steps: research → summarize → notify stakeholder
2. Executes research and summarize (research may hit a simulated transient failure and retry automatically)
3. Halts at the `notify_stakeholder` step, which requires approval
4. Simulates a process restart, then simulates a human approving the step
5. Completes the run and prints the final output plus the full audit trail

A file called `agent_state.db` will be created in the working directory — this is the durable state store. Delete it to start fresh:

```bash
rm agent_state.db
```

You can also inspect it directly:

```bash
sqlite3 agent_state.db "SELECT * FROM runs;"
```

## Extending toward production

This is a reference implementation, not a production system. To harden it:

- Swap `InMemoryQueue` for a real message queue (SQS, Redis Streams, RabbitMQ)
- Swap SQLite for Postgres for concurrent access and horizontal scaling
- Replace `plan_task()` with an LLM call that returns a structured plan
- Replace `notify()` with a real channel (email, Slack, webhook)
- Add worker lease/heartbeat timeouts so a crashed worker's job gets reclaimed rather than stuck
- Add a global step budget / wall-clock timeout per run to guard against runaway loops

## License

See [LICENSE](LICENSE).