# Reliable Distributed Job Queue — Redis Streams + PostgreSQL

An at-least-once job processing example: API submission, idempotency keys, PostgreSQL job lifecycle, Redis Streams consumer groups, parallel workers, retries with exponential backoff, reclaim of abandoned pending stream messages, dead-letter stream, status APIs, and submission load harness.

**Real boundary:** This is a development reference, **not a formally exactly-once system**. DB insert and Redis enqueue are not transactionally atomic; worker completion and `XACK` are not atomic. A durable transactional outbox and reconciler, lease design, concurrency stress testing, dashboard, and infrastructure hardening are required before production use. Task handlers are allow-listed and bounded; never run arbitrary client shell commands.

## Run locally (Docker required for integration)

```bash
python -m pip install -r requirements.txt -r requirements-dev.txt
python -m pytest -q
docker compose up --build
```

Open http://localhost:8000/docs. `POST /jobs` requires `x-api-key: local-demo-key` and a JSON body, for example:

```json
{"kind":"fibonacci","payload":{"n":15},"idempotency_key":"unique-request-0001"}
```

Then `GET /jobs/{id}` to inspect status and result. Supported job kinds: `sum`, `fibonacci`, and `fail_once` to demonstrate retry. `GET /stats` shows states. Optional `python -m scripts.bench --count 50` reports accepted-job submission throughput and P95; it does not measure completed-job throughput.

## Architecture
FastAPI → PostgreSQL durable job state + Redis Streams → worker consumer group → bounded handler → result DB; failed jobs → delayed sorted set or dead-letter stream.

Tests verify deterministic task execution and retry rules without Redis/Postgres. Docker-based integration and fault injection tests have not been executed for this distribution.
