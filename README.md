# AstraQuant

AstraQuant is an event-driven algorithmic trading engine designed to detect and execute high-probability continuation moves following institutional momentum.

The system implements a strict behavioral model:

**Momentum Burst → Market Acceptance → Controlled Continuation**

Rather than predicting price, AstraQuant reacts to verified order-flow behavior while enforcing hard risk constraints.

---

## Philosophy

AstraQuant is built on three principles:

* **No prediction.** Only trade confirmed behavior.
* **Risk first.** Execution is always subordinate to session and drawdown guards.
* **Deterministic infrastructure.** Every action is logged, reproducible, and auditable.

---

## Architecture

The platform is deployed as a containerized microservices stack:

| Service  | Role                                       |
| -------- | ------------------------------------------ |
| `engine` | Core decision engine & trade orchestration |
| `ws`     | Market data ingestion and event streaming  |
| `ai`     | Statistical validation & adaptive filters  |
| `risk`   | Hard risk gates and session governance     |
| `api`    | Execution interface & broker integration   |
| `db`     | Trade + telemetry persistence              |
| `redis`  | Event buffering / coordination             |
| `nginx`  | Controlled external exposure               |

All services run via Docker Compose for deterministic deployment.

---

## Risk Controls (Non-Negotiable)

* Session drawdown guard
* Loss-streak pause logic
* Execution enable/disable switches
* Environment-level trade blocking
* Full execution logging for auditability

---

## Deployment Model

AstraQuant is designed for **VPS-based 24/7 execution**.

```
/opt/astraquant
    ├── docker-compose.yml
    ├── engine/
    ├── risk/
    ├── Strategy services...
```

This layout separates infrastructure from user environments and mirrors production Linux service conventions.

---

## Status

Live research and controlled execution environment.

This repository tracks infrastructure and strategy evolution — runtime data, logs, and secrets are intentionally excluded.

---

## Disclaimer

This project is a research system. It is not financial advice and is operated with strict internal risk controls.
