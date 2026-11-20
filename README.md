# Agent-Ready Real-Time Platform

A real-time data platform that turns NYC Uber/Lyft trip data into fresh,
zone-level operational metrics, with an AI operations agent on top that
answers questions like *"Where is there a driver shortage right now, and why?"*

The idea: an agent is only as good as the data underneath it.

## Architecture

```text
Go replay producer ──> Kafka ──> Flink ──┬──> Redis (live features)  ──┐
 (trip events)        (KRaft)            └──> Iceberg on S3 (history) ──┤
                                                                        ├──> Ops API (HTTP + MCP) ──> Ops agent
                       dbt batch models (baselines, history) ───────────┘
```

- **Producer (Go):** replays real trips as events (requested, arrived,
  picked up, dropped off) on a replay clock.
- **Kafka (Strimzi, KRaft):** event backbone; malformed events go to a DLQ.
- **Flink:** exactly-once raw writes to Iceberg, and live features per zone
  and provider every minute (15m / 30m windows).
- **Redis:** online store for live features; separate ACL user per writer.
- **Iceberg on S3 + dbt:** raw history, clean tables and "normal" baselines
  (same zone, weekday and hour).
- **Ops API:** HTTP and MCP over live and batch features, read-only, with an
  audit log.

## Scale

- 43 months of NYC trip data
- Up to 93M events a month, ~3.2M events a day

## Stack

AWS (EKS, S3, ECR, IAM, Glue) · Terraform · Kubernetes · Go · Apache Kafka ·
Apache Flink · Apache Iceberg · Redis · dbt · Python · FastAPI · MCP

## Repository layout

| Path | Contents |
|---|---|
| `*.tf` | AWS infrastructure (EKS, S3, ECR, IAM, budget, remote state) |
| `producer/` | Go replay producer |
| `flink/` | Flink jobs: raw writer and live features |
| `features/` | Feature definitions (single source for batch and live) |
| `transform/` | dbt models and tests |
| `serving/` | Ops API (HTTP + MCP) |
| `agents/` | Operations agent |
| `k8s/` | Kubernetes manifests (Kafka, Flink, Redis, producer) |
| `ingest/` | Data loading and backfill scripts |
| `docs/` | Design decisions |
