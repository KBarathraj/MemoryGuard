# MemoryGuard

**Behavioral SQL‑anomaly detection for PostgreSQL.**

MemoryGuard watches your database's query patterns, builds a behavioral profile, and alerts you the moment something drifts — before damage is done.

## Architecture

| Module | Purpose |
|--------|---------|
| `src/capture/` | Reads pg_audit logs from PostgreSQL |
| `src/parser/` | Parses SQL into ASTs via SQLGlot |
| `src/memory/` | Stores behavioral baselines in Redis |
| `src/drift/` | Scores anomalies using Z-score + Jaccard similarity |
| `src/alert/` | Sends alerts via Slack / SIEM webhooks |

## Quick Start

```bash
docker-compose up -d
```

## Project Layout

```
memoryguard/
├── README.md
├── docker-compose.yml
├── src/
│   ├── capture/        # pg_audit log reader
│   ├── parser/         # SQLGlot AST parsing
│   ├── memory/         # Redis behavioral store
│   ├── drift/          # Z-score + Jaccard scorer
│   └── alert/          # Slack / SIEM webhook
├── tests/
└── demo/               # seeded Postgres + attack script
```

## License

MIT
