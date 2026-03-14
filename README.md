# 🛡️ MemoryGuard

**Behavioral SQL-anomaly detection for PostgreSQL.** 

MemoryGuard watches your database's query patterns, builds a behavioral profile, and alerts you the moment something drifts — *before* data is exfiltrated. Built to catch compromised application users acting out of character.

---

## 🏎️ Hackathon Judges: Run the Demo!

Want to see MemoryGuard in action? We've built an interactive attack simulation that demonstrates how the engine distinguishes between normal app behavior and an active reconnaissance/exfiltration attack.

### 1. Setup
Make sure you have Python installed.

```bash
# Clone the repository (if you haven't already)
git clone https://github.com/KBarathraj/MemoryGuard.git
cd MemoryGuard

# Optional but recommended: Create a virtual environment
python -m venv venv
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Install the minimal dependencies
pip install -r requirements.txt
```

### 2. Run the Attack Simulation
Run the demo script from the root of the project:

```bash
python -m demo.attack
```

### What you'll see:
1. **Phase 1 (Baseline)**: The system analyzes normal application queries. You'll see `[   ok]` as MemoryGuard learns the baseline behavior.
2. **Phase 2 (Reconnaissance)**: An attacker compromises the app and probes `information_schema` and other unusual tables. MemoryGuard immediately fires `[🚨 ANOMALY]` alerts for **Column Novelty** (accessing tables/columns the app never uses).
3. **Phase 3 (Exfiltration)**: The attacker tries to dump `ssn`, `bank_account`, and `salary` with a massive volume spike. MemoryGuard flags this with maxed-out **Volume Spike** alerts along with column novelty.

---

## 🧠 Architecture Overview

MemoryGuard uses a dual-engine scoring system:
1. **Z-Score Volume Drift**: Tracks the velocity/volume of queries to detect scraping or dumping.
2. **Jaccard Column Novelty**: Fingerprints ASTs (Abstract Syntax Trees) to detect when a user accesses new columns they've never historically touched.

| Module | Purpose |
|--------|---------|
| `src/capture/` | Reads pg_audit logs from PostgreSQL |
| `src/parser/` | Parses SQL into ASTs via SQLGlot to extract column lineage |
| `src/memory/` | Stores behavioral baselines in a rolling Redis window |
| `src/drift/` | Scores anomalies using the dual-engine system |
| `src/alert/` | Sends alerts via Slack / SIEM webhooks when thresholds are breached |

---

## 🗄️ Project Layout

```text
memoryguard/
├── README.md
├── docker-compose.yml
├── src/
│   ├── capture/
│   ├── parser/
│   ├── memory/
│   ├── drift/
│   └── alert/
├── tests/
└── demo/               # The attack simulation
```

## License
MIT
