# Job Radar

A modular, production-quality job aggregator that monitors company career pages and 900+ ATS job boards, sending HTML email digests (plus optional Slack/Discord alerts) whenever new matching engineering roles appear.

## What's improved over the original

| Area | Original | Job Radar |
|---|---|---|
| **Architecture** | 2 000-line monolith | Modular package (`src/sources/`, `src/utils/`) |
| **State storage** | Growing JSON files | SQLite database (queryable, bounded) |
| **Email** | Plain-text | HTML with colour-coded YES/MAYBE buckets + score badges |
| **Notifications** | Email only | Email + Slack + Discord |
| **Logging** | `print()` statements | `logging` module with levels |
| **Config** | Env vars only | YAML file + env var overrides |
| **Job ranking** | yes / maybe / no | 0-100 numeric score |
| **Source fetching** | Sequential | Concurrent via `ThreadPoolExecutor` |
| **Board health** | JSON dead list | SQLite `boards` table with fail counts |
| **Adding sources** | Edit monolith | Create a new file in `src/sources/` |

---

## Quick start

```bash
# 1. Clone & install
git clone https://github.com/YOUR_USERNAME/job-radar.git
cd job-radar
pip install -r requirements.txt

# 2. Configure
cp config.example.yaml config.yaml
# Edit config.yaml with your email / Slack / Discord details

# 3. Run
python -m src.main                         # main mode (6 companies)
python -m src.main --mode boards           # board sweep (900+ ATS boards)
python -m src.main --test-notify           # send a test email
python -m src.main --dry-run --verbose     # see what would happen
```

---

## Configuration

All options can be set in `config.yaml` (copy from `config.example.yaml`) **or** via environment variables. Environment variables always win.

### Key environment variables

| Variable | Purpose |
|---|---|
| `EMAIL_USER` | Gmail account (`you@gmail.com`) |
| `EMAIL_APP_PASSWORD` | [Gmail App Password](https://support.google.com/accounts/answer/185833) |
| `ALERT_TO_EMAIL` | Recipient address |
| `SLACK_WEBHOOK_URL` | Optional Slack incoming webhook |
| `DISCORD_WEBHOOK_URL` | Optional Discord webhook |
| `CONFIG_PATH` | Override config file path |
| `DB_PATH` | Override SQLite path (default: `state/jobs.db`) |
| `BOARDS_CSV` | Override boards CSV path |

---

## Sources

### Main mode (`--mode main`)

| Company | ATS | Filter |
|---|---|---|
| Microsoft | Eightfold | Entry + Mid-Level, US, sort by date |
| NVIDIA | Eightfold | Engineering, Full-Time, US |
| Amazon | Custom JSON | Software Dev + ML Science, US |
| Goldman Sachs | GraphQL | Software Engineering, US cities |
| IBM | Elasticsearch | Software Engineering + Data, Entry Level, US |
| Oracle | HCM REST | 0-2+ yr exp, US, Software + Data |

### Boards mode (`--mode boards`)

Sweeps up to 900+ company boards across:
- **Greenhouse** — `boards-api.greenhouse.io`
- **Lever** — `jobs.lever.co`
- **SmartRecruiters** — `api.smartrecruiters.com`
- **Workday** — CXS endpoint with automatic URL normalization

Each board is bootstrapped silently on first visit (no spam), then only new jobs trigger alerts.

---

## Job scoring

Titles receive a numeric score (0–100):

| Score | Label | Example |
|---|---|---|
| 70–100 | **yes** | `Software Engineer`, `ML Engineer`, `Data Scientist` |
| 40–69 | **maybe** | `Senior Engineer`, `Staff Engineer` (seniority penalty) |
| 0–39 | **no** | `Product Manager`, `QA Tester`, `Intern` (excluded) |

Only `yes` and `maybe` jobs trigger notifications. Within each bucket, jobs are sorted by score descending.

---

## GitHub Actions

Two workflows are included:

| Workflow | Schedule | Purpose |
|---|---|---|
| `main.yml` | Every 20 min | Fetches the 6 main company sources |
| `boards.yml` | Every 30 min | Processes a batch of ATS boards |

### Setup

1. Fork this repo
2. Add secrets: `EMAIL_USER`, `EMAIL_APP_PASSWORD`, `ALERT_TO_EMAIL` (and optionally `SLACK_WEBHOOK_URL`, `DISCORD_WEBHOOK_URL`)
3. Copy your boards CSV into `data/boards/` and commit it
4. Enable the workflows in the **Actions** tab

The workflows commit updated state back to the repo automatically.

---

## Adding a new source

1. Create `src/sources/mycompany.py`
2. Subclass `BaseSource` and implement `fetch()`
3. Register it in `src/main.py → run_main()`

```python
class MyCompanySource(BaseSource):
    name = "mycompany"

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        # fetch + normalize + classify
        ...
```

---

## CLI reference

```
python -m src.main [OPTIONS]

  --config PATH             YAML config file (default: config.yaml)
  --mode {main,boards}      Run mode (default: main)
  --dry-run                 Fetch only; do not save state or notify
  --no-notify               Save state but skip all notifications
  --test-notify             Send a sample notification without updating state
  --verbose, -v             Enable DEBUG logging

Boards options:
  --boards-csv PATH         Path to boards CSV
  --boards-batch-size N     Boards per run (default: config value / 50)
  --boards-timeout N        HTTP timeout for boards
  --boards-workers N        Parallel worker threads
  --boards-run-until-wrap   Full sweep (loop until cursor=0)
  --boards-max-iterations N Safety cap for full sweep (default: 2000)
  --export-dead-csv PATH    Write dead boards report to CSV
```

---

## Project structure

```
job-radar/
├── src/
│   ├── main.py              # CLI + orchestrator
│   ├── config.py            # YAML + env var config
│   ├── database.py          # SQLite state management
│   ├── classifier.py        # 0-100 job title scorer
│   ├── notifier.py          # Email / Slack / Discord
│   ├── sources/
│   │   ├── base.py          # BaseSource + Job dataclass
│   │   ├── eightfold.py     # Microsoft, NVIDIA
│   │   ├── amazon.py
│   │   ├── goldman.py
│   │   ├── ibm.py
│   │   ├── oracle.py
│   │   ├── greenhouse.py    # Board ATS adapters
│   │   ├── lever.py
│   │   ├── smartrecruiters.py
│   │   └── workday.py
│   └── utils/
│       └── http.py          # Shared session factory + retry
├── data/boards/             # CSV board lists (copy here)
├── state/                   # jobs.db lives here (gitignored)
├── .github/workflows/
│   ├── main.yml
│   └── boards.yml
├── config.example.yaml
└── requirements.txt
```
