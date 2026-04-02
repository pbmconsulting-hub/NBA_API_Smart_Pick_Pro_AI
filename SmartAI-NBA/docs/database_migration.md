# Database Migration Guide

## Current Setup: SQLite

SmartAI-NBA uses SQLite (`db/smartai_nba.db`) by default. This works great for:
- Local development
- Single-user deployments
- Quick prototyping

## Why Migrate?

SQLite has limitations for production cloud deployments:

1. **Ephemeral Filesystems** — Streamlit Community Cloud and many PaaS providers
   use ephemeral filesystems. Your SQLite database will be **deleted** on each
   deploy or restart.
2. **Concurrent Writes** — SQLite uses file-level locking. Multiple users writing
   simultaneously can cause "database is locked" errors.
3. **No Network Access** — SQLite runs in-process only. You can't share the
   database between your Streamlit app and the webhook server.

## Option 1: Streamlit Cloud Persistent Storage

If deploying on Streamlit Community Cloud, use the `/mount/src/` persistent
storage path:

```bash
# In your Streamlit Cloud secrets:
DB_PATH = "/mount/src/db/smartai_nba.db"
```

This path persists across app restarts (but not across re-deployments from Git).

## Option 2: PostgreSQL Migration

For true production use, migrate to PostgreSQL:

### Step 1: Set Up PostgreSQL

Use a managed service like:
- **Supabase** (free tier available) — https://supabase.com
- **Neon** (serverless Postgres) — https://neon.tech
- **Railway** — https://railway.app
- **Render** — https://render.com

### Step 2: Update Connection Code

Replace SQLite connections in `tracking/database.py` with `psycopg2`:

```python
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_database_connection():
    return psycopg2.connect(DATABASE_URL)
```

### Step 3: Migrate Schema

The main tables to migrate:

| Table | Purpose |
|-------|---------|
| `bets` | Bet tracking history |
| `entries` | Parlay entry tracking |
| `subscriptions` | Stripe subscription records |
| `prediction_history` | Model calibration data |
| `daily_snapshots` | Per-day performance |
| `all_analysis_picks` | Complete analysis outputs |
| `analysis_sessions` | Persisted analysis sessions |
| `backtest_results` | Backtesting run history |
| `player_game_logs` | Cached player game logs |

### Step 4: Update webhook_server.py

The webhook server also needs to point to PostgreSQL. Update the
`_update_subscription()` function to use `psycopg2` with `DATABASE_URL`.

## Environment Variable

Set `DB_PATH` in your environment to override the default SQLite path:

```bash
DB_PATH=db/smartai_nba.db      # Default SQLite
DATABASE_URL=postgres://...     # For PostgreSQL (requires code changes)
```
