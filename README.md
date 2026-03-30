# NBA_API-ETL — SmartPicksProAI

A decoupled local application featuring a **Python/FastAPI backend** that
harvests NBA player performance data and a **Streamlit frontend** that
displays matchups, player stats, and admin controls in a dark "FinTech
terminal" interface.

The data feeds an ML model that predicts player props (over/unders on points,
rebounds, assists, etc.) and generates daily betting picks.

---

## Project Structure

```
SmartPicksProAI/
├── backend/
│   ├── setup_db.py        # Create the SQLite schema
│   ├── initial_pull.py    # One-time historical data seed
│   ├── data_updater.py    # On-demand incremental update module
│   └── api.py             # FastAPI backend (port 8000)
│
├── frontend/
│   ├── api_service.py     # HTTP client with Streamlit caching
│   └── app.py             # Streamlit dashboard
│
├── requirements.txt       # Python dependencies
└── README.md
```

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Create the database schema

```bash
cd SmartPicksProAI/backend
python setup_db.py
```

Creates `smartpicks.db` with three tables: **Players**, **Games**, and
**Player_Game_Logs**.  Safe to run multiple times — uses `IF NOT EXISTS`.

### 3. Seed historical data (run once)

```bash
cd SmartPicksProAI/backend
python initial_pull.py
```

Fetches every player game log for the **2025-26 NBA regular season** and
populates all three tables.  This may take a minute due to API rate limits.

---

## Running the Application

### Start the FastAPI backend

```bash
cd SmartPicksProAI/backend
python api.py
# or
uvicorn api:app --reload --port 8000
```

The server listens on `http://localhost:8000`.

### Start the Streamlit frontend

In a **separate terminal**:

```bash
cd SmartPicksProAI/frontend
streamlit run app.py
```

The dashboard opens at `http://localhost:8501`.

---

## API Endpoints

### `GET /api/players/{player_id}/last5`

Returns a player's last 5 game logs with computed 5-game stat averages.
Optimised for ML moving-average calculations.

**Example:**
```
GET /api/players/2544/last5
```

**Response:**
```json
{
  "player_id": 2544,
  "first_name": "LeBron",
  "last_name": "James",
  "games": [
    {
      "game_date": "2026-03-20",
      "game_id": "0022501050",
      "pts": 28, "reb": 8, "ast": 9,
      "blk": 1, "stl": 2, "tov": 3, "min": "35:42"
    }
  ],
  "averages": {
    "pts": 27.4, "reb": 7.2, "ast": 8.6,
    "blk": 0.8, "stl": 1.4, "tov": 2.8
  }
}
```

### `GET /api/games/today`

Returns today's NBA matchups.  Checks the local database first; if no games
are found, fetches live data via `ScoreboardV3`.

**Response:**
```json
{
  "date": "2026-03-30",
  "source": "database",
  "games": [
    {"game_id": "0022501100", "matchup": "LAL vs. BOS"}
  ]
}
```

### `POST /api/admin/refresh-data`

Triggers an on-demand incremental data update.  Fetches all player game logs
between the last stored date and yesterday, then appends new rows.

**Response:**
```json
{
  "status": "success",
  "new_records": 342,
  "message": "Added 342 new game log records."
}
```

---

## Frontend Features

| Feature | Description |
|---|---|
| **Today's Matchup Grid** | Displays NBA games scheduled for today in a card layout. |
| **Player Performance Card** | Enter a player ID to view last 5 game logs, stat averages, and a data table. |
| **Admin Sync Button** | Sidebar button triggers `POST /api/admin/refresh-data`, with a spinner and success/error toast. |
| **Response Caching** | GET requests are cached for 1 hour via `@st.cache_data(ttl=3600)` to prevent redundant API calls. |

---

## File Descriptions

| File | Purpose |
|---|---|
| `backend/setup_db.py` | Creates `smartpicks.db` and the three tables with `IF NOT EXISTS` guards. |
| `backend/initial_pull.py` | One-time seed script — pulls the full 2025-26 season via `LeagueGameLog`, cleans/renames columns, handles DNP edge cases, and loads all three tables. |
| `backend/data_updater.py` | Exposes `run_update()` — finds the latest date in the DB, fetches only new games, handles DNP/null stats, and appends them. No scheduling loops. |
| `backend/api.py` | FastAPI app with three endpoints: last-5 stats, today's games, and manual refresh trigger. |
| `frontend/api_service.py` | HTTP client using `requests` with `@st.cache_data` caching and error handling. |
| `frontend/app.py` | Streamlit dashboard with dark FinTech theme, matchup grid, player card, and admin controls. |

---

## Database Schema

**Players**

| Column | Type | Notes |
|---|---|---|
| `player_id` | INTEGER | Primary key |
| `first_name` | TEXT | |
| `last_name` | TEXT | |
| `team_id` | INTEGER | |

**Games**

| Column | Type | Notes |
|---|---|---|
| `game_id` | TEXT | Primary key |
| `game_date` | TEXT | YYYY-MM-DD |
| `matchup` | TEXT | e.g. `"LAL vs. BOS"` |

**Player_Game_Logs**

| Column | Type | Notes |
|---|---|---|
| `log_id` | INTEGER | Primary key, auto-increment |
| `player_id` | INTEGER | Foreign key → Players |
| `game_id` | TEXT | Foreign key → Games |
| `pts` | INTEGER | Points (0 for DNP) |
| `reb` | INTEGER | Rebounds (0 for DNP) |
| `ast` | INTEGER | Assists (0 for DNP) |
| `blk` | INTEGER | Blocks (0 for DNP) |
| `stl` | INTEGER | Steals (0 for DNP) |
| `tov` | INTEGER | Turnovers (0 for DNP) |
| `min` | TEXT | Minutes played (`0:00` for DNP) |