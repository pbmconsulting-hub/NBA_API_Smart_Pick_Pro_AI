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

Creates `smartpicks.db` with eight tables: **Players**, **Teams**, **Games**,
**Player_Game_Logs**, **Team_Game_Stats**, **Defense_Vs_Position**,
**Team_Roster**, and **Injury_Status**.  Safe to run multiple times — uses
`IF NOT EXISTS`.

### 3. Seed historical data (run once)

```bash
cd SmartPicksProAI/backend
python initial_pull.py
```

Fetches every player game log for the **2025-26 NBA regular season**, seeds
the **Teams** table from `nba_api` static data, and populates **Players**,
**Games**, **Player_Game_Logs**, **Team_Game_Stats**, and **Team_Roster**
(including player positions).  This may take a few minutes due to API rate
limits (one request per team for roster data).

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

### `GET /api/players/search?q=name`

Search for players by name (case-insensitive, returns up to 25 results).

**Example:**
```
GET /api/players/search?q=LeBron
```

**Response:**
```json
{
  "results": [
    {
      "player_id": 2544,
      "first_name": "LeBron",
      "last_name": "James",
      "full_name": "LeBron James",
      "team_id": 1610612747,
      "team_abbreviation": "LAL",
      "position": "F"
    }
  ]
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

### `GET /api/teams`

Lists all 30 NBA teams stored in the database, including season-level
pace, offensive rating, and defensive rating computed from Team_Game_Stats.

**Response:**
```json
{
  "teams": [
    {
      "team_id": 1610612737,
      "abbreviation": "ATL",
      "team_name": "Atlanta Hawks",
      "conference": "East",
      "division": "Southeast",
      "pace": 99.8,
      "ortg": 112.3,
      "drtg": 114.1
    }
  ]
}
```

### `GET /api/teams/{team_id}/roster`

Returns the roster for a specific team.

**Response:**
```json
{
  "team_id": 1610612747,
  "players": [
    {
      "player_id": 2544,
      "first_name": "LeBron",
      "last_name": "James",
      "full_name": "LeBron James",
      "position": "F",
      "team_abbreviation": "LAL"
    }
  ]
}
```

### `GET /api/teams/{team_id}/stats?last_n=10`

Returns recent game-level stats for a specific team (default last 10 games,
max 82).

**Response:**
```json
{
  "team_id": 1610612747,
  "games": [
    {
      "game_id": "0022501100",
      "game_date": "2026-03-28",
      "matchup": "LAL vs. BOS",
      "opponent_team_id": 1610612738,
      "is_home": 1,
      "points_scored": 112,
      "points_allowed": 105,
      "pace_est": 99.2,
      "ortg_est": 113.5,
      "drtg_est": 106.4
    }
  ]
}
```

### `POST /api/admin/refresh-data`

Triggers an on-demand incremental data update.  Fetches all player and team
game logs between the last stored date and yesterday, then appends new rows.
Also refreshes season-level team pace/ortg/drtg and defense-vs-position
multipliers.

**Response:**
```json
{
  "status": "success",
  "new_records": 342,
  "message": "Added 342 new game log records."
}
```

### `GET /api/defense-vs-position/{team_abbreviation}`

Returns defense-vs-position multipliers for a specific team.  Each multiplier
indicates how players at a given position perform against this team relative to
the league average.  `> 1.0` means weaker defense; `< 1.0` means tougher.

**Example:**
```
GET /api/defense-vs-position/BOS
```

**Response:**
```json
{
  "team_abbreviation": "BOS",
  "positions": [
    {
      "pos": "G",
      "vs_pts_mult": 0.95,
      "vs_reb_mult": 1.02,
      "vs_ast_mult": 0.98,
      "vs_stl_mult": 1.01,
      "vs_blk_mult": 0.90,
      "vs_3pm_mult": 0.93
    }
  ]
}
```

---

## Frontend Features

| Feature | Description |
|---|---|
| **Today's Matchup Grid** | Displays NBA games scheduled for today in a card layout. |
| **Player Search** | Search for players by name (e.g. "LeBron") — no need to know player IDs. |
| **Player Performance Card** | View last 5 game logs, full stat averages (all 19 stat columns), and a data table. |
| **Team Browser** | Sidebar dropdown to select any NBA team and view its current roster. |
| **Admin Sync Button** | Sidebar button triggers `POST /api/admin/refresh-data`, with a spinner and success/error toast. |
| **Response Caching** | GET requests are cached for 1 hour via `@st.cache_data(ttl=3600)` to prevent redundant API calls. |

---

## File Descriptions

| File | Purpose |
|---|---|
| `backend/setup_db.py` | Creates `smartpicks.db` and all eight tables with `IF NOT EXISTS` guards and `ALTER TABLE` migration support. |
| `backend/initial_pull.py` | One-time seed script — seeds Teams (with conference/division) from `nba_api` static data, pulls the full 2025-26 season via `LeagueGameLog` (player + team level), computes pace/ortg/drtg estimates, fetches rosters via `CommonTeamRoster`, computes Defense_Vs_Position multipliers, and loads all core tables. |
| `backend/data_updater.py` | Exposes `run_update()` — finds the latest date in the DB, fetches only new player and team game logs, handles DNP/null stats, updates Team_Game_Stats, refreshes season-level team ratings and defense-vs-position multipliers. No scheduling loops. |
| `backend/api.py` | FastAPI app with eight endpoints: last-5 stats, player search, today's games, team list (with pace/ortg/drtg), team roster, team game stats, defense-vs-position multipliers, and manual refresh trigger. |
| `frontend/api_service.py` | HTTP client using `requests` with `@st.cache_data` caching and error handling for all eight endpoints. |
| `frontend/app.py` | Streamlit dashboard with dark FinTech theme, matchup grid, player name search, team browser, and admin controls. |

---

## Database Schema

**Players**

| Column | Type | Notes |
|---|---|---|
| `player_id` | INTEGER | Primary key |
| `first_name` | TEXT | Not null |
| `last_name` | TEXT | Not null |
| `full_name` | TEXT | |
| `team_id` | INTEGER | |
| `team_abbreviation` | TEXT | |
| `position` | TEXT | |
| `is_active` | INTEGER | Default 1 |

**Teams**

| Column | Type | Notes |
|---|---|---|
| `team_id` | INTEGER | Primary key |
| `abbreviation` | TEXT | Not null |
| `team_name` | TEXT | Not null |
| `conference` | TEXT | |
| `division` | TEXT | |
| `pace` | REAL | |
| `ortg` | REAL | |
| `drtg` | REAL | |

**Games**

| Column | Type | Notes |
|---|---|---|
| `game_id` | TEXT | Primary key |
| `game_date` | TEXT | YYYY-MM-DD |
| `season` | TEXT | e.g. `"2025-26"` |
| `home_team_id` | INTEGER | FK → Teams |
| `away_team_id` | INTEGER | FK → Teams |
| `home_abbrev` | TEXT | e.g. `"LAL"` |
| `away_abbrev` | TEXT | e.g. `"BOS"` |
| `matchup` | TEXT | e.g. `"LAL vs. BOS"` |
| `home_score` | INTEGER | Home team points |
| `away_score` | INTEGER | Away team points |

**Player_Game_Logs**

| Column | Type | Notes |
|---|---|---|
| `player_id` | INTEGER | Composite PK, FK → Players |
| `game_id` | TEXT | Composite PK, FK → Games |
| `wl` | TEXT | Win/Loss (`W` or `L`) |
| `min` | TEXT | Minutes played (`0:00` for DNP) |
| `pts` | INTEGER | Points (0 for DNP) |
| `reb` | INTEGER | Rebounds |
| `ast` | INTEGER | Assists |
| `stl` | INTEGER | Steals |
| `blk` | INTEGER | Blocks |
| `tov` | INTEGER | Turnovers |
| `fgm` | INTEGER | Field goals made |
| `fga` | INTEGER | Field goals attempted |
| `fg_pct` | REAL | Field goal percentage |
| `fg3m` | INTEGER | Three-pointers made |
| `fg3a` | INTEGER | Three-pointers attempted |
| `fg3_pct` | REAL | Three-point percentage |
| `ftm` | INTEGER | Free throws made |
| `fta` | INTEGER | Free throws attempted |
| `ft_pct` | REAL | Free throw percentage |
| `oreb` | INTEGER | Offensive rebounds |
| `dreb` | INTEGER | Defensive rebounds |
| `pf` | INTEGER | Personal fouls |
| `plus_minus` | REAL | Plus/minus |

**Team_Game_Stats**

| Column | Type | Notes |
|---|---|---|
| `game_id` | TEXT | Composite PK, FK → Games |
| `team_id` | INTEGER | Composite PK, FK → Teams |
| `opponent_team_id` | INTEGER | |
| `is_home` | INTEGER | |
| `points_scored` | INTEGER | |
| `points_allowed` | INTEGER | |
| `pace_est` | REAL | |
| `ortg_est` | REAL | |
| `drtg_est` | REAL | |

**Defense_Vs_Position**

| Column | Type | Notes |
|---|---|---|
| `team_abbreviation` | TEXT | Composite PK |
| `season` | TEXT | Composite PK |
| `pos` | TEXT | Composite PK |
| `vs_pts_mult` | REAL | Default 1.0 |
| `vs_reb_mult` | REAL | Default 1.0 |
| `vs_ast_mult` | REAL | Default 1.0 |
| `vs_stl_mult` | REAL | Default 1.0 |
| `vs_blk_mult` | REAL | Default 1.0 |
| `vs_3pm_mult` | REAL | Default 1.0 |

**Team_Roster**

| Column | Type | Notes |
|---|---|---|
| `team_id` | INTEGER | Composite PK, FK → Teams |
| `player_id` | INTEGER | Composite PK, FK → Players |
| `effective_start_date` | TEXT | Composite PK |
| `effective_end_date` | TEXT | |
| `is_two_way` | INTEGER | Default 0 |
| `is_g_league` | INTEGER | Default 0 |

**Injury_Status**

| Column | Type | Notes |
|---|---|---|
| `player_id` | INTEGER | Composite PK, FK → Players |
| `team_id` | INTEGER | |
| `report_date` | TEXT | Composite PK |
| `status` | TEXT | Not null |
| `reason` | TEXT | |
| `source` | TEXT | |
| `last_updated_ts` | TEXT | |