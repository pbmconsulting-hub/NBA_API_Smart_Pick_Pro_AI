# 🏀 SmartAI-NBA v7

**Your Personal NBA Prop Betting Analysis Engine — Built Locally, No APIs Needed**

SmartAI-NBA v7 is a local web app (Streamlit) that analyzes PrizePicks, Underdog Fantasy,
and DraftKings Pick6 props to find the highest-probability bets using Quantum Matrix Engine 5.6 simulation
and directional force analysis. All math is built from scratch — no external libraries except Streamlit.

---

## 🚀 Quick Start (Complete Beginner Guide)

### Step 1: Make Sure Python is Installed

1. Open your terminal (Mac: search "Terminal", Windows: search "Command Prompt" or "PowerShell")
2. Type: `python --version` and press Enter
3. You should see something like `Python 3.10.x` or higher
4. If you see an error, download Python from [python.org](https://python.org) and install it

### Step 2: Navigate to the App Folder

In your terminal, type:
```bash
cd path/to/SmartAI-NBA
```

Replace `path/to/SmartAI-NBA` with the actual folder path (e.g., `cd ~/Downloads/SmartAI-NBA`).

### Step 3: Install Dependencies

```bash
pip install streamlit nba_api
```

Or using the requirements file:
```bash
pip install -r requirements.txt
```

### Step 4: Run the App

```bash
streamlit run app.py
```

Your browser will automatically open to `http://localhost:8501` with the app running!

---

## 📁 App Structure

```
SmartAI-NBA/
├── app.py                              # Main entry point — home dashboard
├── requirements.txt                    # All dependencies
├── README.md                           # This file
│
├── pages/
│   ├── 0_🏆_Live_Scores_&_Props.py    # Real-time NBA scores & stat leaders
│   ├── 1_📡_Live_Games.py             # Tonight's games + one-click setup
│   ├── 2_🔬_Prop_Scanner.py           # Enter/upload/fetch prop lines
│   ├── 3_⚡_Quantum_Analysis_Matrix.py # Run Neural Analysis — main engine
│   ├── 4_📋_Game_Report.py            # AI-powered game reports (SAFE Score™)
│   ├── 5_💦_Live_Sweat.py             # Live AI Panic Room — in-game tracking
│   ├── 5b_🔮_Player_Simulator.py      # What-if player scenario simulator
│   ├── 6_🧬_Entry_Builder.py          # Build optimal DFS entries (parlays)
│   ├── 7_🎙️_The_Studio.py            # Joseph M. Smith AI analyst desk
│   ├── 8_🛡️_Risk_Shield.py           # Flagged picks to avoid
│   ├── 9_📡_Data_Feed.py              # Fetch live NBA data from APIs
│   ├── 10_🗺️_Correlation_Matrix.py   # Prop correlation analysis
│   ├── 11_📈_Bet_Tracker.py           # Bet tracking & model health
│   ├── 12_📊_Backtester.py            # Historical backtesting engine
│   ├── 13_⚙️_Settings.py             # Configure engine settings
│   └── 14_💎_Subscription_Level.py    # Premium subscription management
│
├── engine/
│   ├── math_helpers.py                 # All math from scratch (no scipy)
│   ├── simulation.py                   # Quantum Matrix Engine 5.6 simulator
│   ├── projections.py                  # Player stat projections
│   ├── edge_detection.py              # Find betting edges + Goblin/Demon/Gold
│   ├── entry_optimizer.py             # Build optimal entries
│   ├── confidence.py                  # Confidence + tier system
│   ├── joseph_brain.py                # Joseph M. Smith AI persona engine
│   ├── arbitrage_matcher.py           # Cross-book EV scanner
│   ├── live_math.py                   # Live game pacing engine
│   └── ...                            # + 25 more engine modules
│
├── data/
│   ├── data_manager.py                # Load/save CSV data + session state
│   ├── nba_data_service.py            # NBA data orchestration service
│   ├── sportsbook_service.py          # Multi-platform prop fetcher
│   ├── live_game_tracker.py           # Live game score tracker
│   ├── nba_stats_backup.py            # Free NBA.com stats fallback
│   ├── player_profile_service.py      # Player context & headshots
│   ├── roster_engine.py               # Active roster & injury engine
│   ├── teams.csv                      # All 30 NBA teams with pace/ratings
│   └── defensive_ratings.csv          # Team defense by position
│
├── agent/
│   ├── payload_builder.py             # Live Sweat game state classifier
│   ├── live_persona.py                # Joseph's live commentary persona
│   └── response_parser.py             # Vibe response parsing
│
├── styles/
│   ├── theme.py                       # Global CSS + education box helpers
│   └── live_theme.py                  # Live Sweat custom CSS
│
├── utils/
│   ├── components.py                  # Shared UI components
│   ├── joseph_widget.py               # Joseph floating widget
│   ├── premium_gate.py                # Premium feature gates
│   └── ...                            # + auth, logger, renderers, etc.
│
├── tracking/
│   ├── bet_tracker.py                 # Log bets + results
│   └── database.py                    # SQLite wrapper
│
└── db/
    └── smartai_nba.db                 # Created automatically on first run
```

---

## 📖 What Each Page Does

### 🏠 Home (app.py)
The dashboard. Shows tonight's slate, quick-start workflow guide, status
dashboard, and links to all pages.

### 🏆 Page 0: Live Scores & Props
Real-time NBA scores and season stat leaders, updated automatically.

### 📡 Page 1: Live Games
Load tonight's matchups, fetch rosters and stats from API-NBA API,
and enrich with Odds API consensus lines. Features:
- **⚡ One-Click Setup** — loads games + fetches live platform props in one step
- **Auto-Load** — fetches tonight's schedule, rosters, player/team stats
- **Fetch Platform Props** — pulls real live lines from PrizePicks, Underdog, DraftKings

### 🔬 Page 2: Prop Scanner
Enter prop lines manually, upload a CSV, or fetch live lines from platforms.
Three input methods:
- Manual form entry
- CSV upload
- Live platform fetch (from the Live Games page)

### ⚡ Page 3: Neural Analysis (Quantum Analysis Matrix)
The main engine. Click **Run Analysis** to simulate each prop. For every prop you'll see:
- **Probability**: % chance of going over (or under) the line
- **Edge**: How far above 50% the probability is (bigger = better)
- **Tier**: Platinum 💎, Gold 🥇, Silver 🥈, or Bronze 🥉
- **Direction**: OVER or UNDER
- **Forces**: All the factors pushing the stat up or down
- **Win Score**: Composite score combining probability, confidence, edge, and risk
- **Fair-Value Odds Explorer**: Slide to see fair odds at different prop lines

### 📋 Page 4: Game Report
AI-powered game reports with SAFE Score™ analysis, collapsible sections,
confidence bars, and entry strategy matrix.

### 💦 Page 5: Live Sweat
The Live AI Panic Room — track your active bets in real-time during games.
Features pace tracking, Joseph M. Smith live commentary, and sweat cards
that show whether your bets are on track to cash.

### 🔮 Page 5b: Player Simulator
What-if scenario simulator. Adjust minutes, pace, matchup factors and see
how projected stats change in real-time.

### 🧬 Page 6: Entry Builder
Build optimal DFS parlays. The engine tests all combinations of top picks
and finds the ones with the highest **Expected Value (EV)**. Supports
PrizePicks, Underdog, and DraftKings payout structures.

### 🎙️ Page 7: The Studio
Joseph M. Smith's broadcast desk. Three modes:
- **Games Tonight** — Joseph breaks down every game
- **Scout a Player** — deep-dive player analysis
- **Build My Bets** — Joseph constructs optimal tickets

### 🛡️ Page 8: Risk Shield
Shows which props to skip and explains exactly WHY:
- Low edge, trap lines, sharp lines, high variance, low confidence
- Educational content explains each risk flag

### 📡 Page 9: Data Feed
Fetch live NBA data from API-NBA API and The Odds API:
- **Smart Update** — only tonight's teams (fast)
- **Full Update** — all NBA player stats
- **Fetch Props** — live odds from 15+ sportsbooks

### 🗺️ Page 10: Correlation Matrix
Analyze how player props correlate with each other within games.
Helps build smarter parlays by avoiding correlated risk.

### 📈 Page 11: Bet Tracker & Model Health
Track your betting results. Features:
- Overall win rate by tier
- Auto-resolve bets against real game results
- Model health monitoring
- Performance predictor and bankroll allocation

### 📊 Page 12: Backtester
Validate the model against historical game logs. See win rates, ROI,
and tier-by-tier performance metrics.

### ⚙️ Page 13: Settings
Configure:
- **Simulation Depth**: 500 (fast) to 5,000 (most accurate)
- **Minimum Edge**: How much edge before showing a pick (default: 5%)
- **Preset Profiles**: Conservative, Balanced, Aggressive, Research
- **API Keys**: Odds API + API-NBA API keys
- **Advanced factors**: Home court boost, fatigue sensitivity, etc.

### 💎 Page 14: Subscription Level
Premium subscription management powered by Stripe.

---

## 🔴 Live NBA Data

SmartAI-NBA uses **real, up-to-date NBA stats** from the **nba_api** Python package and
prop lines from **PrizePicks**, **Underdog Fantasy**, and **DraftKings Pick6**.

### Setup

1. Install all dependencies:
```bash
pip install -r requirements.txt
```
2. Run the app: `streamlit run app.py`

**Alternative — file-based keys (persistent across sessions):**
```bash
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Edit .streamlit/secrets.toml and replace the placeholder values with your real keys
```
4. Go to **📡 Data Feed** (page 9) and click **Smart Update** to fetch tonight's data
5. Go to **📡 Live Games** (page 1) and click **⚡ One-Click Setup** to load games + live props

### When to Update

- **Before each betting session** for best accuracy
- Player and team situations change throughout the season
- The home page shows when data was last updated

### What Gets Updated

| Data | Source | Storage |
|------|--------|---------|
| Player stats (PPG, RPG, APG, etc.) | API-NBA API | `players.csv` |
| Team pace + ratings (ORTG/DRTG) | API-NBA API | `teams.csv` |
| Tonight's games + spreads/totals | API-NBA API + Odds API | Session state |
| Live prop lines | The Odds API (15+ books) | Session state |
| Defensive ratings by position | Calculated from team DRTG | `defensive_ratings.csv` |

### Live Data

The app is designed to work with live data pulled from real APIs.
On first run, no player data will be loaded — go to **📡 Data Feed** and click
**Smart Update** to pull tonight's rosters and stats.

After running an update, the CSVs are written with real data.
The home page shows a banner indicating whether live data has been loaded.

---

## 📊 How to Add Your Own Data

### Adding a New Player
Go to **📡 Data Feed** and run a **Smart Update** to pull tonight's active roster.
Player data is stored in `data/players.csv` — you can also edit it directly.

### Entering Tonight's Props
Go to **🔬 Prop Scanner** (page 2) and either:
1. Type them in the form (one at a time)
2. Upload a CSV using the template provided
3. Or use **📡 Live Games** → **📊 Fetch Platform Props** to pull real live lines automatically

---

## 🔧 Troubleshooting

### "streamlit: command not found"
Run: `pip install streamlit` first.

### "ModuleNotFoundError: No module named 'streamlit'"
Run: `pip install streamlit` and make sure you're using the right Python version.

### "ModuleNotFoundError: No module named 'nba_api'"
The app primarily uses API-NBA API and The Odds API now.
Run `pip install -r requirements.txt` to install all dependencies.

### Live data update takes too long
This is normal! The fetcher adds a 1.5-second delay between API calls to avoid
being blocked by the NBA's servers. A full update (all players + game logs) can
take 5-15 minutes. Let it run — don't close the tab.

### "Port 8501 is already in use"
Another Streamlit app is running. Close it, or run on a different port:
```bash
streamlit run app.py --server.port 8502
```

### App shows "No props loaded"
Go to **📥 Import Props** and click "Load Props from CSV" or enter your own.

### Analysis results seem off
Check that you have:
1. Games configured on **🏀 Today's Games** page
2. Run a **Smart Update** on the **📡 Data Feed** page to load live player data
3. Stat types are lowercase: `points`, `rebounds`, `assists`, `threes`, etc.

---

## 🧠 How the Math Works (Plain English)

### Quantum Matrix Engine 5.6 Simulation
We simulate 1,000 games for each player. In each game:
1. **Minutes** are randomized (sometimes stars rest, sometimes foul trouble)
2. **Stats** are drawn randomly from a bell curve centered on the projection
3. We record whether that game went OVER or UNDER the line

After 1,000 games: `(# games over line) / 1000 = probability of going over`

### Normal Distribution (Bell Curve)
Player stats follow a bell curve — most games near the average, fewer extreme games.
We use the formula `0.5 * (1 + erf(z / √2))` to calculate probabilities.
This is exactly what scipy.stats.norm.cdf does — we just wrote it ourselves!

### Expected Value (EV)
For a parlay with 3 picks:
```
EV = (P(all 3 hit) × 3-pick payout) + (P(2 hit) × 2-hit payout) + ... - entry fee
```
Positive EV = profitable on average. Negative EV = house wins.

---

## 📦 Dependencies

```
streamlit   # UI framework (required)
nba_api     # Live NBA data (optional but recommended)
```

Install both:
```bash
pip install streamlit nba_api
```

All other functionality uses Python's standard library:
`math`, `random`, `statistics`, `csv`, `sqlite3`, `datetime`, `os`, `pathlib`,
`itertools`, `collections`, `json`, `io`, `copy`

---

## 🚀 Production Deployment

### Streamlit Community Cloud

1. **Push to GitHub** — fork or push this repo to your GitHub account.
2. **Connect to Streamlit Cloud** — go to [share.streamlit.io](https://share.streamlit.io) and create a new app from your repo.
3. **Set secrets** — in the Streamlit Cloud dashboard, go to **Settings → Secrets** and paste your secrets (see `.streamlit/secrets.toml.example` for the full list). Do **not** commit `secrets.toml` to Git.
4. **Set `SMARTAI_PRODUCTION=true`** — this enforces Stripe subscription gates.
5. **Set `APP_URL`** — your deployed app URL (e.g., `https://your-app.streamlit.app`).

### Environment Variables Checklist

| Variable | Required | Description |
|----------|----------|-------------|
| `ODDS_API_KEY` | For DraftKings | The Odds API key (powers DraftKings Pick6 props) |
| `STRIPE_SECRET_KEY` | For payments | Stripe secret key |
| `STRIPE_PUBLISHABLE_KEY` | For payments | Stripe publishable key |
| `STRIPE_PRICE_ID` | For payments | Stripe product price ID |
| `STRIPE_WEBHOOK_SECRET` | Recommended | Stripe webhook signing secret |
| `SMARTAI_PRODUCTION` | For production | Must be `"true"` to enforce gates |
| `APP_URL` | For Stripe redirects | Your deployed app URL |
| `DB_PATH` | Optional | Override default SQLite path |
| `LOG_LEVEL` | Optional | DEBUG, INFO, WARNING, ERROR |

### Stripe Webhook Setup

1. Deploy `webhook_server.py` to a hosting platform (Railway, Render, or Fly.io).
2. In the Stripe Dashboard → Developers → Webhooks, register your webhook URL (e.g., `https://your-server.railway.app/webhook`).
3. Select events: `customer.subscription.updated`, `customer.subscription.deleted`, `invoice.payment_failed`.
4. Copy the signing secret (`whsec_...`) and add it as `STRIPE_WEBHOOK_SECRET` to both the webhook server and the Streamlit app.

### Pre-Launch Checklist

- [ ] All environment variables set (see table above)
- [ ] `SMARTAI_PRODUCTION=true` confirmed
- [ ] Stripe keys are live keys (not test keys)
- [ ] API-NBA and Odds API keys are active with sufficient credits
- [ ] Run `scripts/verify_api_endpoints.py` to confirm API connectivity
- [ ] Run `python -m pytest tests/` to confirm all tests pass
- [ ] SQLite database initialized (first run auto-creates it)
- [ ] Stripe webhook endpoint registered and tested with Stripe CLI

---

## ⚠️ Disclaimer

This app is for **personal entertainment and analysis** only.
Always gamble responsibly. Past model performance does not guarantee future results.
Prop betting involves risk. Never bet more than you can afford to lose.

**Responsible Gambling:** If you or someone you know has a gambling problem, call
the National Council on Problem Gambling helpline at **1-800-522-4700** or visit
[www.ncpgambling.org](https://www.ncpgambling.org).
