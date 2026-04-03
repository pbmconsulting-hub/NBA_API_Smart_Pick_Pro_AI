# engine/backtester.py
# Historical backtesting engine for the SmartAI-NBA prediction model.
# Runs the simulation/projection/edge pipeline against historical game log data.
# Standard library only — no numpy/scipy/pandas.

import math
import logging

try:
    from engine.simulation import run_quantum_matrix_simulation
except ImportError:
    run_quantum_matrix_simulation = None

try:
    from data.platform_fetcher import fetch_archived_nba_props
    _ARCHIVE_AVAILABLE = True
except ImportError:
    _ARCHIVE_AVAILABLE = False

_logger = logging.getLogger(__name__)

# Implied probability for -110 odds (standard breakeven for edge calculation)
# -110 → breakeven = 110 / (110 + 100) = 0.5238
_IMPLIED_PROB_MINUS_110 = 110.0 / (110.0 + 100.0)  # ≈ 0.5238


def _season_avg_up_to_date(game_logs, stat_key, cutoff_date_str):
    """Compute the season average of stat_key from game logs up to (not including) cutoff_date_str."""
    values = []
    for g in game_logs:
        gd = g.get("GAME_DATE", g.get("game_date", ""))
        if gd and gd < cutoff_date_str:
            try:
                v = float(g.get(stat_key, 0) or 0)
                values.append(v)
            except (ValueError, TypeError):
                pass
    if not values:
        return None
    return sum(values) / len(values)


def _season_std_up_to_date(game_logs, stat_key, cutoff_date_str):
    """Compute the standard deviation of stat_key from game logs up to cutoff_date_str."""
    values = []
    for g in game_logs:
        gd = g.get("GAME_DATE", g.get("game_date", ""))
        if gd and gd < cutoff_date_str:
            try:
                v = float(g.get(stat_key, 0) or 0)
                values.append(v)
            except (ValueError, TypeError):
                pass
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(variance)


def _round_to_half(value):
    """Round a value to the nearest 0.5 (proxy for a prop line)."""
    return round(value * 2) / 2


def _get_archived_line(player_name, stat_type, date_str, _archive_cache):
    """
    Try to get the actual PrizePicks line from the mirror archive.

    Props for each date are indexed by (player_name_lower, stat_type_lower)
    on first access so that repeated lookups within the same date are O(1).

    Returns:
        tuple: (line_float, True) if found, (None, False) if not.
    """
    if not _ARCHIVE_AVAILABLE:
        return None, False

    if date_str not in _archive_cache:
        raw_props = fetch_archived_nba_props(date_str)
        # Build an index: (player_lower, stat_lower) → line for O(1) lookup
        index = {}
        for prop in raw_props:
            key = (
                prop.get("player_name", "").lower().strip(),
                prop.get("stat_type", "").lower().strip(),
            )
            if key not in index:
                index[key] = prop.get("line")
        _archive_cache[date_str] = index

    key = (player_name.lower().strip(), stat_type.lower().strip())
    line = _archive_cache[date_str].get(key)
    if line is not None:
        return line, True
    return None, False


STAT_KEY_MAP = {
    "points": "PTS",
    "rebounds": "REB",
    "assists": "AST",
    "steals": "STL",
    "blocks": "BLK",
    "threes": "FG3M",
    "turnovers": "TOV",
    # Extended stats
    "ftm": "FTM",
    "fta": "FTA",
    "fga": "FGA",
    "fgm": "FGM",
    "minutes": "MIN",
    "personal_fouls": "PF",
    "offensive_rebounds": "OREB",
    "defensive_rebounds": "DREB",
}

EDGE_BUCKETS = [
    (0.05, 0.10, "5-10%"),
    (0.10, 0.15, "10-15%"),
    (0.15, 1.00, "15%+"),
]

# Standard -110 payout per unit staked (100/110 = 0.909...)
# BEGINNER NOTE: At -110 odds, a $110 bet returns $200 total ($90.90 profit).
# So the payout per $1 staked is $0.909. This is the standard American vig.
PAYOUT_AT_MINUS_110 = 0.909


def run_backtest(season, stat_types, min_edge=0.05, tier_filter=None, game_logs_by_player=None):
    """
    Run a historical backtest.

    Args:
        season (str): NBA season string, e.g. "2024-25"
        stat_types (list): List of stat type strings to backtest, e.g. ["points", "rebounds"]
        min_edge (float): Minimum edge threshold above -110 breakeven (0.05 = 5%)
        tier_filter (str or None): If set, only include picks of this tier
        game_logs_by_player (dict or None): {player_name: [game_log_dicts]}
            If None, returns an empty result (no data available).

    Returns:
        dict: BacktestResult with full metrics
    """
    if not game_logs_by_player:
        return _empty_result("No game log data provided. Get player data first.")

    if run_quantum_matrix_simulation is None:
        return _empty_result("Simulation engine not available.")

    total_picks = 0
    wins = 0
    total_pnl = 0.0  # assuming -110 odds (bet 1.0 to win 0.909)

    results_by_tier = {
        "ELITE":  {"picks": 0, "wins": 0, "pnl": 0.0},
        "STRONG": {"picks": 0, "wins": 0, "pnl": 0.0},
        "VALUE":  {"picks": 0, "wins": 0, "pnl": 0.0},
        "LEAN":   {"picks": 0, "wins": 0, "pnl": 0.0},
    }
    results_by_stat = {st: {"picks": 0, "wins": 0} for st in stat_types}
    results_by_edge_bucket = {label: {"picks": 0, "wins": 0} for _, _, label in EDGE_BUCKETS}

    pick_log = []

    _archive_cache = {}

    for player_name, game_logs in game_logs_by_player.items():
        if not game_logs:
            continue

        # Sort by game date ascending
        sorted_logs = sorted(game_logs, key=lambda g: g.get("GAME_DATE", g.get("game_date", "")))

        for i, game in enumerate(sorted_logs):
            game_date = game.get("GAME_DATE", game.get("game_date", ""))
            if not game_date or i < 5:  # Need at least 5 prior games for meaningful avg
                continue

            for stat_type in stat_types:
                stat_key = STAT_KEY_MAP.get(stat_type, stat_type.upper())
                actual_value = float(game.get(stat_key, 0) or 0)

                # Reconstruct prop line as season average up to this date
                season_avg = _season_avg_up_to_date(sorted_logs, stat_key, game_date)
                if season_avg is None or season_avg <= 0:
                    continue
                archived_line, found = _get_archived_line(
                    player_name, stat_type, game_date, _archive_cache
                )
                if found and archived_line > 0:
                    prop_line = archived_line
                else:
                    prop_line = _round_to_half(season_avg)

                # Estimate standard deviation from prior game logs
                season_std = _season_std_up_to_date(sorted_logs, stat_key, game_date)
                if season_std is None or season_std <= 0:
                    # Fallback: use ~30% CV as a reasonable default
                    season_std = max(0.5, season_avg * 0.30)

                # Prior game log values for KDE sampling (up to this date)
                prior_log_values = []
                for g in sorted_logs[:i]:
                    try:
                        prior_log_values.append(float(g.get(stat_key, 0) or 0))
                    except (ValueError, TypeError):
                        pass

                try:
                    sim_result = run_quantum_matrix_simulation(
                        projected_stat_average=season_avg,
                        stat_standard_deviation=season_std,
                        prop_line=prop_line,
                        number_of_simulations=500,
                        blowout_risk_factor=0.0,
                        pace_adjustment_factor=1.0,
                        matchup_adjustment_factor=1.0,
                        home_away_adjustment=0.0,
                        rest_adjustment_factor=1.0,
                        stat_type=stat_type,
                        recent_game_logs=prior_log_values if len(prior_log_values) >= 15 else None,
                    )
                except Exception as exc:
                    _logger.debug("Backtest simulation failed for %s on %s: %s",
                                  stat_type, game_date, exc)
                    continue

                if not sim_result:
                    continue

                over_prob = sim_result.get("probability_over", 0.5)
                # probability_under = complement of probability_over
                under_prob = 1.0 - over_prob

                # Determine model pick direction first, then compute edge on that side
                pick_direction = "OVER" if over_prob > under_prob else "UNDER"
                model_prob = over_prob if pick_direction == "OVER" else under_prob
                # True edge = model probability for selected side minus -110 breakeven
                edge = model_prob - _IMPLIED_PROB_MINUS_110

                if edge < min_edge:
                    continue

                # Determine tier based on model probability
                if model_prob >= 0.70:
                    tier = "ELITE"
                elif model_prob >= 0.63:
                    tier = "STRONG"
                elif model_prob >= 0.57:
                    tier = "VALUE"
                else:
                    tier = "LEAN"

                if tier_filter and tier != tier_filter:
                    continue

                # Check if pick was correct
                if pick_direction == "OVER":
                    correct = actual_value > prop_line
                else:
                    correct = actual_value < prop_line

                total_picks += 1
                pick_pnl = PAYOUT_AT_MINUS_110 if correct else -1.0
                if correct:
                    wins += 1
                    total_pnl += PAYOUT_AT_MINUS_110  # Win at -110
                else:
                    total_pnl -= 1.0   # Lose bet

                # Update by-tier stats (including per-tier P&L)
                if tier in results_by_tier:
                    results_by_tier[tier]["picks"] += 1
                    results_by_tier[tier]["pnl"] += pick_pnl
                    if correct:
                        results_by_tier[tier]["wins"] += 1

                # Update by-stat stats
                if stat_type in results_by_stat:
                    results_by_stat[stat_type]["picks"] += 1
                    if correct:
                        results_by_stat[stat_type]["wins"] += 1

                # Update by-edge-bucket stats
                for lo, hi, label in EDGE_BUCKETS:
                    if lo <= edge < hi:
                        results_by_edge_bucket[label]["picks"] += 1
                        if correct:
                            results_by_edge_bucket[label]["wins"] += 1
                        break

                pick_log.append({
                    "player": player_name,
                    "date": game_date,
                    "stat": stat_type,
                    "line": prop_line,
                    "actual": actual_value,
                    "direction": pick_direction,
                    "correct": correct,
                    "model_prob": round(model_prob, 4),
                    "tier": tier,
                    "edge": round(edge, 4),
                })

    win_rate = wins / total_picks if total_picks > 0 else 0.0
    roi = (total_pnl / total_picks) if total_picks > 0 else 0.0

    # Win rates by tier (with ROI per tier)
    tier_win_rates = {}
    for tier, data in results_by_tier.items():
        p = data["picks"]
        w = data["wins"]
        t_pnl = data.get("pnl", 0.0)
        tier_win_rates[tier] = {
            "picks": p,
            "wins": w,
            "win_rate": w / p if p > 0 else 0.0,
            "roi": (t_pnl / p) if p > 0 else 0.0,
            "pnl": round(t_pnl, 2),
        }

    # Win rates by stat
    stat_win_rates = {}
    for st, data in results_by_stat.items():
        p = data["picks"]
        w = data["wins"]
        stat_win_rates[st] = {"picks": p, "wins": w, "win_rate": w / p if p > 0 else 0.0}

    # Win rates by edge bucket
    edge_win_rates = {}
    for label, data in results_by_edge_bucket.items():
        p = data["picks"]
        w = data["wins"]
        edge_win_rates[label] = {"picks": p, "wins": w, "win_rate": w / p if p > 0 else 0.0}

    # ---- Sharpe Ratio ----
    # BEGINNER NOTE: The Sharpe ratio measures return-per-unit-of-risk.
    # A Sharpe > 1.0 is good; > 2.0 is excellent for a betting strategy.
    # Formula: Sharpe = mean_return / std_return * sqrt(N)
    sharpe_ratio = _calculate_sharpe_ratio(pick_log)

    # ---- Max Drawdown ----
    # BEGINNER NOTE: Max drawdown tracks the worst peak-to-trough decline
    # in cumulative P&L. A drawdown of -10 means the strategy fell 10 units
    # from its peak before recovering. Smaller (less negative) is better.
    max_drawdown = _calculate_max_drawdown(pick_log)

    # ---- Out-of-Sample Split ----
    # BEGINNER NOTE: In-sample = data used to build the model (first 70%)
    # Out-of-sample = data the model has never seen (last 30%) — a truer
    # test of whether the model generalizes to new situations.
    oos_metrics = _calculate_oos_metrics(pick_log)

    return {
        "status": "ok",
        "message": f"Backtest complete: {total_picks} picks analyzed",
        "season": season,
        "stat_types": stat_types,
        "min_edge": min_edge,
        "tier_filter": tier_filter,
        "total_picks": total_picks,
        "wins": wins,
        "losses": total_picks - wins,
        "win_rate": round(win_rate, 4),
        "roi": round(roi, 4),
        "total_pnl": round(total_pnl, 2),
        "tier_win_rates": tier_win_rates,
        "stat_win_rates": stat_win_rates,
        "edge_win_rates": edge_win_rates,
        "sharpe_ratio": sharpe_ratio,
        "max_drawdown": max_drawdown,
        "oos_metrics": oos_metrics,
        "pick_log": pick_log[-200:],  # Keep last 200 for display
    }


def _calculate_sharpe_ratio(pick_log):
    """
    Calculate the Sharpe ratio of the backtest P&L sequence.

    BEGINNER NOTE: The Sharpe ratio = mean return / std of returns * sqrt(N).
    This tells us if the positive returns are consistent (high Sharpe) or
    driven by a few lucky big wins (low Sharpe).

    Args:
        pick_log (list of dict): Each dict has 'correct' (bool) field.

    Returns:
        float: Sharpe ratio. Positive = profitable, higher = more consistent.
    """
    if len(pick_log) < 3:
        return 0.0

    # Returns: +0.909 for a win at -110, -1.0 for a loss
    returns = [PAYOUT_AT_MINUS_110 if p["correct"] else -1.0 for p in pick_log]
    n = len(returns)
    mean_r = sum(returns) / n
    if n < 2:
        return 0.0
    variance_r = sum((r - mean_r) ** 2 for r in returns) / (n - 1)
    std_r = math.sqrt(variance_r)
    if std_r == 0:
        return 0.0
    return round(mean_r / std_r * math.sqrt(n), 4)


def _calculate_max_drawdown(pick_log):
    """
    Calculate the maximum peak-to-trough drawdown in cumulative P&L.

    BEGINNER NOTE: Drawdown tells you how bad the worst losing streak was.
    If the cumulative P&L went from +15 down to +3, the max drawdown is -12.
    A smaller (less negative) max drawdown means the strategy is less risky.

    Args:
        pick_log (list of dict): Each dict has 'correct' (bool) field.

    Returns:
        float: Maximum drawdown (negative number, e.g. -5.45 means worst
            peak-to-trough decline was 5.45 units). 0.0 if no drawdown.
    """
    if not pick_log:
        return 0.0

    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0

    for p in pick_log:
        cumulative += PAYOUT_AT_MINUS_110 if p["correct"] else -1.0
        if cumulative > peak:
            peak = cumulative
        drawdown = cumulative - peak
        if drawdown < max_dd:
            max_dd = drawdown

    return round(max_dd, 2)


def _calculate_oos_metrics(pick_log):
    """
    Split the pick log into in-sample (first 70%) and out-of-sample (last 30%).

    BEGINNER NOTE: Good models perform well BOTH in-sample (data they were
    tuned on) AND out-of-sample (data they've never seen). If in-sample
    win rate is 60% but OOS is 52%, the model may be overfit.

    Args:
        pick_log (list of dict): List of pick records with 'correct' field.

    Returns:
        dict: {
            'is_picks': int,       # In-sample pick count
            'is_win_rate': float,  # In-sample win rate
            'is_roi': float,       # In-sample ROI per pick
            'oos_picks': int,      # Out-of-sample pick count
            'oos_win_rate': float, # Out-of-sample win rate
            'oos_roi': float,      # Out-of-sample ROI per pick
        }
    """
    n = len(pick_log)
    if n < 10:
        return {
            "is_picks": 0, "is_win_rate": 0.0, "is_roi": 0.0,
            "oos_picks": 0, "oos_win_rate": 0.0, "oos_roi": 0.0,
        }

    split_idx = int(n * 0.70)
    in_sample  = pick_log[:split_idx]
    out_sample = pick_log[split_idx:]

    def _metrics(subset):
        if not subset:
            return 0, 0.0, 0.0
        picks = len(subset)
        wins = sum(1 for p in subset if p["correct"])
        pnl = sum(PAYOUT_AT_MINUS_110 if p["correct"] else -1.0 for p in subset)
        wr = wins / picks
        roi = pnl / picks
        return picks, round(wr, 4), round(roi, 4)

    is_picks, is_wr, is_roi = _metrics(in_sample)
    oos_picks, oos_wr, oos_roi = _metrics(out_sample)

    return {
        "is_picks": is_picks,
        "is_win_rate": is_wr,
        "is_roi": is_roi,
        "oos_picks": oos_picks,
        "oos_win_rate": oos_wr,
        "oos_roi": oos_roi,
    }


def _empty_result(message):
    """Return an empty BacktestResult dict."""
    return {
        "status": "no_data",
        "message": message,
        "total_picks": 0,
        "wins": 0,
        "losses": 0,
        "win_rate": 0.0,
        "roi": 0.0,
        "total_pnl": 0.0,
        "tier_win_rates": {},
        "stat_win_rates": {},
        "edge_win_rates": {},
        "sharpe_ratio": 0.0,
        "max_drawdown": 0.0,
        "oos_metrics": {},
        "pick_log": [],
    }
