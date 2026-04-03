# ============================================================
# FILE: engine/regime_detection.py
# PURPOSE: Regime Change Detection & Bayesian Updating Engine
#          Detects structural shifts in player/team performance
#          and applies Bayesian probability updates to prop
#          predictions. Flags when a player's "regime" has
#          changed (role change, hot/cold streaks, trade effects)
#          so downstream models can adapt accordingly.
# CONNECTS TO: confidence.py (confidence scores),
#              calibration.py (calibration offsets),
#              projections.py (player projections)
# CONCEPTS COVERED: CUSUM change-point detection, Bayesian
#                   updating, adaptive weighting, structural
#                   shift analysis
# ============================================================
import math
import statistics
def _safe_float(value, fallback=0.0):
    """Return *value* if it is a finite float, otherwise *fallback*.
    Last-line-of-defence guard that prevents NaN or ±inf from leaking
    out of the regime detection engine into downstream scoring code.
    """
    try:
        v = float(value)
        if math.isfinite(v):
            return v
        return float(fallback)
    except (ValueError, TypeError):
        return float(fallback)
# ============================================================
# SECTION: Regime Detection Constants
# ============================================================
# Number of standard deviations the recent window must diverge from
# the season average before a regime change is flagged.
REGIME_CHANGE_THRESHOLD = 1.5
# Minimum game log entries required before regime detection is
# meaningful.  Below this threshold the module returns neutral.
MIN_GAMES_FOR_REGIME_DETECTION = 10
# Default prior weight used in Bayesian updates when no explicit
# evidence strength is provided.  0.6 = trust the prior 60%.
BAYESIAN_PRIOR_WEIGHT_DEFAULT = 0.6
# Exponential decay rate (per day) applied when computing recency
# weights.  Higher values discount older games more aggressively.
RECENCY_DECAY_RATE = 0.05
# ============================================================
# END SECTION: Regime Detection Constants
# ============================================================
# ============================================================
# SECTION: Core Regime Detection
# ============================================================
def detect_regime_change(
    game_logs: list[dict],
    stat_key: str = "pts",
    window: int = 10,
) -> dict:
    """Detect structural changes in a player's recent performance.
    Uses a CUSUM-inspired approach: compares the recent *window* games
    against the full-season average.  If the delta exceeds
    REGIME_CHANGE_THRESHOLD standard deviations the regime is flagged
    as changed.
    Args:
        game_logs: List of game-log dicts, each containing at least
            *stat_key*.  Most recent game should be last.
        stat_key: Key inside each game dict to analyse (default "pts").
        window: Number of recent games to compare against the season.
    Returns:
        dict with keys: regime_changed, direction, magnitude,
        confidence, recent_avg, season_avg, detection_method.
    """
    try:
        # --- extract numeric values ---
        values = [
            _safe_float(g.get(stat_key))
            for g in (game_logs or [])
            if g.get(stat_key) is not None
        ]
        # Not enough data → neutral result
        if len(values) < MIN_GAMES_FOR_REGIME_DETECTION:
            return {
                "regime_changed": False,
                "direction": "stable",
                "magnitude": 0.0,
                "confidence": 0.0,
                "recent_avg": 0.0,
                "season_avg": 0.0,
                "detection_method": "insufficient_data",
            }
        season_avg = _safe_float(statistics.mean(values))
        season_std = _safe_float(statistics.pstdev(values), fallback=1.0)
        # Prevent division by zero when all values are identical
        if season_std < 0.001:
            season_std = 1.0
        # Recent window (capped to available data)
        effective_window = min(window, len(values))
        recent_values = values[-effective_window:]
        recent_avg = _safe_float(statistics.mean(recent_values))
        # --- CUSUM-style delta ---
        delta = recent_avg - season_avg
        z_score = delta / season_std
        regime_changed = abs(z_score) >= REGIME_CHANGE_THRESHOLD
        if z_score >= REGIME_CHANGE_THRESHOLD:
            direction = "up"
        elif z_score <= -REGIME_CHANGE_THRESHOLD:
            direction = "down"
        else:
            direction = "stable"
        # Confidence: sigmoid-like mapping of |z| into [0, 1]
        confidence = min(1.0, abs(z_score) / (REGIME_CHANGE_THRESHOLD * 2))
        return {
            "regime_changed": regime_changed,
            "direction": direction,
            "magnitude": round(_safe_float(abs(delta)), 2),
            "confidence": round(_safe_float(confidence), 3),
            "recent_avg": round(recent_avg, 2),
            "season_avg": round(season_avg, 2),
            "detection_method": "cusum_z_score",
        }
    except Exception:
        return {
            "regime_changed": False,
            "direction": "stable",
            "magnitude": 0.0,
            "confidence": 0.0,
            "recent_avg": 0.0,
            "season_avg": 0.0,
            "detection_method": "error_fallback",
        }
# ============================================================
# SECTION: Bayesian Updating
# ============================================================
def bayesian_update_probability(
    prior: float,
    likelihood: float,
    evidence_strength: float = 1.0,
) -> dict:
    """Apply Bayesian updating to a probability estimate.
    Uses a weighted Bayes' rule: when *evidence_strength* < 1 the
    update is damped, blending between the prior and the full
    posterior.
    Args:
        prior: Initial probability estimate (0-1).
        likelihood: Probability of observed evidence given the
            hypothesis (0-1).
        evidence_strength: Weight for the new evidence (0-1).
            1.0 = full Bayesian update, 0.0 = keep prior.
    Returns:
        dict with: posterior, prior, likelihood, bayes_factor,
        update_magnitude.
    """
    try:
        prior = max(0.001, min(0.999, _safe_float(prior, 0.5)))
        likelihood = max(0.001, min(0.999, _safe_float(likelihood, 0.5)))
        evidence_strength = max(0.0, min(1.0, _safe_float(evidence_strength, 1.0)))
        # Full Bayesian posterior: P(H|E) = P(E|H)*P(H) / P(E)
        # where P(E) = P(E|H)*P(H) + P(E|¬H)*P(¬H)
        complement_prior = 1.0 - prior
        complement_likelihood = 1.0 - likelihood
        evidence = likelihood * prior + complement_likelihood * complement_prior
        if evidence < 1e-12:
            evidence = 1e-12
        full_posterior = (likelihood * prior) / evidence
        # Damp update by evidence_strength: blend prior → full posterior
        posterior = prior + evidence_strength * (full_posterior - prior)
        posterior = max(0.001, min(0.999, _safe_float(posterior)))
        # Bayes factor: ratio of likelihood under H vs ¬H
        if complement_likelihood < 1e-12:
            bayes_factor = _safe_float(likelihood / 1e-12)
        else:
            bayes_factor = _safe_float(likelihood / complement_likelihood)
        update_magnitude = abs(posterior - prior)
        return {
            "posterior": round(posterior, 4),
            "prior": round(prior, 4),
            "likelihood": round(likelihood, 4),
            "bayes_factor": round(_safe_float(bayes_factor), 4),
            "update_magnitude": round(_safe_float(update_magnitude), 4),
        }
    except Exception:
        return {
            "posterior": _safe_float(prior, 0.5),
            "prior": _safe_float(prior, 0.5),
            "likelihood": _safe_float(likelihood, 0.5),
            "bayes_factor": 1.0,
            "update_magnitude": 0.0,
        }
# ============================================================
# SECTION: Player Structural Shift Detection
# ============================================================
def detect_player_structural_shift(
    player_data: dict,
    recent_games: list[dict],
) -> dict:
    """Comprehensive structural-change detection across multiple
    performance dimensions.
    Checks four dimensions:
    1. **Role change** — usage rate spike (usage_pct or fga).
    2. **Shot profile** — three-point attempt rate shift (fg3a / fga).
    3. **Minutes change** — significant minutes increase/decrease.
    4. **Efficiency shift** — true-shooting or fg_pct movement.
    Args:
        player_data: Season-level averages dict.  Expected keys
            include avg_min, avg_fga, avg_fg3a, avg_pts, usage_pct,
            ts_pct (all optional — missing keys are skipped).
        recent_games: Recent game logs (list of dicts).
    Returns:
        dict with: has_structural_shift, shift_dimensions,
        shift_severity, description, recommendations.
    """
    try:
        player_data = player_data or {}
        recent_games = recent_games or []
        shift_dimensions: list[str] = []
        details: list[str] = []
        if len(recent_games) < 3:
            return {
                "has_structural_shift": False,
                "shift_dimensions": [],
                "shift_severity": "minor",
                "description": "Insufficient recent games for structural analysis.",
                "recommendations": ["Wait for more game data before adjusting."],
            }
        # --- helper: compare recent average vs season average ---
        def _check_dimension(
            season_key: str,
            game_key: str,
            label: str,
            threshold_pct: float = 15.0,
        ) -> bool:
            season_val = _safe_float(player_data.get(season_key), fallback=0.0)
            if season_val < 0.01:
                return False
            game_vals = [
                _safe_float(g.get(game_key))
                for g in recent_games
                if g.get(game_key) is not None
            ]
            if not game_vals:
                return False
            recent_avg = statistics.mean(game_vals)
            pct_change = ((recent_avg - season_val) / season_val) * 100.0
            if abs(pct_change) >= threshold_pct:
                direction = "increased" if pct_change > 0 else "decreased"
                details.append(
                    f"{label} {direction} by {abs(pct_change):.1f}% "
                    f"(season {season_val:.1f} → recent {recent_avg:.1f})"
                )
                shift_dimensions.append(label)
                return True
            return False
        # 1. Role / usage change (field goal attempts as proxy)
        _check_dimension("avg_fga", "fga", "usage", threshold_pct=20.0)
        # 2. Shot profile — 3-point rate
        season_fga = _safe_float(player_data.get("avg_fga"), 1.0)
        season_fg3a = _safe_float(player_data.get("avg_fg3a"), 0.0)
        if season_fga >= 1.0:
            season_3pt_rate = season_fg3a / season_fga
            game_3pt_rates = []
            for g in recent_games:
                gfga = _safe_float(g.get("fga"), 0.0)
                gfg3a = _safe_float(g.get("fg3a"), 0.0)
                if gfga >= 1.0:
                    game_3pt_rates.append(gfg3a / gfga)
            if game_3pt_rates and season_3pt_rate > 0.01:
                recent_3pt_rate = statistics.mean(game_3pt_rates)
                pct_delta = abs(recent_3pt_rate - season_3pt_rate) / season_3pt_rate * 100
                if pct_delta >= 25.0:
                    direction = "increased" if recent_3pt_rate > season_3pt_rate else "decreased"
                    details.append(
                        f"3PT rate {direction} by {pct_delta:.1f}% "
                        f"(season {season_3pt_rate:.3f} → recent {recent_3pt_rate:.3f})"
                    )
                    shift_dimensions.append("shot_profile")
        # 3. Minutes change
        _check_dimension("avg_min", "min", "minutes", threshold_pct=15.0)
        # 4. Efficiency shift (fg_pct or ts_pct)
        _check_dimension("ts_pct", "ts_pct", "efficiency", threshold_pct=10.0)
        if "efficiency" not in shift_dimensions:
            _check_dimension("avg_fg_pct", "fg_pct", "efficiency", threshold_pct=10.0)
        # --- severity classification ---
        n_shifts = len(shift_dimensions)
        if n_shifts >= 3:
            severity = "major"
        elif n_shifts >= 1:
            severity = "moderate"
        else:
            severity = "minor"
        # --- recommendations ---
        recommendations: list[str] = []
        if "usage" in shift_dimensions:
            recommendations.append(
                "Player's role appears to have changed — re-weight volume props."
            )
        if "shot_profile" in shift_dimensions:
            recommendations.append(
                "Shot selection shift detected — adjust 3PT prop expectations."
            )
        if "minutes" in shift_dimensions:
            recommendations.append(
                "Minutes allocation changed — recalculate counting-stat projections."
            )
        if "efficiency" in shift_dimensions:
            recommendations.append(
                "Efficiency shift detected — monitor for regression or confirmation."
            )
        if not recommendations:
            recommendations.append("No significant structural shift detected.")
        description = "; ".join(details) if details else "No structural shifts detected."
        return {
            "has_structural_shift": n_shifts > 0,
            "shift_dimensions": shift_dimensions,
            "shift_severity": severity,
            "description": description,
            "recommendations": recommendations,
        }
    except Exception:
        return {
            "has_structural_shift": False,
            "shift_dimensions": [],
            "shift_severity": "minor",
            "description": "Error during structural shift analysis.",
            "recommendations": ["Fallback to season averages."],
        }
# ============================================================
# SECTION: Team Regime Change Detection
# ============================================================
def detect_team_regime_change(
    team_data: dict,
    recent_results: list[dict],
) -> dict:
    """Team-level regime detection.
    Looks for signals of coaching changes, trade-deadline effects,
    playoff-mode shifts, or significant scheme changes by checking
    pace, defensive rating, and offensive rating across recent results
    versus the season baseline stored in *team_data*.
    Args:
        team_data: Season-level team averages.  Expected keys:
            pace, def_rtg, off_rtg (all optional).
        recent_results: Recent team game results (list of dicts).
    Returns:
        dict with: regime_changed, regime_type, description,
        affected_stats.
    """
    try:
        team_data = team_data or {}
        recent_results = recent_results or []
        if len(recent_results) < 5:
            return {
                "regime_changed": False,
                "regime_type": "stable",
                "description": "Insufficient recent results for team regime detection.",
                "affected_stats": [],
            }
        affected: list[str] = []
        descriptions: list[str] = []
        def _team_dimension(
            season_key: str,
            game_key: str,
            label: str,
            threshold_pct: float = 10.0,
        ) -> None:
            season_val = _safe_float(team_data.get(season_key), 0.0)
            if season_val < 0.01:
                return
            game_vals = [
                _safe_float(r.get(game_key))
                for r in recent_results
                if r.get(game_key) is not None
            ]
            if not game_vals:
                return
            recent_avg = statistics.mean(game_vals)
            pct_change = ((recent_avg - season_val) / season_val) * 100.0
            if abs(pct_change) >= threshold_pct:
                direction = "up" if pct_change > 0 else "down"
                descriptions.append(
                    f"{label} shifted {direction} by {abs(pct_change):.1f}%"
                )
                affected.append(label)
        # Check key team dimensions
        _team_dimension("pace", "pace", "pace", threshold_pct=8.0)
        _team_dimension("def_rtg", "def_rtg", "defensive_rating", threshold_pct=8.0)
        _team_dimension("off_rtg", "off_rtg", "offensive_rating", threshold_pct=8.0)
        # Determine regime type from pattern of changes
        regime_changed = len(affected) > 0
        if len(affected) >= 3:
            regime_type = "major_scheme_change"
        elif "pace" in affected and "offensive_rating" in affected:
            regime_type = "offensive_scheme_shift"
        elif "defensive_rating" in affected:
            regime_type = "defensive_adjustment"
        elif "pace" in affected:
            regime_type = "pace_change"
        elif regime_changed:
            regime_type = "minor_shift"
        else:
            regime_type = "stable"
        description = (
            "; ".join(descriptions) if descriptions
            else "No significant team regime change detected."
        )
        return {
            "regime_changed": regime_changed,
            "regime_type": regime_type,
            "description": description,
            "affected_stats": affected,
        }
    except Exception:
        return {
            "regime_changed": False,
            "regime_type": "error_fallback",
            "description": "Error during team regime analysis.",
            "affected_stats": [],
        }
# ============================================================
# SECTION: Adaptive Weighting
# ============================================================
def calculate_adaptive_weight(
    sample_size: int,
    recency_days: int = 0,
) -> float:
    """Calculate how much to weight recent data vs season data.
    Larger recent samples and shorter recency windows push the
    weight toward 1.0 (trust recent data).  Small samples and
    stale data push it toward 0.0 (trust season averages).
    The formula blends a sample-size factor with an exponential
    recency decay:
        sample_factor = min(1, sample_size / 30)
        recency_factor = exp(-RECENCY_DECAY_RATE * recency_days)
        weight = BAYESIAN_PRIOR_WEIGHT_DEFAULT * sample_factor * recency_factor
    The returned weight is for **recent** data; the complement
    (1 - weight) applies to season data.
    Args:
        sample_size: Number of recent games available.
        recency_days: How many days ago the most recent game
            occurred.  0 = today.
    Returns:
        float in [0, 1] — weight to apply to recent data.
    """
    try:
        sample_size = max(0, int(_safe_float(sample_size, 0)))
        recency_days = max(0, int(_safe_float(recency_days, 0)))
        # Sample-size ramp: reaches 1.0 at 30 games
        sample_factor = min(1.0, sample_size / 30.0)
        # Recency decay: more recent → higher factor
        recency_factor = math.exp(-RECENCY_DECAY_RATE * recency_days)
        weight = BAYESIAN_PRIOR_WEIGHT_DEFAULT * sample_factor * recency_factor
        return round(max(0.0, min(1.0, _safe_float(weight))), 4)
    except Exception:
        return 0.0
# ============================================================
# SECTION: Full Bayesian Player Update Pipeline
# ============================================================
def run_bayesian_player_update(
    player_data: dict,
    recent_games: list[dict],
    prop_line: float,
    stat_type: str,
) -> dict:
    """Full Bayesian pipeline for a single player prop.
    1. Build a **prior** from season averages.
    2. Run regime detection to gauge how much to trust recent form.
    3. Compute a **likelihood** from recent game hit-rate vs the
       prop line.
    4. Perform a Bayesian update (damped by regime confidence).
    5. Return the posterior estimate and derived edge metrics.
    Args:
        player_data: Season-level player dict.  Expected keys:
            avg_{stat_type} (e.g. avg_pts), games_played.
        recent_games: Recent game-log dicts.
        prop_line: The sportsbook prop line value.
        stat_type: Stat being evaluated (e.g. "pts", "reb", "ast").
    Returns:
        dict with: prior_estimate, posterior_estimate, bayesian_edge,
        regime_adjustment, posterior_over_probability, explanation.
    """
    try:
        player_data = player_data or {}
        recent_games = recent_games or []
        prop_line = _safe_float(prop_line, 0.0)
        # --- 1. Prior from season average ---
        season_key = f"avg_{stat_type}"
        season_avg = _safe_float(player_data.get(season_key, player_data.get(stat_type)), 0.0)
        games_played = int(_safe_float(player_data.get("games_played", 0)))
        if season_avg <= 0.0:
            # Fallback: try to compute from recent games
            vals = [
                _safe_float(g.get(stat_type))
                for g in recent_games
                if g.get(stat_type) is not None
            ]
            season_avg = statistics.mean(vals) if vals else 0.0
        prior_estimate = _safe_float(season_avg)
        # --- 2. Regime detection ---
        regime_result = detect_regime_change(
            recent_games, stat_key=stat_type, window=10
        )
        regime_changed = regime_result.get("regime_changed", False)
        regime_direction = regime_result.get("direction", "stable")
        regime_confidence = _safe_float(regime_result.get("confidence", 0.0))
        # Adaptive weight: how much to trust recent form
        adaptive_w = calculate_adaptive_weight(
            sample_size=len(recent_games), recency_days=0
        )
        # If regime changed, increase trust in recent data
        if regime_changed:
            adaptive_w = min(1.0, adaptive_w + 0.2 * regime_confidence)
        # --- 3. Recent average & likelihood ---
        recent_vals = [
            _safe_float(g.get(stat_type))
            for g in recent_games
            if g.get(stat_type) is not None
        ]
        recent_avg = statistics.mean(recent_vals) if recent_vals else prior_estimate
        # Blended estimate using adaptive weight
        blended_estimate = adaptive_w * recent_avg + (1.0 - adaptive_w) * prior_estimate
        # Likelihood: fraction of recent games that went OVER the prop line
        if recent_vals:
            over_count = sum(1 for v in recent_vals if v > prop_line)
            raw_likelihood = over_count / len(recent_vals)
        else:
            raw_likelihood = 0.5
        # Clamp to avoid degenerate Bayes updates
        likelihood = max(0.05, min(0.95, raw_likelihood))
        # Prior over-probability based on season average vs prop line
        if prior_estimate > 0 and prop_line > 0:
            # Simple z-score–based prior probability
            season_std = 1.0
            if games_played >= MIN_GAMES_FOR_REGIME_DETECTION:
                all_vals = [
                    _safe_float(g.get(stat_type))
                    for g in recent_games
                    if g.get(stat_type) is not None
                ]
                if len(all_vals) >= 2:
                    season_std = max(0.5, _safe_float(statistics.stdev(all_vals), 1.0))
            z = (prior_estimate - prop_line) / season_std
            # Convert z-score to probability via logistic approximation.
            # The 1.7 scaling factor approximates the normal CDF:
            # Φ(z) ≈ 1 / (1 + exp(-1.7 * z)), accurate to ~0.01.
            prior_over_prob = 1.0 / (1.0 + math.exp(-1.7 * z))
        else:
            prior_over_prob = 0.5
        prior_over_prob = max(0.05, min(0.95, prior_over_prob))
        # --- 4. Bayesian update ---
        evidence_strength = adaptive_w
        bayes_result = bayesian_update_probability(
            prior=prior_over_prob,
            likelihood=likelihood,
            evidence_strength=evidence_strength,
        )
        posterior_over_prob = _safe_float(bayes_result.get("posterior", 0.5))
        # --- 5. Regime adjustment ---
        regime_adjustment = 0.0
        if regime_changed:
            if regime_direction == "up":
                regime_adjustment = regime_confidence * 0.1
            elif regime_direction == "down":
                regime_adjustment = -regime_confidence * 0.1
        posterior_estimate = blended_estimate + regime_adjustment * blended_estimate
        bayesian_edge = posterior_estimate - prop_line
        # --- 6. Explanation ---
        parts: list[str] = []
        parts.append(
            f"Season avg {prior_estimate:.1f}, recent avg {recent_avg:.1f}."
        )
        if regime_changed:
            parts.append(
                f"Regime shift detected ({regime_direction}, "
                f"confidence {regime_confidence:.2f})."
            )
        parts.append(
            f"Blended estimate {posterior_estimate:.1f} vs line {prop_line:.1f}."
        )
        if bayesian_edge > 0:
            parts.append(f"Edge of +{bayesian_edge:.1f} favours OVER.")
        elif bayesian_edge < 0:
            parts.append(f"Edge of {bayesian_edge:.1f} favours UNDER.")
        else:
            parts.append("No clear edge detected.")
        explanation = " ".join(parts)
        return {
            "prior_estimate": round(_safe_float(prior_estimate), 2),
            "posterior_estimate": round(_safe_float(posterior_estimate), 2),
            "bayesian_edge": round(_safe_float(bayesian_edge), 2),
            "regime_adjustment": round(_safe_float(regime_adjustment), 4),
            "posterior_over_probability": round(_safe_float(posterior_over_prob), 4),
            "explanation": explanation,
        }
    except Exception:
        return {
            "prior_estimate": 0.0,
            "posterior_estimate": 0.0,
            "bayesian_edge": 0.0,
            "regime_adjustment": 0.0,
            "posterior_over_probability": 0.5,
            "explanation": "Error in Bayesian player update pipeline.",
        }
