"""engine/pipeline/step_3_features.py – Phase 3: Feature engineering.

Enriches raw Player_Game_Logs with:
  • Advanced per-game metrics (TS%, USG%, PER)
  • Rolling averages & standard deviations (3/5/10/20 game windows)
  • Recent-form ratio (last-5 avg / season avg)
  • Game context: rest days, back-to-back, home/away
  • Team context: pace adjustment, defensive matchup factor
  • Defense-vs-position multipliers
  • Box_Score_Advanced & Box_Score_Usage joins
  • Standings-based team strength
"""
import os
from utils.logger import get_logger

_logger = get_logger(__name__)
_ML_READY_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "ml_ready"
)

# Stat columns used for rolling features
_ROLLING_STATS = [
    "pts", "reb", "ast", "stl", "blk", "tov", "fg3m",
    "fgm", "fga", "ftm", "fta", "oreb", "dreb", "pf", "plus_minus",
]
_ROLLING_WINDOWS = [3, 5, 10, 20]


def _parse_minutes(raw_min) -> float:
    """Convert 'MM:SS' string or numeric minutes to float."""
    if raw_min is None:
        return 0.0
    if isinstance(raw_min, (int, float)):
        return float(raw_min)
    try:
        raw_min = str(raw_min).strip()
        if ":" in raw_min:
            parts = raw_min.split(":")
            return float(parts[0]) + float(parts[1]) / 60.0
        return float(raw_min)
    except (ValueError, TypeError):
        return 0.0


def run(context: dict) -> dict:
    """Compute derived features and save ML-ready data.

    Args:
        context: Pipeline context with ``clean_data`` key.

    Returns:
        Updated context with ``feature_data`` key.
    """
    os.makedirs(_ML_READY_DIR, exist_ok=True)
    clean_data = context.get("clean_data", {})
    raw_data = context.get("raw_data", {})
    date_str = context.get("date_str", "unknown")
    feature_data = {}

    try:
        import pandas as pd
        import numpy as np
        from utils.parquet_helpers import save_parquet
        from engine.features.player_metrics import (
            calculate_true_shooting,
            calculate_per,
        )
        from engine.features.feature_engineering import (
            calculate_days_rest_factor,
            calculate_pace_adjustment,
            calculate_defensive_matchup_factor,
        )
        from utils.constants import LEAGUE_AVG_PACE, LEAGUE_AVG_DRTG

        player_df = clean_data.get("player_stats")
        if player_df is None or (hasattr(player_df, "empty") and player_df.empty):
            context["feature_data"] = feature_data
            return context

        df = pd.DataFrame(player_df) if isinstance(player_df, list) else player_df.copy()

        if df.empty:
            context["feature_data"] = feature_data
            return context

        # ── 1. Parse minutes to float ─────────────────────────────────
        if "min" in df.columns:
            df["minutes_float"] = df["min"].apply(_parse_minutes)

        # ── 2. Per-game advanced metrics ──────────────────────────────
        if all(c in df.columns for c in ["pts", "fga", "fta"]):
            df["ts_pct"] = df.apply(
                lambda r: calculate_true_shooting(
                    float(r["pts"]), float(r["fga"]), float(r["fta"])
                ),
                axis=1,
            )
        if all(c in df.columns for c in ["pts", "reb", "ast", "stl", "blk", "tov",
                                          "fga", "fgm", "fta", "ftm"]):
            df["per"] = df.apply(
                lambda r: calculate_per({
                    "pts": r["pts"], "reb": r["reb"], "ast": r["ast"],
                    "stl": r["stl"], "blk": r["blk"], "tov": r["tov"],
                    "fga": r["fga"], "fgm": r["fgm"], "fta": r["fta"],
                    "ftm": r["ftm"],
                    "mp": _parse_minutes(r.get("min", 1)),
                }),
                axis=1,
            )

        # ── 3. Rolling averages & standard deviations ─────────────────
        if "player_id" in df.columns and "game_date" in df.columns:
            df = df.sort_values(["player_id", "game_date"])
            present_stats = [c for c in _ROLLING_STATS if c in df.columns]

            new_cols = {}
            for w in _ROLLING_WINDOWS:
                rolled_mean = (
                    df.groupby("player_id")[present_stats]
                    .apply(lambda g: g.shift(1).rolling(w, min_periods=1).mean())
                )
                rolled_std = (
                    df.groupby("player_id")[present_stats]
                    .apply(lambda g: g.shift(1).rolling(w, min_periods=2).std())
                )
                rolled_mean.index = df.index
                rolled_std.index = df.index
                for stat in present_stats:
                    new_cols[f"{stat}_roll_{w}"] = rolled_mean[stat]
                    if w >= 5:
                        new_cols[f"{stat}_std_{w}"] = rolled_std[stat]

            # Recent form ratio: last-5 avg / season expanding avg (shift 1 to avoid leakage)
            for stat in present_stats:
                season_avg = (
                    df.groupby("player_id")[stat]
                    .apply(lambda g: g.shift(1).expanding(min_periods=1).mean())
                )
                season_avg.index = df.index
                roll5_key = f"{stat}_roll_5"
                if roll5_key in new_cols:
                    new_cols[f"{stat}_form_ratio"] = np.where(
                        season_avg > 0,
                        new_cols[roll5_key] / season_avg,
                        1.0,
                    )

            # Bulk-assign all rolling columns at once to avoid fragmentation
            df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)
        else:
            _logger.debug("Skipping rolling features: missing player_id or game_date")

        # ── 4. Rest days & back-to-back ───────────────────────────────
        if "player_id" in df.columns and "game_date" in df.columns:
            df["game_date_dt"] = pd.to_datetime(df["game_date"], errors="coerce")
            df["prev_game_date"] = df.groupby("player_id")["game_date_dt"].shift(1)
            df["rest_days"] = (df["game_date_dt"] - df["prev_game_date"]).dt.days
            df["rest_days"] = df["rest_days"].fillna(7).clip(0, 14).astype(int)
            df["is_back_to_back"] = (df["rest_days"] <= 1).astype(int)
            df["rest_factor"] = df["rest_days"].apply(calculate_days_rest_factor)
            df.drop(columns=["game_date_dt", "prev_game_date"], inplace=True)

        # ── 5. Home / away ────────────────────────────────────────────
        if all(c in df.columns for c in ["player_id", "home_team_id"]):
            # Determine is_home from the Games join
            player_team_map = {}
            try:
                teams_raw = raw_data.get("teams", [])
                if teams_raw:
                    for t in (teams_raw if isinstance(teams_raw, list)
                              else teams_raw.to_dict("records")):
                        if t.get("team_id") and t.get("abbreviation"):
                            player_team_map[t["abbreviation"]] = t["team_id"]
            except Exception:
                pass

            if "player_team_abbrev" in df.columns:
                df["player_team_id_lookup"] = df["player_team_abbrev"].map(player_team_map)
                df["is_home"] = np.where(
                    df["player_team_id_lookup"] == df["home_team_id"], 1, 0
                )
                df.drop(columns=["player_team_id_lookup"], inplace=True)
            else:
                df["is_home"] = 0

        # ── 6. Pace adjustment & defensive matchup factor ─────────────
        _fill_pace = float(LEAGUE_AVG_PACE)
        _fill_drtg = float(LEAGUE_AVG_DRTG)

        # Build opponent_drtg lookup from Team_Game_Stats
        opp_drtg_map = {}
        try:
            tgs_raw = clean_data.get("team_game_stats") or raw_data.get("team_game_stats", [])
            if tgs_raw is not None:
                tgs_list = tgs_raw if isinstance(tgs_raw, list) else tgs_raw.to_dict("records")
                for row in tgs_list:
                    key = (row.get("game_id"), row.get("team_id"))
                    if key[0] and key[1]:
                        opp_drtg_map[key] = float(row.get("drtg_est") or _fill_drtg)
        except Exception as exc:
            _logger.debug("Failed building opp_drtg_map: %s", exc)

        # Derive opponent team id for each player-game row
        if all(c in df.columns for c in ["home_team_id", "away_team_id"]):
            team_pace_map = {}
            team_drtg_map = {}
            try:
                teams_raw = raw_data.get("teams", [])
                teams_list = teams_raw if isinstance(teams_raw, list) else teams_raw.to_dict("records")
                for t in teams_list:
                    tid = t.get("team_id")
                    if tid:
                        team_pace_map[tid] = float(t.get("pace") or _fill_pace)
                        team_drtg_map[tid] = float(t.get("drtg") or _fill_drtg)
            except Exception:
                pass

            if "is_home" in df.columns:
                df["opp_team_id"] = np.where(
                    df["is_home"] == 1,
                    df["away_team_id"],
                    df["home_team_id"],
                )
            else:
                df["opp_team_id"] = df["away_team_id"]

            # Team pace & opponent pace
            if "team_pace" not in df.columns or df["team_pace"].isna().all():
                df["team_pace"] = _fill_pace
            df["team_pace"] = df["team_pace"].fillna(_fill_pace).astype(float)
            df["opp_pace"] = df["opp_team_id"].map(team_pace_map).fillna(_fill_pace)
            df["opp_drtg"] = df["opp_team_id"].map(team_drtg_map).fillna(_fill_drtg)

            df["pace_adjustment"] = df.apply(
                lambda r: calculate_pace_adjustment(
                    float(r["team_pace"]), float(r["opp_pace"])
                ),
                axis=1,
            )
            df["defensive_matchup_factor"] = df["opp_drtg"].apply(
                lambda d: calculate_defensive_matchup_factor(float(d))
            )

        # ── 7. Defense-vs-Position multipliers ────────────────────────
        try:
            dvp_raw = clean_data.get("defense_vs_position") or raw_data.get("defense_vs_position", [])
            if dvp_raw is not None and "player_position" in df.columns:
                dvp_list = dvp_raw if isinstance(dvp_raw, list) else dvp_raw.to_dict("records")
                if dvp_list:
                    dvp_df = pd.DataFrame(dvp_list)
                    # Normalize column names
                    dvp_df.columns = [c.lower().strip() for c in dvp_df.columns]
                    # Map opponent abbrev
                    opp_abbrev_map = {}
                    try:
                        teams_list = raw_data.get("teams", [])
                        tl = teams_list if isinstance(teams_list, list) else teams_list.to_dict("records")
                        for t in tl:
                            if t.get("team_id") and t.get("abbreviation"):
                                opp_abbrev_map[t["team_id"]] = t["abbreviation"]
                    except Exception:
                        pass

                    if "opp_team_id" in df.columns and opp_abbrev_map:
                        df["opp_abbrev"] = df["opp_team_id"].map(opp_abbrev_map)
                    elif "away_abbrev" in df.columns and "home_abbrev" in df.columns:
                        df["opp_abbrev"] = np.where(
                            df.get("is_home", 0) == 1,
                            df["away_abbrev"],
                            df["home_abbrev"],
                        )
                    else:
                        df["opp_abbrev"] = None

                    # Normalize position to single-char (G/F/C)
                    pos_map = {
                        "PG": "G", "SG": "G", "SF": "F", "PF": "F", "C": "C",
                        "G": "G", "F": "F", "G-F": "G", "F-G": "F", "F-C": "F", "C-F": "C",
                    }
                    df["pos_norm"] = (
                        df["player_position"]
                        .fillna("")
                        .str.strip()
                        .str.upper()
                        .map(pos_map)
                        .fillna("G")
                    )

                    mult_cols = [c for c in dvp_df.columns if c.startswith("vs_")]
                    if "team_abbreviation" in dvp_df.columns and "pos" in dvp_df.columns:
                        dvp_df["pos"] = dvp_df["pos"].str.strip().str.upper()
                        dvp_df["team_abbreviation"] = dvp_df["team_abbreviation"].str.strip().str.upper()
                        dvp_lookup = dvp_df.set_index(["team_abbreviation", "pos"])

                        for mc in mult_cols:
                            df[f"dvp_{mc}"] = df.apply(
                                lambda r: dvp_lookup.at[(r["opp_abbrev"], r["pos_norm"]), mc]
                                if (r.get("opp_abbrev"), r.get("pos_norm")) in dvp_lookup.index
                                else 1.0,
                                axis=1,
                            )
                    df.drop(columns=["opp_abbrev", "pos_norm"], inplace=True, errors="ignore")
        except Exception as exc:
            _logger.debug("Defense-vs-Position join failed: %s", exc)

        # ── 8. Box_Score_Advanced join ────────────────────────────────
        try:
            bsa_raw = clean_data.get("box_score_advanced") or raw_data.get("box_score_advanced", [])
            if bsa_raw is not None:
                bsa_list = bsa_raw if isinstance(bsa_raw, list) else bsa_raw.to_dict("records")
                if bsa_list and "player_id" in df.columns and "game_id" in df.columns:
                    bsa_df = pd.DataFrame(bsa_list)
                    bsa_df.columns = [c.lower().strip() for c in bsa_df.columns]
                    bsa_cols = [c for c in bsa_df.columns if c not in ("game_id", "person_id")]
                    bsa_df = bsa_df.rename(columns={"person_id": "player_id"})
                    # Prefix to avoid collisions
                    bsa_df = bsa_df.rename(columns={c: f"adv_{c}" for c in bsa_cols})
                    df = df.merge(
                        bsa_df, on=["game_id", "player_id"], how="left"
                    )
        except Exception as exc:
            _logger.debug("Box_Score_Advanced join failed: %s", exc)

        # ── 9. Box_Score_Usage join ───────────────────────────────────
        try:
            bsu_raw = clean_data.get("box_score_usage") or raw_data.get("box_score_usage", [])
            if bsu_raw is not None:
                bsu_list = bsu_raw if isinstance(bsu_raw, list) else bsu_raw.to_dict("records")
                if bsu_list and "player_id" in df.columns and "game_id" in df.columns:
                    bsu_df = pd.DataFrame(bsu_list)
                    bsu_df.columns = [c.lower().strip() for c in bsu_df.columns]
                    bsu_cols = [c for c in bsu_df.columns if c not in ("game_id", "person_id")]
                    bsu_df = bsu_df.rename(columns={"person_id": "player_id"})
                    bsu_df = bsu_df.rename(columns={c: f"usage_{c}" for c in bsu_cols})
                    df = df.merge(
                        bsu_df, on=["game_id", "player_id"], how="left"
                    )
        except Exception as exc:
            _logger.debug("Box_Score_Usage join failed: %s", exc)

        # ── 10. Standings join (team strength) ────────────────────────
        try:
            stnd_raw = clean_data.get("standings") or raw_data.get("standings", [])
            if stnd_raw is not None:
                stnd_list = stnd_raw if isinstance(stnd_raw, list) else stnd_raw.to_dict("records")
                if stnd_list:
                    stnd_df = pd.DataFrame(stnd_list)
                    stnd_df.columns = [c.lower().strip() for c in stnd_df.columns]
                    # Prefix standings columns
                    stnd_cols = [c for c in stnd_df.columns if c != "team_id"]
                    stnd_df = stnd_df.rename(columns={c: f"team_stnd_{c}" for c in stnd_cols})
                    if "opp_team_id" in df.columns:
                        opp_rename = {"team_id": "opp_team_id"}
                        for col in stnd_df.columns:
                            if col.startswith("team_stnd_"):
                                opp_rename[col] = col.replace("team_stnd_", "opp_stnd_")
                        opp_stnd = stnd_df.rename(columns=opp_rename)
                        df = df.merge(opp_stnd, on="opp_team_id", how="left")
        except Exception as exc:
            _logger.debug("Standings join failed: %s", exc)

        # ── 11. Fill remaining NaN with 0 for numeric columns ─────────
        num_cols = df.select_dtypes(include="number").columns
        df[num_cols] = df[num_cols].fillna(0)

        # ── Save ──────────────────────────────────────────────────────
        feature_data["player_features"] = df
        path = os.path.join(_ML_READY_DIR, f"player_features_{date_str}.parquet")
        save_parquet(df, path)
        _logger.info(
            "Saved player features → %d rows × %d columns", len(df), len(df.columns)
        )

    except Exception as exc:
        _logger.error("Feature engineering error: %s", exc, exc_info=True)
        feature_data = clean_data

    context["feature_data"] = feature_data
    return context
