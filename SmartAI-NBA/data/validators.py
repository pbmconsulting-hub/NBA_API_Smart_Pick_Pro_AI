# data/validators.py
# Data validation layer for SmartAI-NBA.
# Validates CSV data structure, freshness, and cross-file integrity.
# Standard library only — no numpy/scipy/pandas.

import datetime
import json
import os


REQUIRED_PLAYER_COLUMNS = {"name", "team", "points_avg", "rebounds_avg", "assists_avg"}
REQUIRED_TEAM_COLUMNS = {"abbreviation", "team_name"}
REQUIRED_DEFENSIVE_COLUMNS = {"abbreviation", "vs_PG_pts", "vs_C_pts"}
ALL_NBA_TEAMS = {
    "ATL","BOS","BKN","CHA","CHI","CLE","DAL","DEN","DET","GSW",
    "HOU","IND","LAC","LAL","MEM","MIA","MIL","MIN","NOP","NYK",
    "OKC","ORL","PHI","PHX","POR","SAC","SAS","TOR","UTA","WAS",
}
DATA_STALE_DAYS = 3
TEAM_STALE_DAYS = 7


def validate_players_csv(players_data):
    """
    Validate structure and completeness of players data.

    Args:
        players_data (list of dict): Loaded players data

    Returns:
        list of str: Validation errors (empty list = all good)
    """
    errors = []

    if not players_data:
        errors.append("players.csv is empty or missing")
        return errors

    # Check required columns
    first = players_data[0]
    available = set(first.keys())
    missing = REQUIRED_PLAYER_COLUMNS - available
    if missing:
        errors.append(f"players.csv missing required columns: {sorted(missing)}")

    # Check for null/empty critical fields
    null_count = 0
    for row in players_data:
        if not row.get("name", "").strip():
            null_count += 1
    if null_count > 0:
        errors.append(f"players.csv has {null_count} rows with empty 'name' field")

    return errors


def validate_teams_csv(teams_data):
    """
    Validate structure and completeness of teams data.

    Args:
        teams_data (list of dict): Loaded teams data

    Returns:
        list of str: Validation errors
    """
    errors = []

    if not teams_data:
        errors.append("teams.csv is empty or missing")
        return errors

    # Check required columns
    first = teams_data[0]
    available = set(first.keys())
    missing = REQUIRED_TEAM_COLUMNS - available
    if missing:
        errors.append(f"teams.csv missing required columns: {sorted(missing)}")

    # Check all 30 teams present
    found_abbrevs = {row.get("abbreviation", "").upper() for row in teams_data}
    missing_teams = ALL_NBA_TEAMS - found_abbrevs
    if missing_teams:
        errors.append(f"teams.csv missing {len(missing_teams)} teams: {sorted(missing_teams)}")

    # Check value ranges
    for row in teams_data:
        try:
            drtg = float(row.get("drtg", 0) or 0)
            if drtg > 0 and not (95.0 <= drtg <= 130.0):
                errors.append(
                    f"teams.csv: {row.get('abbreviation')} has suspicious drtg={drtg}"
                )
        except (ValueError, TypeError):
            pass

    return errors


def validate_defensive_ratings_csv(ratings_data):
    """
    Validate structure of defensive ratings data.

    Args:
        ratings_data (list of dict): Loaded defensive ratings data

    Returns:
        list of str: Validation errors
    """
    errors = []

    if not ratings_data:
        errors.append("defensive_ratings.csv is empty or missing — run team stats retrieval")
        return errors

    first = ratings_data[0]
    available = set(first.keys())
    missing = REQUIRED_DEFENSIVE_COLUMNS - available
    if missing:
        errors.append(f"defensive_ratings.csv missing columns: {sorted(missing)}")

    return errors


def check_data_freshness(last_updated_json_path):
    """
    Check if data is stale based on the last_updated.json timestamps.

    Args:
        last_updated_json_path (str): Path to last_updated.json

    Returns:
        dict: {
            'is_stale': bool,
            'days_old': int,
            'warning_message': str,
            'timestamps': dict,
        }
    """
    result = {
        "is_stale": False,
        "days_old": 0,
        "warning_message": "",
        "timestamps": {},
    }

    if not os.path.exists(last_updated_json_path):
        result["is_stale"] = True
        result["warning_message"] = "No data has been retrieved yet. Go to Data Feed to update."
        return result

    try:
        with open(last_updated_json_path, "r", encoding="utf-8") as f:
            timestamps = json.load(f)
        result["timestamps"] = timestamps

        # Find the most recent update
        now = datetime.datetime.now(datetime.timezone.utc)
        most_recent = None

        for key, ts_str in timestamps.items():
            try:
                # Handle both naive and aware datetimes
                ts = datetime.datetime.fromisoformat(ts_str)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=datetime.timezone.utc)
                if most_recent is None or ts > most_recent:
                    most_recent = ts
            except (ValueError, TypeError):
                pass

        if most_recent is None:
            result["is_stale"] = True
            result["warning_message"] = "Could not parse data timestamps."
            return result

        age = now - most_recent
        days_old = age.days
        result["days_old"] = days_old

        if days_old > DATA_STALE_DAYS:
            result["is_stale"] = True
            result["warning_message"] = (
                f"⚠️ Data is {days_old} days old. Go to Data Feed to update."
            )

    except Exception as e:
        result["is_stale"] = True
        result["warning_message"] = f"Could not read last_updated.json: {e}"

    return result


def check_data_integrity(players_data, teams_data):
    """
    Cross-reference players and teams to find orphaned players.

    Args:
        players_data (list of dict): Loaded players data
        teams_data (list of dict): Loaded teams data

    Returns:
        dict: {
            'orphaned_players': list of str,
            'errors': list of str,
            'warnings': list of str,
        }
    """
    result = {"orphaned_players": [], "errors": [], "warnings": []}

    if not teams_data:
        result["warnings"].append("teams.csv is empty — cannot validate player teams")
        return result

    # Build set of known team abbreviations
    known_teams = set()
    for row in teams_data:
        abbr = row.get("abbreviation", "").upper().strip()
        if abbr:
            known_teams.add(abbr)
        # Also add common variants
        name = row.get("team_name", "")
        if name:
            known_teams.add(name.upper())

    # Add common abbreviation variants for robustness
    variant_map = {"NJN": "BKN", "NOH": "NOP", "SEA": "OKC"}
    for k, v in variant_map.items():
        if v in known_teams:
            known_teams.add(k)

    orphaned = []
    for player in players_data:
        team = str(player.get("team", "")).upper().strip()
        if team and team not in known_teams:
            orphaned.append(f"{player.get('name', '?')} ({team})")

    if orphaned:
        result["orphaned_players"] = orphaned
        result["warnings"].append(
            f"{len(orphaned)} players reference unknown teams: "
            + ", ".join(orphaned[:5])
            + (" ..." if len(orphaned) > 5 else "")
        )

    return result


def validate_props(props, players):
    """
    Validate that every prop references a valid player name.

    Args:
        props (list of dict): Prop lines with 'player_name'
        players (list of dict): Player data with 'name'

    Returns:
        list of str: Validation warnings for unknown player names
    """
    if not props or not players:
        return []

    known_names = {p.get("name", "").lower().strip() for p in players if p.get("name")}
    warnings = []

    unknown = []
    for prop in props:
        pname = prop.get("player_name", "").lower().strip()
        if pname and pname not in known_names:
            unknown.append(prop.get("player_name", "?"))

    if unknown:
        unique_unknown = list(dict.fromkeys(unknown))[:10]
        warnings.append(
            f"{len(unknown)} props reference unknown players: "
            + ", ".join(unique_unknown)
        )

    return warnings
