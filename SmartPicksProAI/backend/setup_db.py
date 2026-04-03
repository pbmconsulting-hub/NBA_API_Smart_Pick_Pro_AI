"""
setup_db.py
-----------
Creates the smartpicks.db SQLite database and initialises all tables used by
the SmartPicksProAI data pipeline.

Run this script once before any other pipeline script:
    python setup_db.py

NOTE: The Player_Game_Logs table uses a composite PRIMARY KEY (player_id,
game_id) which replaces the old log_id autoincrement PK.  Because SQLite
does not support ALTER TABLE … ADD PRIMARY KEY, this composite PK only takes
effect on a fresh database.  Existing databases with the old log_id schema
must be re-initialised (delete smartpicks.db and re-run this script followed
by initial_pull.py).
"""

import logging
import sqlite3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DB_PATH = "smartpicks.db"

CREATE_PLAYERS = """
CREATE TABLE IF NOT EXISTS Players (
    player_id          INTEGER PRIMARY KEY,
    first_name         TEXT    NOT NULL,
    last_name          TEXT    NOT NULL,
    full_name          TEXT,
    team_id            INTEGER,
    team_abbreviation  TEXT,
    position           TEXT,
    is_active          INTEGER DEFAULT 1
);
"""

CREATE_TEAMS = """
CREATE TABLE IF NOT EXISTS Teams (
    team_id        INTEGER PRIMARY KEY,
    abbreviation   TEXT    NOT NULL,
    team_name      TEXT    NOT NULL,
    conference     TEXT,
    division       TEXT,
    pace           REAL,
    ortg           REAL,
    drtg           REAL
);
"""

CREATE_GAMES = """
CREATE TABLE IF NOT EXISTS Games (
    game_id        TEXT    PRIMARY KEY,
    game_date      TEXT    NOT NULL,
    season         TEXT,
    home_team_id   INTEGER,
    away_team_id   INTEGER,
    home_abbrev    TEXT,
    away_abbrev    TEXT,
    matchup        TEXT,
    home_score     INTEGER,
    away_score     INTEGER
);
"""

# NOTE: The composite PRIMARY KEY (player_id, game_id) only applies to fresh
# databases.  See module docstring for migration instructions.
CREATE_PLAYER_GAME_LOGS = """
CREATE TABLE IF NOT EXISTS Player_Game_Logs (
    player_id   INTEGER NOT NULL REFERENCES Players(player_id),
    game_id     TEXT    NOT NULL REFERENCES Games(game_id),
    wl          TEXT,
    min         TEXT,
    pts         INTEGER,
    reb         INTEGER,
    ast         INTEGER,
    stl         INTEGER,
    blk         INTEGER,
    tov         INTEGER,
    fgm         INTEGER,
    fga         INTEGER,
    fg_pct      REAL,
    fg3m        INTEGER,
    fg3a        INTEGER,
    fg3_pct     REAL,
    ftm         INTEGER,
    fta         INTEGER,
    ft_pct      REAL,
    oreb        INTEGER,
    dreb        INTEGER,
    pf          INTEGER,
    plus_minus  REAL,
    PRIMARY KEY (player_id, game_id)
);
"""

CREATE_TEAM_GAME_STATS = """
CREATE TABLE IF NOT EXISTS Team_Game_Stats (
    game_id          TEXT    NOT NULL REFERENCES Games(game_id),
    team_id          INTEGER NOT NULL REFERENCES Teams(team_id),
    opponent_team_id INTEGER,
    is_home          INTEGER,
    points_scored    INTEGER,
    points_allowed   INTEGER,
    pace_est         REAL,
    ortg_est         REAL,
    drtg_est         REAL,
    PRIMARY KEY (game_id, team_id)
);
"""

CREATE_DEFENSE_VS_POSITION = """
CREATE TABLE IF NOT EXISTS Defense_Vs_Position (
    team_abbreviation  TEXT    NOT NULL,
    season             TEXT    NOT NULL,
    pos                TEXT    NOT NULL,
    vs_pts_mult        REAL    DEFAULT 1.0,
    vs_reb_mult        REAL    DEFAULT 1.0,
    vs_ast_mult        REAL    DEFAULT 1.0,
    vs_stl_mult        REAL    DEFAULT 1.0,
    vs_blk_mult        REAL    DEFAULT 1.0,
    vs_3pm_mult        REAL    DEFAULT 1.0,
    PRIMARY KEY (team_abbreviation, season, pos)
);
"""

CREATE_TEAM_ROSTER = """
CREATE TABLE IF NOT EXISTS Team_Roster (
    team_id              INTEGER NOT NULL REFERENCES Teams(team_id),
    player_id            INTEGER NOT NULL REFERENCES Players(player_id),
    effective_start_date TEXT,
    effective_end_date   TEXT,
    is_two_way           INTEGER DEFAULT 0,
    is_g_league          INTEGER DEFAULT 0,
    PRIMARY KEY (team_id, player_id, effective_start_date)
);
"""

CREATE_INJURY_STATUS = """
CREATE TABLE IF NOT EXISTS Injury_Status (
    player_id       INTEGER NOT NULL REFERENCES Players(player_id),
    team_id         INTEGER,
    report_date     TEXT    NOT NULL,
    status          TEXT    NOT NULL,
    reason          TEXT,
    source          TEXT,
    last_updated_ts TEXT,
    PRIMARY KEY (player_id, report_date)
);
"""

# ---------------------------------------------------------------------------
# New tables for previously-unhandled NBA API data
# ---------------------------------------------------------------------------

CREATE_STANDINGS = """
CREATE TABLE IF NOT EXISTS Standings (
    season_id              TEXT    NOT NULL,
    team_id                INTEGER NOT NULL REFERENCES Teams(team_id),
    conference             TEXT,
    conference_record      TEXT,
    playoff_rank           INTEGER,
    clinch_indicator       TEXT,
    division               TEXT,
    division_record        TEXT,
    division_rank          INTEGER,
    wins                   INTEGER,
    losses                 INTEGER,
    win_pct                REAL,
    league_rank            INTEGER,
    record                 TEXT,
    home                   TEXT,
    road                   TEXT,
    l10                    TEXT,
    last10_home            TEXT,
    last10_road            TEXT,
    ot                     TEXT,
    three_pts_or_less      TEXT,
    ten_pts_or_more        TEXT,
    long_home_streak       INTEGER,
    long_road_streak       INTEGER,
    long_win_streak        INTEGER,
    long_loss_streak       INTEGER,
    current_home_streak    INTEGER,
    current_road_streak    INTEGER,
    current_streak         INTEGER,
    str_current_streak     TEXT,
    conference_games_back  REAL,
    division_games_back    REAL,
    clinched_conf_title    INTEGER,
    clinched_div_title     INTEGER,
    clinched_playoff       INTEGER,
    eliminated_conf        INTEGER,
    eliminated_div         INTEGER,
    ahead_at_half          TEXT,
    behind_at_half         TEXT,
    tied_at_half           TEXT,
    ahead_at_third         TEXT,
    behind_at_third        TEXT,
    tied_at_third          TEXT,
    score_100pts           TEXT,
    opp_score_100pts       TEXT,
    opp_over_500           TEXT,
    lead_in_fg_pct         TEXT,
    lead_in_reb            TEXT,
    fewer_turnovers        TEXT,
    points_pg              REAL,
    opp_points_pg          REAL,
    diff_points_pg         REAL,
    vs_east                TEXT,
    vs_atlantic            TEXT,
    vs_central             TEXT,
    vs_southeast           TEXT,
    vs_west                TEXT,
    vs_northwest           TEXT,
    vs_pacific             TEXT,
    vs_southwest           TEXT,
    PRIMARY KEY (season_id, team_id)
);
"""

CREATE_SCHEDULE = """
CREATE TABLE IF NOT EXISTS Schedule (
    game_id              TEXT    PRIMARY KEY,
    season_year          TEXT,
    game_date            TEXT    NOT NULL,
    game_code            TEXT,
    game_status          INTEGER,
    game_status_text     TEXT,
    game_sequence        INTEGER,
    game_date_est        TEXT,
    game_time_est        TEXT,
    game_datetime_est    TEXT,
    game_date_utc        TEXT,
    game_time_utc        TEXT,
    game_datetime_utc    TEXT,
    day                  TEXT,
    month_num            INTEGER,
    week_number          INTEGER,
    week_name            TEXT,
    if_necessary         INTEGER,
    series_game_number   TEXT,
    game_label           TEXT,
    game_sub_label       TEXT,
    series_text          TEXT,
    arena_name           TEXT,
    arena_state          TEXT,
    arena_city           TEXT,
    postponed_status     TEXT,
    game_subtype         TEXT,
    is_neutral           INTEGER,
    home_team_id         INTEGER REFERENCES Teams(team_id),
    home_team_name       TEXT,
    home_team_city       TEXT,
    home_team_tricode    TEXT,
    home_team_wins       INTEGER,
    home_team_losses     INTEGER,
    home_team_score      INTEGER,
    home_team_seed       INTEGER,
    away_team_id         INTEGER REFERENCES Teams(team_id),
    away_team_name       TEXT,
    away_team_city       TEXT,
    away_team_tricode    TEXT,
    away_team_wins       INTEGER,
    away_team_losses     INTEGER,
    away_team_score      INTEGER,
    away_team_seed       INTEGER
);
"""

CREATE_PLAY_BY_PLAY = """
CREATE TABLE IF NOT EXISTS Play_By_Play (
    game_id        TEXT    NOT NULL REFERENCES Games(game_id),
    action_number  INTEGER NOT NULL,
    season         TEXT,
    clock          TEXT,
    period         INTEGER,
    team_id        INTEGER REFERENCES Teams(team_id),
    team_tricode   TEXT,
    person_id      INTEGER REFERENCES Players(player_id),
    player_name    TEXT,
    player_name_i  TEXT,
    x_legacy       REAL,
    y_legacy       REAL,
    shot_distance  REAL,
    shot_result    TEXT,
    is_field_goal  INTEGER,
    score_home     TEXT,
    score_away     TEXT,
    points_total   INTEGER,
    location       TEXT,
    description    TEXT,
    action_type    TEXT,
    sub_type       TEXT,
    video_available INTEGER,
    action_id      INTEGER,
    PRIMARY KEY (game_id, action_number)
);
"""

CREATE_PLAYER_BIO = """
CREATE TABLE IF NOT EXISTS Player_Bio (
    player_id            INTEGER PRIMARY KEY REFERENCES Players(player_id),
    player_name          TEXT,
    team_id              INTEGER REFERENCES Teams(team_id),
    team_abbreviation    TEXT,
    age                  REAL,
    player_height        TEXT,
    player_height_inches REAL,
    player_weight        REAL,
    college              TEXT,
    country              TEXT,
    draft_year           TEXT,
    draft_round          TEXT,
    draft_number         TEXT,
    gp                   INTEGER,
    pts                  REAL,
    reb                  REAL,
    ast                  REAL,
    net_rating           REAL,
    oreb_pct             REAL,
    dreb_pct             REAL,
    usg_pct              REAL,
    ts_pct               REAL,
    ast_pct              REAL
);
"""

CREATE_SHOT_CHART = """
CREATE TABLE IF NOT EXISTS Shot_Chart (
    game_id            TEXT    NOT NULL REFERENCES Games(game_id),
    game_event_id      INTEGER NOT NULL,
    player_id          INTEGER NOT NULL REFERENCES Players(player_id),
    season             TEXT,
    player_name        TEXT,
    team_id            INTEGER REFERENCES Teams(team_id),
    team_name          TEXT,
    period             INTEGER,
    minutes_remaining  INTEGER,
    seconds_remaining  INTEGER,
    event_type         TEXT,
    action_type        TEXT,
    shot_type          TEXT,
    shot_zone_basic    TEXT,
    shot_zone_area     TEXT,
    shot_zone_range    TEXT,
    shot_distance      INTEGER,
    loc_x              REAL,
    loc_y              REAL,
    shot_attempted_flag INTEGER,
    shot_made_flag     INTEGER,
    game_date          TEXT,
    htm                TEXT,
    vtm                TEXT,
    PRIMARY KEY (game_id, game_event_id, player_id)
);
"""

CREATE_PLAYER_TRACKING = """
CREATE TABLE IF NOT EXISTS Player_Tracking_Stats (
    game_id                       TEXT    NOT NULL REFERENCES Games(game_id),
    person_id                     INTEGER NOT NULL REFERENCES Players(player_id),
    team_id                       INTEGER REFERENCES Teams(team_id),
    season                        TEXT,
    team_tricode                  TEXT,
    first_name                    TEXT,
    family_name                   TEXT,
    position                      TEXT,
    comment                       TEXT,
    jersey_num                    TEXT,
    minutes                       TEXT,
    speed                         REAL,
    distance                      REAL,
    rebound_chances_offensive     REAL,
    rebound_chances_defensive     REAL,
    rebound_chances_total         REAL,
    touches                       REAL,
    secondary_assists             REAL,
    free_throw_assists            REAL,
    passes                        REAL,
    assists                       REAL,
    contested_fg_made             REAL,
    contested_fg_attempted        REAL,
    contested_fg_pct              REAL,
    uncontested_fg_made           REAL,
    uncontested_fg_attempted      REAL,
    uncontested_fg_pct            REAL,
    fg_pct                        REAL,
    defended_at_rim_fg_made       REAL,
    defended_at_rim_fg_attempted  REAL,
    defended_at_rim_fg_pct        REAL,
    PRIMARY KEY (game_id, person_id)
);
"""

# ---------------------------------------------------------------------------
# Per-game advanced box score tables (from BoxScore*V3 / V2 endpoints)
# ---------------------------------------------------------------------------

CREATE_BOX_SCORE_ADVANCED = """
CREATE TABLE IF NOT EXISTS Box_Score_Advanced (
    game_id         TEXT    NOT NULL REFERENCES Games(game_id),
    person_id       INTEGER NOT NULL REFERENCES Players(player_id),
    team_id         INTEGER REFERENCES Teams(team_id),
    season          TEXT,
    position        TEXT,
    minutes         TEXT,
    est_off_rating  REAL,
    off_rating      REAL,
    est_def_rating  REAL,
    def_rating      REAL,
    est_net_rating  REAL,
    net_rating      REAL,
    ast_pct         REAL,
    ast_to_tov      REAL,
    ast_ratio       REAL,
    oreb_pct        REAL,
    dreb_pct        REAL,
    reb_pct         REAL,
    tov_ratio       REAL,
    efg_pct         REAL,
    ts_pct          REAL,
    usg_pct         REAL,
    est_usg_pct     REAL,
    est_pace        REAL,
    pace            REAL,
    pace_per40      REAL,
    possessions     REAL,
    pie             REAL,
    PRIMARY KEY (game_id, person_id)
);
"""

CREATE_BOX_SCORE_SCORING = """
CREATE TABLE IF NOT EXISTS Box_Score_Scoring (
    game_id              TEXT    NOT NULL REFERENCES Games(game_id),
    person_id            INTEGER NOT NULL REFERENCES Players(player_id),
    team_id              INTEGER REFERENCES Teams(team_id),
    season               TEXT,
    minutes              TEXT,
    pct_fga_2pt          REAL,
    pct_fga_3pt          REAL,
    pct_pts_2pt          REAL,
    pct_pts_mid2pt       REAL,
    pct_pts_3pt          REAL,
    pct_pts_fast_break   REAL,
    pct_pts_ft           REAL,
    pct_pts_off_tov      REAL,
    pct_pts_paint        REAL,
    pct_assisted_2pt     REAL,
    pct_unassisted_2pt   REAL,
    pct_assisted_3pt     REAL,
    pct_unassisted_3pt   REAL,
    pct_assisted_fgm     REAL,
    pct_unassisted_fgm   REAL,
    PRIMARY KEY (game_id, person_id)
);
"""

CREATE_BOX_SCORE_MISC = """
CREATE TABLE IF NOT EXISTS Box_Score_Misc (
    game_id              TEXT    NOT NULL REFERENCES Games(game_id),
    person_id            INTEGER NOT NULL REFERENCES Players(player_id),
    team_id              INTEGER REFERENCES Teams(team_id),
    season               TEXT,
    minutes              TEXT,
    pts_off_tov          INTEGER,
    pts_second_chance    INTEGER,
    pts_fast_break       INTEGER,
    pts_paint            INTEGER,
    opp_pts_off_tov      INTEGER,
    opp_pts_second_chance INTEGER,
    opp_pts_fast_break   INTEGER,
    opp_pts_paint        INTEGER,
    blocks               INTEGER,
    blocks_against       INTEGER,
    fouls_personal       INTEGER,
    fouls_drawn          INTEGER,
    PRIMARY KEY (game_id, person_id)
);
"""

CREATE_BOX_SCORE_HUSTLE = """
CREATE TABLE IF NOT EXISTS Box_Score_Hustle (
    game_id              TEXT    NOT NULL REFERENCES Games(game_id),
    person_id            INTEGER NOT NULL REFERENCES Players(player_id),
    team_id              INTEGER REFERENCES Teams(team_id),
    season               TEXT,
    minutes              TEXT,
    points               INTEGER,
    contested_shots      INTEGER,
    contested_shots_2pt  INTEGER,
    contested_shots_3pt  INTEGER,
    deflections          INTEGER,
    charges_drawn        INTEGER,
    screen_assists       INTEGER,
    screen_ast_pts       INTEGER,
    loose_balls_off      INTEGER,
    loose_balls_def      INTEGER,
    loose_balls_total    INTEGER,
    off_boxouts          INTEGER,
    def_boxouts          INTEGER,
    boxout_team_rebs     INTEGER,
    boxout_player_rebs   INTEGER,
    boxouts              INTEGER,
    PRIMARY KEY (game_id, person_id)
);
"""

CREATE_BOX_SCORE_FOUR_FACTORS = """
CREATE TABLE IF NOT EXISTS Box_Score_Four_Factors (
    game_id              TEXT    NOT NULL REFERENCES Games(game_id),
    person_id            INTEGER NOT NULL REFERENCES Players(player_id),
    team_id              INTEGER REFERENCES Teams(team_id),
    season               TEXT,
    minutes              TEXT,
    efg_pct              REAL,
    fta_rate             REAL,
    team_tov_pct         REAL,
    oreb_pct             REAL,
    opp_efg_pct          REAL,
    opp_fta_rate         REAL,
    opp_tov_pct          REAL,
    opp_oreb_pct         REAL,
    PRIMARY KEY (game_id, person_id)
);
"""

CREATE_BOX_SCORE_USAGE = """
CREATE TABLE IF NOT EXISTS Box_Score_Usage (
    game_id          TEXT    NOT NULL REFERENCES Games(game_id),
    person_id        INTEGER NOT NULL REFERENCES Players(player_id),
    team_id          INTEGER REFERENCES Teams(team_id),
    season           TEXT,
    minutes          TEXT,
    usg_pct          REAL,
    pct_fgm          REAL,
    pct_fga          REAL,
    pct_fg3m         REAL,
    pct_fg3a         REAL,
    pct_ftm          REAL,
    pct_fta          REAL,
    pct_oreb         REAL,
    pct_dreb         REAL,
    pct_reb          REAL,
    pct_ast          REAL,
    pct_tov          REAL,
    pct_stl          REAL,
    pct_blk          REAL,
    pct_blka         REAL,
    pct_pf           REAL,
    pct_pfd          REAL,
    pct_pts          REAL,
    PRIMARY KEY (game_id, person_id)
);
"""

CREATE_BOX_SCORE_MATCHUPS = """
CREATE TABLE IF NOT EXISTS Box_Score_Matchups (
    game_id                TEXT    NOT NULL REFERENCES Games(game_id),
    person_id_off          INTEGER NOT NULL REFERENCES Players(player_id),
    person_id_def          INTEGER NOT NULL REFERENCES Players(player_id),
    team_id                INTEGER REFERENCES Teams(team_id),
    season                 TEXT,
    matchup_min            REAL,
    matchup_min_sort       REAL,
    partial_poss           REAL,
    pct_def_total_time     REAL,
    pct_off_total_time     REAL,
    pct_total_time_both_on REAL,
    switches_on            INTEGER,
    player_pts             INTEGER,
    team_pts               INTEGER,
    matchup_ast            INTEGER,
    matchup_potential_ast  INTEGER,
    matchup_tov            INTEGER,
    matchup_blk            INTEGER,
    matchup_fgm            INTEGER,
    matchup_fga            INTEGER,
    matchup_fg_pct         REAL,
    matchup_fg3m           INTEGER,
    matchup_fg3a           INTEGER,
    matchup_fg3_pct        REAL,
    help_blk               INTEGER,
    help_fgm               INTEGER,
    help_fga               INTEGER,
    help_fg_pct            REAL,
    matchup_ftm            INTEGER,
    matchup_fta            INTEGER,
    shooting_fouls         INTEGER,
    PRIMARY KEY (game_id, person_id_off, person_id_def)
);
"""

# ---------------------------------------------------------------------------
# Season-level dashboard tables
# ---------------------------------------------------------------------------

CREATE_LEAGUE_DASH_PLAYER_STATS = """
CREATE TABLE IF NOT EXISTS League_Dash_Player_Stats (
    season             TEXT    NOT NULL,
    player_id          INTEGER NOT NULL REFERENCES Players(player_id),
    team_id            INTEGER REFERENCES Teams(team_id),
    team_abbreviation  TEXT,
    age                REAL,
    gp                 INTEGER,
    w                  INTEGER,
    l                  INTEGER,
    w_pct              REAL,
    min                REAL,
    fgm                REAL,
    fga                REAL,
    fg_pct             REAL,
    fg3m               REAL,
    fg3a               REAL,
    fg3_pct            REAL,
    ftm                REAL,
    fta                REAL,
    ft_pct             REAL,
    oreb               REAL,
    dreb               REAL,
    reb                REAL,
    ast                REAL,
    tov                REAL,
    stl                REAL,
    blk                REAL,
    blka               REAL,
    pf                 REAL,
    pfd                REAL,
    pts                REAL,
    plus_minus         REAL,
    nba_fantasy_pts    REAL,
    dd2                INTEGER,
    td3                INTEGER,
    PRIMARY KEY (season, player_id)
);
"""

CREATE_LEAGUE_DASH_TEAM_STATS = """
CREATE TABLE IF NOT EXISTS League_Dash_Team_Stats (
    season             TEXT    NOT NULL,
    team_id            INTEGER NOT NULL REFERENCES Teams(team_id),
    gp                 INTEGER,
    w                  INTEGER,
    l                  INTEGER,
    w_pct              REAL,
    min                REAL,
    fgm                REAL,
    fga                REAL,
    fg_pct             REAL,
    fg3m               REAL,
    fg3a               REAL,
    fg3_pct            REAL,
    ftm                REAL,
    fta                REAL,
    ft_pct             REAL,
    oreb               REAL,
    dreb               REAL,
    reb                REAL,
    ast                REAL,
    tov                REAL,
    stl                REAL,
    blk                REAL,
    blka               REAL,
    pf                 REAL,
    pfd                REAL,
    pts                REAL,
    plus_minus         REAL,
    PRIMARY KEY (season, team_id)
);
"""

CREATE_PLAYER_ESTIMATED_METRICS = """
CREATE TABLE IF NOT EXISTS Player_Estimated_Metrics (
    season          TEXT    NOT NULL,
    player_id       INTEGER NOT NULL REFERENCES Players(player_id),
    gp              INTEGER,
    w               INTEGER,
    l               INTEGER,
    w_pct           REAL,
    min             REAL,
    e_off_rating    REAL,
    e_def_rating    REAL,
    e_net_rating    REAL,
    e_ast_ratio     REAL,
    e_oreb_pct      REAL,
    e_dreb_pct      REAL,
    e_reb_pct       REAL,
    e_tov_pct       REAL,
    e_usg_pct       REAL,
    e_pace          REAL,
    PRIMARY KEY (season, player_id)
);
"""

CREATE_TEAM_ESTIMATED_METRICS = """
CREATE TABLE IF NOT EXISTS Team_Estimated_Metrics (
    season          TEXT    NOT NULL,
    team_id         INTEGER NOT NULL REFERENCES Teams(team_id),
    gp              INTEGER,
    w               INTEGER,
    l               INTEGER,
    w_pct           REAL,
    min             REAL,
    e_off_rating    REAL,
    e_def_rating    REAL,
    e_net_rating    REAL,
    e_pace          REAL,
    e_ast_ratio     REAL,
    e_oreb_pct      REAL,
    e_dreb_pct      REAL,
    e_reb_pct       REAL,
    e_tm_tov_pct    REAL,
    PRIMARY KEY (season, team_id)
);
"""

CREATE_PLAYER_CLUTCH_STATS = """
CREATE TABLE IF NOT EXISTS Player_Clutch_Stats (
    season             TEXT    NOT NULL,
    player_id          INTEGER NOT NULL REFERENCES Players(player_id),
    team_id            INTEGER REFERENCES Teams(team_id),
    team_abbreviation  TEXT,
    age                REAL,
    gp                 INTEGER,
    w                  INTEGER,
    l                  INTEGER,
    w_pct              REAL,
    min                REAL,
    fgm                REAL,
    fga                REAL,
    fg_pct             REAL,
    fg3m               REAL,
    fg3a               REAL,
    fg3_pct            REAL,
    ftm                REAL,
    fta                REAL,
    ft_pct             REAL,
    oreb               REAL,
    dreb               REAL,
    reb                REAL,
    ast                REAL,
    tov                REAL,
    stl                REAL,
    blk                REAL,
    blka               REAL,
    pf                 REAL,
    pfd                REAL,
    pts                REAL,
    plus_minus         REAL,
    nba_fantasy_pts    REAL,
    dd2                INTEGER,
    td3                INTEGER,
    PRIMARY KEY (season, player_id)
);
"""

CREATE_TEAM_CLUTCH_STATS = """
CREATE TABLE IF NOT EXISTS Team_Clutch_Stats (
    season         TEXT    NOT NULL,
    team_id        INTEGER NOT NULL REFERENCES Teams(team_id),
    gp             INTEGER,
    w              INTEGER,
    l              INTEGER,
    w_pct          REAL,
    min            REAL,
    fgm            REAL,
    fga            REAL,
    fg_pct         REAL,
    fg3m           REAL,
    fg3a           REAL,
    fg3_pct        REAL,
    ftm            REAL,
    fta            REAL,
    ft_pct         REAL,
    oreb           REAL,
    dreb           REAL,
    reb            REAL,
    ast            REAL,
    tov            REAL,
    stl            REAL,
    blk            REAL,
    blka           REAL,
    pf             REAL,
    pfd            REAL,
    pts            REAL,
    plus_minus     REAL,
    PRIMARY KEY (season, team_id)
);
"""

CREATE_PLAYER_HUSTLE_STATS = """
CREATE TABLE IF NOT EXISTS Player_Hustle_Stats (
    season                TEXT    NOT NULL,
    player_id             INTEGER NOT NULL REFERENCES Players(player_id),
    team_id               INTEGER REFERENCES Teams(team_id),
    team_abbreviation     TEXT,
    age                   REAL,
    gp                    INTEGER,
    min                   REAL,
    contested_shots       REAL,
    contested_shots_2pt   REAL,
    contested_shots_3pt   REAL,
    deflections           REAL,
    charges_drawn         REAL,
    screen_assists        REAL,
    screen_ast_pts        REAL,
    off_loose_balls       REAL,
    def_loose_balls       REAL,
    loose_balls           REAL,
    pct_loose_balls_off   REAL,
    pct_loose_balls_def   REAL,
    off_boxouts           REAL,
    def_boxouts           REAL,
    boxout_team_rebs      REAL,
    boxout_player_rebs    REAL,
    boxouts               REAL,
    pct_boxouts_off       REAL,
    pct_boxouts_def       REAL,
    pct_boxouts_team_reb  REAL,
    pct_boxouts_reb       REAL,
    PRIMARY KEY (season, player_id)
);
"""

CREATE_TEAM_HUSTLE_STATS = """
CREATE TABLE IF NOT EXISTS Team_Hustle_Stats (
    season                TEXT    NOT NULL,
    team_id               INTEGER NOT NULL REFERENCES Teams(team_id),
    min                   REAL,
    contested_shots       REAL,
    contested_shots_2pt   REAL,
    contested_shots_3pt   REAL,
    deflections           REAL,
    charges_drawn         REAL,
    screen_assists        REAL,
    screen_ast_pts        REAL,
    off_loose_balls       REAL,
    def_loose_balls       REAL,
    loose_balls           REAL,
    pct_loose_balls_off   REAL,
    pct_loose_balls_def   REAL,
    off_boxouts           REAL,
    def_boxouts           REAL,
    boxouts               REAL,
    pct_boxouts_off       REAL,
    pct_boxouts_def       REAL,
    PRIMARY KEY (season, team_id)
);
"""

# ---------------------------------------------------------------------------
# Context and historical tables
# ---------------------------------------------------------------------------

CREATE_GAME_ROTATION = """
CREATE TABLE IF NOT EXISTS Game_Rotation (
    game_id       TEXT    NOT NULL REFERENCES Games(game_id),
    team_id       INTEGER NOT NULL REFERENCES Teams(team_id),
    person_id     INTEGER NOT NULL REFERENCES Players(player_id),
    in_time_real  REAL    NOT NULL,
    out_time_real REAL,
    player_pts    INTEGER,
    pt_diff       INTEGER,
    usg_pct       REAL,
    PRIMARY KEY (game_id, person_id, in_time_real)
);
"""

CREATE_DRAFT_HISTORY = """
CREATE TABLE IF NOT EXISTS Draft_History (
    person_id         INTEGER NOT NULL,
    season            TEXT    NOT NULL,
    round_number      INTEGER,
    round_pick        INTEGER,
    overall_pick      INTEGER,
    draft_type        TEXT,
    team_id           INTEGER REFERENCES Teams(team_id),
    team_abbreviation TEXT,
    organization      TEXT,
    organization_type TEXT,
    PRIMARY KEY (person_id, season)
);
"""


CREATE_SYNERGY_PLAY_TYPES = """
CREATE TABLE IF NOT EXISTS Synergy_Play_Types (
    season_id          TEXT    NOT NULL,
    team_id            INTEGER NOT NULL REFERENCES Teams(team_id),
    team_abbreviation  TEXT,
    play_type          TEXT    NOT NULL,
    type_grouping      TEXT    NOT NULL,
    percentile         REAL,
    gp                 INTEGER,
    poss_pct           REAL,
    ppp                REAL,
    fg_pct             REAL,
    ft_poss_pct        REAL,
    tov_poss_pct       REAL,
    sf_poss_pct        REAL,
    plus_one_poss_pct  REAL,
    score_poss_pct     REAL,
    efg_pct            REAL,
    poss               INTEGER,
    pts                INTEGER,
    fgm                INTEGER,
    fga                INTEGER,
    fgmx               INTEGER,
    PRIMARY KEY (season_id, team_id, play_type, type_grouping)
);
"""

CREATE_LEAGUE_LINEUPS = """
CREATE TABLE IF NOT EXISTS League_Lineups (
    season             TEXT    NOT NULL,
    group_id           TEXT    NOT NULL,
    group_name         TEXT,
    team_id            INTEGER REFERENCES Teams(team_id),
    team_abbreviation  TEXT,
    gp                 INTEGER,
    w                  INTEGER,
    l                  INTEGER,
    w_pct              REAL,
    min                REAL,
    fgm                REAL,
    fga                REAL,
    fg_pct             REAL,
    fg3m               REAL,
    fg3a               REAL,
    fg3_pct            REAL,
    ftm                REAL,
    fta                REAL,
    ft_pct             REAL,
    oreb               REAL,
    dreb               REAL,
    reb                REAL,
    ast                REAL,
    tov                REAL,
    stl                REAL,
    blk                REAL,
    blka               REAL,
    pf                 REAL,
    pfd                REAL,
    pts                REAL,
    plus_minus         REAL,
    PRIMARY KEY (season, group_id)
);
"""

CREATE_LEAGUE_LEADERS = """
CREATE TABLE IF NOT EXISTS League_Leaders (
    season      TEXT    NOT NULL,
    player_id   INTEGER NOT NULL REFERENCES Players(player_id),
    rank        INTEGER,
    team        TEXT,
    gp          INTEGER,
    min         REAL,
    fgm         REAL,
    fga         REAL,
    fg_pct      REAL,
    fg3m        REAL,
    fg3a        REAL,
    fg3_pct     REAL,
    ftm         REAL,
    fta         REAL,
    ft_pct      REAL,
    oreb        REAL,
    dreb        REAL,
    reb         REAL,
    ast         REAL,
    stl         REAL,
    blk         REAL,
    tov         REAL,
    pf          REAL,
    pts         REAL,
    eff         REAL,
    ast_tov     REAL,
    stl_tov     REAL,
    PRIMARY KEY (season, player_id)
);
"""

CREATE_COMMON_PLAYER_INFO = """
CREATE TABLE IF NOT EXISTS Common_Player_Info (
    person_id           INTEGER PRIMARY KEY REFERENCES Players(player_id),
    first_name          TEXT,
    last_name           TEXT,
    display_first_last  TEXT,
    player_slug         TEXT,
    birthdate           TEXT,
    school              TEXT,
    country             TEXT,
    last_affiliation    TEXT,
    height              TEXT,
    weight              TEXT,
    season_exp          INTEGER,
    jersey              TEXT,
    position            TEXT,
    roster_status       TEXT,
    team_id             INTEGER REFERENCES Teams(team_id),
    team_name           TEXT,
    team_abbreviation   TEXT,
    from_year           INTEGER,
    to_year             INTEGER,
    dleague_flag        TEXT,
    nba_flag            TEXT,
    draft_year          TEXT,
    draft_round         TEXT,
    draft_number        TEXT
);
"""

CREATE_PLAYER_CAREER_STATS = """
CREATE TABLE IF NOT EXISTS Player_Career_Stats (
    player_id          INTEGER NOT NULL REFERENCES Players(player_id),
    season_id          TEXT    NOT NULL,
    team_id            INTEGER NOT NULL REFERENCES Teams(team_id),
    team_abbreviation  TEXT,
    player_age         REAL,
    gp                 INTEGER,
    gs                 INTEGER,
    min                REAL,
    fgm                REAL,
    fga                REAL,
    fg_pct             REAL,
    fg3m               REAL,
    fg3a               REAL,
    fg3_pct            REAL,
    ftm                REAL,
    fta                REAL,
    ft_pct             REAL,
    oreb               REAL,
    dreb               REAL,
    reb                REAL,
    ast                REAL,
    stl                REAL,
    blk                REAL,
    tov                REAL,
    pf                 REAL,
    pts                REAL,
    PRIMARY KEY (player_id, season_id, team_id)
);
"""

CREATE_TEAM_DETAILS = """
CREATE TABLE IF NOT EXISTS Team_Details (
    team_id              INTEGER PRIMARY KEY REFERENCES Teams(team_id),
    abbreviation         TEXT,
    nickname             TEXT,
    year_founded         INTEGER,
    city                 TEXT,
    arena                TEXT,
    arena_capacity       INTEGER,
    owner                TEXT,
    general_manager      TEXT,
    head_coach           TEXT,
    d_league_affiliation TEXT
);
"""

CREATE_WIN_PROBABILITY_PBP = """
CREATE TABLE IF NOT EXISTS Win_Probability_PBP (
    game_id              TEXT    NOT NULL REFERENCES Games(game_id),
    event_num            INTEGER NOT NULL,
    home_pct             REAL,
    visitor_pct          REAL,
    home_pts             INTEGER,
    visitor_pts          INTEGER,
    home_score_margin    INTEGER,
    period               INTEGER,
    seconds_remaining    REAL,
    home_poss_ind        INTEGER,
    description          TEXT,
    location             TEXT,
    pctimestring         TEXT,
    PRIMARY KEY (game_id, event_num)
);
"""

# New columns added to Players for existing databases.
_PLAYERS_ALTER = [
    "ALTER TABLE Players ADD COLUMN full_name TEXT",
    "ALTER TABLE Players ADD COLUMN team_abbreviation TEXT",
    "ALTER TABLE Players ADD COLUMN position TEXT",
    "ALTER TABLE Players ADD COLUMN is_active INTEGER DEFAULT 1",
]

# New columns added to Games for existing databases.
_GAMES_ALTER = [
    "ALTER TABLE Games ADD COLUMN season TEXT",
    "ALTER TABLE Games ADD COLUMN home_team_id INTEGER",
    "ALTER TABLE Games ADD COLUMN away_team_id INTEGER",
    "ALTER TABLE Games ADD COLUMN home_abbrev TEXT",
    "ALTER TABLE Games ADD COLUMN away_abbrev TEXT",
    "ALTER TABLE Games ADD COLUMN home_score INTEGER",
    "ALTER TABLE Games ADD COLUMN away_score INTEGER",
]

# New stat columns added to Player_Game_Logs for existing databases.
_LOGS_ALTER = [
    "ALTER TABLE Player_Game_Logs ADD COLUMN wl TEXT",
    "ALTER TABLE Player_Game_Logs ADD COLUMN fgm INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN fga INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN fg_pct REAL",
    "ALTER TABLE Player_Game_Logs ADD COLUMN fg3m INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN fg3a INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN fg3_pct REAL",
    "ALTER TABLE Player_Game_Logs ADD COLUMN ftm INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN fta INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN ft_pct REAL",
    "ALTER TABLE Player_Game_Logs ADD COLUMN oreb INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN dreb INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN pf INTEGER",
    "ALTER TABLE Player_Game_Logs ADD COLUMN plus_minus REAL",
]

# Season columns added to recently-created tables for AI query convenience.
_NEW_TABLE_ALTER = [
    "ALTER TABLE Play_By_Play ADD COLUMN season TEXT",
    "ALTER TABLE Shot_Chart ADD COLUMN season TEXT",
    "ALTER TABLE Player_Tracking_Stats ADD COLUMN season TEXT",
]


# ---------------------------------------------------------------------------
# SQL Views for AI convenience (pre-join common query patterns)
# ---------------------------------------------------------------------------

_VIEWS = {
    "v_player_game_full": """
        CREATE VIEW IF NOT EXISTS v_player_game_full AS
        SELECT
            pgl.*,
            p.full_name, p.position AS player_position,
            p.team_abbreviation AS current_team,
            g.game_date, g.season, g.home_team_id, g.away_team_id,
            g.home_abbrev, g.away_abbrev, g.matchup,
            g.home_score, g.away_score,
            bsa.off_rating, bsa.def_rating, bsa.net_rating,
            bsa.ts_pct, bsa.usg_pct, bsa.pace, bsa.pie, bsa.efg_pct
        FROM Player_Game_Logs pgl
        JOIN Players p ON pgl.player_id = p.player_id
        JOIN Games g ON pgl.game_id = g.game_id
        LEFT JOIN Box_Score_Advanced bsa
            ON pgl.player_id = bsa.person_id AND pgl.game_id = bsa.game_id
    """,
    "v_player_season_profile": """
        CREATE VIEW IF NOT EXISTS v_player_season_profile AS
        SELECT
            ldps.*,
            p.full_name, p.position,
            pb.player_height_inches, pb.player_weight, pb.college, pb.country,
            pb.net_rating AS bio_net_rating, pb.ts_pct AS bio_ts_pct,
            pb.usg_pct AS bio_usg_pct,
            pem.e_off_rating, pem.e_def_rating, pem.e_net_rating,
            pem.e_usg_pct, pem.e_pace
        FROM League_Dash_Player_Stats ldps
        JOIN Players p ON ldps.player_id = p.player_id
        LEFT JOIN Player_Bio pb ON ldps.player_id = pb.player_id
        LEFT JOIN Player_Estimated_Metrics pem
            ON ldps.season = pem.season AND ldps.player_id = pem.player_id
    """,
    "v_team_season_profile": """
        CREATE VIEW IF NOT EXISTS v_team_season_profile AS
        SELECT
            ldts.*,
            t.abbreviation, t.team_name, t.conference, t.division,
            t.pace AS team_pace, t.ortg AS team_ortg, t.drtg AS team_drtg,
            tem.e_off_rating, tem.e_def_rating, tem.e_net_rating, tem.e_pace,
            s.wins AS standings_wins, s.losses AS standings_losses,
            s.win_pct AS standings_win_pct, s.playoff_rank,
            s.str_current_streak, s.l10, s.home, s.road,
            s.conference_games_back, s.points_pg, s.opp_points_pg,
            s.diff_points_pg
        FROM League_Dash_Team_Stats ldts
        JOIN Teams t ON ldts.team_id = t.team_id
        LEFT JOIN Team_Estimated_Metrics tem
            ON ldts.season = tem.season AND ldts.team_id = tem.team_id
        LEFT JOIN Standings s ON ldts.season = s.season_id AND ldts.team_id = s.team_id
    """,
    "v_upcoming_matchups": """
        CREATE VIEW IF NOT EXISTS v_upcoming_matchups AS
        SELECT
            sch.game_id, sch.game_date, sch.game_status, sch.game_status_text,
            sch.game_datetime_utc, sch.arena_name, sch.arena_city,
            sch.home_team_id, sch.home_team_tricode,
            sch.home_team_score, sch.home_team_wins, sch.home_team_losses,
            sch.away_team_id, sch.away_team_tricode,
            sch.away_team_score, sch.away_team_wins, sch.away_team_losses,
            ht.pace AS home_pace, ht.ortg AS home_ortg, ht.drtg AS home_drtg,
            at.pace AS away_pace, at.ortg AS away_ortg, at.drtg AS away_drtg
        FROM Schedule sch
        LEFT JOIN Teams ht ON sch.home_team_id = ht.team_id
        LEFT JOIN Teams at ON sch.away_team_id = at.team_id
    """,
    "v_defense_matchup_context": """
        CREATE VIEW IF NOT EXISTS v_defense_matchup_context AS
        SELECT
            dvp.*,
            t.team_name, t.conference, t.division,
            t.pace AS team_pace, t.drtg AS team_drtg
        FROM Defense_Vs_Position dvp
        LEFT JOIN Teams t ON dvp.team_abbreviation = t.abbreviation
    """,
}

# ------------------------------------------------------------------
# Index definitions — (index_name, table, columns)
# ------------------------------------------------------------------
_INDEXES = (
    ("idx_pgl_player_date", "Player_Game_Logs", "(player_id, game_id)"),
    ("idx_pgl_game", "Player_Game_Logs", "(game_id)"),
    ("idx_tgs_team", "Team_Game_Stats", "(team_id)"),
    ("idx_games_date", "Games", "(game_date)"),
    ("idx_games_season", "Games", "(season)"),
    ("idx_players_fullname", "Players", "(full_name)"),
    ("idx_players_team", "Players", "(team_id)"),
    ("idx_standings_conf", "Standings", "(conference)"),
    ("idx_schedule_date", "Schedule", "(game_date)"),
    ("idx_schedule_home", "Schedule", "(home_team_id)"),
    ("idx_schedule_away", "Schedule", "(away_team_id)"),
    ("idx_pbp_game", "Play_By_Play", "(game_id)"),
    ("idx_pbp_person", "Play_By_Play", "(person_id)"),
    ("idx_shotchart_player", "Shot_Chart", "(player_id)"),
    ("idx_shotchart_game", "Shot_Chart", "(game_id)"),
    ("idx_shotchart_season", "Shot_Chart", "(season)"),
    ("idx_tracking_game", "Player_Tracking_Stats", "(game_id)"),
    ("idx_tracking_person", "Player_Tracking_Stats", "(person_id)"),
    ("idx_bsa_season", "Box_Score_Advanced", "(season)"),
    ("idx_bsa_person", "Box_Score_Advanced", "(person_id)"),
    ("idx_bss_person", "Box_Score_Scoring", "(person_id)"),
    ("idx_bsm_person", "Box_Score_Misc", "(person_id)"),
    ("idx_bsh_person", "Box_Score_Hustle", "(person_id)"),
    ("idx_bsff_person", "Box_Score_Four_Factors", "(person_id)"),
    ("idx_bsu_person", "Box_Score_Usage", "(person_id)"),
    ("idx_bsmatch_off", "Box_Score_Matchups", "(person_id_off)"),
    ("idx_bsmatch_def", "Box_Score_Matchups", "(person_id_def)"),
    ("idx_ldps_player", "League_Dash_Player_Stats", "(player_id)"),
    ("idx_ldts_team", "League_Dash_Team_Stats", "(team_id)"),
    ("idx_pem_player", "Player_Estimated_Metrics", "(player_id)"),
    ("idx_tem_team", "Team_Estimated_Metrics", "(team_id)"),
    ("idx_pcs_player", "Player_Clutch_Stats", "(player_id)"),
    ("idx_tcs_team", "Team_Clutch_Stats", "(team_id)"),
    ("idx_phs_player", "Player_Hustle_Stats", "(player_id)"),
    ("idx_ths_team", "Team_Hustle_Stats", "(team_id)"),
    ("idx_rotation_game", "Game_Rotation", "(game_id)"),
    ("idx_rotation_person", "Game_Rotation", "(person_id)"),
    ("idx_synergy_team", "Synergy_Play_Types", "(team_id)"),
    ("idx_lineups_team", "League_Lineups", "(team_id)"),
    ("idx_leaders_player", "League_Leaders", "(player_id)"),
    ("idx_career_player", "Player_Career_Stats", "(player_id)"),
    ("idx_winprob_game", "Win_Probability_PBP", "(game_id)"),
)


def create_tables(db_path: str = DB_PATH) -> None:
    """Create all SmartPicksProAI tables in *db_path*.

    Creates the complete schema for 39 tables spanning core data, per-game
    advanced box scores, season dashboards, and context/historical data.
    Uses IF NOT EXISTS clauses so it is safe to call multiple times.

    Args:
        db_path: Path to the SQLite database file.  Created automatically if
                 it does not already exist.
    """
    logger.info("Connecting to database: %s", db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        cursor = conn.cursor()

        # ---- Core tables ----
        logger.info("Creating core tables …")
        cursor.execute(CREATE_PLAYERS)
        cursor.execute(CREATE_TEAMS)
        cursor.execute(CREATE_GAMES)
        cursor.execute(CREATE_PLAYER_GAME_LOGS)
        cursor.execute(CREATE_TEAM_GAME_STATS)
        cursor.execute(CREATE_DEFENSE_VS_POSITION)
        cursor.execute(CREATE_TEAM_ROSTER)
        cursor.execute(CREATE_INJURY_STATUS)

        # ---- API data tables (added in previous iteration) ----
        logger.info("Creating API data tables …")
        cursor.execute(CREATE_STANDINGS)
        cursor.execute(CREATE_SCHEDULE)
        cursor.execute(CREATE_PLAY_BY_PLAY)
        cursor.execute(CREATE_PLAYER_BIO)
        cursor.execute(CREATE_SHOT_CHART)
        cursor.execute(CREATE_PLAYER_TRACKING)

        # ---- Per-game advanced box score tables ----
        logger.info("Creating per-game advanced box score tables …")
        cursor.execute(CREATE_BOX_SCORE_ADVANCED)
        cursor.execute(CREATE_BOX_SCORE_SCORING)
        cursor.execute(CREATE_BOX_SCORE_MISC)
        cursor.execute(CREATE_BOX_SCORE_HUSTLE)
        cursor.execute(CREATE_BOX_SCORE_FOUR_FACTORS)
        cursor.execute(CREATE_BOX_SCORE_USAGE)
        cursor.execute(CREATE_BOX_SCORE_MATCHUPS)

        # ---- Season-level dashboard tables ----
        logger.info("Creating season dashboard tables …")
        cursor.execute(CREATE_LEAGUE_DASH_PLAYER_STATS)
        cursor.execute(CREATE_LEAGUE_DASH_TEAM_STATS)
        cursor.execute(CREATE_PLAYER_ESTIMATED_METRICS)
        cursor.execute(CREATE_TEAM_ESTIMATED_METRICS)
        cursor.execute(CREATE_PLAYER_CLUTCH_STATS)
        cursor.execute(CREATE_TEAM_CLUTCH_STATS)
        cursor.execute(CREATE_PLAYER_HUSTLE_STATS)
        cursor.execute(CREATE_TEAM_HUSTLE_STATS)

        # ---- Context and historical tables ----
        logger.info("Creating context/historical tables …")
        cursor.execute(CREATE_GAME_ROTATION)
        cursor.execute(CREATE_DRAFT_HISTORY)
        cursor.execute(CREATE_SYNERGY_PLAY_TYPES)
        cursor.execute(CREATE_LEAGUE_LINEUPS)
        cursor.execute(CREATE_LEAGUE_LEADERS)
        cursor.execute(CREATE_COMMON_PLAYER_INFO)
        cursor.execute(CREATE_PLAYER_CAREER_STATS)
        cursor.execute(CREATE_TEAM_DETAILS)
        cursor.execute(CREATE_WIN_PROBABILITY_PBP)

        # ==================================================================
        # Indexes — driven by the _INDEXES tuple for easy maintenance
        # ==================================================================
        logger.info("Creating indexes …")
        for idx_name, table, columns in _INDEXES:
            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} {columns}"
            )

        # ==================================================================
        # ALTER TABLE migrations for existing databases
        # ==================================================================
        for stmt in (_PLAYERS_ALTER + _GAMES_ALTER + _LOGS_ALTER
                     + _NEW_TABLE_ALTER):
            try:
                cursor.execute(stmt)
            except sqlite3.OperationalError as exc:
                error_msg = str(exc).lower()
                if "duplicate" in error_msg or "already exists" in error_msg:
                    pass  # Column already exists — safe to ignore.
                else:
                    logger.warning("Unexpected ALTER TABLE error: %s", exc)

        # ==================================================================
        # SQL Views for AI convenience
        # ==================================================================
        logger.info("Creating views …")
        for view_name, view_sql in _VIEWS.items():
            try:
                cursor.execute(view_sql)
            except sqlite3.OperationalError as exc:
                if "already exists" in str(exc).lower():
                    logger.debug("View %s already exists.", view_name)
                else:
                    logger.warning("Failed to create view %s: %s", view_name, exc)

        conn.commit()
        logger.info("All tables, indexes, and views created successfully.")
    finally:
        conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    create_tables()
