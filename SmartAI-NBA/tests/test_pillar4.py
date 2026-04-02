# ============================================================
# FILE: tests/test_pillar4.py
# PURPOSE: Unit tests for the Pillar 4 — Live AI Panic Room
#          modules: agent/payload_builder.py,
#          agent/live_persona.py (LIVE_JOSEPH_PROMPT),
#          agent/response_parser.py
# ============================================================

import unittest
import json
import sys
import os

# Ensure the project root is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ============================================================
# SECTION 1: agent/payload_builder.py tests
# ============================================================

class TestGrudgeBuffer(unittest.TestCase):
    """Tests for GrudgeBuffer — short-term memory."""

    def setUp(self):
        from agent.payload_builder import GrudgeBuffer
        self.GrudgeBuffer = GrudgeBuffer

    def test_create_default(self):
        buf = self.GrudgeBuffer()
        self.assertEqual(len(buf), 0)

    def test_add_and_get(self):
        buf = self.GrudgeBuffer(maxlen=3)
        buf.add("rant_1")
        buf.add("rant_2")
        self.assertEqual(buf.get_history(), ["rant_1", "rant_2"])

    def test_eviction(self):
        """Buffer should evict oldest when full."""
        buf = self.GrudgeBuffer(maxlen=2)
        buf.add("a")
        buf.add("b")
        buf.add("c")
        self.assertEqual(buf.get_history(), ["b", "c"])
        self.assertEqual(len(buf), 2)

    def test_clear(self):
        buf = self.GrudgeBuffer()
        buf.add("x")
        buf.clear()
        self.assertEqual(len(buf), 0)
        self.assertEqual(buf.get_history(), [])

    def test_empty_string_not_added(self):
        buf = self.GrudgeBuffer()
        buf.add("")
        buf.add(None)
        self.assertEqual(len(buf), 0)

    def test_strips_whitespace(self):
        buf = self.GrudgeBuffer()
        buf.add("  hello  ")
        self.assertEqual(buf.get_history(), ["hello"])

    def test_maxlen_minimum_one(self):
        buf = self.GrudgeBuffer(maxlen=0)
        buf.add("x")
        self.assertEqual(len(buf), 1)


class TestClassifyGameState(unittest.TestCase):
    """Tests for classify_game_state()."""

    def setUp(self):
        from agent.payload_builder import (
            classify_game_state,
            GAME_STATE_HOOK, GAME_STATE_FREE_THROW,
            GAME_STATE_BENCH_SWEAT, GAME_STATE_USAGE_FREEZE,
            GAME_STATE_GARBAGE_MIRACLE, GAME_STATE_LOCKER_ROOM,
            GAME_STATE_REF_SHOW, GAME_STATE_CLEAN_CASH,
            ALL_GAME_STATES,
        )
        self.classify = classify_game_state
        self.HOOK = GAME_STATE_HOOK
        self.FREE_THROW = GAME_STATE_FREE_THROW
        self.BENCH = GAME_STATE_BENCH_SWEAT
        self.USAGE = GAME_STATE_USAGE_FREEZE
        self.GARBAGE = GAME_STATE_GARBAGE_MIRACLE
        self.LOCKER = GAME_STATE_LOCKER_ROOM
        self.REF = GAME_STATE_REF_SHOW
        self.CASH = GAME_STATE_CLEAN_CASH
        self.ALL = ALL_GAME_STATES

    def test_clean_cash(self):
        pace = {"cashed": True, "distance": 0, "blowout_risk": False,
                "foul_trouble": False, "minutes_played": 30, "pct_of_target": 110}
        self.assertEqual(self.classify(pace), self.CASH)

    def test_locker_room_injury(self):
        pace = {"cashed": False, "distance": 5, "blowout_risk": False,
                "foul_trouble": False, "minutes_played": 20, "pct_of_target": 60}
        self.assertEqual(self.classify(pace, injury_status="Questionable"), self.LOCKER)

    def test_locker_room_out(self):
        pace = {"cashed": False, "distance": 5, "blowout_risk": False,
                "foul_trouble": False, "minutes_played": 20, "pct_of_target": 60}
        self.assertEqual(self.classify(pace, injury_status="Out"), self.LOCKER)

    def test_ref_show_foul_trouble(self):
        pace = {"cashed": False, "distance": 10, "blowout_risk": False,
                "foul_trouble": True, "minutes_played": 15, "pct_of_target": 40}
        self.assertEqual(self.classify(pace), self.REF)

    def test_garbage_time(self):
        pace = {"cashed": False, "distance": 10, "blowout_risk": True,
                "foul_trouble": False, "minutes_played": 30, "pct_of_target": 50}
        self.assertEqual(self.classify(pace, score_diff=30), self.GARBAGE)

    def test_bench_sweat(self):
        pace = {"cashed": False, "distance": 10, "blowout_risk": True,
                "foul_trouble": False, "minutes_played": 30, "pct_of_target": 50}
        self.assertEqual(self.classify(pace, score_diff=22), self.BENCH)

    def test_free_throw_merchant(self):
        pace = {"cashed": False, "distance": 5, "blowout_risk": False,
                "foul_trouble": False, "minutes_played": 25, "pct_of_target": 70}
        result = self.classify(
            pace,
            shooting_line="4/20 FG",
            free_throw_line="8/10 FTA",
        )
        self.assertEqual(result, self.FREE_THROW)

    def test_usage_freeze(self):
        pace = {"cashed": False, "distance": 15, "blowout_risk": False,
                "foul_trouble": False, "minutes_played": 5, "pct_of_target": 20}
        self.assertEqual(self.classify(pace), self.USAGE)

    def test_hook_close_distance(self):
        pace = {"cashed": False, "distance": 0.5, "blowout_risk": False,
                "foul_trouble": False, "minutes_played": 30, "pct_of_target": 95}
        self.assertEqual(self.classify(pace), self.HOOK)

    def test_default_fallback(self):
        pace = {"cashed": False, "distance": 10, "blowout_risk": False,
                "foul_trouble": False, "minutes_played": 20, "pct_of_target": 60}
        self.assertEqual(self.classify(pace), self.HOOK)

    def test_invalid_input(self):
        self.assertEqual(self.classify(None), self.HOOK)

    def test_all_states_are_strings(self):
        for state in self.ALL:
            self.assertIsInstance(state, str)
            self.assertTrue(state.isupper())


class TestParseFgPct(unittest.TestCase):
    """Tests for _parse_fg_pct helper."""

    def setUp(self):
        from agent.payload_builder import _parse_fg_pct
        self.parse = _parse_fg_pct

    def test_normal(self):
        self.assertAlmostEqual(self.parse("6/21 FG"), 28.57, places=1)

    def test_perfect(self):
        self.assertAlmostEqual(self.parse("10/10 FG"), 100.0, places=1)

    def test_zero_attempts(self):
        self.assertEqual(self.parse("0/0 FG"), 0.0)

    def test_empty(self):
        self.assertIsNone(self.parse(""))

    def test_garbage(self):
        self.assertIsNone(self.parse("not a stat"))


class TestParseFtAttempts(unittest.TestCase):
    """Tests for _parse_ft_attempts helper."""

    def setUp(self):
        from agent.payload_builder import _parse_ft_attempts
        self.parse = _parse_ft_attempts

    def test_normal(self):
        self.assertEqual(self.parse("10/12 FTA"), 12)

    def test_zero(self):
        self.assertEqual(self.parse("0/0 FTA"), 0)

    def test_empty(self):
        self.assertEqual(self.parse(""), 0)


class TestBuildLiveVibePayload(unittest.TestCase):
    """Tests for build_live_vibe_payload()."""

    def setUp(self):
        from agent.payload_builder import build_live_vibe_payload, GrudgeBuffer
        self.build = build_live_vibe_payload
        self.GrudgeBuffer = GrudgeBuffer

    def test_basic_payload_structure(self):
        ticket = {
            "player_name": "LeBron James",
            "stat_type": "points",
            "line": 24.5,
            "direction": "OVER",
        }
        payload = self.build(ticket)

        self.assertEqual(payload["player_name"], "LeBron James")
        self.assertEqual(payload["ticket_type"], "OVER")
        self.assertEqual(payload["stat"], "Points")
        self.assertEqual(payload["line"], 24.5)
        self.assertIn("recent_rants_history", payload)
        self.assertIsInstance(payload["recent_rants_history"], list)
        self.assertIn("game_state", payload)

    def test_with_live_stats(self):
        ticket = {"player_name": "Test", "stat_type": "points",
                  "line": 25, "direction": "OVER"}
        live = {"pts": 24, "minutes": 30, "fouls": 2}
        payload = self.build(ticket, live_stats=live)
        self.assertEqual(payload["current"], 24)
        self.assertEqual(payload["needed"], 1.0)

    def test_with_game_context(self):
        ticket = {"player_name": "Test", "stat_type": "assists",
                  "line": 8, "direction": "UNDER"}
        game = {
            "home_score": 105, "away_score": 98,
            "period": "4", "game_clock": "2:30",
            "away_team": "Celtics",
        }
        payload = self.build(ticket, game_context=game)
        self.assertEqual(payload["ticket_type"], "UNDER")
        self.assertEqual(payload["opponent"], "Celtics")
        self.assertIn("4", payload["clock"])
        self.assertIn("2:30", payload["clock"])

    def test_grudge_buffer_integration(self):
        buf = self.GrudgeBuffer(maxlen=3)
        buf.add("old rant 1")
        buf.add("old rant 2")
        ticket = {"player_name": "X", "stat_type": "points",
                  "line": 20, "direction": "OVER"}
        payload = self.build(ticket, grudge_buffer=buf)
        self.assertEqual(payload["recent_rants_history"],
                         ["old rant 1", "old rant 2"])

    def test_chat_history_override(self):
        ticket = {"player_name": "X", "stat_type": "points",
                  "line": 20, "direction": "OVER"}
        payload = self.build(ticket, chat_history=["r1", "r2", "r3"])
        self.assertEqual(payload["recent_rants_history"], ["r1", "r2", "r3"])

    def test_empty_ticket(self):
        payload = self.build({})
        self.assertEqual(payload["player_name"], "Unknown")
        self.assertEqual(payload["line"], 0)

    def test_cashed_state(self):
        ticket = {"player_name": "X", "stat_type": "points",
                  "line": 20, "direction": "OVER"}
        live = {"pts": 25, "minutes": 30}
        payload = self.build(ticket, live_stats=live)
        self.assertEqual(payload["game_state"], "THE_CLEAN_CASH")

    def test_all_payload_keys_present(self):
        ticket = {"player_name": "Test", "stat_type": "rebounds",
                  "line": 10, "direction": "OVER"}
        payload = self.build(ticket)
        expected_keys = {
            "player_name", "ticket_type", "stat", "line", "current",
            "needed", "clock", "score_diff", "opponent", "shooting",
            "free_throws", "foul_count", "injury_status", "game_state",
            "recent_rants_history", "minutes_remaining",
        }
        self.assertEqual(set(payload.keys()), expected_keys)

    def test_pace_result_pass_through(self):
        """When pace_result is provided, classifier uses it directly."""
        ticket = {"player_name": "X", "stat_type": "points",
                  "line": 20, "direction": "OVER"}
        pace = {
            "cashed": True, "distance": 0, "blowout_risk": False,
            "foul_trouble": False, "minutes_played": 35,
            "pct_of_target": 120, "minutes_remaining": 5.2,
        }
        payload = self.build(ticket, pace_result=pace)
        self.assertEqual(payload["game_state"], "THE_CLEAN_CASH")
        self.assertEqual(payload["minutes_remaining"], 5.2)

    def test_under_cashing_at_final(self):
        """UNDER bet should classify as CLEAN_CASH when game is over and stat < line."""
        ticket = {"player_name": "X", "stat_type": "points",
                  "line": 25, "direction": "UNDER"}
        live = {"pts": 18, "minutes": 36}
        game = {"home_score": 100, "away_score": 95,
                "period": "4", "game_clock": "0:00"}
        payload = self.build(ticket, live_stats=live, game_context=game)
        self.assertEqual(payload["game_state"], "THE_CLEAN_CASH")

    def test_under_not_cashed_mid_game(self):
        """UNDER bet mid-game should NOT classify as cashed."""
        ticket = {"player_name": "X", "stat_type": "points",
                  "line": 25, "direction": "UNDER"}
        live = {"pts": 10, "minutes": 20}
        game = {"home_score": 55, "away_score": 50,
                "period": "2", "game_clock": "4:00"}
        payload = self.build(ticket, live_stats=live, game_context=game)
        self.assertNotEqual(payload["game_state"], "THE_CLEAN_CASH")


class TestModuleLevelGrudge(unittest.TestCase):
    """Test the module-level grudge buffer singleton."""

    def test_get_grudge_buffer(self):
        from agent.payload_builder import get_grudge_buffer
        buf = get_grudge_buffer()
        self.assertIsNotNone(buf)
        # Should be the same object every time
        self.assertIs(buf, get_grudge_buffer())


# ============================================================
# SECTION 2: agent/live_persona.py — Pillar 4 prompt tests
# ============================================================

class TestLiveJosephPrompt(unittest.TestCase):
    """Tests for the LIVE_JOSEPH_PROMPT constant."""

    def setUp(self):
        from agent.live_persona import LIVE_JOSEPH_PROMPT
        self.prompt = LIVE_JOSEPH_PROMPT

    def test_is_string(self):
        self.assertIsInstance(self.prompt, str)

    def test_length_substantial(self):
        """The prompt should be a substantial system message."""
        self.assertGreater(len(self.prompt), 500)

    def test_anti_repetition_mandate(self):
        self.assertIn("recent_rants_history", self.prompt)
        self.assertIn("FORBIDDEN", self.prompt)

    def test_banned_cliches(self):
        self.assertIn("down to the wire", self.prompt)
        self.assertIn("clutch", self.prompt)

    def test_reality_anchoring(self):
        self.assertIn("clock", self.prompt)
        self.assertIn("opponent", self.prompt)
        self.assertIn("score_diff", self.prompt)

    def test_all_8_game_states(self):
        for state in [
            "THE_HOOK", "FREE_THROW_MERCHANT", "BENCH_SWEAT",
            "USAGE_FREEZE_OUT", "GARBAGE_TIME_MIRACLE",
            "LOCKER_ROOM_TRAGEDY", "THE_REF_SHOW", "THE_CLEAN_CASH",
        ]:
            self.assertIn(state, self.prompt, f"Missing game state: {state}")

    def test_over_under_flip(self):
        """Prompt must mention both OVER and UNDER directions."""
        self.assertIn("OVER", self.prompt)
        self.assertIn("UNDER", self.prompt)

    def test_sub_vibe_roulette(self):
        self.assertIn("Rage", self.prompt)
        self.assertIn("Conspiracy", self.prompt)
        self.assertIn("Delusional Hype", self.prompt)
        self.assertIn("Deep Depression", self.prompt)

    def test_output_format_schema(self):
        self.assertIn("vibe_status", self.prompt)
        self.assertIn("ticker_tape_headline", self.prompt)
        self.assertIn("joseph_rant", self.prompt)

    def test_vibe_status_options_in_prompt(self):
        for status in ("Panic", "Hype", "Disgust", "Victory", "Sweating"):
            self.assertIn(status, self.prompt)


class TestSubVibeOptions(unittest.TestCase):
    """Tests for SUB_VIBE_OPTIONS tuple."""

    def test_is_tuple(self):
        from agent.live_persona import SUB_VIBE_OPTIONS
        self.assertIsInstance(SUB_VIBE_OPTIONS, tuple)

    def test_has_four_options(self):
        from agent.live_persona import SUB_VIBE_OPTIONS
        self.assertEqual(len(SUB_VIBE_OPTIONS), 4)

    def test_expected_values(self):
        from agent.live_persona import SUB_VIBE_OPTIONS
        self.assertIn("Rage", SUB_VIBE_OPTIONS)
        self.assertIn("Conspiracy", SUB_VIBE_OPTIONS)
        self.assertIn("Delusional Hype", SUB_VIBE_OPTIONS)
        self.assertIn("Deep Depression", SUB_VIBE_OPTIONS)


class TestBuildLiveJosephMessages(unittest.TestCase):
    """Tests for build_live_joseph_messages()."""

    def setUp(self):
        from agent.live_persona import build_live_joseph_messages
        self.build = build_live_joseph_messages

    def test_returns_list_of_two(self):
        payload = {"player_name": "LeBron", "game_state": "THE_HOOK",
                   "recent_rants_history": []}
        msgs = self.build(payload)
        self.assertIsInstance(msgs, list)
        self.assertEqual(len(msgs), 2)

    def test_system_message_is_prompt(self):
        payload = {"player_name": "LeBron", "game_state": "THE_HOOK",
                   "recent_rants_history": []}
        msgs = self.build(payload)
        self.assertEqual(msgs[0]["role"], "system")
        self.assertIn("Joseph M. Smith", msgs[0]["content"])

    def test_user_message_contains_payload(self):
        payload = {"player_name": "LeBron", "line": 24.5,
                   "game_state": "THE_HOOK",
                   "recent_rants_history": ["old rant"]}
        msgs = self.build(payload)
        self.assertEqual(msgs[1]["role"], "user")
        self.assertIn("LeBron", msgs[1]["content"])
        self.assertIn("24.5", msgs[1]["content"])

    def test_sub_vibe_override(self):
        payload = {"game_state": "THE_HOOK", "recent_rants_history": []}
        msgs = self.build(payload, sub_vibe="Conspiracy")
        self.assertIn("Conspiracy", msgs[1]["content"])

    def test_random_sub_vibe_used(self):
        from agent.live_persona import SUB_VIBE_OPTIONS
        payload = {"game_state": "THE_HOOK", "recent_rants_history": []}
        msgs = self.build(payload)
        # One of the sub-vibe options should appear
        found = any(sv in msgs[1]["content"] for sv in SUB_VIBE_OPTIONS)
        self.assertTrue(found)


class TestGetJosephLiveReactionStillWorks(unittest.TestCase):
    """Ensure the existing fast-path reaction function still works after Pillar 4 enhancement."""

    def setUp(self):
        from agent.live_persona import get_joseph_live_reaction
        self.react = get_joseph_live_reaction

    def test_cashed(self):
        result = self.react({"cashed": True})
        self.assertIsInstance(result, str)
        self.assertGreater(len(result), 0)

    def test_behind_pace_over(self):
        result = self.react({"cashed": False, "blowout_risk": False,
                             "foul_trouble": False, "on_pace": False,
                             "direction": "OVER"})
        self.assertIsInstance(result, str)

    def test_under_on_pace(self):
        result = self.react({"cashed": False, "blowout_risk": False,
                             "foul_trouble": False, "on_pace": True,
                             "direction": "UNDER"})
        self.assertIsInstance(result, str)

    def test_invalid_input(self):
        result = self.react(None)
        self.assertIsInstance(result, str)


class TestStreamJosephTextStillWorks(unittest.TestCase):
    """Ensure stream_joseph_text still works after Pillar 4 enhancement."""

    def test_yields_characters(self):
        from agent.live_persona import stream_joseph_text
        chars = list(stream_joseph_text("Hi", delay=0))
        self.assertEqual(chars, ["H", "i"])


# ============================================================
# SECTION 3: agent/response_parser.py tests
# ============================================================

class TestValidVibeStatuses(unittest.TestCase):
    """Tests for the VALID_VIBE_STATUSES constant."""

    def test_all_present(self):
        from agent.response_parser import VALID_VIBE_STATUSES
        self.assertIn("Panic", VALID_VIBE_STATUSES)
        self.assertIn("Hype", VALID_VIBE_STATUSES)
        self.assertIn("Disgust", VALID_VIBE_STATUSES)
        self.assertIn("Victory", VALID_VIBE_STATUSES)
        self.assertIn("Sweating", VALID_VIBE_STATUSES)

    def test_is_tuple(self):
        from agent.response_parser import VALID_VIBE_STATUSES
        self.assertIsInstance(VALID_VIBE_STATUSES, tuple)


class TestVibeResponseSchema(unittest.TestCase):
    """Tests for the JSON schema definition."""

    def test_schema_is_dict(self):
        from agent.response_parser import VIBE_RESPONSE_SCHEMA
        self.assertIsInstance(VIBE_RESPONSE_SCHEMA, dict)

    def test_required_fields(self):
        from agent.response_parser import VIBE_RESPONSE_SCHEMA
        required = VIBE_RESPONSE_SCHEMA.get("required", [])
        self.assertIn("vibe_status", required)
        self.assertIn("ticker_tape_headline", required)
        self.assertIn("joseph_rant", required)

    def test_properties_complete(self):
        from agent.response_parser import VIBE_RESPONSE_SCHEMA
        props = VIBE_RESPONSE_SCHEMA.get("properties", {})
        self.assertIn("vibe_status", props)
        self.assertIn("ticker_tape_headline", props)
        self.assertIn("joseph_rant", props)


class TestValidateVibeResponse(unittest.TestCase):
    """Tests for validate_vibe_response()."""

    def setUp(self):
        from agent.response_parser import validate_vibe_response
        self.validate = validate_vibe_response

    def test_valid_response(self):
        data = {
            "vibe_status": "Panic",
            "ticker_tape_headline": "DYING ON THE HOOK!",
            "joseph_rant": "He needs ONE more bucket!",
        }
        result = self.validate(data)
        self.assertEqual(result["vibe_status"], "Panic")
        self.assertEqual(result["ticker_tape_headline"], "DYING ON THE HOOK!")
        self.assertEqual(result["joseph_rant"], "He needs ONE more bucket!")

    def test_headline_uppercased(self):
        data = {
            "vibe_status": "Hype",
            "ticker_tape_headline": "let's go baby",
            "joseph_rant": "Test rant",
        }
        result = self.validate(data)
        self.assertEqual(result["ticker_tape_headline"], "LET'S GO BABY")

    def test_headline_truncated_to_five_words(self):
        data = {
            "vibe_status": "Sweating",
            "ticker_tape_headline": "one two three four five six seven eight nine ten",
            "joseph_rant": "Test rant",
        }
        result = self.validate(data)
        # Headlines > 8 words get truncated to 5 words
        self.assertLessEqual(len(result["ticker_tape_headline"].split()), 5)

    def test_headline_moderate_length_kept(self):
        """Headlines with 6-8 words are allowed (flexibility zone)."""
        data = {
            "vibe_status": "Sweating",
            "ticker_tape_headline": "one two three four five six",
            "joseph_rant": "Test rant",
        }
        result = self.validate(data)
        self.assertEqual(len(result["ticker_tape_headline"].split()), 6)

    def test_invalid_vibe_status(self):
        data = {
            "vibe_status": "Invalid",
            "ticker_tape_headline": "TEST",
            "joseph_rant": "Rant",
        }
        with self.assertRaises(ValueError):
            self.validate(data)

    def test_empty_headline(self):
        data = {
            "vibe_status": "Panic",
            "ticker_tape_headline": "",
            "joseph_rant": "Rant",
        }
        with self.assertRaises(ValueError):
            self.validate(data)

    def test_empty_rant(self):
        data = {
            "vibe_status": "Panic",
            "ticker_tape_headline": "TEST",
            "joseph_rant": "",
        }
        with self.assertRaises(ValueError):
            self.validate(data)

    def test_not_dict_raises(self):
        with self.assertRaises(ValueError):
            self.validate("not a dict")

    def test_all_vibe_statuses_accepted(self):
        from agent.response_parser import VALID_VIBE_STATUSES
        for status in VALID_VIBE_STATUSES:
            data = {
                "vibe_status": status,
                "ticker_tape_headline": "TEST",
                "joseph_rant": "Test rant",
            }
            result = self.validate(data)
            self.assertEqual(result["vibe_status"], status)


class TestParseVibeResponse(unittest.TestCase):
    """Tests for parse_vibe_response()."""

    def setUp(self):
        from agent.response_parser import parse_vibe_response
        self.parse = parse_vibe_response

    def test_valid_json_string(self):
        raw = json.dumps({
            "vibe_status": "Victory",
            "ticker_tape_headline": "CASHED IT EASY!",
            "joseph_rant": "We never had a doubt!",
        })
        result = self.parse(raw)
        self.assertEqual(result["vibe_status"], "Victory")

    def test_json_in_markdown_fence(self):
        raw = '```json\n{"vibe_status":"Panic","ticker_tape_headline":"OH NO!","joseph_rant":"Bad!"}\n```'
        result = self.parse(raw)
        self.assertEqual(result["vibe_status"], "Panic")

    def test_json_embedded_in_text(self):
        raw = (
            'Here is my response:\n'
            '{"vibe_status":"Hype","ticker_tape_headline":"LETS GOOO!","joseph_rant":"Hype!"}\n'
            'Hope you like it!'
        )
        result = self.parse(raw)
        self.assertEqual(result["vibe_status"], "Hype")

    def test_fallback_on_garbage(self):
        result = self.parse("this is not json at all", "THE_HOOK")
        self.assertEqual(result["vibe_status"], "Sweating")
        self.assertIn("joseph_rant", result)
        self.assertIn("ticker_tape_headline", result)

    def test_fallback_on_empty(self):
        result = self.parse("", "THE_CLEAN_CASH")
        self.assertEqual(result["vibe_status"], "Victory")

    def test_fallback_on_none(self):
        result = self.parse(None)
        self.assertIn(result["vibe_status"],
                      ("Panic", "Hype", "Disgust", "Victory", "Sweating"))

    def test_always_returns_dict(self):
        for input_val in [None, "", "garbage", 42, [], "{}",
                          '{"wrong": "keys"}']:
            result = self.parse(str(input_val) if input_val is not None else None)
            self.assertIsInstance(result, dict)
            self.assertIn("vibe_status", result)
            self.assertIn("ticker_tape_headline", result)
            self.assertIn("joseph_rant", result)


class TestFallbackResponse(unittest.TestCase):
    """Tests for _fallback_response()."""

    def setUp(self):
        from agent.response_parser import _fallback_response
        self.fallback = _fallback_response

    def test_default(self):
        result = self.fallback()
        self.assertEqual(result["vibe_status"], "Sweating")

    def test_victory_for_cash(self):
        result = self.fallback("THE_CLEAN_CASH")
        self.assertEqual(result["vibe_status"], "Victory")

    def test_panic_for_locker_room(self):
        result = self.fallback("LOCKER_ROOM_TRAGEDY")
        self.assertEqual(result["vibe_status"], "Panic")


class TestBuildOpenaiResponseFormat(unittest.TestCase):
    """Tests for build_openai_response_format()."""

    def test_structure(self):
        from agent.response_parser import build_openai_response_format
        fmt = build_openai_response_format()
        self.assertEqual(fmt["type"], "json_schema")
        self.assertIn("json_schema", fmt)
        self.assertEqual(fmt["json_schema"]["name"], "joseph_vibe_check")
        self.assertTrue(fmt["json_schema"]["strict"])


class TestStateToDefaultVibe(unittest.TestCase):
    """Tests for _STATE_TO_DEFAULT_VIBE mapping."""

    def test_all_states_mapped(self):
        from agent.response_parser import _STATE_TO_DEFAULT_VIBE, VALID_VIBE_STATUSES
        from agent.payload_builder import ALL_GAME_STATES
        for state in ALL_GAME_STATES:
            self.assertIn(state, _STATE_TO_DEFAULT_VIBE)
            self.assertIn(_STATE_TO_DEFAULT_VIBE[state], VALID_VIBE_STATUSES)


class TestGenerateVibeCssClass(unittest.TestCase):
    """Tests for generate_vibe_css_class()."""

    def setUp(self):
        from agent.response_parser import generate_vibe_css_class
        self.css_class = generate_vibe_css_class

    def test_panic(self):
        self.assertEqual(self.css_class("Panic"), "panic-glow")

    def test_hype(self):
        self.assertEqual(self.css_class("Hype"), "hype-glow")

    def test_disgust(self):
        self.assertEqual(self.css_class("Disgust"), "disgust-glow")

    def test_victory(self):
        self.assertEqual(self.css_class("Victory"), "victory-glow")

    def test_sweating(self):
        self.assertEqual(self.css_class("Sweating"), "sweating-glow")

    def test_unknown_defaults(self):
        self.assertEqual(self.css_class("Unknown"), "sweating-glow")

    def test_all_valid_statuses_have_class(self):
        from agent.response_parser import VALID_VIBE_STATUSES
        for status in VALID_VIBE_STATUSES:
            cls = self.css_class(status)
            self.assertTrue(cls.endswith("-glow"), f"{status} → {cls}")
            self.assertGreater(len(cls), 0)


class TestGetVibeEmoji(unittest.TestCase):
    """Tests for get_vibe_emoji()."""

    def setUp(self):
        from agent.response_parser import get_vibe_emoji
        self.emoji = get_vibe_emoji

    def test_panic(self):
        self.assertEqual(self.emoji("Panic"), "🚨")

    def test_hype(self):
        self.assertEqual(self.emoji("Hype"), "🔥")

    def test_victory(self):
        self.assertEqual(self.emoji("Victory"), "🏆")

    def test_sweating(self):
        self.assertEqual(self.emoji("Sweating"), "😰")

    def test_disgust(self):
        self.assertEqual(self.emoji("Disgust"), "🤢")

    def test_unknown_defaults(self):
        self.assertEqual(self.emoji("Unknown"), "😰")

    def test_all_valid_statuses_have_emoji(self):
        from agent.response_parser import VALID_VIBE_STATUSES
        for status in VALID_VIBE_STATUSES:
            e = self.emoji(status)
            self.assertIsInstance(e, str)
            self.assertGreater(len(e), 0)


class TestPanicRoomCss(unittest.TestCase):
    """Tests for get_panic_room_css()."""

    def setUp(self):
        from styles.live_theme import get_panic_room_css
        self.css = get_panic_room_css()

    def test_returns_style_block(self):
        self.assertIn("<style>", self.css)
        self.assertIn("</style>", self.css)

    def test_panic_glow_class(self):
        self.assertIn("panic-glow", self.css)

    def test_hype_glow_class(self):
        self.assertIn("hype-glow", self.css)

    def test_disgust_glow_class(self):
        self.assertIn("disgust-glow", self.css)

    def test_victory_glow_class(self):
        self.assertIn("victory-glow", self.css)

    def test_sweating_glow_class(self):
        self.assertIn("sweating-glow", self.css)

    def test_panic_pulse_animation(self):
        self.assertIn("panicPulse", self.css)

    def test_victory_shimmer_animation(self):
        self.assertIn("victoryShimmer", self.css)

    def test_headline_class(self):
        self.assertIn("panic-room-headline", self.css)

    def test_rant_class(self):
        self.assertIn("panic-room-rant", self.css)

    def test_vibe_badge_class(self):
        self.assertIn("panic-room-vibe-badge", self.css)


class TestRenderPanicRoomCard(unittest.TestCase):
    """Tests for render_panic_room_card()."""

    def setUp(self):
        from styles.live_theme import render_panic_room_card
        self.render = render_panic_room_card

    def test_returns_html_string(self):
        html = self.render("Panic", "DYING ON THE HOOK!", "He needs ONE MORE!")
        self.assertIsInstance(html, str)
        self.assertIn("panic-room-card", html)

    def test_glow_class_applied(self):
        html = self.render("Panic", "TEST!", "Rant text")
        self.assertIn("panic-glow", html)

    def test_victory_glow(self):
        html = self.render("Victory", "CASHED IT!", "We won!")
        self.assertIn("victory-glow", html)

    def test_headline_rendered(self):
        html = self.render("Hype", "LET'S GO!", "Hype rant")
        self.assertIn("LET&#x27;S GO!", html)

    def test_rant_rendered(self):
        html = self.render("Sweating", "SWEAT!", "Some dramatic text here")
        self.assertIn("Some dramatic text here", html)

    def test_player_name_shown(self):
        html = self.render("Panic", "TEST!", "Rant", player_name="LeBron James")
        self.assertIn("LeBron James", html)

    def test_game_state_badge(self):
        html = self.render("Panic", "TEST!", "Rant", game_state="THE_HOOK")
        self.assertIn("The Hook", html)

    def test_emoji_in_badge(self):
        html = self.render("Victory", "WIN!", "Rant")
        self.assertIn("🏆", html)

    def test_xss_safe(self):
        """Ensure HTML entities are escaped."""
        html = self.render("Panic", "<script>alert(1)</script>",
                          "He needs <b>one</b>")
        self.assertNotIn("<script>", html)
        self.assertNotIn("<b>", html)


# ============================================================
# SECTION 4: Integration tests — full pipeline
# ============================================================

class TestPillar4Integration(unittest.TestCase):
    """End-to-end integration: payload → messages → parse response."""

    def test_full_pipeline(self):
        from agent.payload_builder import build_live_vibe_payload, GrudgeBuffer
        from agent.live_persona import build_live_joseph_messages
        from agent.response_parser import parse_vibe_response

        # Build payload
        ticket = {
            "player_name": "LeBron James",
            "stat_type": "points",
            "line": 24.5,
            "direction": "OVER",
        }
        live = {"pts": 24, "minutes": 38, "fouls": 2}
        game = {
            "home_score": 105, "away_score": 101,
            "period": "4", "game_clock": "2:14",
            "away_team": "Celtics",
        }
        buf = GrudgeBuffer()
        buf.add("Previous rant about free throws")

        payload = build_live_vibe_payload(ticket, live, game, grudge_buffer=buf)

        # Verify payload
        self.assertEqual(payload["player_name"], "LeBron James")
        self.assertEqual(payload["needed"], 0.5)
        self.assertEqual(payload["recent_rants_history"],
                         ["Previous rant about free throws"])

        # Build messages
        msgs = build_live_joseph_messages(payload, sub_vibe="Rage")
        self.assertEqual(len(msgs), 2)
        self.assertIn("Rage", msgs[1]["content"])

        # Simulate an LLM response
        fake_response = json.dumps({
            "vibe_status": "Sweating",
            "ticker_tape_headline": "DYING ON THE HOOK!",
            "joseph_rant": (
                "LeBron is at 24 points with 2:14 left against the Celtics "
                "and he needs ONE MORE! SCORE THE BALL!"
            ),
        })
        result = parse_vibe_response(fake_response, payload["game_state"])
        self.assertEqual(result["vibe_status"], "Sweating")
        self.assertIn("LeBron", result["joseph_rant"])

        # Update grudge buffer
        buf.add(result["joseph_rant"])
        self.assertEqual(len(buf), 2)

    def test_under_pipeline(self):
        from agent.payload_builder import build_live_vibe_payload
        from agent.live_persona import build_live_joseph_messages

        ticket = {
            "player_name": "Jayson Tatum",
            "stat_type": "rebounds",
            "line": 8.5,
            "direction": "UNDER",
        }
        live = {"reb": 3, "minutes": 28, "fouls": 1}
        game = {"home_score": 90, "away_score": 85, "period": "3",
                "game_clock": "5:00", "away_team": "Lakers"}

        payload = build_live_vibe_payload(ticket, live, game)
        self.assertEqual(payload["ticket_type"], "UNDER")

        msgs = build_live_joseph_messages(payload)
        self.assertEqual(msgs[0]["role"], "system")
        self.assertIn("UNDER", msgs[0]["content"])


# ============================================================
# SECTION 5: File existence checks
# ============================================================

class TestPillar4FileStructure(unittest.TestCase):
    """Verify the Pillar 4 files exist with proper structure."""

    def test_payload_builder_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..",
                           "agent", "payload_builder.py")
        self.assertTrue(os.path.isfile(path))

    def test_response_parser_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..",
                           "agent", "response_parser.py")
        self.assertTrue(os.path.isfile(path))

    def test_live_persona_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..",
                           "agent", "live_persona.py")
        self.assertTrue(os.path.isfile(path))

    def test_payload_builder_syntax(self):
        path = os.path.join(os.path.dirname(__file__), "..",
                           "agent", "payload_builder.py")
        with open(path, "r", encoding="utf-8") as f:
            source = f.read()
        compile(source, "payload_builder.py", "exec")

    def test_response_parser_syntax(self):
        path = os.path.join(os.path.dirname(__file__), "..",
                           "agent", "response_parser.py")
        with open(path, "r", encoding="utf-8") as f:
            source = f.read()
        compile(source, "response_parser.py", "exec")

    def test_live_persona_syntax(self):
        path = os.path.join(os.path.dirname(__file__), "..",
                           "agent", "live_persona.py")
        with open(path, "r", encoding="utf-8") as f:
            source = f.read()
        compile(source, "live_persona.py", "exec")


if __name__ == "__main__":
    unittest.main()
