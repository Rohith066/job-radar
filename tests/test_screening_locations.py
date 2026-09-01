"""Location classification regression tests.

Covers the two failure directions the previous boolean `is_us_location` had:
real US cities rejected by substring collisions, and bare "Remote" silently
assumed to be US.
"""
from __future__ import annotations

import pytest

from src.screening.locations import (
    analyze_location, US, US_REMOTE, NON_US, AMBIGUOUS,
)
from src.sources.base import is_us_location


# ── The matrix from the specification ─────────────────────────────────────
@pytest.mark.parametrize("location,expected", [
    ("Remote",                    AMBIGUOUS),
    ("Remote - US",               US_REMOTE),
    ("US Remote",                 US_REMOTE),
    ("Remote, USA",               US_REMOTE),
    ("United States",             US),
    ("United States - Remote",    US_REMOTE),
    ("Remote (United States)",    US_REMOTE),
    ("New York, NY",              US),
    ("New York City",             US),
    ("San Francisco, California", US),
    ("Austin, Texas",             US),
    ("Toronto, Canada",           NON_US),
    ("London, UK",                NON_US),
    ("Remote - Canada",           NON_US),
    ("Remote - Europe",           NON_US),
])
def test_location_matrix(location, expected):
    assert analyze_location(location).classification == expected


# ── Substring collisions that used to reject real US cities ───────────────
@pytest.mark.parametrize("location", [
    "Milwaukee, WI",       # contains "uk"
    "Waukesha, WI",        # contains "uk"
    "Indianapolis, IN",    # contains "india"
    "Indiana",             # contains "india"
    "Chinatown, San Francisco, CA",   # contains "china"
])
def test_us_cities_are_not_rejected_by_substring_collisions(location):
    r = analyze_location(location)
    assert r.classification in (US, US_REMOTE), f"{location} -> {r.classification} ({r.reason})"


# ── Full state names ──────────────────────────────────────────────────────
@pytest.mark.parametrize("location", [
    "Austin, Texas", "San Francisco, California", "Seattle, Washington",
    "Boston, Massachusetts", "Denver, Colorado", "Columbus, Ohio",
    "Portland, Oregon", "Nashville, Tennessee",
])
def test_full_state_names_are_recognised(location):
    assert analyze_location(location).classification == US


@pytest.mark.parametrize("location", [
    "Seattle, WA", "Chicago, IL", "Boston, MA", "Denver, CO",
    "Louisville, KY", "Newark, DE", "Washington, DC",
])
def test_state_abbreviations_are_recognised(location):
    assert analyze_location(location).classification == US


# ── country_focus interaction ─────────────────────────────────────────────
def test_bare_remote_on_us_board_is_us_remote():
    r = analyze_location("Remote", country_focus="US")
    assert r.classification == US_REMOTE
    assert r.confidence == "medium"


def test_bare_remote_on_global_board_is_ambiguous():
    r = analyze_location("Remote", country_focus="Global")
    assert r.classification == AMBIGUOUS


def test_bare_remote_on_unknown_board_is_ambiguous():
    """A generic 'Remote' must never be blindly assumed US."""
    for focus in ("", None, "unknown"):
        assert analyze_location("Remote", country_focus=focus).classification == AMBIGUOUS


def test_country_focus_cannot_override_explicit_non_us():
    """Board metadata resolves ambiguity; it does not overrule the job."""
    r = analyze_location("Remote - Canada", country_focus="US")
    assert r.classification == NON_US


def test_explicit_us_location_beats_global_board():
    r = analyze_location("Austin, TX", country_focus="Global")
    assert r.classification == US


# ── Remote variants ───────────────────────────────────────────────────────
@pytest.mark.parametrize("location", [
    "Remote - US", "US Remote", "Remote, USA", "United States - Remote",
    "Remote (United States)", "Remote - Austin, TX", "Remote — Nationwide",
])
def test_us_remote_variants(location):
    assert analyze_location(location).classification == US_REMOTE


def test_multi_region_remote_including_us_is_kept():
    r = analyze_location("Remote - US, Canada")
    assert r.classification == US_REMOTE


# ── Missing / placeholder ─────────────────────────────────────────────────
@pytest.mark.parametrize("location", ["", "   ", "Unknown Location", "N/A", "TBD"])
def test_missing_location_is_ambiguous_not_rejected(location):
    r = analyze_location(location)
    assert r.classification == AMBIGUOUS
    assert r.is_plausibly_us


# ── Ambiguous state names ─────────────────────────────────────────────────
def test_bare_georgia_is_ambiguous():
    """Georgia is both a US state and a country."""
    assert analyze_location("Georgia").classification == AMBIGUOUS


def test_georgia_with_a_us_signal_is_us():
    assert analyze_location("Atlanta, Georgia").classification == US


# ── Ambiguity is preserved, never silently discarded ──────────────────────
def test_ambiguous_locations_survive_the_filter():
    """False negatives cost applications — ambiguous must pass the gate."""
    for loc in ("Remote", "Somewhereville", "", "Unknown Location"):
        assert is_us_location(loc) is True, loc


def test_confirmed_non_us_is_filtered():
    for loc in ("Remote - Argentina", "London, UK", "Bangalore, India", "Remote - Europe"):
        assert is_us_location(loc) is False, loc


def test_is_us_location_accepts_country_focus():
    assert is_us_location("Remote", "US") is True
    assert is_us_location("Remote - Canada", "US") is False


def test_analysis_is_deterministic():
    assert analyze_location("Remote - US") == analyze_location("Remote - US")


def test_every_verdict_carries_a_reason():
    for loc in ("Remote", "Austin, TX", "London, UK", "", "Remote - US"):
        r = analyze_location(loc)
        assert r.reason, loc
        assert r.reasons, loc
        assert r.confidence in ("high", "medium", "low")


# ── US cities that collide with foreign city names ────────────────────────
@pytest.mark.parametrize("location", [
    "Dublin, OH", "Dublin, CA", "Toronto, OH", "Rome, NY", "Vienna, VA",
    "Melbourne, Florida", "Paris, TX", "Berlin, NH", "Athens, GA",
])
def test_us_cities_sharing_a_foreign_name_are_us(location):
    """A foreign city name must not override an explicit US state."""
    assert analyze_location(location).classification == US


@pytest.mark.parametrize("location", ["Toronto", "Dublin", "Bogota", "Melbourne"])
def test_bare_foreign_city_names_are_non_us(location):
    """With no US signal at all, a foreign city name settles it."""
    assert analyze_location(location).classification == NON_US


@pytest.mark.parametrize("location,expected", [
    ("Bogota, DC, co",  NON_US),
    ("Chennai, TN, in", NON_US),
    ("Tel Aviv, il",    US),      # accepted false positive: routed to review, not dropped
    ("Seattle, WA, US", US),
    ("US, WA, Seattle", US),
])
def test_trailing_country_code_slot(location, expected):
    """Some ATS feeds emit 'City, Region, cc' where the last field is a country
    code, not a US state."""
    assert analyze_location(location).classification == expected


@pytest.mark.parametrize("location,expected", [
    ("Chinatown, San Francisco, CA", US),      # CA = California, not Canada
    ("Toronto, ON, ca",              NON_US),  # CA = Canada, city is foreign
    ("Mumbai, MH, in",               NON_US),  # IN = India, not Indiana
    ("Indianapolis, Marion, IN",     US),      # IN = Indiana
    ("London, England, gb",          NON_US),
])
def test_iso_code_and_state_abbreviation_collisions(location, expected):
    assert analyze_location(location).classification == expected
