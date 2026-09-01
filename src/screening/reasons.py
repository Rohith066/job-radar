"""Structured reason codes and their human-readable renderings.

Codes are the internal currency — they are stable, greppable, and safe to
persist in state. Display text is generated from them at the edge (email,
shortlist, shadow report) so wording can change without invalidating stored
state or test assertions.
"""
from __future__ import annotations

# ── Title / seniority ──────────────────────────────────────────────────────
ENTRY_LEVEL_EXPLICIT   = "ENTRY_LEVEL_EXPLICIT"
NEW_GRAD_EXPLICIT      = "NEW_GRAD_EXPLICIT"
JUNIOR_TITLE           = "JUNIOR_TITLE"
ASSOCIATE_TITLE        = "ASSOCIATE_TITLE"
EARLY_CAREER_TITLE     = "EARLY_CAREER_TITLE"
LEVEL_ONE_TITLE        = "LEVEL_ONE_TITLE"
LEVEL_TWO_AMBIGUOUS    = "LEVEL_TWO_AMBIGUOUS"
LEVEL_THREE_TITLE      = "LEVEL_THREE_TITLE"
LEVEL_FOUR_PLUS_TITLE  = "LEVEL_FOUR_PLUS_TITLE"
SENIOR_TITLE           = "SENIOR_TITLE"
STAFF_TITLE            = "STAFF_TITLE"
PRINCIPAL_TITLE        = "PRINCIPAL_TITLE"
LEAD_TITLE             = "LEAD_TITLE"
ARCHITECT_TITLE        = "ARCHITECT_TITLE"
MANAGER_TITLE          = "MANAGER_TITLE"
DIRECTOR_TITLE         = "DIRECTOR_TITLE"
EXECUTIVE_TITLE        = "EXECUTIVE_TITLE"
INTERNSHIP_TITLE       = "INTERNSHIP_TITLE"
CLEARANCE_TITLE        = "CLEARANCE_TITLE"
PROFILE_MISMATCH_TITLE = "PROFILE_MISMATCH_TITLE"

# ── Role family ────────────────────────────────────────────────────────────
ROLE_FAMILY_TARGET     = "ROLE_FAMILY_TARGET"
ROLE_FAMILY_SECONDARY  = "ROLE_FAMILY_SECONDARY"
ROLE_FAMILY_ADJACENT   = "ROLE_FAMILY_ADJACENT"
ROLE_FAMILY_AMBIGUOUS  = "ROLE_FAMILY_AMBIGUOUS"
ROLE_FAMILY_UNRELATED  = "ROLE_FAMILY_UNRELATED"
ROLE_FAMILY_UNKNOWN    = "ROLE_FAMILY_UNKNOWN"

# ── Location ───────────────────────────────────────────────────────────────
US_CONFIRMED           = "US_CONFIRMED"
US_REMOTE_CONFIRMED    = "US_REMOTE_CONFIRMED"
US_REMOTE_VIA_BOARD    = "US_REMOTE_VIA_BOARD"
US_VIA_BOARD           = "US_VIA_BOARD"
LOCATION_AMBIGUOUS     = "LOCATION_AMBIGUOUS"
REMOTE_UNSCOPED        = "REMOTE_UNSCOPED"
NON_US_CONFIRMED       = "NON_US_CONFIRMED"
LOCATION_MISSING       = "LOCATION_MISSING"

# ── Experience ─────────────────────────────────────────────────────────────
EXPERIENCE_NONE_REQUIRED = "EXPERIENCE_NONE_REQUIRED"
EXPERIENCE_0_2         = "EXPERIENCE_0_2"
EXPERIENCE_1_2         = "EXPERIENCE_1_2"
EXPERIENCE_2           = "EXPERIENCE_2"
EXPERIENCE_2_3         = "EXPERIENCE_2_3"
EXPERIENCE_3           = "EXPERIENCE_3"
EXPERIENCE_4           = "EXPERIENCE_4"
EXPERIENCE_5_PLUS      = "EXPERIENCE_5_PLUS"
EXPERIENCE_7_PLUS      = "EXPERIENCE_7_PLUS"
EXPERIENCE_UNKNOWN     = "EXPERIENCE_UNKNOWN"
EXPERIENCE_LOW_CONF    = "EXPERIENCE_LOW_CONF"

# ── Freshness ──────────────────────────────────────────────────────────────
FRESH_LT_6H            = "FRESH_LT_6H"
FRESH_LT_24H           = "FRESH_LT_24H"
FRESH_LT_3D            = "FRESH_LT_3D"
STALE_POSTING          = "STALE_POSTING"
NO_POSTED_DATE         = "NO_POSTED_DATE"

# ── Recall protection ──────────────────────────────────────────────────────
TARGET_ROLE_RECALL_FLOOR = "TARGET_ROLE_RECALL_FLOOR"

_TEXT: dict[str, str] = {
    ENTRY_LEVEL_EXPLICIT:  "Explicit entry-level title",
    NEW_GRAD_EXPLICIT:     "New grad title",
    JUNIOR_TITLE:          "Junior title",
    ASSOCIATE_TITLE:       "Associate title",
    EARLY_CAREER_TITLE:    "Early-career title",
    LEVEL_ONE_TITLE:       "Level I / 1 role",
    LEVEL_TWO_AMBIGUOUS:   "Level II / 2 — ambiguous, kept for review",
    LEVEL_THREE_TITLE:     "Level III / 3 role — above entry level",
    LEVEL_FOUR_PLUS_TITLE: "Level IV+ role — well above entry level",
    SENIOR_TITLE:          "Senior title",
    STAFF_TITLE:           "Staff title",
    PRINCIPAL_TITLE:       "Principal title",
    LEAD_TITLE:            "Lead title",
    ARCHITECT_TITLE:       "Architect title",
    MANAGER_TITLE:         "Manager title",
    DIRECTOR_TITLE:        "Director title",
    EXECUTIVE_TITLE:       "Executive title (VP / Head / Chief)",
    INTERNSHIP_TITLE:      "Internship / co-op / part-time",
    CLEARANCE_TITLE:       "Security clearance or citizenship gate in title",
    PROFILE_MISMATCH_TITLE: "Outside the search profile (robotics / vision / hardware)",
    ROLE_FAMILY_TARGET:    "Target technical role family",
    ROLE_FAMILY_SECONDARY: "Adjacent technical role family",
    ROLE_FAMILY_ADJACENT:  "Adjacent occupation — review only",
    ROLE_FAMILY_AMBIGUOUS: "Technical role, family unclear",
    ROLE_FAMILY_UNRELATED: "Role family outside the search",
    ROLE_FAMILY_UNKNOWN:   "Role family not recognised",
    US_CONFIRMED:          "US location confirmed",
    US_REMOTE_CONFIRMED:   "US Remote confirmed",
    US_REMOTE_VIA_BOARD:   "Remote on a US-focused board",
    US_VIA_BOARD:          "US inferred from board metadata",
    LOCATION_AMBIGUOUS:    "Location ambiguous — could be US",
    REMOTE_UNSCOPED:       "Remote with no country scope",
    NON_US_CONFIRMED:      "Confirmed non-US location",
    LOCATION_MISSING:      "No location given",
    EXPERIENCE_NONE_REQUIRED: "No experience required",
    EXPERIENCE_0_2:        "0–2 years experience",
    EXPERIENCE_1_2:        "1–2 years experience",
    EXPERIENCE_2:          "2 years experience",
    EXPERIENCE_2_3:        "2–3 years experience",
    EXPERIENCE_3:          "3 years experience",
    EXPERIENCE_4:          "4 years experience",
    EXPERIENCE_5_PLUS:     "5+ years experience required",
    EXPERIENCE_7_PLUS:     "7+ years experience required",
    EXPERIENCE_UNKNOWN:    "Experience requirement not stated",
    EXPERIENCE_LOW_CONF:   "Experience figure found but context is weak",
    FRESH_LT_6H:           "Posted in the last 6 hours",
    FRESH_LT_24H:          "Posted in the last 24 hours",
    FRESH_LT_3D:           "Posted in the last 3 days",
    STALE_POSTING:         "Posting is more than a week old",
    NO_POSTED_DATE:        "No reliable posting date",
    TARGET_ROLE_RECALL_FLOOR:
        "Target role within the experience ceiling — held at review despite "
        "unscoped location",
}


def describe(code: str) -> str:
    """Human-readable text for one reason code (falls back to the code)."""
    return _TEXT.get(code, code)


def describe_all(codes) -> list[str]:
    return [describe(c) for c in codes]
