"""Company-level filtering for Job Radar.

Three lists:
  HARD_EXCLUDE_COMPANIES  — always reject (score → 0, job dropped entirely)
  TARGET_COMPANIES        — confirmed H1B sponsors, score boost applied
  Everything else         — neutral, pass through unchanged

Usage:
    from .company_filter import company_score_adjustment

    adj, reason = company_score_adjustment(job.company)
    if adj == -999:
        continue   # hard exclude — discard job
    final_score = min(100, base_score + adj)
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# HARD EXCLUDES — always score 0, never alert
# ---------------------------------------------------------------------------
HARD_EXCLUDE_COMPANIES: list[str] = [
    # Federal contractors — clearance required, OPT/H1B ineligible
    "booz allen",
    "saic",
    "leidos",
    "raytheon",
    "general dynamics",
    "northrop grumman",
    "l3harris",
    "bae systems",
    "caci international",
    "peraton",
    "maximus",
    "csa group",
    "engility",
    "dxc technology",
    # Staffing agencies — won't place OPT candidates reliably
    "jobot",
    "robert half",
    "insight global",
    "teksystems",
    "kforce",
    "cybercoders",
    "randstad",
    "adecco",
    "manpower",
    "kelly services",
    "hirequest",
    "collabera",
    "infosys bpm",
    "wipro",
    "cognizant bpo",
    "modis",
    "aerotek",
    "apex group",
    # Healthcare specialists — domain mismatch, often no sponsorship
    "aptitude health",
    "cobalt service",
    "memic group",
]

# ---------------------------------------------------------------------------
# TARGET COMPANIES — confirmed 10+ H1B LCA filings, get a score boost
# ---------------------------------------------------------------------------
TARGET_COMPANIES: list[tuple[str, int]] = [
    # (company name fragment, bonus points)

    # Financial Services / Wall Street
    ("goldman sachs", 10),
    ("jpmorgan", 10),
    ("jp morgan", 10),
    ("citigroup", 10),
    ("citibank", 10),
    ("american express", 10),
    ("bloomberg", 10),
    ("mastercard", 10),
    ("visa", 8),
    ("s&p global", 8),
    ("moody", 8),
    ("factset", 8),
    ("morgan stanley", 10),
    ("ubs", 8),
    ("deutsche bank", 8),
    ("barclays", 8),
    ("lseg", 8),
    ("blackstone", 8),
    ("two sigma", 8),
    ("point72", 8),
    ("schonfeld", 8),
    ("capital one", 8),
    ("fidelity", 8),
    ("vanguard", 8),

    # Technology
    ("stripe", 10),
    ("databricks", 10),
    ("snowflake", 8),
    ("spotify", 8),
    ("salesforce", 8),
    ("adobe", 8),
    ("palantir", 6),
    ("squarespace", 6),
    ("etsy", 6),
    ("duolingo", 8),
    ("fanatics", 6),
    ("warner bros", 6),
    ("new york times", 6),
    ("nielsen", 6),
    ("nielseniq", 6),

    # Consulting / Professional Services
    ("deloitte", 10),
    ("accenture", 10),
    ("pwc", 10),
    ("ernst & young", 8),
    (" ey ", 8),
    ("kpmg", 8),
    ("capgemini", 8),
]


def company_score_adjustment(company: str) -> tuple[int, str]:
    """Return (adjustment, reason).

    adjustment == -999  →  hard exclude, discard this job entirely
    adjustment >  0     →  priority boost (target company)
    adjustment == 0     →  neutral (unknown company)
    """
    c = (company or "").lower().strip()
    if not c:
        return 0, "no_company"

    # Hard exclude check
    for excluded in HARD_EXCLUDE_COMPANIES:
        if excluded in c:
            return -999, f"excluded:{excluded}"

    # Target company boost
    for target, bonus in TARGET_COMPANIES:
        if target in c:
            return bonus, f"target:{target}"

    return 0, "unknown"
