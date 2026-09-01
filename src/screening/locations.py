"""US / US-Remote / non-US location classification.

Replaces `is_us_location`, whose substring matching produced false negatives on
real US cities — "Milwaukee, WI" was rejected because it contains "uk", and
"Indianapolis, IN" because it contains "india". Both are fixed here by matching
country names on token boundaries.

The other change is that the verdict is no longer boolean. A bare "Remote" on a
board with no country scope is genuinely unknown, and the previous code assumed
US. It now returns AMBIGUOUS, which the caller routes to review rather than
either trusting or discarding.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from . import reasons as R

US_STATE_ABBRS = frozenset({
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id", "il",
    "in", "ia", "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt",
    "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri",
    "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy", "dc",
})

US_STATE_NAMES = frozenset({
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "hawaii", "idaho", "illinois",
    "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine", "maryland",
    "massachusetts", "michigan", "minnesota", "mississippi", "missouri",
    "montana", "nebraska", "nevada", "new hampshire", "new jersey",
    "new mexico", "new york", "north carolina", "north dakota", "ohio",
    "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina",
    "south dakota", "tennessee", "texas", "utah", "vermont", "virginia",
    "washington", "west virginia", "wisconsin", "wyoming",
    "district of columbia", "puerto rico",
})

# US state names that are also country or foreign-region names. Only accepted
# as US evidence when a second US signal is present.
_AMBIGUOUS_STATE_NAMES = frozenset({"georgia", "washington"})

# Unambiguous US cities that commonly appear without a state.
US_CITY_NAMES = frozenset({
    "new york city", "nyc", "san francisco", "los angeles", "chicago",
    "philadelphia", "houston", "phoenix", "san antonio", "san diego",
    "san jose", "austin", "dallas", "seattle", "denver", "boston", "atlanta",
    "miami", "minneapolis", "detroit", "pittsburgh", "charlotte", "nashville",
    "raleigh", "columbus", "indianapolis", "milwaukee", "salt lake city",
    "kansas city", "st. louis", "saint louis", "baltimore", "sacramento",
    "las vegas", "orlando", "tampa", "cincinnati", "cleveland", "st. paul",
    "silicon valley", "bay area", "mountain view", "palo alto", "sunnyvale",
    "redmond", "bellevue", "menlo park", "santa clara", "cupertino",
    "brooklyn", "manhattan", "washington dc", "washington d.c.",
})

# Countries and regions. These are decisive: no US signal overrides them.
NON_US_COUNTRIES = frozenset({
    "argentina", "colombia", "brazil", "brasil", "mexico", "méxico", "chile",
    "peru", "costa rica", "canada", "united kingdom", "uk", "england",
    "scotland", "wales", "ireland", "australia", "new zealand", "india",
    "germany", "france", "spain", "netherlands", "belgium", "poland",
    "portugal", "italy", "sweden", "norway", "denmark", "finland",
    "switzerland", "austria", "czechia", "czech republic", "romania",
    "hungary", "greece", "singapore", "japan", "china", "hong kong", "taiwan",
    "south korea", "korea", "vietnam", "thailand", "malaysia", "south africa",
    "nigeria", "kenya", "egypt", "morocco", "philippines", "indonesia",
    "pakistan", "bangladesh", "sri lanka", "ukraine", "russia", "israel",
    "turkey", "uae", "dubai", "abu dhabi", "saudi arabia", "qatar",
    "latin america", "latam", "south america", "central america", "europe",
    "emea", "apac", "anz", "asia", "africa", "middle east", "nordics",
    "benelux", "dach",
    # Additional country names, including endonyms that appear verbatim in ATS
    # location fields ("Deutschland, remote").
    "estonia", "latvia", "lithuania", "slovakia", "slovenia", "croatia",
    "serbia", "bulgaria", "iceland", "luxembourg", "belarus", "moldova",
    "deutschland", "osterreich", "österreich", "espana", "españa", "italia",
    "polska", "nederland", "belgie", "belgië", "belgique", "schweiz", "suisse",
    "sverige", "norge", "danmark", "suomi", "portugal", "eire",
    "armenia", "azerbaijan", "kazakhstan", "uzbekistan", "nepal", "myanmar",
    "cambodia", "mongolia", "ghana", "ethiopia", "tanzania", "uganda",
    "senegal", "cameroon", "angola", "mozambique", "tunisia", "algeria",
    "ecuador", "bolivia", "paraguay", "uruguay", "venezuela", "guatemala",
    "honduras", "nicaragua", "el salvador", "dominican republic",
})

# ISO 3166 alpha-3 codes, which ATS feeds emit in parentheses: "Remote (IND)".
# Matched on the ORIGINAL string requiring upper case, and only after US
# signals have been considered, so a US location always wins.
_ISO3_NON_US = frozenset({
    "IND", "DEU", "GBR", "FRA", "ESP", "ITA", "NLD", "BEL", "CHE", "AUT",
    "SWE", "NOR", "DNK", "FIN", "POL", "PRT", "IRL", "CZE", "ROU", "HUN",
    "GRC", "EST", "LVA", "LTU", "SVK", "SVN", "HRV", "SRB", "BGR", "UKR",
    "RUS", "TUR", "ISR", "ARE", "SAU", "QAT", "EGY", "ZAF", "NGA", "KEN",
    "CAN", "MEX", "BRA", "ARG", "CHL", "COL", "PER", "CRI", "URY",
    "CHN", "JPN", "KOR", "TWN", "HKG", "SGP", "MYS", "THA", "VNM", "PHL",
    "IDN", "PAK", "BGD", "LKA", "AUS", "NZL", "NPL", "KAZ",
})

# Foreign city names. Deliberately NOT decisive: the US has a Dublin (OH), a
# Toronto (OH), a Rome (NY), a Vienna (VA) and a Melbourne (FL), and rejecting
# those cost real applications. A foreign city only settles the question when
# the string carries no US signal at all.
NON_US_CITIES = frozenset({
    "toronto", "vancouver", "montreal", "ottawa", "calgary", "london",
    "dublin", "berlin", "munich", "paris", "madrid", "barcelona", "amsterdam",
    "lisbon", "warsaw", "krakow", "prague", "bucharest", "bangalore",
    "bengaluru", "hyderabad", "mumbai", "delhi", "pune", "chennai", "gurgaon",
    "noida", "sydney", "melbourne", "auckland", "tokyo", "seoul", "shanghai",
    "beijing", "shenzhen", "tel aviv", "sao paulo", "são paulo",
    "buenos aires", "bogota", "bogotá", "mexico city", "guadalajara",
    "manila", "jakarta", "kuala lumpur", "zurich", "stockholm", "copenhagen",
    "oslo", "helsinki", "milan", "rome", "vienna", "brussels", "lima",
    "florianopolis", "florianópolis", "porto alegre", "curitiba", "recife",
    "medellin", "medellín", "cali", "monterrey", "guatemala city", "quito",
    "tallinn", "riga", "vilnius", "sofia", "belgrade", "zagreb", "ljubljana",
    "wroclaw", "wrocław", "gdansk", "gdańsk", "poznan", "poznań", "brno",
    "cluj", "timisoara", "hamburg", "cologne", "köln", "frankfurt",
    "stuttgart", "düsseldorf", "dusseldorf", "valencia", "seville", "porto",
    "rotterdam", "utrecht", "antwerp", "gothenburg", "malmo", "malmö",
    "edinburgh", "glasgow", "bristol", "leeds", "cork", "galway",
})

# Two-letter ISO country codes that appear in the trailing slot of the
# "City, Region, cc" format some ATS feeds emit ("Bogota, DC, co").
_ISO_NON_US = frozenset({
    "co", "in", "il", "ca", "uk", "gb", "de", "fr", "br", "mx", "au", "nz",
    "jp", "cn", "sg", "ie", "es", "it", "nl", "se", "no", "dk", "fi", "pl",
    "pt", "ch", "at", "za", "ng", "ke", "eg", "ae", "sa", "tr", "ru", "ua",
    "ph", "id", "pk", "bd", "lk", "vn", "th", "my", "kr", "tw", "hk", "ar",
    "cl", "pe", "cr", "gr", "hu", "ro", "cz",
})

# ISO codes that are also US state abbreviations, so they cannot settle the
# question on their own.
_ISO_US_STATE_COLLISIONS = frozenset({"ca", "in", "il", "co", "de", "ar", "id"})

_REMOTE_RE = re.compile(r"\bremote\b|\bwork\s+from\s+home\b|\bwfh\b|\bdistributed\b|\banywhere\b",
                        re.IGNORECASE)
_US_WORD_RE = re.compile(
    r"\bunited\s+states\b|\bu\.?\s?s\.?\s?a\.?\b|\busa\b"
    r"|\bu\.\s?s\.?(?![a-z0-9])|\bus\b"
    r"|\bstateside\b|\bnationwide\b|\bcontinental\s+us\b|\bconus\b",
    re.IGNORECASE,
)
_UNKNOWN_RE = re.compile(r"^\s*(unknown(\s+location)?|n/?a|tbd|various|multiple|global|worldwide)?\s*$",
                         re.IGNORECASE)

US = "US"
US_REMOTE = "US_REMOTE"
NON_US = "NON_US"
AMBIGUOUS = "AMBIGUOUS"


@dataclass(frozen=True)
class LocationAnalysis:
    classification: str            # US | US_REMOTE | NON_US | AMBIGUOUS
    normalized_location: str
    reason: str                    # short human-readable explanation
    confidence: str                # high | medium | low
    reasons: tuple[str, ...] = field(default_factory=tuple)

    @property
    def is_us(self) -> bool:
        return self.classification in (US, US_REMOTE)

    @property
    def is_plausibly_us(self) -> bool:
        """True when the job may be US-based — i.e. anything but a confirmed
        non-US location. Callers use this to preserve ambiguous jobs."""
        return self.classification != NON_US


def normalize_location(location: str) -> str:
    loc = (location or "").strip().lower()
    loc = loc.replace("—", "-").replace("–", "-")
    loc = re.sub(r"\s+", " ", loc)
    return loc.strip(" ,-")


def _token_hit(loc: str, phrases) -> str | None:
    for p in phrases:
        if re.search(r"(?<![a-z])" + re.escape(p) + r"(?![a-z])", loc):
            return p
    return None


def _has_state_abbr(loc: str) -> str | None:
    """Match ', ST' — the standard 'City, ST' shape — plus a trailing ' ST'."""
    for m in re.finditer(r"(?:,|\s)\s*([a-z]{2})(?![a-z])", loc):
        if m.group(1) in US_STATE_ABBRS:
            return m.group(1)
    return None


def _normalize_focus(country_focus: str | None) -> str:
    f = (country_focus or "").strip().lower()
    if f in ("us", "usa", "united states", "u.s.", "us only"):
        return "us"
    if f in ("global", "worldwide", "international"):
        return "global"
    return "unknown"


def _iso3_hit(raw: str) -> str | None:
    """Find an upper-case ISO alpha-3 country code in the original string."""
    for m in re.finditer(r"(?<![A-Za-z])([A-Z]{3})(?![A-Za-z])", raw or ""):
        if m.group(1) in _ISO3_NON_US:
            return m.group(1)
    return None


def analyze_location(location: str, country_focus: str | None = None) -> LocationAnalysis:
    """Classify a job location, optionally informed by board metadata.

    `country_focus` comes from the board CSVs ('US' | 'Global' | '') and is used
    only to resolve cases the location string leaves open — it can promote a
    bare "Remote" to US_REMOTE, but it can never override an explicit non-US
    location.
    """
    loc = normalize_location(location)
    focus = _normalize_focus(country_focus)
    is_remote = bool(_REMOTE_RE.search(loc))

    # ── Missing / placeholder location ─────────────────────────────────────
    if not loc or _UNKNOWN_RE.match(loc):
        if focus == "us":
            return LocationAnalysis(
                AMBIGUOUS, loc, "No location given; board is US-focused", "low",
                (R.LOCATION_MISSING, R.US_VIA_BOARD),
            )
        return LocationAnalysis(AMBIGUOUS, loc, "No location given", "low",
                                (R.LOCATION_MISSING,))

    # ── Trailing ISO country-code slot: "Bogota, DC, co" ───────────────────
    # Only meaningful with three or more components, where the last field is a
    # country rather than a US state abbreviation.
    parts = [c.strip() for c in loc.split(",") if c.strip()]
    if len(parts) >= 3 and len(parts[-1]) == 2:
        cc = parts[-1]
        if cc == "us":
            pass  # explicit US country slot — fall through to the US signals
        elif cc in _ISO_NON_US:
            # Seven of these codes are also US state abbreviations (CA, IN, IL,
            # CO, DE, AR, ID). For those, only treat the slot as a country when
            # the city itself is foreign — otherwise "Chinatown, San Francisco,
            # CA" would read as Canada.
            if cc not in _ISO_US_STATE_COLLISIONS or _token_hit(
                parts[0], NON_US_CITIES | NON_US_COUNTRIES
            ):
                return LocationAnalysis(NON_US, loc, f"Non-US country code: {cc}",
                                        "high", (R.NON_US_CONFIRMED,))

    # ── Confirmed non-US country or region wins over everything ────────────
    non_us = _token_hit(loc, NON_US_COUNTRIES)
    if non_us:
        # "Remote - US, Canada" style multi-region postings still include the US.
        us_word = _US_WORD_RE.search(loc)
        if us_word and is_remote:
            return LocationAnalysis(
                US_REMOTE, loc, f"Remote covering the US (also lists {non_us})", "medium",
                (R.US_REMOTE_CONFIRMED,),
            )
        return LocationAnalysis(NON_US, loc, f"Non-US location: {non_us}", "high",
                                (R.NON_US_CONFIRMED,))

    # ── Positive US evidence ───────────────────────────────────────────────
    us_signals: list[str] = []
    if _US_WORD_RE.search(loc):
        us_signals.append("US country name")
    state_abbr = _has_state_abbr(loc)
    if state_abbr:
        us_signals.append(f"state {state_abbr.upper()}")
    state_name = _token_hit(loc, US_STATE_NAMES - _AMBIGUOUS_STATE_NAMES)
    if state_name:
        us_signals.append(f"state {state_name.title()}")
    city = _token_hit(loc, US_CITY_NAMES)
    if city:
        us_signals.append(f"city {city.title()}")
    # Ambiguous state names count only alongside another US signal.
    amb_state = _token_hit(loc, _AMBIGUOUS_STATE_NAMES)
    if amb_state and us_signals:
        us_signals.append(f"state {amb_state.title()}")
    elif amb_state and focus == "us":
        us_signals.append(f"state {amb_state.title()} (US board)")

    if us_signals:
        why = ", ".join(us_signals)
        if is_remote:
            return LocationAnalysis(US_REMOTE, loc, f"US Remote — {why}", "high",
                                    (R.US_REMOTE_CONFIRMED,))
        return LocationAnalysis(US, loc, f"US — {why}", "high", (R.US_CONFIRMED,))

    # ── Foreign city or ISO country code with no US signal anywhere ────────
    foreign_city = _token_hit(loc, NON_US_CITIES)
    if foreign_city:
        return LocationAnalysis(NON_US, loc, f"Non-US city: {foreign_city}", "medium",
                                (R.NON_US_CONFIRMED,))
    iso3 = _iso3_hit(location or "")
    if iso3:
        return LocationAnalysis(NON_US, loc, f"Non-US country code: {iso3}", "high",
                                (R.NON_US_CONFIRMED,))

    # ── Remote with no geographic scope in the string ──────────────────────
    if is_remote:
        if focus == "us":
            return LocationAnalysis(
                US_REMOTE, loc, "Remote on a US-focused board", "medium",
                (R.US_REMOTE_VIA_BOARD,),
            )
        return LocationAnalysis(
            AMBIGUOUS, loc, "Remote with no country scope", "low",
            (R.REMOTE_UNSCOPED,),
        )

    # ── Unrecognised place name ────────────────────────────────────────────
    if focus == "us":
        return LocationAnalysis(
            AMBIGUOUS, loc, "Unrecognised location on a US-focused board", "low",
            (R.LOCATION_AMBIGUOUS, R.US_VIA_BOARD),
        )
    return LocationAnalysis(AMBIGUOUS, loc, "Unrecognised location", "low",
                            (R.LOCATION_AMBIGUOUS,))
