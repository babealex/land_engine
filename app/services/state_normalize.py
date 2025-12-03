# app/services/state_normalize.py

from typing import Dict


def _abbr_to_name_map() -> Dict[str, str]:
    """
    Internal helper: 2-letter state abbreviation -> full lowercase name.
    """
    return {
        "al": "alabama",
        "ak": "alaska",
        "az": "arizona",
        "ar": "arkansas",
        "ca": "california",
        "co": "colorado",
        "ct": "connecticut",
        "de": "delaware",
        "fl": "florida",
        "ga": "georgia",
        "hi": "hawaii",
        "id": "idaho",
        "il": "illinois",
        "in": "indiana",
        "ia": "iowa",
        "ks": "kansas",
        "ky": "kentucky",
        "la": "louisiana",
        "me": "maine",
        "md": "maryland",
        "ma": "massachusetts",
        "mi": "michigan",
        "mn": "minnesota",
        "ms": "mississippi",
        "mo": "missouri",
        "mt": "montana",
        "ne": "nebraska",
        "nv": "nevada",
        "nh": "new hampshire",
        "nj": "new jersey",
        "nm": "new mexico",
        "ny": "new york",
        "nc": "north carolina",
        "nd": "north dakota",
        "oh": "ohio",
        "ok": "oklahoma",
        "or": "oregon",
        "pa": "pennsylvania",
        "ri": "rhode island",
        "sc": "south carolina",
        "sd": "south dakota",
        "tn": "tennessee",
        "tx": "texas",
        "ut": "utah",
        "vt": "vermont",
        "va": "virginia",
        "wa": "washington",
        "wv": "west virginia",
        "wi": "wisconsin",
        "wy": "wyoming",
    }


def normalize_state(value: str) -> str:
    """
    Normalize any state input or spreadsheet value to full lowercase name,
    e.g. 'MI' -> 'michigan', 'Michigan' -> 'michigan'.
    """

    s = str(value).strip().lower()
    if not s:
        return ""

    abbr_to_name = _abbr_to_name_map()

    # If it's exactly a known full name, keep it
    if s in abbr_to_name.values():
        return s

    # If it's a 2-letter abbreviation, map to full name
    if len(s) == 2 and s in abbr_to_name:
        return abbr_to_name[s]

    # Otherwise just return the cleaned string
    return s


def normalize_county(value: str) -> str:
    """
    Normalize county names:
      'Clare County' -> 'clare'
      ' clare '      -> 'clare'
      'CLARE'        -> 'clare'
    """
    s = str(value).strip().lower()
    if not s:
        return ""
    if s.endswith(" county"):
        s = s[:-7]
    return s.strip()
