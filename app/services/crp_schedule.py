# app/services/crp_schedule.py

import os
import logging
from functools import lru_cache

import pandas as pd

logger = logging.getLogger(__name__)

# --------------------------------------------------
# Paths
# --------------------------------------------------

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Master workbook that holds CRP / CSP / EQIP tabs
MASTER_XLSX = os.path.join(DATA_DIR, "USDA_CONSERVATION_MASTER.xlsx")

# >>> IMPORTANT <<<
# Change this if your CRP tab is named something else.
# Examples you might have:
#   "payment_schedules_CRP_2025"
#   "CRP_payment_schedules"
CRP_SHEET_NAME = "payment_schedules_CRP_2025"


# --------------------------------------------------
# State normalization
# --------------------------------------------------

_FULL_TO_ABBR = {
    "alabama": "al",
    "alaska": "ak",
    "arizona": "az",
    "arkansas": "ar",
    "california": "ca",
    "colorado": "co",
    "connecticut": "ct",
    "delaware": "de",
    "florida": "fl",
    "georgia": "ga",
    "hawaii": "hi",
    "idaho": "id",
    "illinois": "il",
    "indiana": "in",
    "iowa": "ia",
    "kansas": "ks",
    "kentucky": "ky",
    "louisiana": "la",
    "maine": "me",
    "maryland": "md",
    "massachusetts": "ma",
    "michigan": "mi",
    "minnesota": "mn",
    "mississippi": "ms",
    "missouri": "mo",
    "montana": "mt",
    "nebraska": "ne",
    "nevada": "nv",
    "new hampshire": "nh",
    "new jersey": "nj",
    "new mexico": "nm",
    "new york": "ny",
    "north carolina": "nc",
    "north dakota": "nd",
    "ohio": "oh",
    "oklahoma": "ok",
    "oregon": "or",
    "pennsylvania": "pa",
    "rhode island": "ri",
    "south carolina": "sc",
    "south dakota": "sd",
    "tennessee": "tn",
    "texas": "tx",
    "utah": "ut",
    "vermont": "vt",
    "virginia": "va",
    "washington": "wa",
    "west virginia": "wv",
    "wisconsin": "wi",
    "wyoming": "wy",
}


def normalize_state_key(s: str) -> str:
    """
    Turn 'Alabama', 'AL', ' alAbAmA ' into a canonical key 'al'.

    If the input is already a 2-letter code, we just lowercase it.
    If it's a full name, map it via _FULL_TO_ABBR when possible.
    """
    if s is None:
        return ""
    raw = str(s).strip().lower()
    if len(raw) == 2:
        return raw
    return _FULL_TO_ABBR.get(raw, raw)


def normalize_county_key(s: str) -> str:
    """
    'Autauga', 'AUTAUGA ', 'autauga county' -> 'autauga'
    """
    if s is None:
        return ""
    raw = str(s).strip().lower()
    # drop a trailing " county" if present
    if raw.endswith(" county"):
        raw = raw[: -len(" county")]
    return raw


# --------------------------------------------------
# Loader
# --------------------------------------------------

@lru_cache(maxsize=1)
def _load_crp_df() -> pd.DataFrame:
    """
    Load the CRP schedule from the master workbook and
    add normalized state/county keys.
    """
    try:
        df = pd.read_excel(MASTER_XLSX, sheet_name=CRP_SHEET_NAME)

    except Exception as e:
        logger.warning(
            "WARNING: could not load CRP schedule from %s: %s",
            MASTER_XLSX,
            e,
        )
        return pd.DataFrame()

    # Normalize column names just in case
    df.columns = [str(c).strip() for c in df.columns]

    # Expect at least these columns, matching your screenshot:
    # state, county, crp_practice_code, crp_practice_name,
    # base_rental_rate, signup_incentive_percent,
    # maintenance_payment_rate, contract_length_years, notes
    if "state" not in df.columns or "county" not in df.columns:
        logger.warning(
            "CRP sheet %s missing 'state' or 'county' columns. Columns: %s",
            CRP_SHEET_NAME,
            df.columns.tolist(),
        )
        return pd.DataFrame()

    df["state_key"] = df["state"].apply(normalize_state_key)
    df["county_key"] = df["county"].apply(normalize_county_key)

    return df


# --------------------------------------------------
# Public helpers
# --------------------------------------------------

def get_crp_rows_for_state_county(state: str, county: str) -> pd.DataFrame:
    """
    Return all CRP rows matching a given state + county, using
    normalized keys so that:

      - state='alabama', 'Alabama', 'AL' all match Excel 'Alabama'
      - county='autauga', 'AUTAUGA', 'Autauga County' all match 'Autauga'
    """
    df = _load_crp_df()

    # If we couldn't load anything, or the normalized columns aren't there,
    # just return an empty DataFrame and let the caller handle it.
    if df.empty or "state_key" not in df.columns or "county_key" not in df.columns:
        return pd.DataFrame()

    state_key = normalize_state_key(state)
    county_key = normalize_county_key(county)

    mask = (df["state_key"] == state_key) & (df["county_key"] == county_key)
    return df.loc[mask].copy()



def list_crp_counties_for_state(state: str):
    """
    Return `(state_key, sorted_county_list)` for all counties in CRP for this state.
    """
    df = _load_crp_df()
    if df.empty:
        return None, []

    state_key = normalize_state_key(state)
    mask = df["state_key"] == state_key
    sub = df.loc[mask]

    if sub.empty:
        return None, []

    counties = sorted(sub["county"].dropna().unique().tolist())
    return state_key, counties
