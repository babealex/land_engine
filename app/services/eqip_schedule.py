# app/services/eqip_schedule.py

import os
import logging
from functools import lru_cache
from typing import Optional, Set

import pandas as pd

from .state_normalize import normalize_state, normalize_county

logger = logging.getLogger(__name__)

# Base paths: app/ as this file's parent
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # .../land_engine/app
DATA_DIR = os.path.join(BASE_DIR, "data")
MASTER_XLSX = os.path.join(DATA_DIR, "USDA_CONSERVATION_MASTER.xlsx")

EQIP_SHEET_NAME = "payment_schedules_EQIP_2025"

# Units that should be treated as per-acre
PER_ACRE_UNITS: Set[str] = {
    "ac",
    "acre",
    "acres",
    "ac.",
    "ac/yr",
    "acr",
}

_eqip_df: Optional[pd.DataFrame] = None


@lru_cache(maxsize=1)
def _load_eqip_schedule() -> Optional[pd.DataFrame]:
    """
    Lazy-load the EQIP schedule from the master Excel file.
    - Normalizes column names to lowercase
    - Adds state_norm / county_norm using normalize_state / normalize_county
    """
    global _eqip_df

    if _eqip_df is not None:
        return _eqip_df

    try:
        df = pd.read_excel(MASTER_XLSX, sheet_name=EQIP_SHEET_NAME)
    except Exception as e:
        logger.warning(
            "could not load EQIP schedule from %s (sheet=%s): %s",
            MASTER_XLSX,
            EQIP_SHEET_NAME,
            e,
        )
        _eqip_df = pd.DataFrame()
        return _eqip_df

    # Normalize column names
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Sanity-check required columns
    required = [
        "state",
        "county",
        "practice_code",
        "scenario_code",
        "scenario_name",
        "unit",
        "payment_type",
        "unit_rate",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        logger.warning("EQIP schedule is missing columns: %s", missing)

    # Add normalized keys
    df["state_norm"] = df["state"].apply(normalize_state)
    df["county_norm"] = df["county"].apply(normalize_county)

    _eqip_df = df
    return _eqip_df


def get_eqip_rows_for_state_county(state: str, county: str) -> Optional[pd.DataFrame]:
    """
    Return all EQIP rows for a given (state, county), using normalized keys.
    Accepts 'MI' vs 'Michigan', 'Clare' vs 'Clare County', etc.
    """
    df = _load_eqip_schedule()
    if df is None or df.empty:
        return None

    state_key = normalize_state(state)
    county_key = normalize_county(county)

    subset = df[
        (df["state_norm"] == state_key) &
        (df["county_norm"] == county_key)
    ].copy()

    if subset.empty:
        return None

    return subset
