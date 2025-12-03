# app/services/csp_schedule.py

import os
import logging
from functools import lru_cache
from typing import Optional

import pandas as pd

from .state_normalize import normalize_state, normalize_county

logger = logging.getLogger(__name__)

# Base paths: app/ as this file's parent
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # .../land_engine/app
DATA_DIR = os.path.join(BASE_DIR, "data")
MASTER_XLSX = os.path.join(DATA_DIR, "USDA_CONSERVATION_MASTER.xlsx")

CSP_SHEET_NAME = "payment_schedules_CSP_2025"

_csp_df: Optional[pd.DataFrame] = None


@lru_cache(maxsize=1)
def _load_csp_schedule() -> Optional[pd.DataFrame]:
    """
    Lazy-load the CSP schedule from the master Excel file.
    - Normalizes column names to lowercase
    - Adds state_norm / county_norm
    """
    global _csp_df

    if _csp_df is not None:
        return _csp_df

    try:
        df = pd.read_excel(MASTER_XLSX, sheet_name=CSP_SHEET_NAME)
    except Exception as e:
        logger.warning(
            "could not load CSP schedule from %s (sheet=%s): %s",
            MASTER_XLSX,
            CSP_SHEET_NAME,
            e,
        )
        _csp_df = pd.DataFrame()
        return _csp_df

    df.columns = [str(c).strip().lower() for c in df.columns]

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
        logger.warning("CSP schedule is missing columns: %s", missing)

    df["state_norm"] = df["state"].apply(normalize_state)
    df["county_norm"] = df["county"].apply(normalize_county)

    _csp_df = df
    return _csp_df


def get_csp_rows_for_state_county(state: str, county: str) -> Optional[pd.DataFrame]:
    """
    Return all CSP rows for a given (state, county) using normalized keys.
    """
    df = _load_csp_schedule()
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
