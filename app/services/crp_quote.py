# app/services/crp_quote.py

import math
from typing import List

import pandas as pd

from ..programs.models_crp import CrpPracticeRevenue, CrpQuoteResponse
from .crp_schedule import get_crp_rows_for_state_county  # <-- reuse the schedule loader


def _safe_float(value, default: float = 0.0) -> float:
    """
    Convert value to float, but:
      - if it's NaN, inf, or invalid, return default.
    """
    try:
        x = float(value)
    except (TypeError, ValueError):
        return default

    if math.isnan(x) or math.isinf(x):
        return default

    return x


def quote_crp(state: str, county: str, acres: float) -> CrpQuoteResponse:
    """
    Return CRP rental revenue estimates for a given state/county/acres.

    Uses the normalized/ cached CRP schedule from crp_schedule.get_crp_rows_for_state_county,
    so inputs like 'MI' vs 'Michigan' and 'Clare' vs 'Clare County' all work.
    """

    df: pd.DataFrame = get_crp_rows_for_state_county(state, county)

    # If no rows, return empty quote (FastAPI decides whether to 404).
    if df is None or df.empty:
        return CrpQuoteResponse(
            state=str(state).strip().lower(),
            county=str(county).strip().lower(),
            acres=acres,
            practices=[],
        )

    # Normalize the column names just in case
    df.columns = [str(c).strip().lower() for c in df.columns]

    practices: List[CrpPracticeRevenue] = []

    for _, row in df.iterrows():
        base_rate = _safe_float(row.get("base_rental_rate", 0.0), default=0.0)

        # Default contract length to 10 if missing/invalid
        contract_years_raw = _safe_float(row.get("contract_length_years", 10), default=10.0)
        contract_years = int(contract_years_raw) or 10

        annual_payment = base_rate * acres
        total_contract_payment = annual_payment * contract_years

        practices.append(
            CrpPracticeRevenue(
                crp_practice_code=str(row.get("crp_practice_code", "")),
                crp_practice_name=str(row.get("crp_practice_name", "")),
                base_rental_rate=base_rate,
                annual_payment=annual_payment,
                total_contract_payment=total_contract_payment,
            )
        )

    # Sort highest annual payment first
    practices.sort(key=lambda p: p.annual_payment, reverse=True)

    return CrpQuoteResponse(
        state=str(state).strip().lower(),
        county=str(county).strip().lower(),
        acres=acres,
        practices=practices,
    )
