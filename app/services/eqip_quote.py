# app/services/eqip_quote.py

from typing import List

import pandas as pd

from .eqip_schedule import get_eqip_rows_for_state_county, PER_ACRE_UNITS
from ..programs.models_eqip import EqipPracticeQuote, EqipQuoteResponse

DEFAULT_CONTRACT_YEARS = 1  # one-year payments unless spreadsheet says otherwise


def _safe_float(value, default: float = 0.0) -> float:
    try:
        x = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(x):
        return default
    return x


def quote_eqip(state: str, county: str, acres: float) -> EqipQuoteResponse:
    """
    EQIP revenue engine using normalized state+county indexing.
    - If unit in PER_ACRE_UNITS → per-acre payment * acres
    - Otherwise → flat payment
    """

    rows = get_eqip_rows_for_state_county(state, county)

    # No rows found → return empty (FastAPI decides when to 404)
    if rows is None or rows.empty:
        return EqipQuoteResponse(
            state=state.lower(),
            county=county.lower(),
            acres=acres,
            practices=[],
        )

    rows = rows.copy()
    rows.columns = [str(c).strip().lower() for c in rows.columns]

    practices: List[EqipPracticeQuote] = []

    for _, row in rows.iterrows():
        unit = str(row.get("unit", "")).strip().lower()
        unit_rate = _safe_float(row.get("unit_rate", 0.0))

        if unit in PER_ACRE_UNITS:
            payment_basis = "per_acre"
            payment_per_acre = unit_rate
            annual_payment = unit_rate * acres
        else:
            payment_basis = "flat"
            payment_per_acre = 0.0
            annual_payment = unit_rate

        total_contract_payment = annual_payment * DEFAULT_CONTRACT_YEARS

        practices.append(
            EqipPracticeQuote(
                practice_code=str(row.get("practice_code", "")),
                scenario_code=str(row.get("scenario_code", "")),
                scenario_name=str(row.get("scenario_name", "")),
                unit=str(row.get("unit", "")),
                payment_type=str(row.get("payment_type", "")),
                unit_rate=unit_rate,
                payment_basis=payment_basis,
                payment_per_acre=payment_per_acre,
                annual_payment=annual_payment,
                total_contract_payment=total_contract_payment,
                contract_years=DEFAULT_CONTRACT_YEARS,
            )
        )

    practices.sort(key=lambda p: p.annual_payment, reverse=True)

    return EqipQuoteResponse(
        state=state.lower(),
        county=county.lower(),
        acres=acres,
        practices=practices,
    )
