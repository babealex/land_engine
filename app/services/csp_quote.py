# app/services/csp_quote.py

from typing import List

import pandas as pd

from .csp_schedule import get_csp_rows_for_state_county
from .eqip_schedule import PER_ACRE_UNITS  # reuse unit logic
from ..programs.models_csp import CspPracticeQuote, CspQuoteResponse

# For now, treat CSP payments as 1-contract, one-time payments.
DEFAULT_CONTRACT_YEARS = 1


def _safe_float(value, default: float = 0.0) -> float:
    try:
        x = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(x):
        return default
    return x


def quote_csp(state: str, county: str, acres: float) -> CspQuoteResponse:
    """
    Convert CSP payment schedule rows into revenue numbers
    for a given (state, county, acres).
    Same unit rules as EQIP.
    """
    rows = get_csp_rows_for_state_county(state, county)

    if rows is None or rows.empty:
        return CspQuoteResponse(
            state=state.lower(),
            county=county.lower(),
            acres=acres,
            practices=[],
        )

    rows = rows.copy()
    rows.columns = [str(c).strip().lower() for c in rows.columns]

    practices: List[CspPracticeQuote] = []

    for _, row in rows.iterrows():
        unit = str(row.get("unit", "")).strip().lower()
        unit_rate = _safe_float(row.get("unit_rate", 0.0), default=0.0)

        if unit in PER_ACRE_UNITS:
            payment_basis = "per_acre"
            payment_per_acre = unit_rate
            annual_payment = payment_per_acre * acres
        else:
            payment_basis = "flat"
            payment_per_acre = 0.0
            annual_payment = unit_rate

        total_contract_payment = annual_payment * DEFAULT_CONTRACT_YEARS

        practices.append(
            CspPracticeQuote(
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

    return CspQuoteResponse(
        state=state.lower(),
        county=county.lower(),
        acres=acres,
        practices=practices,
    )
