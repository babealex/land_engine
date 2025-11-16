# programs/crp.py

from pathlib import Path
import csv
from typing import Dict, Tuple

# Load CRP base rates from CSV: state+county -> base_rate_per_acre
CRP_RATES_PATH = Path(__file__).parent / "crp_rates.csv"

_crp_rate_cache: Dict[Tuple[str, str], float] = {}

def load_crp_rates() -> None:
    global _crp_rate_cache
    _crp_rate_cache.clear()
    with CRP_RATES_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            state = row["state"].strip().upper()
            county = row["county"].strip().upper()
            rate = float(row["base_rate_per_acre"])
            _crp_rate_cache[(state, county)] = rate

# Load once at import
load_crp_rates()

def get_crp_rate(state: str, county: str) -> float:
    """
    Return the CRP base rental rate ($/acre/year) for a given state+county.
    Raises KeyError if not found.
    """
    key = (state.strip().upper(), county.strip().upper())
    return _crp_rate_cache[key]

def estimate_crp_payments(
    state: str,
    county: str,
    acres: float,
    practice_multiplier: float = 1.0,
) -> Tuple[float, float, int]:
    """
    Estimate CRP Year-1 and annual payments for a parcel based on:
    - county base rate (from CRP_RATES)
    - acres
    - practice multiplier (1.0 generic, >1.0 for more lucrative practices)
    
    Returns: (year1_payout, annual_payout, contract_years)
    """
    if acres <= 0:
        return 0.0, 0.0, 0

    base_rate = get_crp_rate(state, county)  # $/acre/year
    annual = base_rate * practice_multiplier * acres  # core CRP rent

    # Simple model for incentives:
    signup_bonus = 0.5 * annual  # Year-1 signing incentive
    cost_share = 200.0 * acres   # assume $400/acre restoration, 50% cost-share

    year1 = annual + signup_bonus + cost_share

    contract_years = 15  # typical contract length; can adjust later

    return year1, annual, contract_years
