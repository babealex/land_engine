from pydantic import BaseModel
from typing import List

class EqipPracticeQuote(BaseModel):
    practice_code: str
    scenario_code: str
    scenario_name: str
    unit: str
    payment_type: str
    unit_rate: float
    payment_basis: str  # "per_acre" or "flat"
    payment_per_acre: float  # 0 for non-per-acre practices
    annual_payment: float
    total_contract_payment: float
    contract_years: int

class EqipQuoteResponse(BaseModel):
    state: str
    county: str
    acres: float
    practices: List[EqipPracticeQuote]
