# app/models_crp.py

from typing import List
from pydantic import BaseModel


class CrpPracticeRevenue(BaseModel):
    crp_practice_code: str
    crp_practice_name: str
    base_rental_rate: float
    annual_payment: float
    total_contract_payment: float


class CrpQuoteResponse(BaseModel):
    state: str
    county: str
    acres: float
    practices: List[CrpPracticeRevenue]
