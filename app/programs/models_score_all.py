from pydantic import BaseModel
from typing import Optional

from .models_eqip import EqipQuoteResponse
from .crp import CrpQuoteResponse  # adjust if your CRP model file is named differently


class ScoreAllProgramsResponse(BaseModel):
    state: str
    county: str
    acres: float
    crp: Optional[CrpQuoteResponse]
    eqip: Optional[EqipQuoteResponse]
