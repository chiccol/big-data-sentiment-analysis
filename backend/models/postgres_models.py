from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class AggregatedPostgresData(BaseModel):
    date: datetime
    reddit: Optional[float] = None
    trustpilot: Optional[float] = None
    youtube: Optional[float] = None

class AggregatedPostgresResponse(BaseModel):
    aggregated_data: List[AggregatedPostgresData]

class SuperAggregatedPostgresData(BaseModel):
    date: datetime
    # company: str
    score: Optional[float] = None

class SuperAggregatedPostgresResponse(BaseModel):
    aggregated_data: List[SuperAggregatedPostgresData]
