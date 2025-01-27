from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class WordCloudItem(BaseModel):
    company: str
    word: str
    count: int
    date: str | None = None
    
class AllWordCloudData(BaseModel):
    data: List[WordCloudItem]
    
class Companies(BaseModel):
    companies: List[str]
    
    
class WordCount(BaseModel):
    word: str
    count: int

class TopWordsResponse(BaseModel):
    company: str
    top_words: List[WordCount]
    
class BigramCount(BaseModel):
    bigram: str
    count: int

class TopBigramsResponse(BaseModel):
    company: str
    top_bigrams: List[BigramCount]

class TrigramCount(BaseModel):
    trigram: str
    count: int

class TopTrigramsResponse(BaseModel):
    company: str
    top_trigrams: List[TrigramCount]

class Summary(BaseModel):
    str

class SourceSummary(BaseModel):
    positive: Summary
    negative: Summary
    neutral: Summary
    
class InfoSummary(BaseModel):
    from_date: datetime
    

class SummaryModel(BaseModel):
    company: str
    info: InfoSummary
    youtube: SourceSummary
    reddit: SourceSummary
    trustpilot: SourceSummary
    

class LastComment(BaseModel):
    reddit: Optional[str] = None
    trustpilot: Optional[str] = None
    youtube: Optional[str] = None