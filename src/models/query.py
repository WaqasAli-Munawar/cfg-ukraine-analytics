"""
Query models for CFG Ukraine Analytics
"""
from pydantic import BaseModel, Field
from typing import Optional, List
from enum import Enum


class QueryCategory(str, Enum):
    """Analytics query categories"""
    GENERAL = "general"
    DESCRIPTIVE = "descriptive"      # What happened?
    DIAGNOSTIC = "diagnostic"        # Why did it happen?
    PREDICTIVE = "predictive"        # What will happen?
    PRESCRIPTIVE = "prescriptive"    # What should we do?


class TemporalContext(BaseModel):
    """Time-related context extracted from query"""
    start_period: Optional[str] = None
    end_period: Optional[str] = None
    granularity: Optional[str] = None  # monthly, quarterly, annual
    is_forecast: bool = False


class QueryClassification(BaseModel):
    """Result of query classification"""
    category: QueryCategory
    confidence: float = Field(ge=0, le=1)
    
    # Extracted entities
    metrics: List[str] = Field(default_factory=list)
    dimensions: List[str] = Field(default_factory=list)
    temporal: TemporalContext = Field(default_factory=TemporalContext)
    
    # Query intent
    comparison_type: Optional[str] = None  # YoY, QoQ, vs_budget, vs_forecast
    
    # Reasoning
    reasoning: str = ""


class UserQuery(BaseModel):
    """User query input"""
    text: str
    conversation_id: Optional[str] = None
    user_id: Optional[str] = None