"""
Query endpoints for analytics
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import time

from src.agents.classifier_agent import QueryClassifierAgent
from src.models.query import QueryClassification, QueryCategory

router = APIRouter(prefix="/query", tags=["Query"])

# Initialize classifier
classifier = QueryClassifierAgent()


class QueryRequest(BaseModel):
    """Request body for query endpoint"""
    query: str
    conversation_id: Optional[str] = None


class QueryResponse(BaseModel):
    """Response body for query endpoint"""
    query: str
    classification: dict
    answer: Optional[str] = None
    sources: List[str] = []
    latency_ms: float


@router.post("/classify", response_model=dict)
async def classify_query(request: QueryRequest):
    """
    Classify a query into analytics categories.
    
    Categories:
    - general: Non-analytics questions
    - descriptive: "What happened?" questions
    - diagnostic: "Why did it happen?" questions
    - predictive: "What will happen?" questions
    - prescriptive: "What should we do?" questions
    """
    start_time = time.time()
    
    try:
        classification = classifier.classify(request.query)
        latency_ms = (time.time() - start_time) * 1000
        
        return {
            "query": request.query,
            "classification": {
                "category": classification.category.value,
                "confidence": classification.confidence,
                "metrics": classification.metrics,
                "dimensions": classification.dimensions,
                "temporal": classification.temporal.model_dump(),
                "comparison_type": classification.comparison_type,
                "reasoning": classification.reasoning,
            },
            "latency_ms": round(latency_ms, 2),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ask", response_model=QueryResponse)
async def ask_question(request: QueryRequest):
    """
    Ask a financial analytics question.
    
    This endpoint:
    1. Classifies the query
    2. Retrieves relevant data (TODO)
    3. Generates an answer (TODO)
    """
    start_time = time.time()
    
    try:
        # Step 1: Classify query
        classification = classifier.classify(request.query)
        
        # Step 2: Generate response based on category
        # TODO: Implement retrieval agents for each category
        if classification.category == QueryCategory.GENERAL:
            answer = "Hello! I'm CFG Ukraine Analytics Assistant. I can help you with financial and operational analytics queries. Try asking about EBITDA, revenue, margins, forecasts, or recommendations!"
        else:
            answer = f"[{classification.category.value.upper()}] I understood your query about {', '.join(classification.metrics) if classification.metrics else 'financial metrics'}. Data retrieval will be implemented in the next step when connected to OneLake."
        
        latency_ms = (time.time() - start_time) * 1000
        
        return QueryResponse(
            query=request.query,
            classification={
                "category": classification.category.value,
                "confidence": classification.confidence,
                "metrics": classification.metrics,
                "reasoning": classification.reasoning,
            },
            answer=answer,
            sources=[],
            latency_ms=round(latency_ms, 2),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))