"""
Query endpoints for analytics
NOW WITH CHART DATA IN RESPONSES
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import time

from src.agents.classifier_agent import QueryClassifierAgent
from src.agents.descriptive_agent import DescriptiveAgent
from src.agents.diagnostic_agent import DiagnosticAgent
from src.models.query import QueryClassification, QueryCategory

router = APIRouter(prefix="/query", tags=["Query"])

# Initialize agents
classifier = QueryClassifierAgent()
descriptive_agent = DescriptiveAgent()
diagnostic_agent = DiagnosticAgent()


class QueryRequest(BaseModel):
    """Request body for query endpoint"""
    query: str
    conversation_id: Optional[str] = None


class QueryResponse(BaseModel):
    """Response body for query endpoint"""
    query: str
    classification: Dict[str, Any]
    answer: str
    chart: Optional[Dict[str, Any]] = None  # NEW: Chart JSON
    sources: List[str] = []
    latency_ms: float


@router.post("/classify", response_model=Dict[str, Any])
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
    2. Routes to appropriate agent
    3. Retrieves relevant data
    4. Generates text answer + chart
    """
    start_time = time.time()
    
    try:
        # Step 1: Classify query
        classification = classifier.classify(request.query)
        
        # Step 2: Route to appropriate agent and get response + chart
        if classification.category == QueryCategory.GENERAL:
            answer = "Hello! I'm CFG Ukraine Analytics Assistant. I can help you with:\n\n" \
                    "📊 **Descriptive Analytics** - \"What happened?\" (trends, historical data)\n" \
                    "🔍 **Diagnostic Analytics** - \"Why did it happen?\" (variance analysis, root causes)\n" \
                    "📈 **Predictive Analytics** - \"What will happen?\" (forecasts, projections)\n" \
                    "💡 **Prescriptive Analytics** - \"What should we do?\" (recommendations, optimization)\n\n" \
                    "Try asking questions like:\n" \
                    "- Show me EBITDA trend for last 4 years\n" \
                    "- Why did revenue decrease in Q3?\n" \
                    "- Forecast cash flow for next quarter"
            chart = None
            
        elif classification.category == QueryCategory.DESCRIPTIVE:
            # Descriptive agent returns text + line/bar chart
            data = descriptive_agent.retrieve(classification)
            answer = descriptive_agent.format_response(data, classification)
            chart = data.get('chart')  # NEW: Include chart JSON
            
        elif classification.category == QueryCategory.DIAGNOSTIC:
            # Diagnostic agent returns text + waterfall chart
            data = diagnostic_agent.retrieve(classification)
            answer = diagnostic_agent.format_response(data, classification)
            chart = data.get('chart')  # NEW: Include waterfall chart JSON
            
        elif classification.category == QueryCategory.PREDICTIVE:
            answer = f"[PREDICTIVE] Forecast analysis for {', '.join(classification.metrics) if classification.metrics else 'key metrics'}.\n\n" \
                    "⚠️ Predictive agent coming soon! This will include:\n" \
                    "- Time series forecasting\n" \
                    "- Trend projections\n" \
                    "- Confidence intervals\n" \
                    "- Scenario analysis"
            chart = None
            
        elif classification.category == QueryCategory.PRESCRIPTIVE:
            answer = f"[PRESCRIPTIVE] Recommendations for optimizing {', '.join(classification.metrics) if classification.metrics else 'performance'}.\n\n" \
                    "⚠️ Prescriptive agent coming soon! This will include:\n" \
                    "- Action recommendations\n" \
                    "- Optimization strategies\n" \
                    "- Risk mitigation plans\n" \
                    "- Expected impact analysis"
            chart = None
            
        else:
            answer = "I couldn't determine how to handle this query. Please try rephrasing."
            chart = None
        
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
            chart=chart,  # NEW: Chart data included
            sources=[],
            latency_ms=round(latency_ms, 2),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/examples")
async def get_example_queries():
    """
    Get example queries for each analytics category.
    """
    return {
        "examples": {
            "descriptive": [
                "Show me CFG Ukraine EBITDA trend for the last 4 years",
                "What was the revenue in 2024?",
                "Display gross margin for the last 8 quarters",
                "Compare revenue between 2023 and 2024",
            ],
            "diagnostic": [
                "Why did revenue drop in Q3 2024?",
                "Explain the EBITDA variance in Q2",
                "What caused the gross margin decrease?",
                "Analyze the revenue variance vs budget",
            ],
            "predictive": [
                "Forecast EBITDA for the next 12 months",
                "Project cash flow for Q1 2025",
                "What will revenue be in 2025?",
                "Predict gross margin trend",
            ],
            "prescriptive": [
                "How can we improve working capital efficiency?",
                "Recommend ways to increase EBITDA margin",
                "What should we do to reduce costs?",
                "Suggest strategies to optimize cash flow",
            ],
        }
    }