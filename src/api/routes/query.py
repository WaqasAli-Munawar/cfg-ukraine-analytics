"""
Query API Routes for CFG Ukraine Agentic RAG
Routes queries to appropriate agents and returns responses with charts
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import time

from src.agents.classifier_agent import QueryClassifierAgent
from src.agents.descriptive_agent import DescriptiveAgent
from src.agents.diagnostic_agent import DiagnosticAgent
from src.agents.predictive_agent import PredictiveAgent
from src.agents.prescriptive_agent import PrescriptiveAgent
from src.agents.general_agent import GeneralAgent  # NEW IMPORT
from src.utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/query", tags=["Query"])


# Request/Response Models
class QueryRequest(BaseModel):
    query: str
    
    class Config:
        json_schema_extra = {
            "example": {
                "query": "Show me the financial trend for FY24"
            }
        }


class QueryResponse(BaseModel):
    query: str
    classification: Dict[str, Any]
    answer: str
    chart: Optional[Dict[str, Any]] = None
    relevant_accounts: Optional[List[Dict[str, Any]]] = None
    sources: List[str] = []
    latency_ms: float


class ClassifyRequest(BaseModel):
    query: str


class ClassifyResponse(BaseModel):
    query: str
    category: str
    confidence: float
    metrics: List[str]
    temporal: Dict[str, Any]
    comparison_type: Optional[str]
    reasoning: str


# Initialize agents (singleton pattern)
_classifier_agent = None
_descriptive_agent = None
_diagnostic_agent = None
_predictive_agent = None
_prescriptive_agent = None
_general_agent = None  # NEW


def get_classifier():
    global _classifier_agent
    if _classifier_agent is None:
        _classifier_agent = QueryClassifierAgent()
    return _classifier_agent


def get_descriptive_agent():
    global _descriptive_agent
    if _descriptive_agent is None:
        _descriptive_agent = DescriptiveAgent()
    return _descriptive_agent


def get_diagnostic_agent():
    global _diagnostic_agent
    if _diagnostic_agent is None:
        _diagnostic_agent = DiagnosticAgent()
    return _diagnostic_agent


def get_predictive_agent():
    global _predictive_agent
    if _predictive_agent is None:
        _predictive_agent = PredictiveAgent()
    return _predictive_agent


def get_prescriptive_agent():
    global _prescriptive_agent
    if _prescriptive_agent is None:
        _prescriptive_agent = PrescriptiveAgent()
    return _prescriptive_agent


def get_general_agent():  # NEW FUNCTION
    global _general_agent
    if _general_agent is None:
        _general_agent = GeneralAgent()
    return _general_agent


@router.post("/classify", response_model=ClassifyResponse)
async def classify_query(request: ClassifyRequest):
    """
    Classify a query into one of the 5 analytics categories.
    
    Categories:
    - **general**: Greetings, help, system questions
    - **descriptive**: "What happened?" - Historical data and trends
    - **diagnostic**: "Why did it happen?" - Root cause analysis
    - **predictive**: "What will happen?" - Forecasting
    - **prescriptive**: "What should we do?" - Recommendations
    """
    try:
        classifier = get_classifier()
        classification = classifier.classify(request.query)
        
        return ClassifyResponse(
            query=request.query,
            category=classification.category.value,
            confidence=classification.confidence,
            metrics=classification.metrics,
            temporal=classification.temporal.model_dump(),
            comparison_type=classification.comparison_type,
            reasoning=classification.reasoning,
        )
    except Exception as e:
        logger.error(f"Classification error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ask", response_model=QueryResponse)
async def ask_query(request: QueryRequest):
    """
    Process a natural language query and return an answer with visualization.
    
    This endpoint:
    1. Classifies the query into a category
    2. Routes to the appropriate agent
    3. Retrieves data from OneLake with semantic search
    4. Generates a response with an interactive chart
    
    Example queries:
    - "Hello, what can you do?" (general)
    - "Show me the financial trend for FY24" (descriptive)
    - "Why did revenue change in Q3?" (diagnostic)
    - "What will our financials look like next quarter?" (predictive)
    - "What should we do to improve performance?" (prescriptive)
    """
    start_time = time.time()
    
    try:
        # Step 1: Classify the query
        classifier = get_classifier()
        classification = classifier.classify(request.query)
        
        logger.info(
            "Query classified",
            query=request.query,
            category=classification.category.value,
            confidence=classification.confidence,
        )
        
        # Step 2: Route to appropriate agent
        answer = ""
        chart = None
        relevant_accounts = []
        sources = ["Microsoft Fabric OneLake", "Qdrant Vector Database"]
        
        if classification.category.value == "descriptive":
            agent = get_descriptive_agent()
            data = agent.retrieve(classification)
            answer = agent.format_response(data, classification)
            chart = data.get('chart')
            relevant_accounts = data.get('relevant_accounts', [])
            
        elif classification.category.value == "diagnostic":
            agent = get_diagnostic_agent()
            data = agent.retrieve(classification)
            answer = agent.format_response(data, classification)
            chart = data.get('chart')
            relevant_accounts = data.get('relevant_accounts', [])
            
        elif classification.category.value == "predictive":
            agent = get_predictive_agent()
            data = agent.retrieve(classification)
            answer = agent.format_response(data, classification)
            chart = data.get('chart')
            relevant_accounts = data.get('relevant_accounts', [])
            
        elif classification.category.value == "prescriptive":
            agent = get_prescriptive_agent()
            data = agent.retrieve(classification)
            answer = agent.format_response(data, classification)
            chart = data.get('chart')
            relevant_accounts = data.get('relevant_accounts', [])
            
        else:
            # General query - use GeneralAgent for contextual responses
            agent = get_general_agent()
            result = agent.respond(request.query, classification)
            answer = result["answer"]
            sources = result.get("sources", [])
            # No chart for general queries
        
        # Calculate latency
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
            chart=chart,
            relevant_accounts=relevant_accounts[:5] if relevant_accounts else None,
            sources=sources,
            latency_ms=round(latency_ms, 2),
        )
        
    except Exception as e:
        logger.error(f"Query processing error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/examples")
async def get_example_queries():
    """
    Get example queries for each analytics category.
    """
    return {
        "general": [
            "Hello, what can you do?",
            "Tell me about yourself",
            "How do I use this system?",
            "What data do you have access to?",
        ],
        "descriptive": [
            "Show me the financial trend for FY24",
            "What was the total amount in 2024?",
            "Display monthly financial data",
            "Show me cash-related accounts trend",
        ],
        "diagnostic": [
            "Why did revenue change in Q3?",
            "Explain the variance in September",
            "What caused the amount change month over month?",
            "Analyze the factors behind the Q4 performance",
        ],
        "predictive": [
            "What will our financials look like next quarter?",
            "Forecast the trend for FY25",
            "Predict the next 3 months performance",
            "What's the projected growth rate?",
        ],
        "prescriptive": [
            "What should we do to improve performance?",
            "Give me recommendations for next quarter",
            "What actions should we take based on recent trends?",
            "How can we optimize our financial position?",
        ],
    }


@router.get("/health")
async def query_health():
    """
    Check health of query processing components.
    """
    try:
        classifier = get_classifier()
        
        return {
            "status": "healthy",
            "components": {
                "classifier": "ready",
                "general_agent": "ready",  # NEW
                "descriptive_agent": "ready",
                "diagnostic_agent": "ready",
                "predictive_agent": "ready",
                "prescriptive_agent": "ready",
            },
            "data_sources": {
                "onelake": "connected",
                "qdrant": "connected",
            },
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
        }