"""
Query Classification Agent
Classifies user queries into analytics categories
"""
from openai import OpenAI
from typing import List, Optional
import json

from src.models.query import (
    QueryCategory,
    QueryClassification,
    TemporalContext,
    UserQuery,
)
from src.utils.config import get_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


class QueryClassifierAgent:
    """
    Classifies incoming queries into analytics categories.
    Uses OpenAI GPT for classification with structured output.
    """
    
    SYSTEM_PROMPT = """You are a financial analytics query classifier for CFG Ukraine (an agricultural company).

Classify each query into ONE of these categories:

1. GENERAL - Non-analytics questions (greetings, help, system questions)
   Examples: "Hello", "What can you do?", "Help me"

2. DESCRIPTIVE - "What happened?" questions about historical data
   Examples: "Show EBITDA for last 4 years", "What was Q3 revenue?", "Monthly production trend"

3. DIAGNOSTIC - "Why did it happen?" questions seeking root causes
   Examples: "Why did margin decrease?", "What caused revenue drop?", "Explain the variance"

4. PREDICTIVE - "What will happen?" questions about future
   Examples: "Forecast next quarter EBITDA", "Project cash flow", "Expected revenue 2025"

5. PRESCRIPTIVE - "What should we do?" questions seeking recommendations
   Examples: "How to improve margins?", "Recommend cost reductions", "Optimize working capital"

Also extract:
- metrics: Financial/operational metrics mentioned (e.g., EBITDA, revenue, gross_margin, production)
- dimensions: Grouping dimensions (e.g., region, product, quarter, year)
- temporal: Time context (start_period, end_period, granularity like monthly/quarterly/annual)
- comparison_type: If comparing (YoY, QoQ, vs_budget, vs_forecast)

Respond in JSON format only."""

    USER_PROMPT_TEMPLATE = """Classify this query:

Query: {query}

Respond with JSON:
{{
    "category": "general|descriptive|diagnostic|predictive|prescriptive",
    "confidence": 0.0-1.0,
    "metrics": ["list", "of", "metrics"],
    "dimensions": ["list", "of", "dimensions"],
    "temporal": {{
        "start_period": "e.g., 2024-Q1 or null",
        "end_period": "e.g., 2024-Q4 or null",
        "granularity": "monthly|quarterly|annual or null",
        "is_forecast": true/false
    }},
    "comparison_type": "YoY|QoQ|vs_budget|vs_forecast or null",
    "reasoning": "Brief explanation of classification"
}}"""

    def __init__(self):
        self.settings = get_settings()
        
        if not self.settings.openai_api_key:
            logger.warning("OpenAI API key not configured!")
            self.client = None
        else:
            self.client = OpenAI(api_key=self.settings.openai_api_key)
    
    def classify(self, query: str) -> QueryClassification:
        """
        Classify a user query into an analytics category.
        
        Args:
            query: User's natural language query
            
        Returns:
            QueryClassification with category and extracted entities
        """
        if not self.client:
            logger.error("OpenAI client not initialized")
            return self._fallback_classification(query)
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user", "content": self.USER_PROMPT_TEMPLATE.format(query=query)},
                ],
                temperature=0,
                max_tokens=500,
            )
            
            result_text = response.choices[0].message.content
            
            # Parse JSON response
            # Handle potential markdown code blocks
            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0]
            elif "```" in result_text:
                result_text = result_text.split("```")[1].split("```")[0]
            
            result = json.loads(result_text.strip())
            
            # Build classification object
            classification = QueryClassification(
                category=QueryCategory(result["category"]),
                confidence=result.get("confidence", 0.8),
                metrics=result.get("metrics", []),
                dimensions=result.get("dimensions", []),
                temporal=TemporalContext(
                    start_period=result.get("temporal", {}).get("start_period"),
                    end_period=result.get("temporal", {}).get("end_period"),
                    granularity=result.get("temporal", {}).get("granularity"),
                    is_forecast=result.get("temporal", {}).get("is_forecast", False),
                ),
                comparison_type=result.get("comparison_type"),
                reasoning=result.get("reasoning", ""),
            )
            
            logger.info(
                "Query classified",
                query=query[:50],
                category=classification.category.value,
                confidence=classification.confidence,
            )
            
            return classification
            
        except Exception as e:
            logger.error(f"Classification failed: {e}")
            return self._fallback_classification(query)
    
    def _fallback_classification(self, query: str) -> QueryClassification:
        """Simple keyword-based fallback when LLM is unavailable."""
        query_lower = query.lower()
        
        # Simple keyword matching
        if any(word in query_lower for word in ["why", "cause", "reason", "explain", "variance"]):
            category = QueryCategory.DIAGNOSTIC
        elif any(word in query_lower for word in ["forecast", "predict", "project", "expect", "will"]):
            category = QueryCategory.PREDICTIVE
        elif any(word in query_lower for word in ["recommend", "should", "improve", "optimize", "suggest"]):
            category = QueryCategory.PRESCRIPTIVE
        elif any(word in query_lower for word in ["show", "what", "how much", "trend", "history"]):
            category = QueryCategory.DESCRIPTIVE
        else:
            category = QueryCategory.GENERAL
        
        return QueryClassification(
            category=category,
            confidence=0.5,
            reasoning="Fallback keyword-based classification",
        )


# Entry point for testing
if __name__ == "__main__":
    print("=" * 60)
    print("🤖 Query Classifier Agent - Test")
    print("=" * 60)
    
    classifier = QueryClassifierAgent()
    
    # Test queries
    test_queries = [
        "Hello, what can you do?",
        "Show me CFG Ukraine EBITDA trend for the last 4 years",
        "Why did gross margin decrease in Q3 2024?",
        "Forecast EBITDA for the next 12 months",
        "How can we improve working capital efficiency?",
        "What was the revenue in 2023 vs 2022?",
    ]
    
    for query in test_queries:
        print(f"\n📝 Query: {query}")
        result = classifier.classify(query)
        print(f"   Category: {result.category.value}")
        print(f"   Confidence: {result.confidence:.0%}")
        print(f"   Metrics: {result.metrics}")
        print(f"   Reasoning: {result.reasoning}")
    
    print("\n" + "=" * 60)
    