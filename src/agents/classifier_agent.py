"""
Query Classification Agent
Classifies user queries into analytics categories
"""
from openai import OpenAI
from typing import List, Optional
import json
import re

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

1. GENERAL - Non-analytics questions including:
   - Greetings: "Hello", "Hi", "Good morning"
   - Help/capabilities: "What can you do?", "Help me", "How do you work?"
   - Meta/system questions: "What data do you have?", "What backend are you using?", "How are you built?"
   - Off-topic questions not related to CFG Ukraine financials
   Examples: "Hello", "What can you do?", "Help me", "What data are you using?", "How does this system work?"

2. DESCRIPTIVE - "What happened?" questions about ACTUAL CFG Ukraine historical financial/operational data
   Must reference specific CFG Ukraine metrics, periods, or performance
   Examples: "Show EBITDA for last 4 years", "What was Q3 revenue?", "Monthly production trend", "What is our gross margin?"

3. DIAGNOSTIC - "Why did it happen?" questions seeking root causes for CFG Ukraine performance
   Examples: "Why did margin decrease?", "What caused revenue drop?", "Explain the variance"

4. PREDICTIVE - "What will happen?" questions about CFG Ukraine's future
   Examples: "Forecast next quarter EBITDA", "Project cash flow", "Expected revenue 2025"

5. PRESCRIPTIVE - "What should we do?" questions seeking recommendations for CFG Ukraine
   Examples: "How to improve margins?", "Recommend cost reductions", "Optimize working capital"

IMPORTANT RULES:
- If the query is about the SYSTEM itself (not CFG Ukraine data), classify as GENERAL
- If the query is a greeting or casual conversation, classify as GENERAL
- Only classify as DESCRIPTIVE/DIAGNOSTIC/PREDICTIVE/PRESCRIPTIVE if it's asking about actual CFG Ukraine financial or operational data
- "What data do you have?" → GENERAL (asking about system capabilities)
- "What is our revenue?" → DESCRIPTIVE (asking about actual CFG data)

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

    # Patterns for general/meta questions (checked BEFORE other categories)
    GENERAL_PATTERNS = [
        # Greetings
        r"^(hi|hello|hey|good\s*(morning|afternoon|evening)|greetings)[\s\!\?\.\,]*$",
        r"^(hi|hello|hey)\s+there[\s\!\?\.\,]*$",
        # Help/capabilities
        r"(what can you|what do you|how can you|how do you)\s*(do|help|assist)",
        r"(help|assist)\s*me",
        r"what\s*(are\s+)?(your|the)\s*(capabilities|features|functions)",
        # Meta/system questions
        r"(what|which)\s*(data|database|backend|system|technology|stack)",
        r"how\s*(does|do)\s*(this|the|you|it)\s*(system|work|function)",
        r"(what|how)\s*(are|is)\s*(you|this)\s*(built|made|created|using)",
        r"(tell|explain).*(about|how).*(yourself|this system|you work)",
        r"what\s*(kind of|type of)\s*(data|information|system)",
        r"(where|how)\s*(do|does)\s*(the|your)\s*data\s*(come|from)",
        # Simple question words without financial context
        r"^what[\s\?\!]*$",
        r"^how[\s\?\!]*$",
        r"^why[\s\?\!]*$",
    ]
    
    # Financial/business keywords that indicate actual data queries
    FINANCIAL_KEYWORDS = [
        "ebitda", "revenue", "profit", "margin", "cost", "expense", "income",
        "cash flow", "cashflow", "balance", "asset", "liability", "equity",
        "roi", "roic", "roe", "gross", "net", "operating", "capex", "opex",
        "budget", "actual", "forecast", "variance", "trend", "growth",
        "production", "yield", "volume", "efficiency", "kpi",
        "q1", "q2", "q3", "q4", "fy", "ytd", "mtd", "yoy", "qoq",
        "2020", "2021", "2022", "2023", "2024", "2025",
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
        "quarter", "annual", "monthly", "weekly", "daily",
        "cfg", "ukraine", "department", "segment", "entity",
    ]

    def __init__(self):
        self.settings = get_settings()
        
        if not self.settings.openai_api_key:
            logger.warning("OpenAI API key not configured!")
            self.client = None
        else:
            self.client = OpenAI(api_key=self.settings.openai_api_key)
        
        # Compile regex patterns
        self.general_patterns = [re.compile(p, re.IGNORECASE) for p in self.GENERAL_PATTERNS]
    
    def _is_general_query(self, query: str) -> bool:
        """Check if query is a general/meta question (not about actual data)."""
        query_lower = query.lower().strip()
        
        # Check against general patterns
        for pattern in self.general_patterns:
            if pattern.search(query_lower):
                # But verify it doesn't contain financial keywords
                has_financial_context = any(kw in query_lower for kw in self.FINANCIAL_KEYWORDS)
                if not has_financial_context:
                    return True
        
        return False
    
    def _has_financial_context(self, query: str) -> bool:
        """Check if query contains financial/business keywords."""
        query_lower = query.lower()
        return any(kw in query_lower for kw in self.FINANCIAL_KEYWORDS)
    
    def classify(self, query: str) -> QueryClassification:
        """
        Classify a user query into an analytics category.
        
        Args:
            query: User's natural language query
            
        Returns:
            QueryClassification with category and extracted entities
        """
        # Quick check for obvious general queries
        if self._is_general_query(query):
            logger.info(
                "Query classified as GENERAL (pattern match)",
                query=query[:50],
            )
            return QueryClassification(
                category=QueryCategory.GENERAL,
                confidence=0.95,
                reasoning="Detected as general/meta question (not about CFG Ukraine data)",
            )
        
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
        """Smart keyword-based fallback when LLM is unavailable."""
        query_lower = query.lower().strip()
        
        # First: Check if it's a general/meta question
        if self._is_general_query(query):
            return QueryClassification(
                category=QueryCategory.GENERAL,
                confidence=0.9,
                reasoning="Fallback: Detected as general/meta question",
            )
        
        # Second: Check if query has any financial context
        has_financial = self._has_financial_context(query)
        
        if not has_financial:
            # No financial keywords = likely general question
            return QueryClassification(
                category=QueryCategory.GENERAL,
                confidence=0.7,
                reasoning="Fallback: No financial context detected",
            )
        
        # Third: Classify based on intent keywords (only if financial context exists)
        if any(word in query_lower for word in ["why", "cause", "reason", "explain why", "due to"]):
            category = QueryCategory.DIAGNOSTIC
            confidence = 0.75
        elif any(word in query_lower for word in ["forecast", "predict", "project", "expect", "will be", "next year", "next quarter"]):
            category = QueryCategory.PREDICTIVE
            confidence = 0.75
        elif any(word in query_lower for word in ["recommend", "should we", "improve", "optimize", "suggest", "how to", "how can we"]):
            category = QueryCategory.PRESCRIPTIVE
            confidence = 0.75
        elif any(word in query_lower for word in ["show", "display", "trend", "history", "what was", "what is", "how much"]):
            category = QueryCategory.DESCRIPTIVE
            confidence = 0.7
        else:
            # Has financial context but unclear intent = default to descriptive
            category = QueryCategory.DESCRIPTIVE
            confidence = 0.6
        
        return QueryClassification(
            category=category,
            confidence=confidence,
            reasoning="Fallback keyword-based classification",
        )


# Entry point for testing
if __name__ == "__main__":
    print("=" * 60)
    print("🤖 Query Classifier Agent - Test")
    print("=" * 60)
    
    classifier = QueryClassifierAgent()
    
    # Test queries - including edge cases
    test_queries = [
        # General queries (should be GENERAL)
        "Hello, what can you do?",
        "hi",
        "What data are you using at the backend?",
        "How does this system work?",
        "What kind of data do you have?",
        "Help me",
        
        # Financial queries (should NOT be GENERAL)
        "Show me CFG Ukraine EBITDA trend for the last 4 years",
        "What is our revenue?",
        "Why did gross margin decrease in Q3 2024?",
        "Forecast EBITDA for the next 12 months",
        "How can we improve working capital efficiency?",
        "What was the revenue in 2023 vs 2022?",
    ]
    
    print("\n" + "-" * 60)
    for query in test_queries:
        print(f"\n📝 Query: {query}")
        result = classifier.classify(query)
        print(f"   Category: {result.category.value}")
        print(f"   Confidence: {result.confidence:.0%}")
        print(f"   Reasoning: {result.reasoning}")
    
    print("\n" + "=" * 60)