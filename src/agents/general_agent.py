"""
General Query Agent
Handles general/meta questions with intelligent LLM-powered responses
"""
from openai import OpenAI
from typing import Dict, Any, Optional
import json

from src.models.query import QueryClassification
from src.utils.config import get_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


class GeneralAgent:
    """
    Handles general, meta, and conversational queries.
    Uses LLM to generate contextual, helpful responses.
    """
    
    SYSTEM_PROMPT = """You are a helpful AI assistant for the CFG Ukraine Financial Analytics system.
You are part of SALIC's (Saudi Agricultural & Livestock Investment Company) portfolio analytics platform.

About the System:
- This is an Agentic RAG (Retrieval-Augmented Generation) system
- Connected to Microsoft Fabric OneLake (Gold layer) for CFG Ukraine financial data
- Uses Qdrant vector database for semantic search
- Has 4 specialized analytics agents:
  1. Descriptive Agent: Answers "What happened?" questions with historical data, trends, and charts
  2. Diagnostic Agent: Answers "Why did it happen?" questions with root cause analysis
  3. Predictive Agent: Answers "What will happen?" questions with forecasting
  4. Prescriptive Agent: Answers "What should we do?" questions with recommendations

Available Data (CFG Ukraine - Agricultural Company):
- Financial statements (monthly, quarterly, annual)
- Balance sheet, profit & loss, cash flow statements
- Trial balance and GL-level data
- Operational KPIs (production, yield, expenses, volumes)
- Budget, forecast, and long-term plan (LTP)
- Historical data (3-5 years)
- Treasury data (bank positions, payment schedules)

Your Role:
- Answer general questions about the system, its capabilities, and how to use it
- Be friendly, helpful, and conversational
- Guide users on how to ask effective questions
- Provide context about what analytics are available
- Never make up financial data - for actual data questions, guide them to ask specific queries

Response Style:
- Be concise but helpful
- Use bullet points sparingly
- Maintain a professional yet friendly tone
- If asked about specific data, explain you need a more specific query and give examples"""

    USER_PROMPT_TEMPLATE = """User Question: {query}

Please provide a helpful, contextual response. If they're asking about capabilities, explain what the system can do. If they're greeting you, respond naturally. If they're asking how to use the system, provide guidance with examples.

Keep your response concise (2-4 paragraphs max)."""

    # Fallback responses for common general queries (used if LLM fails)
    FALLBACK_RESPONSES = {
        "greeting": """👋 Hello! I'm the CFG Ukraine Financial Analytics Assistant.

I can help you analyze CFG Ukraine's financial and operational data. Just ask me questions like:
- "Show me the EBITDA trend for the last 4 years"
- "Why did gross margin decrease in Q3?"
- "Forecast revenue for next quarter"

What would you like to know?""",

        "capabilities": """I'm an AI-powered financial analytics assistant with 4 specialized capabilities:

📊 **Descriptive Analytics** - I can show you what happened (trends, summaries, historical data)
🔍 **Diagnostic Analytics** - I can explain why something happened (root cause analysis, variance explanations)
🔮 **Predictive Analytics** - I can forecast what will happen (projections, predictions)
💡 **Prescriptive Analytics** - I can recommend what you should do (actionable insights, optimization suggestions)

I have access to CFG Ukraine's financial statements, operational KPIs, budgets, and historical data. What would you like to explore?""",

        "about": """I'm the CFG Ukraine Financial Analytics Assistant, part of SALIC's portfolio analytics platform.

**Technical Stack:**
- Backend: FastAPI with Python
- Data Source: Microsoft Fabric OneLake (Gold layer)
- Vector Database: Qdrant for semantic search
- AI: OpenAI GPT models for natural language understanding

**Data Available:**
- Financial statements (P&L, Balance Sheet, Cash Flow)
- Operational KPIs (production, yield, volumes)
- Budget vs Actual comparisons
- 3-5 years of historical data

Ask me any financial question about CFG Ukraine!""",

        "help": """Here's how to get the most out of this system:

**Ask Specific Questions:**
- ✅ "What was the EBITDA in Q3 2024?"
- ✅ "Why did revenue decrease last month?"
- ❌ "Tell me everything" (too broad)

**Example Queries by Type:**
- **Descriptive:** "Show me the monthly revenue trend for FY24"
- **Diagnostic:** "Explain the variance between actual and budget"
- **Predictive:** "Forecast gross margin for next quarter"
- **Prescriptive:** "How can we improve working capital efficiency?"

What would you like to analyze?""",

        "default": """👋 Welcome to CFG Ukraine Financial Analytics!

I can help you with:

📊 **Descriptive Analytics** - "What happened?"
   Example: "Show me the financial trend for FY24"

🔍 **Diagnostic Analytics** - "Why did it happen?"
   Example: "Why did revenue change in Q3?"

🔮 **Predictive Analytics** - "What will happen?"
   Example: "What will our financials look like next quarter?"

💡 **Prescriptive Analytics** - "What should we do?"
   Example: "What actions should we take to improve performance?"

Try asking a specific question about CFG Ukraine's financial data!"""
    }

    def __init__(self):
        self.settings = get_settings()
        
        if not self.settings.openai_api_key:
            logger.warning("OpenAI API key not configured for GeneralAgent!")
            self.client = None
        else:
            self.client = OpenAI(api_key=self.settings.openai_api_key)
    
    def _detect_intent(self, query: str) -> str:
        """Detect the intent of a general query for fallback responses."""
        query_lower = query.lower().strip()
        
        # Greetings
        if any(word in query_lower for word in ["hello", "hi", "hey", "good morning", "good afternoon", "good evening"]):
            return "greeting"
        
        # Capabilities
        if any(phrase in query_lower for phrase in [
            "what can you", "what do you", "capabilities", "features",
            "what are you able", "how can you help", "what's possible"
        ]):
            return "capabilities"
        
        # About/System info
        if any(phrase in query_lower for phrase in [
            "about yourself", "who are you", "what are you", "tell me about",
            "how do you work", "what system", "what backend", "what data",
            "how are you built", "technology", "stack"
        ]):
            return "about"
        
        # Help
        if any(phrase in query_lower for phrase in [
            "help", "how to use", "how do i", "guide", "tutorial",
            "get started", "examples", "show me how"
        ]):
            return "help"
        
        return "default"
    
    def respond(self, query: str, classification: Optional[QueryClassification] = None) -> Dict[str, Any]:
        """
        Generate a contextual response to a general query.
        
        Args:
            query: User's natural language query
            classification: Optional classification result
            
        Returns:
            Dict with 'answer' and optional metadata
        """
        if not self.client:
            # Use fallback responses
            intent = self._detect_intent(query)
            return {
                "answer": self.FALLBACK_RESPONSES.get(intent, self.FALLBACK_RESPONSES["default"]),
                "sources": [],
                "intent": intent,
            }
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user", "content": self.USER_PROMPT_TEMPLATE.format(query=query)},
                ],
                temperature=0.7,
                max_tokens=500,
            )
            
            answer = response.choices[0].message.content
            
            logger.info(
                "General query answered",
                query=query[:50],
            )
            
            return {
                "answer": answer,
                "sources": [],
                "intent": self._detect_intent(query),
            }
            
        except Exception as e:
            logger.error(f"GeneralAgent LLM call failed: {e}")
            # Fallback to static responses
            intent = self._detect_intent(query)
            return {
                "answer": self.FALLBACK_RESPONSES.get(intent, self.FALLBACK_RESPONSES["default"]),
                "sources": [],
                "intent": intent,
            }


# Entry point for testing
if __name__ == "__main__":
    print("=" * 60)
    print("🤖 General Agent - Test")
    print("=" * 60)
    
    agent = GeneralAgent()
    
    test_queries = [
        "Hello!",
        "What can you do?",
        "Tell me about yourself",
        "How do I use this system?",
        "What data are you using at the backend?",
        "Can you talk to me?",
        "What is your capabilities?",
    ]
    
    for query in test_queries:
        print(f"\n📝 Query: {query}")
        result = agent.respond(query)
        print(f"   Intent: {result.get('intent', 'unknown')}")
        print(f"   Answer: {result['answer'][:100]}...")
    
    print("\n" + "=" * 60)