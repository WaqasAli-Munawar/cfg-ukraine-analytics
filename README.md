# CFG Ukraine Agentic RAG System

🚀 **Enterprise Financial Analytics powered by AI Agents, Microsoft Fabric OneLake, and RAG**

![Python](https://img.shields.io/badge/Python-3.13-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green)
![OpenAI](https://img.shields.io/badge/OpenAI-GPT--4-orange)
![Qdrant](https://img.shields.io/badge/Qdrant-Vector%20DB-purple)

## 🎯 Overview

This system provides intelligent financial analytics for CFG Ukraine using a multi-agent architecture that combines:

- **Microsoft Fabric OneLake** - Real financial data source
- **Qdrant Vector Database** - Semantic search capabilities
- **OpenAI GPT-4** - Natural language understanding
- **4 Specialized AI Agents** - Each handling different analytics types

## 🏗️ Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                     User Query                               │
│              "Show me financial trends"                      │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                 Query Classifier Agent                       │
│            (GPT-4o-mini, 90-95% accuracy)                   │
└─────────────────────┬───────────────────────────────────────┘
                      │
        ┌─────────────┼─────────────┬─────────────┐
        ▼             ▼             ▼             ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ Descriptive  │ │ Diagnostic   │ │ Predictive   │ │ Prescriptive │
│    Agent     │ │    Agent     │ │    Agent     │ │    Agent     │
│"What happened"│ │"Why happened"│ │"What will be"│ │"What to do"  │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │                │                │                │
       └────────────────┴────────────────┴────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                    RAG Retriever                             │
│         (Semantic Search + Structured Data)                  │
└─────────────────────┬───────────────────────────────────────┘
                      │
        ┌─────────────┴─────────────┐
        ▼                           ▼
┌──────────────────┐      ┌──────────────────┐
│  Qdrant Vector   │      │  OneLake Data    │
│    Database      │      │    Service       │
│  (1,590 docs)    │      │  (91,872 rows)   │
└──────────────────┘      └──────────────────┘
```

## 📊 Features

### 4 Analytics Agents

| Agent | Question | Capabilities |
|-------|----------|--------------|
| **Descriptive** | "What happened?" | Trends, summaries, historical data |
| **Diagnostic** | "Why did it happen?" | Variance analysis, root causes |
| **Predictive** | "What will happen?" | Forecasting, projections |
| **Prescriptive** | "What should we do?" | Recommendations, actions |

### Key Capabilities

- 🔍 **Semantic Search** - Find accounts by meaning, not just keywords
- 📈 **Interactive Charts** - Plotly visualizations with every response
- 🔄 **Real-time Data** - Direct connection to OneLake
- 💾 **Smart Caching** - ETag-based change detection
- 🚀 **Fast Response** - 3-5 second end-to-end latency

## 🛠️ Tech Stack

- **Backend**: Python 3.13, FastAPI
- **AI/ML**: OpenAI GPT-4o-mini, text-embedding-3-small
- **Vector DB**: Qdrant
- **Data Source**: Microsoft Fabric OneLake
- **Caching**: Redis
- **Visualization**: Plotly

## 📦 Installation

### Prerequisites

- Python 3.13+
- Docker Desktop
- Azure Service Principal (for OneLake)
- OpenAI API Key

### Setup
```bash
# Clone repository
git clone https://github.com/your-org/cfg-ukraine-analytics.git
cd cfg-ukraine-analytics

# Create virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Copy environment file
copy .env.example .env
# Edit .env with your credentials

# Start Docker services
docker-compose up -d

# Initialize embeddings (first time only)
python -m src.services.embedding_service

# Start API server
python -m src.api.main
```

## 🚀 Usage

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/query/ask` | POST | Process natural language query |
| `/query/classify` | POST | Classify query category |
| `/query/examples` | GET | Get example queries |
| `/query/health` | GET | Check system health |
| `/health` | GET | API health check |

### Example Queries
```bash
# Descriptive
curl -X POST "http://localhost:8000/query/ask" \
  -H "Content-Type: application/json" \
  -d '{"query": "Show me financial trends for FY24"}'

# Diagnostic
curl -X POST "http://localhost:8000/query/ask" \
  -H "Content-Type: application/json" \
  -d '{"query": "Why did revenue change in Q3?"}'

# Predictive
curl -X POST "http://localhost:8000/query/ask" \
  -H "Content-Type: application/json" \
  -d '{"query": "What will our financials look like next quarter?"}'

# Prescriptive
curl -X POST "http://localhost:8000/query/ask" \
  -H "Content-Type: application/json" \
  -d '{"query": "What should we do to improve performance?"}'
```

### Swagger UI

Open http://localhost:8000/docs for interactive API documentation.

## 📊 Data Sources

### OneLake Files (FCCS Folder)

| File | Records | Description |
|------|---------|-------------|
| FCCS_ACTUAL_POWERBI.csv | 91,872 | Actual financial data |
| FCCS_FORECAST_BUDGET_POWERBI.csv | - | Budget/Forecast data |
| FCC_ACCOUNT_BI.csv | 1,377 | Chart of accounts |
| FCC_ENTITY_BI.csv | 58 | Entity master |
| FCC_DEPARTMENT_BI.csv | 155 | Department master |

### Vector Collections (Qdrant)

| Collection | Documents | Purpose |
|------------|-----------|---------|
| cfg_accounts | 1,377 | Account semantic search |
| cfg_entities | 58 | Entity semantic search |
| cfg_departments | 155 | Department semantic search |

## 📈 Performance

| Metric | Value |
|--------|-------|
| Query Classification | 90-95% accuracy |
| End-to-End Latency | 3-5 seconds |
| Embedding Creation | 1,590 docs in ~2 min |
| Cache Hit Rate | ~80% |

## 🔧 Configuration

### Environment Variables
```env
# OpenAI
OPENAI_API_KEY=sk-...

# Azure/OneLake
AZURE_TENANT_ID=...
AZURE_CLIENT_ID=...
AZURE_CLIENT_SECRET=...
ONELAKE_WORKSPACE_ID=...
ONELAKE_LAKEHOUSE_ID=...

# Infrastructure
QDRANT_HOST=localhost
QDRANT_PORT=6333
REDIS_HOST=localhost
REDIS_PORT=6379
```

## 📁 Project Structure
```
cfg-ukraine-analytics/
├── src/
│   ├── agents/
│   │   ├── classifier_agent.py
│   │   ├── descriptive_agent.py
│   │   ├── diagnostic_agent.py
│   │   ├── predictive_agent.py
│   │   └── prescriptive_agent.py
│   ├── api/
│   │   ├── main.py
│   │   └── routes/
│   │       └── query.py
│   ├── connectors/
│   │   └── onelake_connector.py
│   ├── services/
│   │   ├── embedding_service.py
│   │   ├── onelake_data_service.py
│   │   └── rag_retriever.py
│   ├── models/
│   │   └── query.py
│   └── utils/
│       ├── config.py
│       ├── logger.py
│       └── visualizer.py
├── data/
│   └── charts/
├── docker-compose.yml
├── requirements.txt
├── .env
└── README.md
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## 📄 License

MIT License - see LICENSE file for details.

## 👥 Team

- **Developer**: Waqas Ali
- **Organization**: CFG Ukraine

---

**Built with ❤️ using Python, FastAPI, OpenAI, and Microsoft Fabric**