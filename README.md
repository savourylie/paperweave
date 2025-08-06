# 🧬 PaperWeave

**Transform Scientific Discovery Through AI-Powered Knowledge Graphs**

PaperWeave is an advanced knowledge graph platform that transforms millions of arXiv research papers into an explorable, interconnected network. By combining comprehensive data pipelines, citation analysis, and institutional connections, PaperWeave accelerates scientific discovery through intelligent paper recommendations and network analysis.

---

## 🚀 Key Achievements

### 📊 Scale & Performance
- **2.69M Papers** loaded from arXiv with full metadata
- **7.78M Citations** mapped through OpenAlex integration  
- **577K Papers** actively participating in citation network (21% coverage)
- **2000 papers/second** bulk loading performance
- **360GB+ dataset** processing capabilities with optimized pipelines

### 🔗 Data Integration
- **Complete arXiv Pipeline**: Full OAI-PMH integration with automated daily updates
- **OpenAlex Integration**: DOI matching and citation graph construction
- **Institution Data**: Author affiliations and organizational hierarchies
- **Real-time Updates**: Automated daily synchronization with latest papers

---

## 🏗️ Architecture

### Knowledge Graph Schema

**Nodes:**
- **Paper** - arXiv papers with metadata (title, abstract, DOI, categories)
- **Author** - Paper authors with OpenAlex IDs and ORCID when available  
- **Category** - arXiv subject classifications
- **Organization** - Institutional affiliations from OpenAlex

**Relationships:**
- **WROTE** - Author → Paper (authorship)
- **HAS_CATEGORY** - Paper → Category (classification)
- **CITES** - Paper → Paper (7.78M citation relationships)
- **IS_AFFILIATED_WITH** - Author → Organization (institutional ties)

### Technology Stack
- **Database**: Neo4j (graph database optimized for complex relationships)
- **Data Processing**: Python 3.11+ with pandas, requests, lxml
- **Pipeline Management**: OAI-PMH client with automated scheduling
- **Environment**: UV package manager for fast dependency management
- **Future**: FastAPI backend + Next.js frontend (planned)

---

## 🛠️ Installation & Setup

### Prerequisites
- Python 3.11+
- Neo4j Database
- UV package manager

### Quick Start

```bash
# Clone the repository
git clone https://github.com/your-username/paperweave.git
cd paperweave

# Install dependencies
uv sync

# Configure environment
cp .env.example .env
# Edit .env with your Neo4j credentials and email

# Load initial dataset (2.69M papers, ~20 minutes)
uv run python src/arxiv_loader_optimized.py all 2000

# Process OpenAlex citations (360GB dataset, ~13 hours)  
uv run python src/production_openalex_loader.py
uv run python src/citation_loader.py

# Start automated daily updates
uv run python src/scheduler.py daemon
```

---

## 📈 Data Pipeline

### Core Operations

#### Initial Data Loading
```bash
# Full arXiv dataset
uv run python src/arxiv_loader_optimized.py all 2000

# Subset for testing  
uv run python src/arxiv_loader_optimized.py 10000 2000
```

#### Daily Updates (Automated)
```bash
# Manual update
uv run python src/arxiv_updater.py

# Scheduled daemon (runs daily at 23:30 ET)
uv run python src/scheduler.py daemon
```

#### Citation Integration
```bash
# Phase 1: DOI matching ✅ COMPLETED
uv run python src/production_openalex_loader.py

# Phase 2: Citation relationships ✅ COMPLETED  
uv run python src/citation_loader.py

# Dataset analysis
uv run python src/quick_dataset_summary.py
```

### Performance Metrics
- **Bulk Loading**: 2000 papers/second
- **Daily Updates**: <1 minute for typical volume (100-500 papers)
- **OpenAlex Processing**: 5,414 works/sec, 7.44 MB/sec on HDD
- **Citation Coverage**: 21.4% of papers participate in citation network

---

## 📊 Database Statistics

| Metric | Count | Description |
|--------|-------|-------------|
| **Papers** | 2,694,127 | Complete arXiv dataset with metadata |
| **Citations** | 7,787,968 | Paper-to-paper citation relationships |
| **Active Papers** | 577,492 | Papers that cite or are cited by others |
| **Authors** | ~8M+ | Unique authors across all papers |
| **Categories** | 150+ | arXiv subject classifications |
| **Organizations** | ~50K+ | Institutional affiliations |

---

## 🔄 Data Sources

### arXiv Integration
- **Source**: OAI-PMH API (https://oaipmh.arxiv.org/oai)
- **Coverage**: Complete historical dataset + daily updates
- **Compliance**: Full adherence to arXiv API usage guidelines
- **Update Schedule**: Daily at 23:30 ET

### OpenAlex Integration  
- **Source**: OpenAlex dataset (https://openalex.org)
- **Data Size**: 360GB+ of citation and institutional data
- **Processing**: 927 files across 463 directories
- **Matching**: DOI-based alignment for reliable data linking

---

## 🧪 Development & Analysis

### Jupyter Notebooks
```bash
uv run jupyter lab notebooks/
```

Explore data patterns, citation networks, and institutional connections through interactive notebooks.

### Key Scripts
- `src/arxiv_loader_optimized.py` - High-performance bulk loader
- `src/production_openalex_loader.py` - OpenAlex DOI matcher  
- `src/citation_loader.py` - Citation relationship builder
- `src/arxiv_updater.py` - Incremental update engine
- `src/scheduler.py` - Automated update daemon

---

## 🔮 Future Roadmap

### Immediate Enhancements
- **Citation Updates**: Incremental updates for new OpenAlex releases
- **Entity Resolution**: Advanced author/institution deduplication
- **Data Quality**: Validation and conflict resolution pipelines

### Platform Development
- **Web Interface**: FastAPI backend with Neo4j integration
- **Frontend**: Next.js dashboard for knowledge graph exploration
- **API**: RESTful endpoints for paper discovery and citation analysis
- **Recommendations**: ML-powered paper suggestion engine

### Advanced Features
- **Semantic Search**: LLM-powered paper content analysis
- **Collaboration Tools**: Team research workflows and shared collections
- **Analytics Dashboard**: Citation patterns and research trend analysis
- **Export Tools**: Integration with reference managers and research tools

---

## 📄 Documentation

- [`architecture.md`](architecture.md) - Detailed system architecture with diagrams
- [`CLAUDE.md`](CLAUDE.md) - Development guide and commands
- [`INCREMENTAL_UPDATE_GUIDE.md`](INCREMENTAL_UPDATE_GUIDE.md) - Update system details
- [`FAIR_USE_COMPLIANCE.md`](FAIR_USE_COMPLIANCE.md) - arXiv API compliance

---

## 🤝 Contributing

PaperWeave is an open-source project welcoming contributions from the research and development community.

### Areas for Contribution
- **Data Pipeline Optimization** - Improve processing speed and efficiency
- **Citation Analysis** - Advanced network analysis algorithms  
- **Frontend Development** - React/Next.js interface development
- **API Development** - FastAPI backend implementation
- **Documentation** - User guides and API documentation
- **Testing** - Unit tests and integration test suites

### Getting Started
1. Fork the repository
2. Create a feature branch
3. Make your changes with proper documentation
4. Submit a pull request with clear description

---

## 📜 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- **arXiv** for providing open access to scientific literature through their OAI-PMH API
- **OpenAlex** for comprehensive citation and institutional data
- **Neo4j** for powerful graph database capabilities
- **Research Community** for advancing open science and data sharing

---

## 📧 Contact

For questions, suggestions, or collaboration opportunities, please open an issue on GitHub.

**Built with ❤️ for the scientific community**
