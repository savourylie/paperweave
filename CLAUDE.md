# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PaperWeave is an AI-powered knowledge graph platform for arXiv papers that transforms research papers into an explorable, connected network. The system includes smart recommendations, team collaboration features, and LLM-powered insights for accelerating scientific discovery.

## Technology Stack

- **Backend**: FastAPI (planned)
- **Frontend**: Next.js (planned) 
- **Database**: Neo4j (implemented with live data pipeline)
- **Data Processing**: Python with pandas, requests, lxml
- **Data Pipeline**: OAI-PMH client with automated daily updates
- **Citation Data**: OpenAlex integration for citation graphs and institutional data
- **Environment**: Python 3.11+ managed with uv

## Development Commands

### Environment Setup
```bash
# Install dependencies (using uv package manager)
uv sync

# Copy environment configuration
cp .env.example .env
# Edit .env with your Neo4j credentials and contact email
```

### Data Pipeline Operations

#### Initial Data Loading
```bash
# Load full arXiv dataset (one-time setup, ~20 minutes)
uv run python src/arxiv_loader_optimized.py all 2000

# Load subset for testing (e.g., 10,000 papers)
uv run python src/arxiv_loader_optimized.py 10000 2000
```

#### Daily Updates
```bash
# Test OAI-PMH connection
uv run python src/oai_pmh_client.py

# Run manual incremental update
uv run python src/arxiv_updater.py

# Test the scheduler
uv run python src/scheduler.py test

# Start daily scheduler daemon
uv run python src/scheduler.py daemon
```

#### OpenAlex Citation Integration
```bash
# PHASE 1: DOI Matching (✅ COMPLETED)
# Test mode - validate with 50K records (~1 minute)
uv run python src/production_openalex_loader.py test

# Production mode - process full 360GB dataset (~13 hours)
# Will prompt for confirmation before starting
uv run python src/production_openalex_loader.py

# PHASE 2: Citation Relationships (✅ COMPLETED)
# Create citation relationships between matched papers
uv run python src/citation_loader.py

# Dataset analysis
uv run python src/quick_dataset_summary.py
```

### Running the Application
```bash
# Run main application
uv run python main.py

# Run Jupyter notebooks for data exploration
uv run jupyter lab notebooks/
```

### Data Management
- **Bulk Dataset**: `data/arxiv-metadata-oai-snapshot.json` (2.69M papers loaded)
- **OpenAlex Dataset**: `data/works/` (360GB+ of citation and institutional data)
- **Daily Updates**: Automated via OAI-PMH (https://oaipmh.arxiv.org/oai)
- **Update Schedule**: Daily at 23:30 ET (configurable via UPDATE_TIME in .env)
- **Performance**: ~2000 papers/second for bulk loading, <1 minute for daily updates

## Architecture

See `architecture.md` for a detailed system architecture diagram and component descriptions.

### Knowledge Graph Schema
The core data model (defined in `src/data_models/PaperWeave.json`) represents:

**Nodes:**
- `Paper` - arXiv papers with properties: arxiv_id, title, abstract, submitter, journal_ref, doi, report_no, license, update_date
  - Enhanced with OpenAlex data: cited_by_count, openalex_id, topics, concepts
- `Author` - paper authors with name property
  - Enhanced with OpenAlex data: openalex_id, orcid (when available)
- `Category` - arXiv subject categories with id and name
- `Organization` - author affiliations from OpenAlex institutional data
  - Properties: openalex_id, name, country_code, type, ror, lineage_ids

**Relationships (Currently Implemented):**
- `WROTE` - authorship (Author → Paper)
- `HAS_CATEGORY` - paper categorization (Paper → Category)
- `CITES` - paper citation relationships (Paper → Paper) - 7.78M citations loaded
- `IS_AFFILIATED_WITH` - author affiliations (Author → Organization)

**Relationships (Future Enhancements):**
- `IS_PART_OF` - organizational hierarchies (Organization → Organization)

### Data Pipeline Components

#### Core Data Processing
- `src/arxiv_loader_optimized.py` - High-performance bulk loader (~2000 papers/sec)
- `src/oai_pmh_client.py` - OAI-PMH client for arXiv API integration
- `src/arxiv_updater.py` - Incremental update engine for daily synchronization
- `src/scheduler.py` - Automated scheduling daemon for daily updates

#### OpenAlex Integration (Production Complete)
- `src/production_openalex_loader.py` - Production OpenAlex DOI matcher (360GB capable)
  - Phase 1 complete: DOI matching with 626K successful assignments
  - Performance: 5,414 works/sec, 7.44 MB/sec on HDD storage
  - Database-side filtering eliminates memory bottlenecks
- `src/citation_loader.py` - Phase 2 citation relationship creator (in development)
- `src/quick_dataset_summary.py` - Fast dataset analysis (927 files, 360GB)
- Legacy optimizers: `src/openalex_loader.py`, `src/ultra_fast_loader.py`, `src/fast_openalex_loader.py`
- `src/data_models/openalex.py` - Comprehensive data models for OpenAlex integration
  - Raw OpenAlex parsing models (preserves original JSON structure)
  - Neo4j storage models (optimized for graph database)
  - Mapping utilities (institution → organization terminology handling)
- `data/works/` - OpenAlex dataset dumps organized by update date (350GB)

#### Supporting Files
- `src/data_models/PaperWeave.json` - Neo4j graph schema definitions
- `.env.example` - Environment configuration template
- `architecture.md` - Detailed system architecture documentation

### Directory Structure
- `src/` - Source code for data pipeline and processing
- `notebooks/` - Jupyter notebooks for data exploration and analysis
- `data/` - Raw arXiv metadata files
- `main.py` - Application entry point

### Documentation
- `architecture.md` - Complete system architecture with mermaid diagrams
- `INCREMENTAL_UPDATE_GUIDE.md` - Detailed explanation of the update system
- `FAIR_USE_COMPLIANCE.md` - arXiv API compliance documentation

## Data Pipeline Workflow

### Initial Setup (One-time)
1. **Environment Setup**: Configure Neo4j connection in `.env`
2. **Bulk Loading**: Import historical data using optimized loader
3. **Constraint Creation**: Ensure data integrity with unique constraints

### Daily Operations (Automated)
1. **OAI-PMH Harvesting**: Fetch new/updated papers since last run
2. **Data Transformation**: Convert OAI metadata to internal schema
3. **Incremental Updates**: Upsert papers, authors, and categories
4. **Relationship Management**: Update author and category relationships
5. **Logging & Monitoring**: Track success metrics and errors

### Performance Characteristics
- **Bulk Loading**: 2000 papers/second, 20 minutes for full dataset
- **Daily Updates**: <1 minute for typical daily volume (100-500 papers)
- **Data Volume**: 2.69M papers with 7.78M citations, growing daily
- **Update Frequency**: Daily at 23:30 ET (when arXiv releases new papers)
- **Citation Coverage**: 577K papers participate in citation network (21% of dataset)

## Current Progress & Integration Status

### ✅ Completed Components
- **arXiv Data Pipeline**: Full OAI-PMH integration with automated daily updates
- **Neo4j Knowledge Graph**: Live database with 2.69M papers, 7.78M citations, authors, and categories
- **Data Models**: Consolidated OpenAlex integration models in `src/data_models/openalex.py`
- **Performance Optimization**: 2000 papers/second bulk loading, <1 minute daily updates

### ✅ COMPLETED: OpenAlex DOI Matching (Phase 1)
- **Status**: Production deployment completed successfully
- **Processing Time**: 13.4 hours for full 360GB dataset (5.4x improvement over initial implementation)
- **Performance**: 5,414 works/sec, 7.44 MB/sec on mechanical HDD storage
- **Results**: 626,366 papers now have both DOI and OpenAlex ID assigned
- **Match Rate**: 0.307% (538K+ total matches from 175M+ DOI records processed)
- **Files Processed**: 927 files across 463 directories successfully completed

### ✅ COMPLETED: Citation Relationships (Phase 2)
- **Status**: Citation graph successfully built with 7,787,968 total citations
- **Coverage**: 577,492 papers participate in citation network (21.4% of total dataset)
- **Approach**: Processed OpenAlex `referenced_works` data to build comprehensive citation graph
- **Performance**: Efficient bulk relationship creation using Neo4j UNWIND operations
- **Result**: Full citation network connecting arXiv papers through OpenAlex data integration

### 🔮 Future Enhancements
- **Citation Updates**: Implement incremental citation updates for new OpenAlex data
- **Entity Resolution**: Advanced matching for author/institution deduplication
- **Citation Completeness**: Handle papers not in arXiv dataset
- **Data Quality**: Validation and conflict resolution for metadata discrepancies
- **Web Interface**: FastAPI backend and Next.js frontend for knowledge graph exploration

### 📝 Design Decisions (MVP)
- **Terminology Mapping**: OpenAlex "institutions" → Neo4j "Organization" nodes
- **Data Source Priority**: OpenAlex data enhances (doesn't replace) arXiv data
- **Matching Strategy**: DOI-based alignment for clean, reliable matching
- **Duplication Handling**: Accept temporary duplication, resolve in future iterations

## Development Notes

- **Database**: Neo4j is fully implemented with live data pipeline
- **Data Pipeline**: Complete OAI-PMH integration with automated daily updates
- **Performance**: Highly optimized for large-scale data processing
- **Monitoring**: Comprehensive logging and error tracking
- **Scalability**: Designed to handle arXiv's full dataset and ongoing growth
- **Core Infrastructure**: FastAPI backend and Next.js frontend remain planned