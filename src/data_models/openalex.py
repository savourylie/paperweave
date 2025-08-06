"""
PaperWeave Data Models for OpenAlex Integration

This module contains all data models for working with OpenAlex data and
integrating it with our arXiv knowledge graph in Neo4j.

Models are organized into three main categories:
1. Raw OpenAlex Data Models - For parsing JSON from OpenAlex API/dumps
2. Neo4j Storage Models - Simplified models optimized for graph database storage
3. Mapping and Relationship Models - For connecting arXiv and OpenAlex data
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime


# ============================================================================
# RAW OPENALEX DATA MODELS (for parsing OpenAlex JSON)
# ============================================================================

class OpenAlexInstitution(BaseModel):
    """
    Raw OpenAlex Institution model for parsing JSON data.
    
    Note: OpenAlex uses 'institutions' terminology in their raw data.
    This model preserves the original OpenAlex field names and structure.
    When storing in Neo4j, these will be mapped to 'Organization' nodes
    to match our existing graph schema.
    """
    id: str = Field(..., description="OpenAlex institution ID")
    display_name: str = Field(..., description="Institution display name")
    ror: Optional[str] = Field(None, description="ROR (Research Organization Registry) ID")
    country_code: Optional[str] = Field(None, description="ISO country code")
    type: Optional[str] = Field(None, description="Institution type (facility, education, etc.)")
    lineage: List[str] = Field(default_factory=list, description="Parent institution lineage IDs")


class OpenAlexAuthor(BaseModel):
    """Raw OpenAlex Author model for parsing JSON data"""
    id: str = Field(..., description="OpenAlex author ID")
    display_name: str = Field(..., description="Author display name")
    orcid: Optional[str] = Field(None, description="ORCID identifier")


class OpenAlexAuthorship(BaseModel):
    """Raw OpenAlex Authorship model (author-work relationship)"""
    author_position: str = Field(..., description="Position in author list (first, middle, last)")
    author: OpenAlexAuthor = Field(..., description="Author details")
    institutions: List[OpenAlexInstitution] = Field(default_factory=list, description="Author's institutions")
    countries: List[str] = Field(default_factory=list, description="Institution countries")
    is_corresponding: bool = Field(False, description="Is corresponding author")
    raw_author_name: Optional[str] = Field(None, description="Raw author name from source")
    raw_affiliation_strings: List[str] = Field(default_factory=list, description="Raw affiliation text")


class OpenAlexConcept(BaseModel):
    """Raw OpenAlex Concept model for parsing JSON data"""
    id: str = Field(..., description="OpenAlex concept ID")
    wikidata: Optional[str] = Field(None, description="Wikidata URL")
    display_name: str = Field(..., description="Concept display name")
    level: int = Field(..., description="Concept hierarchy level (0-5)")
    score: float = Field(..., description="Relevance score to work (0-1)")


class OpenAlexTopic(BaseModel):
    """Raw OpenAlex Topic model for parsing JSON data"""
    id: str = Field(..., description="OpenAlex topic ID")
    display_name: str = Field(..., description="Topic display name")
    score: float = Field(..., description="Relevance score to work (0-1)")
    subfield: Optional[Dict[str, str]] = Field(None, description="Subfield information")
    field: Optional[Dict[str, str]] = Field(None, description="Field information")
    domain: Optional[Dict[str, str]] = Field(None, description="Domain information")


class OpenAlexLocation(BaseModel):
    """Raw OpenAlex Location model (where work is hosted)"""
    source: Optional[Dict[str, Any]] = Field(None, description="Source venue information")
    pdf_url: Optional[str] = Field(None, description="PDF URL")
    landing_page_url: Optional[str] = Field(None, description="Landing page URL")
    is_oa: bool = Field(False, description="Is open access")
    version: Optional[str] = Field(None, description="Version type")
    license: Optional[str] = Field(None, description="License")
    doi: Optional[str] = Field(None, description="DOI")


class OpenAlexWork(BaseModel):
    """Raw OpenAlex Work model for parsing JSON data (papers, books, etc.)"""
    id: str = Field(..., description="OpenAlex work ID")
    doi: Optional[str] = Field(None, description="DOI")
    display_name: Optional[str] = Field(None, description="Work title")
    title: Optional[str] = Field(None, description="Work title (alternative field)")
    publication_year: Optional[int] = Field(None, description="Publication year")
    publication_date: Optional[str] = Field(None, description="Publication date")
    language: Optional[str] = Field(None, description="Primary language")
    type: str = Field(..., description="Work type (article, book, etc.)")
    
    # Author and institution information
    authorships: List[OpenAlexAuthorship] = Field(default_factory=list, description="Author information")
    authors_count: int = Field(0, description="Number of authors")
    institutions_distinct_count: int = Field(0, description="Number of distinct institutions")
    countries_distinct_count: int = Field(0, description="Number of distinct countries")
    
    # Citation information
    referenced_works: List[str] = Field(default_factory=list, description="List of cited work IDs")
    referenced_works_count: int = Field(0, description="Number of cited works")
    cited_by_count: int = Field(0, description="Number of times cited")
    
    # Classification information
    concepts: List[OpenAlexConcept] = Field(default_factory=list, description="Associated concepts")
    topics: List[OpenAlexTopic] = Field(default_factory=list, description="Associated topics")
    primary_topic: Optional[OpenAlexTopic] = Field(None, description="Primary topic")
    
    # Publication information
    primary_location: Optional[OpenAlexLocation] = Field(None, description="Primary publication location")
    locations: List[OpenAlexLocation] = Field(default_factory=list, description="All publication locations")
    best_oa_location: Optional[OpenAlexLocation] = Field(None, description="Best open access location")
    
    # Metadata
    is_retracted: bool = Field(False, description="Is retracted")
    is_paratext: bool = Field(False, description="Is paratext")
    updated_date: str = Field(..., description="Last updated timestamp")
    created_date: str = Field(..., description="Creation date")


# ============================================================================
# NEO4J STORAGE MODELS (optimized for graph database)
# ============================================================================

class Organization(BaseModel):
    """
    Organization node for Neo4j storage (matches existing schema).
    
    This model represents how OpenAlex institutions are stored in our Neo4j database.
    Despite OpenAlex calling them 'institutions', our Neo4j schema uses 'Organization'
    nodes, so this model serves as the mapping target for OpenAlexInstitution objects.
    """
    openalex_id: str = Field(..., description="OpenAlex institution ID (source: OpenAlexInstitution.id)")
    name: str = Field(..., description="Organization display name (source: OpenAlexInstitution.display_name)")
    country_code: Optional[str] = Field(None, description="ISO country code")
    type: Optional[str] = Field(None, description="Organization/institution type")
    ror: Optional[str] = Field(None, description="ROR identifier")
    lineage_ids: List[str] = Field(default_factory=list, description="Parent organization lineage")


class AuthorAffiliation(BaseModel):
    """Author-Organization affiliation relationship for Neo4j"""
    author_openalex_id: str = Field(..., description="OpenAlex author ID")
    author_name: str = Field(..., description="Author display name")
    organization_openalex_id: str = Field(..., description="OpenAlex organization ID")
    organization_name: str = Field(..., description="Organization display name")
    is_corresponding: bool = Field(False, description="Is corresponding author")
    position: Optional[str] = Field(None, description="Author position (first, middle, last)")


class Citation(BaseModel):
    """Citation relationship for Neo4j (CITES edge)"""
    citing_work_id: str = Field(..., description="OpenAlex ID of citing work")
    cited_work_id: str = Field(..., description="OpenAlex ID of cited work")
    arxiv_citing_id: Optional[str] = Field(None, description="arXiv ID of citing paper (if available)")
    arxiv_cited_id: Optional[str] = Field(None, description="arXiv ID of cited paper (if available)")


class PaperEnrichment(BaseModel):
    """Additional metadata to enrich existing arXiv papers"""
    arxiv_id: str = Field(..., description="arXiv paper ID")
    openalex_id: str = Field(..., description="Corresponding OpenAlex work ID")
    cited_by_count: int = Field(0, description="Number of times cited")
    institution_names: List[str] = Field(default_factory=list, description="Author institution names")
    country_codes: List[str] = Field(default_factory=list, description="Institution country codes")
    concept_names: List[str] = Field(default_factory=list, description="OpenAlex concept names")
    topic_names: List[str] = Field(default_factory=list, description="OpenAlex topic names")


# ============================================================================
# MAPPING AND RELATIONSHIP MODELS
# ============================================================================

class ArxivOpenAlexMapping(BaseModel):
    """Model for tracking arXiv to OpenAlex mappings"""
    arxiv_id: str = Field(..., description="arXiv paper ID")
    openalex_id: str = Field(..., description="OpenAlex work ID")
    doi: Optional[str] = Field(None, description="DOI used for matching (if available)")
    match_confidence: float = Field(..., description="Confidence score of the match (0-1)")
    match_method: str = Field(..., description="Method used for matching (doi, title_similarity, etc.)")
    verified: bool = Field(False, description="Human verified match")
    created_at: datetime = Field(default_factory=datetime.now, description="When mapping was created")


class ProcessingStats(BaseModel):
    """Statistics for tracking OpenAlex data processing"""
    total_works_processed: int = Field(0, description="Total OpenAlex works processed")
    arxiv_matches_found: int = Field(0, description="Number of arXiv matches found")
    citations_created: int = Field(0, description="Number of citation relationships created")
    institutions_created: int = Field(0, description="Number of institution nodes created")
    affiliations_created: int = Field(0, description="Number of author-institution affiliations created")
    processing_date: datetime = Field(default_factory=datetime.now, description="When processing occurred")
    data_source_date: str = Field(..., description="Date of the OpenAlex data being processed")


# ============================================================================
# UTILITY MODELS FOR DATA PROCESSING  
# ============================================================================

def map_institution_to_organization(institution: OpenAlexInstitution) -> Organization:
    """
    Convert an OpenAlex Institution to a Neo4j Organization node.
    
    This function handles the terminology mapping between OpenAlex's 'institutions' 
    and our Neo4j schema's 'Organization' nodes.
    """
    return Organization(
        openalex_id=institution.id,
        name=institution.display_name,
        country_code=institution.country_code,
        type=institution.type,
        ror=institution.ror,
        lineage_ids=institution.lineage
    )

class DOIMatch(BaseModel):
    """Model for DOI-based matching results"""
    arxiv_id: str = Field(..., description="arXiv paper ID")
    openalex_id: str = Field(..., description="OpenAlex work ID")
    doi: str = Field(..., description="DOI that matched")
    confidence: float = Field(1.0, description="Match confidence (1.0 for exact DOI match)")


class TitleMatch(BaseModel):
    """Model for title-based matching results"""
    arxiv_id: str = Field(..., description="arXiv paper ID")
    openalex_id: str = Field(..., description="OpenAlex work ID")
    arxiv_title: str = Field(..., description="arXiv paper title")
    openalex_title: str = Field(..., description="OpenAlex work title")
    similarity_score: float = Field(..., description="Title similarity score (0-1)")
    confidence: float = Field(..., description="Overall match confidence (0-1)")


class BatchProcessingResult(BaseModel):
    """Result of processing a batch of OpenAlex data"""
    batch_id: str = Field(..., description="Unique identifier for this batch")
    file_path: str = Field(..., description="Path to the processed file")
    total_records: int = Field(..., description="Total records in the file")
    processed_records: int = Field(..., description="Successfully processed records")
    arxiv_matches: int = Field(..., description="Records matched to arXiv papers")
    citations_added: int = Field(..., description="Citation relationships created")
    institutions_added: int = Field(..., description="New institutions created")
    processing_time_seconds: float = Field(..., description="Time taken to process batch")
    errors: List[str] = Field(default_factory=list, description="List of errors encountered")