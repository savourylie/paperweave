"""
PHASE 2: OpenAlex Citation Relationship Creator

After Phase 1 success (626K papers with OpenAlex IDs), this creates citation relationships
by processing the same OpenAlex dataset and extracting referenced_works data.

🎯 PHASE 2 OBJECTIVES:
- Process OpenAlex referenced_works from 360GB dataset
- Create CITES relationships between papers in Neo4j
- Much higher match rates (OpenAlex ID → OpenAlex ID matching)
- Build the citation graph foundation for PaperWeave

🚀 PERFORMANCE OPTIMIZATIONS:
- Pre-load 626K OpenAlex IDs for fast lookup
- Bulk relationship creation with UNWIND
- Comprehensive progress tracking
- Database-side filtering for maximum efficiency
"""

import json
import gzip
import logging
import time
from pathlib import Path
from typing import Dict, Set, List, Optional, Tuple
from datetime import datetime
import os
from neo4j import GraphDatabase
from pydantic import BaseModel

from data_models.openalex import OpenAlexWork

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CitationStats(BaseModel):
    """Citation processing statistics"""
    files_processed: int = 0
    total_works_processed: int = 0
    works_with_references: int = 0
    total_reference_pairs: int = 0
    successful_citations: int = 0
    processing_time_seconds: float = 0.0
    works_per_second: float = 0.0
    citations_per_second: float = 0.0
    data_processed_mb: float = 0.0

class CitationLoader:
    """Phase 2: Create citation relationships using OpenAlex IDs"""
    
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        # Optimized driver for citation processing
        self.driver = GraphDatabase.driver(
            neo4j_uri, 
            auth=(neo4j_user, neo4j_password),
            max_connection_lifetime=3600,
            max_connection_pool_size=50,
            connection_acquisition_timeout=60
        )
        self.data_dir = Path("data/works")
        self.neo4j_openalex_ids = None  # Will cache our 626K OpenAlex IDs
        self._prepare_citation_database()
        
    def close(self):
        """Close Neo4j connection"""
        self.driver.close()
    
    def _prepare_citation_database(self):
        """Prepare database for citation processing"""
        logger.info("🔧 Preparing database for Phase 2 citation processing...")
        
        with self.driver.session() as session:
            # Ensure citation relationship index exists
            session.run("CREATE INDEX paper_openalex_id_index IF NOT EXISTS FOR (p:Paper) ON (p.openalex_id)")
            
            # Get count of papers with OpenAlex IDs (our Phase 1 results)
            result = session.run("""
                MATCH (p:Paper) 
                WHERE p.openalex_id IS NOT NULL AND p.openalex_id <> ''
                RETURN count(p) as papers_with_openalex_id
            """)
            papers_with_openalex = result.single()["papers_with_openalex_id"]
            
            # Check existing citation relationships
            result = session.run("MATCH ()-[r:CITES]->() RETURN count(r) as existing_citations")
            existing_citations = result.single()["existing_citations"]
            
            logger.info(f"📊 Phase 2 Database Status:")
            logger.info(f"  Papers with OpenAlex ID: {papers_with_openalex:,}")
            logger.info(f"  Existing citations: {existing_citations:,}")
            logger.info(f"  Ready for citation creation: {papers_with_openalex > 600000}")
            
        logger.info("✅ Database ready for Phase 2 citation processing")
    
    def _load_neo4j_openalex_ids(self) -> Set[str]:
        """Load all Neo4j OpenAlex IDs into memory for fast citation matching"""
        if self.neo4j_openalex_ids is not None:
            return self.neo4j_openalex_ids
            
        logger.info("🔄 Loading 626K+ OpenAlex IDs into memory for citation matching...")
        start_time = time.time()
        
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p:Paper) 
                WHERE p.openalex_id IS NOT NULL AND p.openalex_id <> ''
                RETURN p.openalex_id
            """)
            
            self.neo4j_openalex_ids = {record["p.openalex_id"] for record in result}
        
        load_time = time.time() - start_time
        logger.info(f"✅ Loaded {len(self.neo4j_openalex_ids):,} OpenAlex IDs in {load_time:.2f} seconds")
        return self.neo4j_openalex_ids
    
    def process_file_citations(self, file_path: Path, max_records: int = None, batch_size: int = 1000) -> CitationStats:
        """Process file to extract and create citation relationships"""
        logger.info(f"📄 Processing {file_path.name} for citations (batch={batch_size:,})")
        
        # Load OpenAlex IDs if not already loaded
        neo4j_openalex_ids = self._load_neo4j_openalex_ids()
        
        stats = CitationStats()
        start_time = time.time()
        
        # Get file size for throughput calculations
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        logger.info(f"📁 File size: {file_size_mb:.1f} MB")
        
        citation_batch = []
        
        try:
            with gzip.open(file_path, 'rt') as f:
                for line_num, line in enumerate(f, 1):
                    if max_records and line_num > max_records:
                        break
                    
                    try:
                        data = json.loads(line.strip())
                        work = OpenAlexWork(**data)
                        stats.total_works_processed += 1
                        
                        # Only process works that have references AND are in our Neo4j database
                        if (work.id and work.referenced_works and 
                            work.id in neo4j_openalex_ids):
                            
                            stats.works_with_references += 1
                            
                            # Extract citation relationships
                            for referenced_work_id in work.referenced_works:
                                if referenced_work_id in neo4j_openalex_ids:
                                    citation_batch.append({
                                        'citing_id': work.id,
                                        'cited_id': referenced_work_id
                                    })
                                    stats.total_reference_pairs += 1
                            
                            # Process batch when full
                            if len(citation_batch) >= batch_size:
                                created = self._create_citation_batch(citation_batch)
                                stats.successful_citations += created
                                citation_batch = []
                        
                        # Progress logging every 250K records
                        if line_num % 250000 == 0:
                            elapsed = time.time() - start_time
                            works_per_sec = stats.total_works_processed / elapsed if elapsed > 0 else 0
                            citations_per_sec = stats.successful_citations / elapsed if elapsed > 0 else 0
                            
                            logger.info(f"📈 Progress: {line_num:,} records | {works_per_sec:.0f} works/sec | {stats.successful_citations:,} citations created")
                            
                    except Exception as e:
                        # Skip malformed records silently for maximum speed
                        continue
                
                # Process remaining batch
                if citation_batch:
                    created = self._create_citation_batch(citation_batch)
                    stats.successful_citations += created
                
        except Exception as e:
            logger.error(f"❌ Error processing file {file_path}: {e}")
            
        # Calculate final statistics
        end_time = time.time()
        stats.processing_time_seconds = end_time - start_time
        stats.data_processed_mb = file_size_mb * (stats.total_works_processed / max_records if max_records else 1.0)
        
        if stats.processing_time_seconds > 0:
            stats.works_per_second = stats.total_works_processed / stats.processing_time_seconds
            stats.citations_per_second = stats.successful_citations / stats.processing_time_seconds
        
        stats.files_processed = 1
        
        self._log_citation_summary(stats, file_path, max_records)
        return stats
    
    def _create_citation_batch(self, citations: List[Dict[str, str]]) -> int:
        """Create citation relationships in bulk"""
        if not citations:
            return 0
        
        start_time = time.time()
        
        try:
            with self.driver.session() as session:
                # Bulk create citation relationships using UNWIND
                result = session.run("""
                    UNWIND $citations AS citation
                    MATCH (citing:Paper {openalex_id: citation.citing_id})
                    MATCH (cited:Paper {openalex_id: citation.cited_id})
                    MERGE (citing)-[:CITES]->(cited)
                    RETURN count(*) as created_citations
                """, citations=citations)
                
                created = result.single()["created_citations"]
                
        except Exception as e:
            logger.error(f"❌ Citation batch creation error: {e}")
            return 0
            
        processing_time = time.time() - start_time
        
        if created > 0:
            rate = created / processing_time if processing_time > 0 else 0
            logger.debug(f"🔗 Citation batch: {created:,} relationships created in {processing_time:.2f}s ({rate:.0f} citations/sec)")
        
        return created
    
    def _log_citation_summary(self, stats: CitationStats, file_path: Path, max_records: Optional[int]):
        """Log citation processing summary"""
        logger.info("=" * 100)
        logger.info(f"🔗 CITATION SUMMARY - {file_path.name}")
        logger.info("=" * 100)
        logger.info(f"📊 Records processed: {stats.total_works_processed:,}")
        logger.info(f"📊 Works with references: {stats.works_with_references:,}")
        logger.info(f"📊 Reference pairs found: {stats.total_reference_pairs:,}")
        logger.info(f"📊 Citations created: {stats.successful_citations:,}")
        logger.info(f"📊 Citation success rate: {stats.successful_citations/stats.total_reference_pairs*100 if stats.total_reference_pairs > 0 else 0:.2f}%")
        logger.info(f"⏱️  Processing time: {stats.processing_time_seconds:.2f} seconds")
        logger.info(f"🚀 Throughput: {stats.works_per_second:.0f} works/sec, {stats.citations_per_second:.0f} citations/sec")
        logger.info("=" * 100)

    def _quick_citation_overview(self) -> Tuple[int, int, float]:
        """Quick overview of citation processing scope"""
        logger.info("🔍 CITATION PROCESSING OVERVIEW...")
        
        analysis_start = time.time()
        date_dirs = sorted([d for d in self.data_dir.iterdir() if d.is_dir()])
        
        total_files = 0
        total_size_mb = 0.0
        
        for date_dir in date_dirs:
            part_files = list(date_dir.glob("part_*.gz"))
            total_files += len(part_files)
            total_size_mb += sum(f.stat().st_size for f in part_files) / (1024 * 1024)
        
        # Estimate records based on Phase 1 data
        estimated_records = int(total_size_mb * 461)  # ~461 records per MB
        
        analysis_time = time.time() - analysis_start
        
        logger.info("")
        logger.info("📊 PHASE 2 CITATION DATASET OVERVIEW:")
        logger.info(f"   📁 Total directories: {len(date_dirs):,}")
        logger.info(f"   📄 Total files: {total_files:,}")
        logger.info(f"   💾 Total size: {total_size_mb:.1f} MB ({total_size_mb/1024:.1f} GB)")
        logger.info(f"   🎯 Estimated records: {estimated_records:,}")
        logger.info(f"   🔗 Expected citation opportunities: Thousands of relationships")
        logger.info(f"   ⏱️ Analysis time: {analysis_time:.1f} seconds")
        logger.info("")
        
        return total_files, estimated_records, total_size_mb

    def process_full_dataset_citations(self, batch_size: int = 1000) -> CitationStats:
        """Process complete dataset to create all citation relationships"""
        logger.info("🚀 STARTING PHASE 2: CITATION RELATIONSHIP CREATION")
        logger.info("=" * 100)
        logger.info("🔗 PHASE 2 OBJECTIVES:")
        logger.info("  ✅ Extract citation data from OpenAlex referenced_works")
        logger.info("  ✅ Create CITES relationships between papers")
        logger.info("  ✅ Build citation graph using 626K+ matched papers")
        logger.info("  ✅ High match rates with OpenAlex ID → OpenAlex ID matching")
        logger.info("=" * 100)
        
        # Quick dataset overview
        total_files_count, total_records_count, total_size_mb = self._quick_citation_overview()
        
        # Pre-load OpenAlex IDs for the entire processing run
        neo4j_openalex_ids = self._load_neo4j_openalex_ids()
        
        total_stats = CitationStats()
        overall_start_time = time.time()
        
        # Initialize progress tracking
        files_completed = 0
        records_processed = 0
        mb_processed = 0.0
        
        # Get all data directories sorted by date
        date_dirs = sorted([d for d in self.data_dir.iterdir() if d.is_dir()])
        logger.info(f"🚀 PROCESSING {len(date_dirs)} DIRECTORIES FOR CITATIONS...")
        
        for dir_num, date_dir in enumerate(date_dirs, 1):
            logger.info(f"\n{'='*80}")
            logger.info(f"📂 Processing directory {dir_num}/{len(date_dirs)}: {date_dir.name}")
            logger.info(f"{'='*80}")
            
            # Get all part files in this directory
            part_files = sorted(date_dir.glob("part_*.gz"))
            logger.info(f"📄 Found {len(part_files)} part files for citation processing")
            
            for file_num, part_file in enumerate(part_files, 1):
                file_size_mb = part_file.stat().st_size / (1024*1024)
                
                logger.info(f"\n📄 File {file_num}/{len(part_files)}: {part_file.name}")
                logger.info(f"📏 Size: {file_size_mb:.1f} MB")
                
                # Process entire file for citations
                file_stats = self.process_file_citations(part_file, max_records=None, batch_size=batch_size)
                
                # Update progress tracking
                files_completed += 1
                records_processed += file_stats.total_works_processed
                mb_processed += file_stats.data_processed_mb
                
                # Aggregate statistics
                total_stats.files_processed += 1
                total_stats.total_works_processed += file_stats.total_works_processed
                total_stats.works_with_references += file_stats.works_with_references
                total_stats.total_reference_pairs += file_stats.total_reference_pairs
                total_stats.successful_citations += file_stats.successful_citations
                total_stats.data_processed_mb += file_stats.data_processed_mb
                
                # Update overall timing
                elapsed = time.time() - overall_start_time
                total_stats.processing_time_seconds = elapsed
                if elapsed > 0:
                    total_stats.works_per_second = total_stats.total_works_processed / elapsed
                    total_stats.citations_per_second = total_stats.successful_citations / elapsed
                
                # Calculate comprehensive progress
                files_progress = (files_completed / total_files_count) * 100
                records_progress = (records_processed / total_records_count) * 100 if total_records_count > 0 else 0
                mb_progress = (mb_processed / total_size_mb) * 100 if total_size_mb > 0 else 0
                
                # Detailed progress report
                logger.info(f"\n📊 COMPREHENSIVE CITATION PROGRESS:")
                logger.info(f"   📁 Files: {files_completed:,}/{total_files_count:,} ({files_progress:.1f}%)")
                logger.info(f"   📄 Records: {records_processed:,}/{total_records_count:,} ({records_progress:.1f}%)")
                logger.info(f"   💾 Data: {mb_processed:.1f}/{total_size_mb:.1f} MB ({mb_progress:.1f}%)")
                logger.info(f"   🔗 Citations created: {total_stats.successful_citations:,}")
                logger.info(f"   📚 Works with references: {total_stats.works_with_references:,}")
                logger.info(f"   ⚡ Current speed: {total_stats.works_per_second:.0f} works/sec, {total_stats.citations_per_second:.0f} citations/sec")
                
                # Time estimates
                if total_stats.works_per_second > 0:
                    remaining_records = total_records_count - records_processed
                    remaining_hours = remaining_records / total_stats.works_per_second / 3600
                    elapsed_hours = elapsed / 3600
                    total_estimated_hours = elapsed_hours + remaining_hours
                    
                    logger.info(f"   ⏱️ Elapsed: {elapsed_hours:.1f}h | Remaining: {remaining_hours:.1f}h | Total Est: {total_estimated_hours:.1f}h")
                    
                    # Progress bar visualization
                    progress_bar_length = 50
                    filled_length = int(progress_bar_length * mb_progress / 100)
                    bar = '█' * filled_length + '░' * (progress_bar_length - filled_length)
                    logger.info(f"   📊 Progress: |{bar}| {mb_progress:.1f}%")
        
        logger.info("\n🎉 PHASE 2 CITATION PROCESSING COMPLETE!")
        self._log_citation_summary(total_stats, Path("FULL_360GB_CITATION_DATASET"), None)
        return total_stats

def main():
    """Phase 2 citation loader main function"""
    import sys
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j") 
    neo4j_password = os.getenv("NEO4J_PASSWORD")
    
    if not neo4j_password:
        raise ValueError("NEO4J_PASSWORD environment variable is required")
    
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        if mode == "test":
            # Phase 2 test mode
            loader = CitationLoader(neo4j_uri, neo4j_user, neo4j_password)
            try:
                test_file = Path("data/works/updated_date=2025-01-26/part_000.gz")
                logger.info("🧪 PHASE 2 TEST - Citation processing on 50K records")
                loader.process_file_citations(test_file, max_records=50000, batch_size=500)
            finally:
                loader.close()
            return
        elif mode == "analyze":
            # Citation analysis only
            loader = CitationLoader(neo4j_uri, neo4j_user, neo4j_password)
            try:
                logger.info("🔍 PHASE 2 CITATION ANALYSIS MODE")
                loader._quick_citation_overview()
                logger.info("✅ Citation analysis complete")
            finally:
                loader.close()
            return
        elif mode == "help":
            print("Phase 2 Citation Loader Usage:")
            print("  python citation_loader.py          # Process full dataset for citations")
            print("  python citation_loader.py test     # Test mode (50K records)")
            print("  python citation_loader.py analyze  # Analyze citation opportunities only")
            print("  python citation_loader.py help     # Show this help")
            return
    
    # Full Phase 2 production mode
    logger.info("🚀 PHASE 2 PRODUCTION: Creating citation relationships")
    logger.info("🔗 Target: Build citation graph using 626K+ papers with OpenAlex IDs")
    logger.info("⚡ Expected: Higher match rates than Phase 1")
    
    response = input("\n🚨 Start Phase 2 citation processing of full 360GB dataset? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        logger.info("Phase 2 operation cancelled by user")
        return
    
    loader = CitationLoader(neo4j_uri, neo4j_user, neo4j_password)
    
    try:
        loader.process_full_dataset_citations(batch_size=1000)
    finally:
        loader.close()

if __name__ == "__main__":
    main()