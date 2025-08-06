"""
OPTIMIZED OpenAlex Citation Loader for handling 350GB of data efficiently

Key optimizations:
1. Bulk Neo4j operations with larger batches
2. Pre-load Neo4j DOI set into memory
3. Streaming processing with minimal memory footprint
4. Detailed performance tracking
"""

import json
import gzip
import logging
import time
from pathlib import Path
from typing import Dict, Set, List, Optional
from datetime import datetime
import os
from neo4j import GraphDatabase
from pydantic import BaseModel, Field

from data_models.openalex import OpenAlexWork

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OptimizedPerformanceStats(BaseModel):
    """Performance statistics for optimized loader"""
    files_processed: int = 0
    total_works_processed: int = 0
    works_with_doi: int = 0
    neo4j_matches: int = 0
    processing_time_seconds: float = 0.0
    works_per_second: float = 0.0
    mb_per_second: float = 0.0
    data_processed_mb: float = 0.0

class OptimizedCitationLoader:
    """High-performance loader optimized for large datasets"""
    
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.data_dir = Path("data/works")
        self.neo4j_dois = None  # Will cache Neo4j DOIs in memory
        
    def close(self):
        """Close Neo4j connection"""
        self.driver.close()
    
    def _load_neo4j_dois(self) -> Set[str]:
        """Load all Neo4j DOIs into memory for fast lookup"""
        if self.neo4j_dois is not None:
            return self.neo4j_dois
            
        logger.info("Loading all Neo4j DOIs into memory...")
        start_time = time.time()
        
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p:Paper) 
                WHERE p.doi IS NOT NULL AND p.doi <> ''
                RETURN p.doi
            """)
            
            self.neo4j_dois = {record["p.doi"] for record in result}
        
        load_time = time.time() - start_time
        logger.info(f"Loaded {len(self.neo4j_dois):,} DOIs in {load_time:.2f} seconds")
        return self.neo4j_dois
    
    def process_file_optimized(self, file_path: Path, max_records: int = None, batch_size: int = 5000) -> OptimizedPerformanceStats:
        """Process single file with optimizations"""
        logger.info(f"Processing {file_path.name} (batch_size={batch_size})")
        
        # Load Neo4j DOIs if not already loaded
        neo4j_dois = self._load_neo4j_dois()
        
        stats = OptimizedPerformanceStats()
        start_time = time.time()
        
        # Get file size
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        
        update_batch = []
        
        try:
            with gzip.open(file_path, 'rt') as f:
                for line_num, line in enumerate(f, 1):
                    if max_records and line_num > max_records:
                        break
                    
                    try:
                        data = json.loads(line.strip())
                        work = OpenAlexWork(**data)
                        stats.total_works_processed += 1
                        
                        if work.doi and work.id:
                            stats.works_with_doi += 1
                            clean_doi = work.doi.replace("https://doi.org/", "")
                            
                            # Fast in-memory lookup instead of database query
                            if clean_doi in neo4j_dois:
                                update_batch.append({
                                    'doi': clean_doi,
                                    'openalex_id': work.id
                                })
                            
                            # Process batch when full
                            if len(update_batch) >= batch_size:
                                matched = self._process_batch_bulk(update_batch)
                                stats.neo4j_matches += matched
                                update_batch = []
                        
                        # Progress logging
                        if line_num % 50000 == 0:
                            elapsed = time.time() - start_time
                            works_per_sec = stats.total_works_processed / elapsed if elapsed > 0 else 0
                            
                            logger.info(f"Progress: {line_num:,} records processed")
                            logger.info(f"  Speed: {works_per_sec:.0f} works/sec")
                            logger.info(f"  Matches: {stats.neo4j_matches:,} / {stats.works_with_doi:,} DOIs")
                            logger.info(f"  Match rate: {stats.neo4j_matches/stats.works_with_doi*100 if stats.works_with_doi > 0 else 0:.3f}%")
                            
                    except Exception as e:
                        if line_num % 10000 == 0:  # Only log parsing errors occasionally
                            logger.warning(f"Error parsing line {line_num}: {e}")
                        continue
                
                # Process remaining batch
                if update_batch:
                    matched = self._process_batch_bulk(update_batch)
                    stats.neo4j_matches += matched
                
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            
        # Calculate final statistics
        end_time = time.time()
        stats.processing_time_seconds = end_time - start_time
        stats.data_processed_mb = file_size_mb * (stats.total_works_processed / max_records if max_records else 1.0)
        
        if stats.processing_time_seconds > 0:
            stats.works_per_second = stats.total_works_processed / stats.processing_time_seconds
            stats.mb_per_second = stats.data_processed_mb / stats.processing_time_seconds
        
        stats.files_processed = 1
        
        self._log_optimized_summary(stats, file_path, max_records)
        return stats
    
    def _process_batch_bulk(self, updates: List[Dict[str, str]]) -> int:
        """Process batch with optimized bulk operations"""
        if not updates:
            return 0
        
        start_time = time.time()
        
        with self.driver.session() as session:
            # Use UNWIND for bulk operations - much faster than individual queries
            result = session.run("""
                UNWIND $updates AS update
                MATCH (p:Paper {doi: update.doi})
                SET p.openalex_id = update.openalex_id
                RETURN count(p) as matched_papers
            """, updates=updates)
            
            matched = result.single()["matched_papers"]
            
        processing_time = time.time() - start_time
        
        if matched > 0:
            rate = matched / processing_time if processing_time > 0 else 0
            logger.debug(f"Bulk update: {matched:,} matches in {processing_time:.2f}s ({rate:.0f} matches/sec)")
        
        return matched
    
    def _log_optimized_summary(self, stats: OptimizedPerformanceStats, file_path: Path, max_records: Optional[int]):
        """Log optimized performance summary"""
        logger.info("=" * 80)
        logger.info(f"OPTIMIZED PERFORMANCE SUMMARY - {file_path.name}")
        logger.info("=" * 80)
        logger.info(f"Records processed: {stats.total_works_processed:,} {f'(limited to {max_records:,})' if max_records else ''}")
        logger.info(f"Works with DOI: {stats.works_with_doi:,}")
        logger.info(f"Neo4j matches: {stats.neo4j_matches:,}")
        logger.info(f"Match rate: {stats.neo4j_matches/stats.works_with_doi*100 if stats.works_with_doi > 0 else 0:.3f}%")
        logger.info("")
        logger.info(f"Processing time: {stats.processing_time_seconds:.2f} seconds")
        logger.info(f"Throughput: {stats.works_per_second:.0f} works/sec, {stats.mb_per_second:.2f} MB/sec")
        logger.info("")
        
        # Extrapolate to full dataset
        if stats.mb_per_second > 0:
            total_time_hours = (350 * 1024) / stats.mb_per_second / 3600
            total_matches = stats.neo4j_matches * (350 * 1024 / stats.data_processed_mb) if stats.data_processed_mb > 0 else 0
            
            logger.info("EXTRAPOLATION TO FULL 350GB DATASET:")
            logger.info(f"  Estimated time: {total_time_hours:.1f} hours ({total_time_hours/24:.1f} days)")
            logger.info(f"  Estimated matches: {total_matches:,.0f}")
            
            if total_time_hours > 48:
                logger.warning("⚠️  Estimated time > 48 hours - consider further optimization!")
            elif total_time_hours <= 24:
                logger.info("✅ Processing time looks reasonable (< 24 hours)")
        
        logger.info("=" * 80)

    def process_full_dataset(self, batch_size: int = 5000) -> OptimizedPerformanceStats:
        """Process the complete OpenAlex dataset (all files)"""
        logger.info("🚀 STARTING FULL OPENAPI DATASET PROCESSING")
        logger.info("=" * 80)
        logger.info("This will process ALL OpenAlex files in data/works/")
        logger.info("Estimated time: ~12 hours for 350GB")
        logger.info("=" * 80)
        
        # Load Neo4j DOIs once for all files
        neo4j_dois = self._load_neo4j_dois()
        
        total_stats = OptimizedPerformanceStats()
        start_time = time.time()
        
        # Get all data directories sorted by date
        date_dirs = sorted([d for d in self.data_dir.iterdir() if d.is_dir()])
        logger.info(f"Found {len(date_dirs)} date directories to process")
        
        for dir_num, date_dir in enumerate(date_dirs, 1):
            logger.info(f"\\n{'='*60}")
            logger.info(f"Processing directory {dir_num}/{len(date_dirs)}: {date_dir.name}")
            logger.info(f"{'='*60}")
            
            # Get all part files in this directory
            part_files = sorted(date_dir.glob("part_*.gz"))
            
            for file_num, part_file in enumerate(part_files, 1):
                logger.info(f"\\nFile {file_num}/{len(part_files)}: {part_file.name}")
                logger.info(f"Size: {part_file.stat().st_size / (1024*1024):.1f} MB")
                
                # Process entire file (no max_records limit)
                file_stats = self.process_file_optimized(part_file, max_records=None, batch_size=batch_size)
                
                # Aggregate statistics
                total_stats.files_processed += 1
                total_stats.total_works_processed += file_stats.total_works_processed
                total_stats.works_with_doi += file_stats.works_with_doi
                total_stats.neo4j_matches += file_stats.neo4j_matches
                total_stats.data_processed_mb += file_stats.data_processed_mb
                
                # Update overall timing
                elapsed = time.time() - start_time
                total_stats.processing_time_seconds = elapsed
                if elapsed > 0:
                    total_stats.works_per_second = total_stats.total_works_processed / elapsed
                    total_stats.mb_per_second = total_stats.data_processed_mb / elapsed
                
                # Progress report
                logger.info(f"\\n📊 OVERALL PROGRESS:")
                logger.info(f"Files completed: {total_stats.files_processed}")
                logger.info(f"Total works processed: {total_stats.total_works_processed:,}")
                logger.info(f"Total matches found: {total_stats.neo4j_matches:,}")
                logger.info(f"Data processed: {total_stats.data_processed_mb:.1f} MB")
                logger.info(f"Overall speed: {total_stats.works_per_second:.0f} works/sec, {total_stats.mb_per_second:.2f} MB/sec")
                
                if total_stats.mb_per_second > 0:
                    remaining_mb = (350 * 1024) - total_stats.data_processed_mb
                    remaining_hours = remaining_mb / total_stats.mb_per_second / 3600
                    logger.info(f"Estimated time remaining: {remaining_hours:.1f} hours")
        
        logger.info("\\n🎉 FULL DATASET PROCESSING COMPLETE!")
        self._log_optimized_summary(total_stats, Path("FULL_DATASET"), None)
        return total_stats

def main():
    """Production loader for full dataset processing"""
    import sys
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Neo4j connection settings
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD")
    
    if not neo4j_password:
        raise ValueError("NEO4J_PASSWORD environment variable is required")
    
    # Check command line arguments
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        if mode == "test":
            # Test mode - run small validation
            loader = OptimizedCitationLoader(neo4j_uri, neo4j_user, neo4j_password)
            try:
                test_file = Path("data/works/updated_date=2025-01-26/part_000.gz")
                logger.info("🧪 RUNNING TEST MODE")
                logger.info("Processing 50K records for validation...")
                loader.process_file_optimized(test_file, max_records=50000, batch_size=5000)
            finally:
                loader.close()
            return
        elif mode == "help":
            print("OpenAlex Loader Usage:")
            print("  python openalex_loader.py          # Process full 350GB dataset")
            print("  python openalex_loader.py test     # Test mode (50K records)")
            print("  python openalex_loader.py help     # Show this help")
            return
    
    # Full production mode
    logger.info("🚀 PRODUCTION MODE: Processing full OpenAlex dataset")
    logger.info("⚠️  This will take approximately 12 hours to complete")
    
    response = input("\\nProceed with full dataset processing? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        logger.info("Operation cancelled by user")
        return
    
    loader = OptimizedCitationLoader(neo4j_uri, neo4j_user, neo4j_password)
    
    try:
        loader.process_full_dataset(batch_size=5000)
    finally:
        loader.close()

if __name__ == "__main__":
    main()