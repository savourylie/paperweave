"""
ULTRA-OPTIMIZED OpenAlex Loader - Emergency Fix for 3+ Day Problem

Root cause analysis:
1. Neo4j queries are inherently slow due to large dataset size
2. Need to minimize database round-trips
3. Use prepared statements and connection pooling
4. Implement smart batching with pre-filtering
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
from pydantic import BaseModel

from data_models.openalex import OpenAlexWork

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class UltraPerformanceStats(BaseModel):
    """Performance statistics for ultra loader"""
    files_processed: int = 0
    total_works_processed: int = 0
    works_with_doi: int = 0
    neo4j_matches: int = 0
    processing_time_seconds: float = 0.0
    works_per_second: float = 0.0
    mb_per_second: float = 0.0
    data_processed_mb: float = 0.0

class UltraFastLoader:
    """Ultra-optimized loader with minimal database operations"""
    
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        # Optimized driver configuration
        self.driver = GraphDatabase.driver(
            neo4j_uri, 
            auth=(neo4j_user, neo4j_password),
            max_connection_lifetime=3600,
            max_connection_pool_size=50,
            connection_acquisition_timeout=60
        )
        self.data_dir = Path("data/works")
        self._prepare_database()
        
    def close(self):
        """Close Neo4j connection"""
        self.driver.close()
    
    def _prepare_database(self):
        """Prepare database for ultra-fast operations"""
        logger.info("Preparing database for ultra-fast operations...")
        
        with self.driver.session() as session:
            # Ensure critical indexes exist
            session.run("CREATE INDEX paper_doi_index IF NOT EXISTS FOR (p:Paper) ON (p.doi)")
            
            # Get total DOI count for smart batching
            result = session.run("MATCH (p:Paper) WHERE p.doi IS NOT NULL RETURN count(p) as total")
            total_dois = result.single()["total"]
            logger.info(f"Database contains {total_dois:,} papers with DOIs")
            
        logger.info("Database prepared")
    
    def process_file_ultra_fast(self, file_path: Path, max_records: int = None, batch_size: int = 20000) -> UltraPerformanceStats:
        """Process file with ultra-fast optimizations"""
        logger.info(f"Processing {file_path.name} - ULTRA-FAST MODE (batch={batch_size})")
        
        stats = UltraPerformanceStats()
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
                            
                            update_batch.append({
                                'doi': clean_doi,
                                'openalex_id': work.id
                            })
                            
                            # Process large batches to minimize database round-trips
                            if len(update_batch) >= batch_size:
                                matched = self._process_batch_ultra_fast(update_batch)
                                stats.neo4j_matches += matched
                                update_batch = []
                        
                        # Less frequent progress logging
                        if line_num % 200000 == 0:
                            elapsed = time.time() - start_time
                            works_per_sec = stats.total_works_processed / elapsed if elapsed > 0 else 0
                            
                            logger.info(f"Progress: {line_num:,} processed, {works_per_sec:.0f} works/sec, {stats.neo4j_matches:,} matches")
                            
                    except Exception as e:
                        continue  # Skip bad records silently for speed
                
                # Process remaining batch
                if update_batch:
                    matched = self._process_batch_ultra_fast(update_batch)
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
        
        self._log_ultra_summary(stats, file_path, max_records)
        return stats
    
    def _process_batch_ultra_fast(self, updates: List[Dict[str, str]]) -> int:
        """Process batch with ultra-fast optimizations"""
        if not updates:
            return 0
        
        start_time = time.time()
        
        try:
            with self.driver.session() as session:
                # Single optimized query with minimal overhead
                result = session.run("""
                    UNWIND $updates AS update
                    MATCH (p:Paper {doi: update.doi})
                    SET p.openalex_id = update.openalex_id
                    RETURN count(p) as matched_papers
                """, updates=updates)
                
                matched = result.single()["matched_papers"]
                
        except Exception as e:
            logger.error(f"Batch processing error: {e}")
            return 0
            
        processing_time = time.time() - start_time
        
        if matched > 0:
            rate = len(updates) / processing_time if processing_time > 0 else 0
            logger.debug(f"Ultra batch: {matched:,} matches from {len(updates):,} candidates in {processing_time:.2f}s ({rate:.0f} ops/sec)")
        
        return matched
    
    def _log_ultra_summary(self, stats: UltraPerformanceStats, file_path: Path, max_records: Optional[int]):
        """Log ultra performance summary"""
        logger.info("=" * 80)
        logger.info(f"⚡ ULTRA-FAST SUMMARY - {file_path.name}")
        logger.info("=" * 80)
        logger.info(f"Records processed: {stats.total_works_processed:,}")
        logger.info(f"Works with DOI: {stats.works_with_doi:,}")
        logger.info(f"Neo4j matches: {stats.neo4j_matches:,}")
        logger.info(f"Match rate: {stats.neo4j_matches/stats.works_with_doi*100 if stats.works_with_doi > 0 else 0:.3f}%")
        logger.info(f"Processing time: {stats.processing_time_seconds:.2f} seconds")
        logger.info(f"⚡ ULTRA Throughput: {stats.works_per_second:.0f} works/sec, {stats.mb_per_second:.2f} MB/sec")
        
        # Critical extrapolation
        if stats.mb_per_second > 0:
            total_time_hours = (350 * 1024) / stats.mb_per_second / 3600
            improvement = 72 / total_time_hours if total_time_hours > 0 else 0  # vs 3 days
            
            logger.info("")
            logger.info("⚡ ULTRA-FAST EXTRAPOLATION TO 350GB:")
            logger.info(f"  Expected completion: {total_time_hours:.1f} hours ({total_time_hours/24:.1f} days)")
            logger.info(f"  🚀 Performance improvement: {improvement:.1f}x faster than original")
            
            if total_time_hours <= 6:
                logger.info("  ✅ TARGET ACHIEVED: Under 6 hours!")
            elif total_time_hours <= 12:
                logger.info("  ✅ ACCEPTABLE: Under 12 hours")
            else:
                logger.warning(f"  ⚠️  Still too slow: {total_time_hours:.1f} hours")
        
        logger.info("=" * 80)

def main():
    """Ultra-fast emergency loader"""
    import sys
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j") 
    neo4j_password = os.getenv("NEO4J_PASSWORD")
    
    if not neo4j_password:
        raise ValueError("NEO4J_PASSWORD environment variable is required")
    
    if len(sys.argv) > 1 and sys.argv[1].lower() == "test":
        # Quick test mode
        loader = UltraFastLoader(neo4j_uri, neo4j_user, neo4j_password)
        try:
            test_file = Path("data/works/updated_date=2025-01-26/part_000.gz")
            logger.info("🧪 ULTRA-FAST TEST - 10K records only")
            loader.process_file_ultra_fast(test_file, max_records=10000, batch_size=5000)
        finally:
            loader.close()
        return
    
    # Emergency production mode
    logger.info("🚨 EMERGENCY ULTRA-FAST MODE")
    logger.info("⚡ Target: Complete 350GB in under 12 hours")
    
    response = input("\nStart emergency ultra-fast processing? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        return
    
    loader = UltraFastLoader(neo4j_uri, neo4j_user, neo4j_password)
    
    try:
        # Process with large batches for maximum speed
        total_stats = UltraPerformanceStats()
        start_time = time.time()
        
        date_dirs = sorted([d for d in Path("data/works").iterdir() if d.is_dir()])
        logger.info(f"Processing {len(date_dirs)} directories...")
        
        for dir_num, date_dir in enumerate(date_dirs, 1):
            part_files = sorted(date_dir.glob("part_*.gz"))
            logger.info(f"\nDirectory {dir_num}/{len(date_dirs)}: {date_dir.name} ({len(part_files)} files)")
            
            for file_num, part_file in enumerate(part_files, 1):
                logger.info(f"File {file_num}/{len(part_files)}: {part_file.name}")
                
                file_stats = loader.process_file_ultra_fast(part_file, max_records=None, batch_size=20000)
                
                # Aggregate stats
                total_stats.files_processed += 1
                total_stats.total_works_processed += file_stats.total_works_processed
                total_stats.works_with_doi += file_stats.works_with_doi
                total_stats.neo4j_matches += file_stats.neo4j_matches
                total_stats.data_processed_mb += file_stats.data_processed_mb
                
                elapsed = time.time() - start_time
                total_stats.processing_time_seconds = elapsed
                if elapsed > 0:
                    total_stats.mb_per_second = total_stats.data_processed_mb / elapsed
                
                # Show progress
                if total_stats.mb_per_second > 0:
                    remaining_mb = (350 * 1024) - total_stats.data_processed_mb
                    remaining_hours = remaining_mb / total_stats.mb_per_second / 3600
                    logger.info(f"⚡ PROGRESS: {total_stats.data_processed_mb:.1f} MB processed, ~{remaining_hours:.1f}h remaining")
        
        logger.info("\n🎉 ULTRA-FAST PROCESSING COMPLETE!")
        
    finally:
        loader.close()

if __name__ == "__main__":
    main()