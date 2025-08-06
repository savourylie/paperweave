"""
Performance-optimized OpenAlex Citation Loader with detailed timing and throughput metrics
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

# Configure logging with more detail
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PerformanceStats(BaseModel):
    """Detailed performance statistics"""
    total_files_processed: int = 0
    total_works_processed: int = 0
    total_works_with_doi: int = 0
    total_neo4j_matches: int = 0
    total_processing_time_seconds: float = 0.0
    avg_works_per_second: float = 0.0
    avg_mb_per_second: float = 0.0
    total_data_size_mb: float = 0.0
    
    # Detailed timing breakdowns
    file_reading_time: float = 0.0
    json_parsing_time: float = 0.0
    neo4j_matching_time: float = 0.0
    batch_processing_time: float = 0.0

class PerformanceTestLoader:
    """Performance-optimized loader with detailed metrics"""
    
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.data_dir = Path("data/works")
        
    def close(self):
        """Close Neo4j connection"""
        self.driver.close()
    
    def test_single_file_performance(self, file_path: Path, max_records: int = None) -> PerformanceStats:
        """Test performance on a single file with detailed metrics"""
        logger.info(f"Testing performance on {file_path}")
        
        stats = PerformanceStats()
        start_time = time.time()
        
        # Get file size
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        stats.total_data_size_mb = file_size_mb
        logger.info(f"File size: {file_size_mb:.2f} MB")
        
        update_batch = []
        batch_size = 1000
        
        # Timing variables
        file_read_start = time.time()
        json_parse_time = 0.0
        neo4j_time = 0.0
        
        try:
            with gzip.open(file_path, 'rt') as f:
                stats.file_reading_time = time.time() - file_read_start
                
                for line_num, line in enumerate(f, 1):
                    if max_records and line_num > max_records:
                        break
                    
                    # Time JSON parsing
                    parse_start = time.time()
                    try:
                        data = json.loads(line.strip())
                        work = OpenAlexWork(**data)
                        json_parse_time += time.time() - parse_start
                        
                        stats.total_works_processed += 1
                        
                        if work.doi and work.id:
                            stats.total_works_with_doi += 1
                            clean_doi = work.doi.replace("https://doi.org/", "")
                            
                            update_batch.append({
                                'doi': clean_doi,
                                'openalex_id': work.id
                            })
                            
                            # Process batch
                            if len(update_batch) >= batch_size:
                                batch_start = time.time()
                                matched = self._process_batch_with_timing(update_batch)
                                batch_time = time.time() - batch_start
                                
                                stats.total_neo4j_matches += matched
                                neo4j_time += batch_time
                                update_batch = []
                        
                        # Progress logging
                        if line_num % 10000 == 0:
                            elapsed = time.time() - start_time
                            works_per_sec = stats.total_works_processed / elapsed if elapsed > 0 else 0
                            mb_per_sec = (file_size_mb * (line_num / stats.total_works_processed if stats.total_works_processed > 0 else 0)) / elapsed if elapsed > 0 else 0
                            
                            logger.info(f"Progress: {line_num:,} records processed")
                            logger.info(f"  Speed: {works_per_sec:.0f} works/sec, {mb_per_sec:.2f} MB/sec")
                            logger.info(f"  Works with DOI: {stats.total_works_with_doi:,}")
                            logger.info(f"  Neo4j matches: {stats.total_neo4j_matches:,}")
                            
                    except Exception as e:
                        logger.warning(f"Error parsing line {line_num}: {e}")
                        continue
                
                # Process remaining batch
                if update_batch:
                    batch_start = time.time()
                    matched = self._process_batch_with_timing(update_batch)
                    batch_time = time.time() - batch_start
                    
                    stats.total_neo4j_matches += matched
                    neo4j_time += batch_time
                
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            
        # Calculate final statistics
        end_time = time.time()
        stats.total_processing_time_seconds = end_time - start_time
        stats.json_parsing_time = json_parse_time
        stats.neo4j_matching_time = neo4j_time
        stats.batch_processing_time = neo4j_time
        stats.total_files_processed = 1
        
        if stats.total_processing_time_seconds > 0:
            stats.avg_works_per_second = stats.total_works_processed / stats.total_processing_time_seconds
            stats.avg_mb_per_second = stats.total_data_size_mb / stats.total_processing_time_seconds
        
        self._log_performance_summary(stats, file_path)
        return stats
    
    def _process_batch_with_timing(self, updates: List[Dict[str, str]]) -> int:
        """Process batch with timing"""
        if not updates:
            return 0
        
        start_time = time.time()
        
        with self.driver.session() as session:
            result = session.run("""
                UNWIND $updates AS update
                MATCH (p:Paper {doi: update.doi})
                SET p.openalex_id = update.openalex_id
                RETURN count(p) as matched_papers
            """, updates=updates)
            
            matched = result.single()["matched_papers"]
            
        processing_time = time.time() - start_time
        
        if matched > 0:
            logger.info(f"Batch: {matched:,} matches in {processing_time:.2f}s ({len(updates):,} candidates)")
        
        return matched
    
    def _log_performance_summary(self, stats: PerformanceStats, file_path: Path):
        """Log detailed performance summary"""
        logger.info("=" * 80)
        logger.info(f"PERFORMANCE SUMMARY for {file_path.name}")
        logger.info("=" * 80)
        logger.info(f"Total records processed: {stats.total_works_processed:,}")
        logger.info(f"Records with DOI: {stats.total_works_with_doi:,}")
        logger.info(f"Neo4j matches found: {stats.total_neo4j_matches:,}")
        logger.info(f"File size: {stats.total_data_size_mb:.2f} MB")
        logger.info("")
        logger.info("TIMING BREAKDOWN:")
        logger.info(f"  Total time: {stats.total_processing_time_seconds:.2f} seconds")
        logger.info(f"  JSON parsing: {stats.json_parsing_time:.2f} seconds ({stats.json_parsing_time/stats.total_processing_time_seconds*100:.1f}%)")
        logger.info(f"  Neo4j operations: {stats.neo4j_matching_time:.2f} seconds ({stats.neo4j_matching_time/stats.total_processing_time_seconds*100:.1f}%)")
        logger.info("")
        logger.info("THROUGHPUT:")
        logger.info(f"  Works per second: {stats.avg_works_per_second:.0f}")
        logger.info(f"  MB per second: {stats.avg_mb_per_second:.2f}")
        logger.info(f"  Match rate: {stats.total_neo4j_matches/stats.total_works_with_doi*100 if stats.total_works_with_doi > 0 else 0:.3f}%")
        logger.info("")
        
        # Extrapolate to full dataset
        if stats.avg_mb_per_second > 0:
            total_time_hours = (350 * 1024) / stats.avg_mb_per_second / 3600  # 350GB in hours
            logger.info("EXTRAPOLATION TO FULL 350GB DATASET:")
            logger.info(f"  Estimated time: {total_time_hours:.1f} hours ({total_time_hours/24:.1f} days)")
            logger.info(f"  Estimated matches: {stats.total_neo4j_matches * (350*1024 / stats.total_data_size_mb):,.0f}")
        
        logger.info("=" * 80)
    
    def test_multiple_files(self, max_files: int = 3, max_records_per_file: int = 50000):
        """Test performance across multiple files"""
        logger.info(f"Testing performance across {max_files} files, {max_records_per_file:,} records each")
        
        files_tested = 0
        total_stats = PerformanceStats()
        
        for date_dir in sorted(self.data_dir.iterdir()):
            if not date_dir.is_dir() or files_tested >= max_files:
                continue
                
            for part_file in sorted(date_dir.glob("part_*.gz")):
                if files_tested >= max_files:
                    break
                    
                logger.info(f"\n{'='*60}")
                logger.info(f"Testing file {files_tested + 1}/{max_files}: {part_file.name}")
                logger.info(f"{'='*60}")
                
                file_stats = self.test_single_file_performance(part_file, max_records_per_file)
                
                # Aggregate stats
                total_stats.total_files_processed += 1
                total_stats.total_works_processed += file_stats.total_works_processed
                total_stats.total_works_with_doi += file_stats.total_works_with_doi
                total_stats.total_neo4j_matches += file_stats.total_neo4j_matches
                total_stats.total_processing_time_seconds += file_stats.total_processing_time_seconds
                total_stats.total_data_size_mb += file_stats.total_data_size_mb
                
                files_tested += 1
                break
        
        # Calculate aggregate metrics
        if total_stats.total_processing_time_seconds > 0:
            total_stats.avg_works_per_second = total_stats.total_works_processed / total_stats.total_processing_time_seconds
            total_stats.avg_mb_per_second = total_stats.total_data_size_mb / total_stats.total_processing_time_seconds
        
        logger.info("\n" + "="*80)
        logger.info("AGGREGATE PERFORMANCE ACROSS ALL FILES")
        logger.info("="*80)
        logger.info(f"Files tested: {total_stats.total_files_processed}")
        logger.info(f"Total records: {total_stats.total_works_processed:,}")
        logger.info(f"Total matches: {total_stats.total_neo4j_matches:,}")
        logger.info(f"Total data: {total_stats.total_data_size_mb:.2f} MB")
        logger.info(f"Total time: {total_stats.total_processing_time_seconds:.2f} seconds")
        logger.info(f"Average speed: {total_stats.avg_works_per_second:.0f} works/sec, {total_stats.avg_mb_per_second:.2f} MB/sec")
        
        if total_stats.avg_mb_per_second > 0:
            total_time_hours = (350 * 1024) / total_stats.avg_mb_per_second / 3600
            logger.info(f"Estimated time for 350GB: {total_time_hours:.1f} hours ({total_time_hours/24:.1f} days)")
        
        return total_stats

def main():
    """Main function for performance testing"""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Neo4j connection settings
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD")
    
    if not neo4j_password:
        raise ValueError("NEO4J_PASSWORD environment variable is required")
    
    # Create loader and run performance tests
    loader = PerformanceTestLoader(neo4j_uri, neo4j_user, neo4j_password)
    
    try:
        # Test multiple files for comprehensive performance analysis
        loader.test_multiple_files(max_files=3, max_records_per_file=100000)
        
    finally:
        loader.close()

if __name__ == "__main__":
    main()