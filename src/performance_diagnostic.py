"""
Emergency Performance Diagnostic Tool

Identify bottlenecks in the OpenAlex loader that's causing 3+ day processing times
"""

import time
import json
import gzip
import logging
from pathlib import Path
from typing import Dict, Set
import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def diagnose_performance():
    """Run comprehensive performance diagnostics"""
    load_dotenv()
    
    logger.info("🚨 EMERGENCY PERFORMANCE DIAGNOSTIC")
    logger.info("=" * 80)
    
    # Test 1: Neo4j connection speed
    logger.info("Test 1: Neo4j Connection Performance")
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD"))
    )
    
    start_time = time.time()
    with driver.session() as session:
        result = session.run("RETURN 1 as test")
        _ = result.single()["test"]
    connection_time = time.time() - start_time
    logger.info(f"  Basic connection: {connection_time:.4f} seconds")
    
    # Test 2: DOI loading performance  
    logger.info("\\nTest 2: DOI Loading Performance")
    start_time = time.time()
    with driver.session() as session:
        result = session.run("""
            MATCH (p:Paper) 
            WHERE p.doi IS NOT NULL 
            RETURN count(p) as doi_count
        """)
        doi_count = result.single()["doi_count"]
    count_time = time.time() - start_time
    logger.info(f"  DOI count query: {count_time:.2f} seconds ({doi_count:,} papers)")
    
    # Test 3: Sample DOI fetch performance
    logger.info("\\nTest 3: Sample DOI Fetch Performance")
    start_time = time.time()
    with driver.session() as session:
        result = session.run("""
            MATCH (p:Paper) 
            WHERE p.doi IS NOT NULL 
            RETURN p.doi
            LIMIT 10000
        """)
        dois = [record["p.doi"] for record in result]
    sample_fetch_time = time.time() - start_time
    logger.info(f"  Fetch 10K DOIs: {sample_fetch_time:.2f} seconds")
    
    # Test 4: Full DOI loading (the current bottleneck?)
    logger.info("\\nTest 4: FULL DOI Loading (Current Implementation)")
    start_time = time.time()
    with driver.session() as session:
        result = session.run("""
            MATCH (p:Paper) 
            WHERE p.doi IS NOT NULL AND p.doi <> ''
            RETURN p.doi
        """)
        all_dois = {record["p.doi"] for record in result}
    full_load_time = time.time() - start_time
    logger.info(f"  Load ALL DOIs into memory: {full_load_time:.2f} seconds ({len(all_dois):,} DOIs)")
    
    if full_load_time > 60:
        logger.error("🚨 BOTTLENECK FOUND: DOI loading is taking too long!")
        logger.error("   This is likely the main performance issue.")
    
    # Test 5: Batch update performance
    logger.info("\\nTest 5: Batch Update Performance")
    test_updates = [
        {'doi': '10.1103/PhysRevD.76.013009', 'openalex_id': 'https://openalex.org/W123456'},
        {'doi': '10.1103/PhysRevA.75.043613', 'openalex_id': 'https://openalex.org/W123457'},
        {'doi': '10.1103/PhysRevD.76.044016', 'openalex_id': 'https://openalex.org/W123458'},
    ]
    
    start_time = time.time()
    with driver.session() as session:
        result = session.run("""
            UNWIND $updates AS update
            MATCH (p:Paper {doi: update.doi})
            SET p.openalex_id = update.openalex_id
            RETURN count(p) as matched_papers
        """, updates=test_updates)
        matched = result.single()["matched_papers"]
    batch_time = time.time() - start_time
    logger.info(f"  Batch update 3 papers: {batch_time:.4f} seconds ({matched} matched)")
    
    # Test 6: File reading performance
    logger.info("\\nTest 6: File Reading Performance")
    test_file = Path("data/works/updated_date=2025-01-26/part_000.gz")
    
    if test_file.exists():
        start_time = time.time()
        record_count = 0
        with gzip.open(test_file, 'rt') as f:
            for line_num, line in enumerate(f, 1):
                if line_num > 10000:  # Test first 10K records
                    break
                try:
                    data = json.loads(line.strip())
                    record_count += 1
                except:
                    continue
        file_time = time.time() - start_time
        logger.info(f"  Read/parse 10K records: {file_time:.2f} seconds ({record_count:,} valid)")
        logger.info(f"  File reading speed: {record_count/file_time:.0f} records/sec")
    
    # Test 7: Memory usage check
    logger.info("\\nTest 7: Memory Usage Analysis")
    import psutil
    process = psutil.Process()
    memory_mb = process.memory_info().rss / 1024 / 1024
    logger.info(f"  Current memory usage: {memory_mb:.1f} MB")
    
    if len(all_dois) > 0:
        memory_per_doi = memory_mb / len(all_dois) * 1000
        logger.info(f"  Memory per DOI: {memory_per_doi:.3f} KB")
    
    # Analysis and recommendations
    logger.info("\\n" + "=" * 80)
    logger.info("🔍 PERFORMANCE ANALYSIS & RECOMMENDATIONS")
    logger.info("=" * 80)
    
    if full_load_time > 30:
        logger.error("❌ MAJOR ISSUE: DOI loading is too slow!")
        logger.error("   Recommendation: Implement streaming/chunked DOI lookup")
        logger.error("   Alternative: Use database-side filtering instead of in-memory")
        
    if batch_time > 0.1:
        logger.warning("⚠️  Batch updates are slow")
        logger.warning("   Recommendation: Reduce batch size or optimize query")
        
    if memory_mb > 1000:
        logger.warning("⚠️  High memory usage detected")
        logger.warning("   Recommendation: Implement memory-efficient streaming")
    
    # Estimate current performance
    if full_load_time > 0:
        estimated_files_per_hour = 3600 / full_load_time  # Assuming DOI load per file
        logger.info(f"\\n📊 ESTIMATED PERFORMANCE:")
        logger.info(f"  Files processable per hour: {estimated_files_per_hour:.1f}")
        logger.info(f"  This explains the 3+ day processing time!")
    
    driver.close()

if __name__ == "__main__":
    diagnose_performance()