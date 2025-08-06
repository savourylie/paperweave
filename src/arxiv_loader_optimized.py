import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional
import os

from dotenv import load_dotenv
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


class OptimizedArxivNeo4jLoader:
    def __init__(self, uri: str, username: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        
    def close(self):
        self.driver.close()
    
    def clear_database(self):
        """Clear all nodes and relationships from the database"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            logger.info("Database cleared")
    
    def create_constraints(self):
        """Create unique constraints for node properties"""
        with self.driver.session() as session:
            session.run("CREATE CONSTRAINT paper_arxiv_id IF NOT EXISTS FOR (p:Paper) REQUIRE p.arxiv_id IS UNIQUE")
            session.run("CREATE CONSTRAINT author_name IF NOT EXISTS FOR (a:Author) REQUIRE a.name IS UNIQUE")
            session.run("CREATE CONSTRAINT category_id IF NOT EXISTS FOR (c:Category) REQUIRE c.id IS UNIQUE")
            logger.info("Constraints created")
    
    def process_paper_batch(self, papers_batch: List[Dict]) -> None:
        """Process a batch of papers in a single transaction"""
        if not papers_batch:
            return
            
        with self.driver.session() as session:
            # Prepare batch data
            papers_data = []
            authors_data = []
            categories_data = []
            author_relationships = []
            category_relationships = []
            
            for paper in papers_batch:
                # Parse update_date
                update_date = None
                if paper.get('update_date'):
                    try:
                        update_date = datetime.strptime(paper['update_date'], '%Y-%m-%d')
                    except ValueError:
                        pass
                
                # Paper data
                papers_data.append({
                    'arxiv_id': paper['id'],
                    'title': paper.get('title', ''),
                    'abstract': paper.get('abstract', ''),
                    'submitter': paper.get('submitter', ''),
                    'journal_ref': paper.get('journal-ref'),
                    'doi': paper.get('doi'),
                    'report_no': paper.get('report-no'),
                    'license': paper.get('license'),
                    'update_date': update_date
                })
                
                # Authors data
                authors_parsed = paper.get('authors_parsed', [])
                for author_data in authors_parsed:
                    last_name = author_data[0] if len(author_data) > 0 else ""
                    first_name = author_data[1] if len(author_data) > 1 else ""
                    suffix = author_data[2] if len(author_data) > 2 else ""
                    
                    full_name = f"{first_name} {last_name}".strip()
                    if suffix:
                        full_name += f" {suffix}"
                    
                    if full_name:
                        authors_data.append({'name': full_name})
                        author_relationships.append({
                            'author_name': full_name,
                            'paper_id': paper['id']
                        })
                
                # Categories data
                categories_str = paper.get('categories', '')
                if categories_str:
                    categories = [cat.strip() for cat in categories_str.split(' ') if cat.strip()]
                    for category_id in categories:
                        categories_data.append({
                            'id': category_id,
                            'name': category_id
                        })
                        category_relationships.append({
                            'category_id': category_id,
                            'paper_id': paper['id']
                        })
            
            # Execute batch operations
            session.run("""
                UNWIND $papers AS paper
                MERGE (p:Paper {arxiv_id: paper.arxiv_id})
                SET p.title = paper.title,
                    p.abstract = paper.abstract,
                    p.submitter = paper.submitter,
                    p.journal_ref = paper.journal_ref,
                    p.doi = paper.doi,
                    p.report_no = paper.report_no,
                    p.license = paper.license,
                    p.update_date = paper.update_date
            """, papers=papers_data)
            
            # Create authors
            if authors_data:
                session.run("""
                    UNWIND $authors AS author
                    MERGE (a:Author {name: author.name})
                """, authors=authors_data)
            
            # Create categories
            if categories_data:
                session.run("""
                    UNWIND $categories AS category
                    MERGE (c:Category {id: category.id})
                    SET c.name = category.name
                """, categories=categories_data)
            
            # Create author relationships
            if author_relationships:
                session.run("""
                    UNWIND $relationships AS rel
                    MATCH (a:Author {name: rel.author_name})
                    MATCH (p:Paper {arxiv_id: rel.paper_id})
                    MERGE (a)-[:WROTE]->(p)
                """, relationships=author_relationships)
            
            # Create category relationships
            if category_relationships:
                session.run("""
                    UNWIND $relationships AS rel
                    MATCH (p:Paper {arxiv_id: rel.paper_id})
                    MATCH (c:Category {id: rel.category_id})
                    MERGE (p)-[:HAS_CATEGORY]->(c)
                """, relationships=category_relationships)
    
    def load_arxiv_data(self, data_file: str, limit: Optional[int] = None, batch_size: int = 1000) -> None:
        """Load arXiv data from JSON file into Neo4j using optimized batch processing"""
        start_time = time.time()
        logger.info(f"Starting optimized load of arXiv data from {data_file} (batch_size: {batch_size})")
        
        with open(data_file, 'r', encoding='utf-8') as f:
            count = 0
            batch = []
            batch_start_time = time.time()
            
            for line_num, line in enumerate(f, 1):
                try:
                    paper_data = json.loads(line.strip())
                    batch.append(paper_data)
                    count += 1
                    
                    # Process batch when it reaches batch_size
                    if len(batch) >= batch_size:
                        self.process_paper_batch(batch)
                        
                        # Log progress
                        batch_time = time.time() - batch_start_time
                        papers_per_second = batch_size / batch_time
                        total_elapsed = time.time() - start_time
                        
                        logger.info(f"Processed {count} papers | "
                                  f"Batch: {papers_per_second:.1f} papers/sec | "
                                  f"Avg: {count / total_elapsed:.1f} papers/sec | "
                                  f"Total time: {total_elapsed:.1f}s")
                        
                        # Reset for next batch
                        batch = []
                        batch_start_time = time.time()
                    
                    if limit and count >= limit:
                        break
                        
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON on line {line_num}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing line {line_num}: {e}")
                    continue
            
            # Process remaining papers in final batch
            if batch:
                self.process_paper_batch(batch)
        
        total_time = time.time() - start_time
        avg_rate = count / total_time if total_time > 0 else 0
        
        logger.info(f"Finished loading {count} papers in {total_time:.1f}s")
        logger.info(f"Average rate: {avg_rate:.1f} papers/second")
        
        # Estimate time for full dataset
        if limit:
            estimated_total = 2_300_000
            estimated_time = estimated_total / avg_rate if avg_rate > 0 else 0
            hours = estimated_time / 3600
            logger.info(f"Estimated time for full dataset (~{estimated_total:,} papers): "
                      f"{estimated_time:.0f}s ({hours:.1f} hours)")


def main():
    import sys
    
    # Database connection parameters
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    
    # Data file path
    data_file = "data/arxiv-metadata-oai-snapshot.json"
    
    # Parse command line arguments
    limit = None
    batch_size = 1000
    
    if len(sys.argv) > 1:
        if sys.argv[1].lower() == "all":
            limit = None
        else:
            try:
                limit = int(sys.argv[1])
            except ValueError:
                print("Usage: python src/arxiv_loader_optimized.py [number_of_papers|all] [batch_size]")
                sys.exit(1)
    else:
        limit = 10000  # Default limit for testing
    
    if len(sys.argv) > 2:
        try:
            batch_size = int(sys.argv[2])
        except ValueError:
            print("Invalid batch size")
            sys.exit(1)
    
    # Initialize loader
    loader = OptimizedArxivNeo4jLoader(uri, username, password)
    
    try:
        # Clear existing data
        loader.clear_database()
        
        # Create constraints
        loader.create_constraints()
        
        # Load data
        if limit:
            logger.info(f"Loading {limit} papers with batch size {batch_size}...")
        else:
            logger.info(f"Loading ALL papers with batch size {batch_size}...")
        loader.load_arxiv_data(data_file, limit=limit, batch_size=batch_size)
        
    finally:
        loader.close()


if __name__ == "__main__":
    main()