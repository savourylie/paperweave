import json
import logging
import os
import time
from datetime import datetime
from typing import Dict, List, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


class ArxivNeo4jLoader:
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
            # Create constraints
            session.run("CREATE CONSTRAINT paper_arxiv_id IF NOT EXISTS FOR (p:Paper) REQUIRE p.arxiv_id IS UNIQUE")
            session.run("CREATE CONSTRAINT author_name IF NOT EXISTS FOR (a:Author) REQUIRE a.name IS UNIQUE")
            session.run("CREATE CONSTRAINT category_id IF NOT EXISTS FOR (c:Category) REQUIRE c.id IS UNIQUE")
            logger.info("Constraints created")
    
    def create_paper(self, paper_data: Dict) -> None:
        """Create a Paper node with its properties"""
        with self.driver.session() as session:
            # Parse update_date
            update_date = None
            if paper_data.get('update_date'):
                try:
                    update_date = datetime.strptime(paper_data['update_date'], '%Y-%m-%d')
                except ValueError:
                    logger.warning(f"Invalid date format for paper {paper_data.get('id')}: {paper_data.get('update_date')}")
            
            session.run("""
                MERGE (p:Paper {arxiv_id: $arxiv_id})
                SET p.title = $title,
                    p.abstract = $abstract,
                    p.submitter = $submitter,
                    p.journal_ref = $journal_ref,
                    p.doi = $doi,
                    p.report_no = $report_no,
                    p.license = $license,
                    p.update_date = $update_date
            """, 
                arxiv_id=paper_data['id'],
                title=paper_data.get('title', ''),
                abstract=paper_data.get('abstract', ''),
                submitter=paper_data.get('submitter', ''),
                journal_ref=paper_data.get('journal-ref'),
                doi=paper_data.get('doi'),
                report_no=paper_data.get('report-no'),
                license=paper_data.get('license'),
                update_date=update_date
            )
    
    def create_authors_and_relationships(self, paper_data: Dict) -> None:
        """Create Author nodes and WROTE relationships"""
        authors_parsed = paper_data.get('authors_parsed', [])
        if not authors_parsed:
            return
            
        with self.driver.session() as session:
            for author_data in authors_parsed:
                # Construct full name from parsed components
                last_name = author_data[0] if len(author_data) > 0 else ""
                first_name = author_data[1] if len(author_data) > 1 else ""
                suffix = author_data[2] if len(author_data) > 2 else ""
                
                full_name = f"{first_name} {last_name}".strip()
                if suffix:
                    full_name += f" {suffix}"
                
                if not full_name:
                    continue
                
                # Create author and relationship
                session.run("""
                    MERGE (a:Author {name: $name})
                    WITH a
                    MATCH (p:Paper {arxiv_id: $arxiv_id})
                    MERGE (a)-[:WROTE]->(p)
                """, 
                    name=full_name,
                    arxiv_id=paper_data['id']
                )
    
    def create_categories_and_relationships(self, paper_data: Dict) -> None:
        """Create Category nodes and HAS_CATEGORY relationships"""
        categories_str = paper_data.get('categories', '')
        if not categories_str:
            return
            
        categories = [cat.strip() for cat in categories_str.split(' ') if cat.strip()]
        
        with self.driver.session() as session:
            for category_id in categories:
                # Create category and relationship
                session.run("""
                    MERGE (c:Category {id: $category_id})
                    SET c.name = $category_id
                    WITH c
                    MATCH (p:Paper {arxiv_id: $arxiv_id})
                    MERGE (p)-[:HAS_CATEGORY]->(c)
                """, 
                    category_id=category_id,
                    arxiv_id=paper_data['id']
                )
    
    def load_arxiv_data(self, data_file: str, limit: Optional[int] = None) -> None:
        """Load arXiv data from JSON file into Neo4j"""
        start_time = time.time()
        logger.info(f"Starting to load arXiv data from {data_file}")
        
        with open(data_file, 'r', encoding='utf-8') as f:
            count = 0
            batch_size = 1000
            batch_start_time = time.time()
            
            for line_num, line in enumerate(f, 1):
                try:
                    paper_data = json.loads(line.strip())
                    
                    # Create paper node
                    self.create_paper(paper_data)
                    
                    # Create authors and relationships
                    self.create_authors_and_relationships(paper_data)
                    
                    # Create categories and relationships
                    self.create_categories_and_relationships(paper_data)
                    
                    count += 1
                    
                    if count % batch_size == 0:
                        batch_time = time.time() - batch_start_time
                        papers_per_second = batch_size / batch_time
                        total_elapsed = time.time() - start_time
                        
                        logger.info(f"Processed {count} papers | "
                                  f"Batch: {papers_per_second:.1f} papers/sec | "
                                  f"Avg: {count / total_elapsed:.1f} papers/sec | "
                                  f"Total time: {total_elapsed:.1f}s")
                        
                        # Reset batch timer
                        batch_start_time = time.time()
                    
                    if limit and count >= limit:
                        break
                        
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON on line {line_num}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing line {line_num}: {e}")
                    continue
        
        total_time = time.time() - start_time
        avg_rate = count / total_time if total_time > 0 else 0
        
        logger.info(f"Finished loading {count} papers in {total_time:.1f}s")
        logger.info(f"Average rate: {avg_rate:.1f} papers/second")
        
        # Estimate time for full dataset
        if limit:
            # Estimate total papers in dataset (approximately 2.3M based on arXiv)
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
    if len(sys.argv) > 1:
        if sys.argv[1].lower() == "all":
            limit = None
        else:
            try:
                limit = int(sys.argv[1])
            except ValueError:
                print("Usage: python src/arxiv_loader.py [number_of_papers|all]")
                sys.exit(1)
    else:
        limit = 10000  # Default limit for testing
    
    # Initialize loader
    loader = ArxivNeo4jLoader(uri, username, password)
    
    try:
        # Clear existing data
        loader.clear_database()
        
        # Create constraints
        loader.create_constraints()
        
        # Load data
        if limit:
            logger.info(f"Loading {limit} papers...")
        else:
            logger.info("Loading ALL papers...")
        loader.load_arxiv_data(data_file, limit=limit)
        
    finally:
        loader.close()


if __name__ == "__main__":
    main()