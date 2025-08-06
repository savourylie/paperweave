import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

from dotenv import load_dotenv
from neo4j import GraphDatabase

from oai_pmh_client import OAIPMHClient

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ArxivUpdater:
    """Manages incremental updates of arXiv metadata using OAI-PMH."""
    
    def __init__(self, neo4j_uri: str, neo4j_username: str, neo4j_password: str):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_username, neo4j_password))
        self.oai_client = OAIPMHClient()
        
    def close(self):
        self.driver.close()
    
    def get_last_update_timestamp(self) -> Optional[datetime]:
        """Get the timestamp of the last successful update."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (u:UpdateLog) 
                RETURN u.last_update_time as last_update 
                ORDER BY u.last_update_time DESC 
                LIMIT 1
            """)
            
            record = result.single()
            if record and record['last_update']:
                return datetime.fromisoformat(record['last_update'])
            
            # If no update log exists, get the latest paper update_date
            result = session.run("""
                MATCH (p:Paper) 
                WHERE p.update_date IS NOT NULL 
                RETURN MAX(p.update_date) as max_date
            """)
            
            record = result.single()
            if record and record['max_date']:
                return record['max_date']
            
            # Default to 7 days ago for first run
            return datetime.now() - timedelta(days=7)
    
    def record_update_timestamp(self, timestamp: datetime):
        """Record the timestamp of a successful update."""
        with self.driver.session() as session:
            session.run("""
                MERGE (u:UpdateLog {id: 'main'})
                SET u.last_update_time = $timestamp,
                    u.update_count = COALESCE(u.update_count, 0) + 1
            """, timestamp=timestamp.isoformat())
    
    def convert_oai_record_to_paper_data(self, oai_record: Dict) -> Optional[Dict]:
        """Convert OAI-PMH record to our paper data format."""
        try:
            if oai_record.get('status') == 'deleted':
                return {
                    'id': oai_record.get('arxiv_id'),
                    'status': 'deleted'
                }
            
            # Map Dublin Core fields to our schema
            paper_data = {
                'id': oai_record.get('arxiv_id'),
                'title': oai_record.get('title', ''),
                'abstract': oai_record.get('description', ''),
                'categories': ' '.join(oai_record.get('sets', [])),
                'update_date': oai_record.get('datestamp', ''),
                'submitter': oai_record.get('creator', ''),
                'authors': oai_record.get('creator', ''),  # Will need to parse multiple creators
                'journal-ref': None,
                'doi': None,
                'report-no': None,
                'license': None
            }
            
            # Handle multiple authors if present
            creators = oai_record.get('creator', [])
            if isinstance(creators, list):
                paper_data['authors'] = ', '.join(creators)
                # Create simple authors_parsed format
                paper_data['authors_parsed'] = []
                for creator in creators:
                    # Simple name parsing - could be improved
                    name_parts = creator.strip().split()
                    if len(name_parts) >= 2:
                        last_name = name_parts[-1]
                        first_names = ' '.join(name_parts[:-1])
                        paper_data['authors_parsed'].append([last_name, first_names, ''])
                    elif len(name_parts) == 1:
                        paper_data['authors_parsed'].append([name_parts[0], '', ''])
            else:
                # Single author
                if creators:
                    name_parts = creators.strip().split()
                    if len(name_parts) >= 2:
                        last_name = name_parts[-1]
                        first_names = ' '.join(name_parts[:-1])
                        paper_data['authors_parsed'] = [[last_name, first_names, '']]
                    else:
                        paper_data['authors_parsed'] = [[creators, '', '']]
                else:
                    paper_data['authors_parsed'] = []
            
            return paper_data
            
        except Exception as e:
            logger.warning(f"Failed to convert OAI record: {e}")
            return None
    
    def delete_paper(self, arxiv_id: str):
        """Delete a paper and its relationships from the database."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p:Paper {arxiv_id: $arxiv_id})
                DETACH DELETE p
                RETURN count(p) as deleted_count
            """, arxiv_id=arxiv_id)
            
            record = result.single()
            deleted_count = record['deleted_count'] if record else 0
            
            if deleted_count > 0:
                logger.info(f"Deleted paper {arxiv_id}")
            else:
                logger.warning(f"Paper {arxiv_id} not found for deletion")
    
    def upsert_paper_batch(self, papers_batch: List[Dict]) -> Dict[str, int]:
        """Insert or update a batch of papers."""
        if not papers_batch:
            return {'updated': 0, 'deleted': 0, 'errors': 0}
        
        stats = {'updated': 0, 'deleted': 0, 'errors': 0}
        
        # Separate deleted papers from updates
        deleted_papers = [p for p in papers_batch if p.get('status') == 'deleted']
        active_papers = [p for p in papers_batch if p.get('status') != 'deleted']
        
        # Handle deletions
        for paper in deleted_papers:
            try:
                self.delete_paper(paper['id'])
                stats['deleted'] += 1
            except Exception as e:
                logger.error(f"Error deleting paper {paper['id']}: {e}")
                stats['errors'] += 1
        
        # Handle updates/inserts using our existing optimized loader logic
        if active_papers:
            try:
                self._process_active_papers_batch(active_papers)
                stats['updated'] += len(active_papers)
            except Exception as e:
                logger.error(f"Error processing active papers batch: {e}")
                stats['errors'] += len(active_papers)
        
        return stats
    
    def _process_active_papers_batch(self, papers_batch: List[Dict]):
        """Process active papers using optimized batch operations."""
        with self.driver.session() as session:
            # Prepare batch data (similar to optimized loader)
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
                        update_date = datetime.fromisoformat(paper['update_date'].replace('Z', '+00:00'))
                    except ValueError:
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
            
            # Execute batch operations (using MERGE for upsert behavior)
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
                    p.update_date = paper.update_date,
                    p.last_modified = datetime()
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
            
            # Update author relationships (remove old ones first)
            if author_relationships:
                # Remove existing author relationships for these papers
                paper_ids = [rel['paper_id'] for rel in author_relationships]
                session.run("""
                    UNWIND $paper_ids AS paper_id
                    MATCH (p:Paper {arxiv_id: paper_id})<-[r:WROTE]-()
                    DELETE r
                """, paper_ids=list(set(paper_ids)))
                
                # Create new relationships
                session.run("""
                    UNWIND $relationships AS rel
                    MATCH (a:Author {name: rel.author_name})
                    MATCH (p:Paper {arxiv_id: rel.paper_id})
                    MERGE (a)-[:WROTE]->(p)
                """, relationships=author_relationships)
            
            # Update category relationships (remove old ones first)
            if category_relationships:
                paper_ids = [rel['paper_id'] for rel in category_relationships]
                session.run("""
                    UNWIND $paper_ids AS paper_id
                    MATCH (p:Paper {arxiv_id: paper_id})-[r:HAS_CATEGORY]->()
                    DELETE r
                """, paper_ids=list(set(paper_ids)))
                
                # Create new relationships
                session.run("""
                    UNWIND $relationships AS rel
                    MATCH (p:Paper {arxiv_id: rel.paper_id})
                    MATCH (c:Category {id: rel.category_id})
                    MERGE (p)-[:HAS_CATEGORY]->(c)
                """, relationships=category_relationships)
    
    def run_incremental_update(self, batch_size: int = 1000) -> Dict[str, int]:
        """Run an incremental update of arXiv metadata."""
        start_time = time.time()
        
        # Get last update timestamp
        last_update = self.get_last_update_timestamp()
        logger.info(f"Starting incremental update from {last_update}")
        
        total_stats = {'updated': 0, 'deleted': 0, 'errors': 0}
        
        try:
            # Harvest records incrementally
            for batch in self.oai_client.harvest_incremental(last_update):
                # Convert OAI records to our format
                papers_batch = []
                for oai_record in batch:
                    paper_data = self.convert_oai_record_to_paper_data(oai_record)
                    if paper_data:
                        papers_batch.append(paper_data)
                
                # Process batch
                if papers_batch:
                    batch_stats = self.upsert_paper_batch(papers_batch)
                    for key in total_stats:
                        total_stats[key] += batch_stats[key]
                    
                    logger.info(f"Processed batch: {batch_stats}")
            
            # Record successful update
            self.record_update_timestamp(datetime.now())
            
            elapsed_time = time.time() - start_time
            logger.info(f"Incremental update completed in {elapsed_time:.1f}s")
            logger.info(f"Total stats: {total_stats}")
            
            return total_stats
            
        except Exception as e:
            logger.error(f"Incremental update failed: {e}")
            raise


def main():
    """Run incremental update."""
    # Database connection parameters
    uri = os.getenv("NEO4J_URI") or "neo4j://localhost:7687"
    username = os.getenv("NEO4J_USERNAME") or "neo4j"
    password = os.getenv("NEO4J_PASSWORD")
    if not password:
        raise ValueError("NEO4J_PASSWORD environment variable is required")
    
    updater = ArxivUpdater(uri, username, password)
    
    try:
        stats = updater.run_incremental_update()
        print(f"Update completed: {stats}")
    finally:
        updater.close()


if __name__ == "__main__":
    main()