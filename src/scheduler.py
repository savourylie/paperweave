import logging
import os
import time
from datetime import datetime, timedelta

import schedule
from dotenv import load_dotenv

from arxiv_updater import ArxivUpdater

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('arxiv_updater.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ArxivUpdateScheduler:
    """Scheduler for running daily arXiv updates."""
    
    def __init__(self):
        # Database connection parameters
        self.neo4j_uri = os.getenv("NEO4J_URI") or "neo4j://localhost:7687"
        self.neo4j_username = os.getenv("NEO4J_USERNAME") or "neo4j"
        self.neo4j_password = os.getenv("NEO4J_PASSWORD")
        if not self.neo4j_password:
            raise ValueError("NEO4J_PASSWORD environment variable is required")
        
        # Schedule configuration
        self.update_time = os.getenv("UPDATE_TIME", "23:30")  # Default to 11:30 PM ET
        
    def run_scheduled_update(self):
        """Run the scheduled update with error handling."""
        logger.info("=" * 50)
        logger.info("Starting scheduled arXiv update")
        
        updater = None
        try:
            updater = ArxivUpdater(
                self.neo4j_uri, 
                self.neo4j_username, 
                self.neo4j_password
            )
            
            stats = updater.run_incremental_update()
            
            logger.info(f"Scheduled update completed successfully: {stats}")
            
            # Log summary statistics
            total_papers = stats['updated'] + stats['deleted']
            if total_papers > 0:
                logger.info(f"Summary: {total_papers} papers processed "
                          f"({stats['updated']} updated, {stats['deleted']} deleted, "
                          f"{stats['errors']} errors)")
            else:
                logger.info("No new papers found")
                
        except Exception as e:
            logger.error(f"Scheduled update failed: {e}", exc_info=True)
            
        finally:
            if updater:
                updater.close()
            
        logger.info("Scheduled arXiv update completed")
        logger.info("=" * 50)
    
    def test_update(self):
        """Run a test update immediately."""
        logger.info("Running test update...")
        self.run_scheduled_update()
    
    def start_scheduler(self):
        """Start the scheduler daemon."""
        logger.info(f"Starting arXiv update scheduler")
        logger.info(f"Daily updates scheduled for {self.update_time}")
        logger.info(f"Neo4j URI: {self.neo4j_uri}")
        
        # Schedule daily update
        schedule.every().day.at(self.update_time).do(self.run_scheduled_update)
        
        # Optional: Schedule a test run shortly after startup
        # schedule.every(2).minutes.do(self.run_scheduled_update)
        
        logger.info("Scheduler started. Press Ctrl+C to stop.")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
        except Exception as e:
            logger.error(f"Scheduler error: {e}", exc_info=True)


def main():
    """Main entry point."""
    import sys
    
    scheduler = ArxivUpdateScheduler()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            # Run test update
            scheduler.test_update()
        elif sys.argv[1] == "daemon":
            # Start scheduler daemon
            scheduler.start_scheduler()
        else:
            print("Usage: python scheduler.py [test|daemon]")
            print("  test   - Run a single test update")
            print("  daemon - Start the scheduler daemon")
            sys.exit(1)
    else:
        # Default: start scheduler
        scheduler.start_scheduler()


if __name__ == "__main__":
    main()