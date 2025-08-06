"""
Quick Dataset Summary - Fast overview without full analysis

Provides key statistics about the OpenAlex dataset:
- Total directories and files
- Size distribution
- Sample record counts from largest files
"""

import logging
import time
from pathlib import Path
from typing import List, Tuple
import gzip

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def quick_dataset_summary():
    """Provide quick overview of dataset structure and size"""
    logger.info("🔍 QUICK DATASET SUMMARY")
    logger.info("=" * 80)
    
    data_dir = Path("data/works")
    
    # Get all directories and their basic info
    date_dirs = sorted([d for d in data_dir.iterdir() if d.is_dir()])
    total_dirs = len(date_dirs)
    
    logger.info(f"📁 Total directories: {total_dirs}")
    
    # Quick file and size analysis
    total_files = 0
    total_size_mb = 0.0
    file_sizes = []
    
    logger.info("📊 Analyzing file structure...")
    
    for date_dir in date_dirs:
        part_files = list(date_dir.glob("part_*.gz"))
        dir_files = len(part_files)
        dir_size_mb = sum(f.stat().st_size for f in part_files) / (1024 * 1024)
        
        total_files += dir_files
        total_size_mb += dir_size_mb
        
        if dir_files > 0:  # Only record directories with files
            file_sizes.append((date_dir.name, dir_files, dir_size_mb))
    
    logger.info(f"📄 Total files: {total_files:,}")
    logger.info(f"💾 Total size: {total_size_mb:.1f} MB ({total_size_mb/1024:.1f} GB)")
    
    # Show largest directories
    file_sizes.sort(key=lambda x: x[2], reverse=True)  # Sort by size
    
    logger.info("\n📊 TOP 10 LARGEST DIRECTORIES:")
    for i, (dir_name, files, size_mb) in enumerate(file_sizes[:10], 1):
        logger.info(f"  {i:2d}. {dir_name}: {files} files, {size_mb:.1f} MB")
    
    # Sample record counts from largest files
    logger.info("\n🔢 SAMPLING RECORD COUNTS FROM LARGEST FILES:")
    
    sampled_records = 0
    sampled_size_mb = 0.0
    
    for dir_name, files, size_mb in file_sizes[:5]:  # Sample top 5 directories
        dir_path = data_dir / dir_name
        part_files = sorted(dir_path.glob("part_*.gz"))
        
        for part_file in part_files:
            try:
                with gzip.open(part_file, 'rt') as f:
                    records = sum(1 for _ in f)
                sampled_records += records
                sampled_size_mb += part_file.stat().st_size / (1024 * 1024)
                
                logger.info(f"  📄 {dir_name}/{part_file.name}: {records:,} records ({part_file.stat().st_size / (1024 * 1024):.1f} MB)")
                
            except Exception as e:
                logger.warning(f"  ⚠️ Could not read {part_file.name}: {e}")
    
    # Extrapolate total records
    if sampled_size_mb > 0:
        records_per_mb = sampled_records / sampled_size_mb
        estimated_total_records = int(total_size_mb * records_per_mb)
        
        logger.info(f"\n📊 EXTRAPOLATION:")
        logger.info(f"  Sampled: {sampled_records:,} records from {sampled_size_mb:.1f} MB")
        logger.info(f"  Rate: {records_per_mb:.0f} records/MB")
        logger.info(f"  🎯 Estimated total records: {estimated_total_records:,}")
        
        # Processing time estimates
        works_per_sec = 5000  # Conservative estimate based on our tests
        total_processing_hours = estimated_total_records / works_per_sec / 3600
        
        logger.info(f"  ⏱️ Estimated processing time: {total_processing_hours:.1f} hours")
    
    logger.info("\n" + "=" * 80)
    logger.info("✅ Quick summary complete")

def main():
    quick_dataset_summary()

if __name__ == "__main__":
    main()