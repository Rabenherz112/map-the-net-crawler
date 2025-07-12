#!/usr/bin/env python3
"""
Cleanup script for stuck queue items in "processing" status.
This script helps resolve issues where crawlers crash or are interrupted
without properly cleaning up their processing status.
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timedelta
from database import DatabaseManager
from config import DB_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def cleanup_stuck_queue_items(timeout_minutes=30, dry_run=False):
    """
    Clean up queue items that have been stuck in 'processing' status for too long.
    
    Args:
        timeout_minutes: Items stuck in processing for longer than this will be reset
        dry_run: If True, only show what would be done without making changes
    """
    db = DatabaseManager()
    
    try:
        db.connect()
        
        # Find items stuck in processing status for too long
        timeout_seconds = timeout_minutes * 60
        cutoff_time = datetime.now() - timedelta(seconds=timeout_seconds)
        
        cursor = db.connection.cursor(dictionary=True)
        
        # Get stuck items
        query = """
            SELECT id, url, domain_name, processed_at, error_message
            FROM discovery_queue 
            WHERE status = 'processing' 
            AND processed_at < %s
            ORDER BY processed_at ASC
        """
        
        cursor.execute(query, (cutoff_time,))
        stuck_items = cursor.fetchall()
        
        if not stuck_items:
            logger.info(f"No items found stuck in processing for more than {timeout_minutes} minutes")
            return
        
        logger.info(f"Found {len(stuck_items)} items stuck in processing status:")
        
        for item in stuck_items:
            stuck_duration = datetime.now() - item['processed_at']
            logger.info(f"  - ID {item['id']}: {item['url']} (stuck for {stuck_duration})")
        
        if dry_run:
            logger.info("DRY RUN: Would reset these items to 'pending' status")
            return
        
        # Reset stuck items to pending status
        stuck_ids = [str(item['id']) for item in stuck_items]
        update_query = f"""
            UPDATE discovery_queue 
            SET status = 'pending', 
                processed_at = NULL, 
                error_message = 'Reset from stuck processing status'
            WHERE id IN ({','.join(stuck_ids)})
        """
        
        cursor.execute(update_query)
        db.connection.commit()
        
        logger.info(f"Successfully reset {len(stuck_items)} items from 'processing' to 'pending' status")
        
    except Exception as e:
        logger.error(f"Error cleaning up stuck queue items: {e}")
    finally:
        if cursor:
            cursor.close()
        db.close()

def get_queue_stats():
    """Get and display current queue statistics"""
    db = DatabaseManager()
    
    try:
        db.connect()
        stats = db.get_queue_stats()
        
        logger.info("Current queue statistics:")
        for status, count in stats.items():
            logger.info(f"  - {status}: {count}")
        
        # Show processing items with their age
        cursor = db.connection.cursor(dictionary=True)
        cursor.execute("""
            SELECT COUNT(*) as count, 
                   MIN(processed_at) as oldest,
                   MAX(processed_at) as newest
            FROM discovery_queue 
            WHERE status = 'processing'
        """)
        
        result = cursor.fetchone()
        if result and result['count'] > 0:
            logger.info(f"Processing items: {result['count']} total")
            if result['oldest']:
                oldest_age = datetime.now() - result['oldest']
                logger.info(f"  - Oldest: {oldest_age}")
            if result['newest']:
                newest_age = datetime.now() - result['newest']
                logger.info(f"  - Newest: {newest_age}")
        
    except Exception as e:
        logger.error(f"Error getting queue stats: {e}")
    finally:
        if cursor:
            cursor.close()
        db.close()

def main():
    parser = argparse.ArgumentParser(description='Clean up stuck queue items')
    parser.add_argument('--timeout', type=int, default=30, 
                       help='Timeout in minutes for stuck items (default: 30)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without making changes')
    parser.add_argument('--stats-only', action='store_true',
                       help='Only show queue statistics without cleaning up')
    
    args = parser.parse_args()
    
    if args.stats_only:
        get_queue_stats()
        return
    
    logger.info(f"Cleaning up items stuck in processing for more than {args.timeout} minutes...")
    cleanup_stuck_queue_items(args.timeout, args.dry_run)
    
    if not args.dry_run:
        logger.info("Showing updated queue statistics:")
        get_queue_stats()

if __name__ == "__main__":
    main() 