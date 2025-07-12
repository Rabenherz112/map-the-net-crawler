#!/usr/bin/env python3
"""
Cleanup script for stuck queue items and transactions.
This script helps resolve the "Transaction already in progress" error.
"""

import logging
from database import DatabaseManager
from config import COLLECTION_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def cleanup_stuck_items():
    """Clean up stuck queue items and transactions"""
    db = DatabaseManager()
    
    try:
        # Clean up any stuck transactions first
        logger.info("Cleaning up stuck transactions...")
        if db.cleanup_stuck_transactions():
            logger.info("Found and cleaned up stuck transaction")
        else:
            logger.info("No stuck transactions found")
        
        # Reset any items stuck in 'processing' status
        logger.info("Checking for stuck processing items...")
        cursor = db.connection.cursor(dictionary=True)
        
        # Find items stuck in processing status
        cursor.execute("""
            SELECT id, url, domain_name, processed_at, error_message
            FROM discovery_queue 
            WHERE status = 'processing'
            ORDER BY processed_at ASC
        """)
        
        stuck_items = cursor.fetchall()
        
        if stuck_items:
            logger.info(f"Found {len(stuck_items)} items stuck in processing status")
            
            # Reset them to pending status
            stuck_ids = [str(item['id']) for item in stuck_items]
            update_query = f"""
                UPDATE discovery_queue 
                SET status = 'pending', 
                    processed_at = NULL, 
                    error_message = 'Reset from stuck processing status (cleanup script)'
                WHERE id IN ({','.join(stuck_ids)})
            """
            
            cursor.execute(update_query)
            db.connection.commit()
            
            logger.info(f"Reset {len(stuck_items)} stuck items to pending status")
            
            # Show details of reset items
            for item in stuck_items:
                logger.info(f"  - {item['domain_name']} ({item['url']}) - stuck since {item['processed_at']}")
        else:
            logger.info("No stuck processing items found")
        
        # Show current queue stats
        stats = db.get_queue_stats()
        logger.info(f"Current queue stats: {stats}")
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
    finally:
        db.close()

def main():
    """Main cleanup function"""
    print("=== Queue Cleanup Script ===")
    print("This script will clean up stuck transactions and processing items.")
    print()
    
    cleanup_stuck_items()
    
    print()
    print("Cleanup completed!")

if __name__ == "__main__":
    main() 