#!/usr/bin/env python3
"""
Archive old collection logs to manage database size.
This script can archive logs older than a specified date and optionally export them to CSV.
"""

import argparse
import logging
import csv
import os
import sys
from datetime import datetime, timedelta
from database import DatabaseManager
from config import DB_CONFIG

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CollectionLogsArchiver:
    def __init__(self):
        self.db = DatabaseManager()
    
    def get_logs_statistics(self):
        """Get statistics about collection logs"""
        cursor = None
        try:
            if not self.db.connection:
                logger.error("Database connection not available")
                return None
            
            cursor = self.db.connection.cursor(dictionary=True)
            
            # Get total count
            cursor.execute("SELECT COUNT(*) as total FROM collection_logs")
            total_count = cursor.fetchone()['total']
            
            # Get count by status
            cursor.execute("""
                SELECT status, COUNT(*) as count 
                FROM collection_logs 
                GROUP BY status
            """)
            status_counts = cursor.fetchall()
            
            # Get date range
            cursor.execute("""
                SELECT 
                    MIN(collected_at) as oldest,
                    MAX(collected_at) as newest
                FROM collection_logs
            """)
            date_range = cursor.fetchone()
            
            # Get count by agent
            cursor.execute("""
                SELECT agent_name, COUNT(*) as count 
                FROM collection_logs 
                WHERE agent_name IS NOT NULL
                GROUP BY agent_name
                ORDER BY count DESC
                LIMIT 10
            """)
            agent_counts = cursor.fetchall()
            
            return {
                'total': total_count,
                'status_counts': status_counts,
                'date_range': date_range,
                'agent_counts': agent_counts
            }
            
        except Exception as e:
            logger.error(f"Error getting log statistics: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
    
    def get_old_logs(self, days_old, status_filter=None):
        """Get logs older than specified days"""
        cursor = None
        try:
            if not self.db.connection:
                logger.error("Database connection not available")
                return []
            
            cursor = self.db.connection.cursor(dictionary=True)
            
            cutoff_date = datetime.now() - timedelta(days=days_old)
            
            query = """
                SELECT id, domain_name, status, error_message, collected_at,
                       processing_time, relationships_found, urls_discovered, 
                       url, agent_name
                FROM collection_logs 
                WHERE collected_at < %s
            """
            params = [cutoff_date]
            
            if status_filter:
                query += " AND status = %s"
                params.append(status_filter)
            
            query += " ORDER BY collected_at ASC"
            
            cursor.execute(query, params)
            logs = cursor.fetchall()
            
            return logs
            
        except Exception as e:
            logger.error(f"Error getting old logs: {e}")
            return []
        finally:
            if cursor:
                cursor.close()
    
    def export_logs_to_csv(self, logs, filename):
        """Export logs to CSV file"""
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'id', 'domain_name', 'status', 'error_message', 'collected_at',
                    'processing_time', 'relationships_found', 'urls_discovered',
                    'url', 'agent_name'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for log in logs:
                    # Convert datetime objects to strings for CSV
                    log_copy = log.copy()
                    if log_copy['collected_at']:
                        log_copy['collected_at'] = log_copy['collected_at'].strftime('%Y-%m-%d %H:%M:%S')
                    writer.writerow(log_copy)
            
            logger.info(f"Exported {len(logs)} logs to {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting logs to CSV: {e}")
            return False
    
    def delete_old_logs(self, days_old, status_filter=None, dry_run=False):
        """Delete logs older than specified days"""
        cursor = None
        try:
            if not self.db.connection:
                logger.error("Database connection not available")
                return 0
            
            cursor = self.db.connection.cursor()
            
            cutoff_date = datetime.now() - timedelta(days=days_old)
            
            query = "DELETE FROM collection_logs WHERE collected_at < %s"
            params = [cutoff_date]
            
            if status_filter:
                query += " AND status = %s"
                params.append(status_filter)
            
            if dry_run:
                # For dry run, just count the records that would be deleted
                count_query = query.replace("DELETE FROM", "SELECT COUNT(*) FROM")
                cursor.execute(count_query, params)
                count = cursor.fetchone()[0]
                logger.info(f"Would delete {count} logs older than {days_old} days")
                return count
            else:
                cursor.execute(query, params)
                deleted_count = cursor.rowcount
                self.db.connection.commit()
                logger.info(f"Successfully deleted {deleted_count} logs older than {days_old} days")
                return deleted_count
                
        except Exception as e:
            logger.error(f"Error deleting old logs: {e}")
            if not dry_run and self.db.connection:
                self.db.connection.rollback()
            return 0
        finally:
            if cursor:
                cursor.close()
    
    def archive_logs(self, days_old, export_csv=True, status_filter=None, dry_run=False):
        """Archive logs older than specified days"""
        logger.info(f"{'DRY RUN' if dry_run else 'ARCHIVING'} - Collection logs older than {days_old} days")
        logger.info("=" * 60)
        
        # Get old logs
        old_logs = self.get_old_logs(days_old, status_filter)
        
        if not old_logs:
            logger.info("No old logs found to archive")
            return
        
        logger.info(f"Found {len(old_logs)} logs to archive")
        
        # Show some examples
        logger.info("Example logs to archive:")
        for log in old_logs[:5]:
            logger.info(f"  - {log['domain_name']} ({log['status']}) - {log['collected_at']}")
        if len(old_logs) > 5:
            logger.info(f"  ... and {len(old_logs) - 5} more")
        
        # Export to CSV if requested
        if export_csv and not dry_run:
            # Create resources/collection_logs_archive directory if it doesn't exist
            archive_dir = os.path.join('resources', 'collection_logs_archive')
            os.makedirs(archive_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = os.path.join(archive_dir, f"collection_logs_archive_{timestamp}.csv")
            
            if self.export_logs_to_csv(old_logs, filename):
                logger.info(f"Logs exported to {filename}")
            else:
                logger.error("Failed to export logs to CSV")
        
        # Delete old logs
        deleted_count = self.delete_old_logs(days_old, status_filter, dry_run)
        
        logger.info("=" * 60)
        if dry_run:
            logger.info(f"Dry run completed - would archive {len(old_logs)} logs")
        else:
            logger.info(f"Archiving completed - archived {deleted_count} logs")
    
    def show_statistics(self):
        """Show detailed statistics about collection logs"""
        stats = self.get_logs_statistics()
        
        if not stats:
            logger.error("Could not retrieve log statistics")
            return
        
        logger.info("Collection Logs Statistics")
        logger.info("=" * 60)
        logger.info(f"Total logs: {stats['total']}")
        
        if stats['date_range']['oldest'] and stats['date_range']['newest']:
            logger.info(f"Date range: {stats['date_range']['oldest']} to {stats['date_range']['newest']}")
        
        logger.info("\nLogs by status:")
        for status_count in stats['status_counts']:
            logger.info(f"  - {status_count['status']}: {status_count['count']}")
        
        if stats['agent_counts']:
            logger.info("\nTop agents by log count:")
            for agent_count in stats['agent_counts']:
                logger.info(f"  - {agent_count['agent_name']}: {agent_count['count']}")
    
    def cleanup_by_status(self, status, days_old, dry_run=False):
        """Clean up logs of a specific status older than specified days"""
        logger.info(f"{'DRY RUN' if dry_run else 'CLEANUP'} - {status} logs older than {days_old} days")
        logger.info("=" * 60)
        
        old_logs = self.get_old_logs(days_old, status)
        
        if not old_logs:
            logger.info(f"No {status} logs found older than {days_old} days")
            return
        
        logger.info(f"Found {len(old_logs)} {status} logs to clean up")
        
        # Show some examples
        logger.info(f"Example {status} logs to clean up:")
        for log in old_logs[:5]:
            logger.info(f"  - {log['domain_name']} - {log['collected_at']}")
        if len(old_logs) > 5:
            logger.info(f"  ... and {len(old_logs) - 5} more")
        
        # Delete logs
        deleted_count = self.delete_old_logs(days_old, status, dry_run)
        
        logger.info("=" * 60)
        if dry_run:
            logger.info(f"Dry run completed - would clean up {len(old_logs)} {status} logs")
        else:
            logger.info(f"Cleanup completed - removed {deleted_count} {status} logs")
    
    def close(self):
        """Close database connection"""
        self.db.close()


def main():
    parser = argparse.ArgumentParser(description='Archive old collection logs')
    parser.add_argument('--days', type=int, default=30, help='Archive logs older than N days (default: 30)')
    parser.add_argument('--status', choices=['pending', 'processing', 'completed', 'failed'], 
                       help='Filter by specific status')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    parser.add_argument('--stats-only', action='store_true', help='Only show statistics, no archiving')
    parser.add_argument('--no-export', action='store_true', help='Skip CSV export')
    parser.add_argument('--force', action='store_true', help='Force archiving without confirmation')
    
    args = parser.parse_args()
    
    if args.stats_only:
        # Only show statistics
        archiver = CollectionLogsArchiver()
        try:
            archiver.show_statistics()
        finally:
            archiver.close()
        return
    
    if not args.dry_run and not args.force:
        # Ask for confirmation
        print(f"WARNING: This will archive collection logs older than {args.days} days!")
        if args.status:
            print(f"This will only affect logs with status: {args.status}")
        print("This action cannot be undone.")
        response = input("Are you sure you want to continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Archiving cancelled.")
            return
    
    archiver = CollectionLogsArchiver()
    try:
        if args.status:
            # Clean up specific status
            archiver.cleanup_by_status(args.status, args.days, args.dry_run)
        else:
            # Archive all old logs
            archiver.archive_logs(args.days, not args.no_export, args.status, args.dry_run)
    finally:
        archiver.close()


if __name__ == "__main__":
    main() 