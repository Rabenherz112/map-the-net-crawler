#!/usr/bin/env python3
"""
Cleanup script to remove all itch.io and github.io subdomains from the database.
This script will clean up the discovery_queue, domains, and relationships tables.
"""

import argparse
import logging
import re
import sys
from database import DatabaseManager
from config import DB_CONFIG

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UGCCleanup:
    def __init__(self):
        self.db = DatabaseManager()
        # Patterns to match itch.io and github.io subdomains
        self.ugc_patterns = [
            re.compile(r'^[^.]+\.itch\.io$', re.IGNORECASE),
            re.compile(r'^[^.]+\.github\.io$', re.IGNORECASE),
            re.compile(r'^[^.]+\.wordpress\.com$', re.IGNORECASE),
        ]
    
    def is_ugc_subdomain(self, domain):
        """Check if domain is a UGC subdomain (itch.io or github.io)"""
        if not domain:
            return False
        
        domain_lower = domain.lower()
        for pattern in self.ugc_patterns:
            if pattern.search(domain_lower):
                return True
        return False
    
    def get_ugc_domains(self):
        """Get all UGC subdomains from the domains table"""
        cursor = None
        try:
            if not self.db.connection:
                logger.error("Database connection not available")
                return []
            
            cursor = self.db.connection.cursor(dictionary=True)
            query = "SELECT id, domain_name FROM domains"
            cursor.execute(query)
            domains = cursor.fetchall()
            
            ugc_domains = []
            for domain in domains:
                if self.is_ugc_subdomain(domain['domain_name']):
                    ugc_domains.append(domain)
            
            return ugc_domains
            
        except Exception as e:
            logger.error(f"Error getting UGC domains: {e}")
            return []
        finally:
            if cursor:
                cursor.close()
    
    def get_ugc_queue_items(self):
        """Get all UGC subdomains from the discovery_queue"""
        cursor = None
        try:
            if not self.db.connection:
                logger.error("Database connection not available")
                return []
            
            cursor = self.db.connection.cursor(dictionary=True)
            query = "SELECT id, domain_name, url FROM discovery_queue"
            cursor.execute(query)
            items = cursor.fetchall()
            
            ugc_items = []
            for item in items:
                if self.is_ugc_subdomain(item['domain_name']):
                    ugc_items.append(item)
            
            return ugc_items
            
        except Exception as e:
            logger.error(f"Error getting UGC queue items: {e}")
            return []
        finally:
            if cursor:
                cursor.close()
    
    def get_ugc_relationships(self):
        """Get all relationships involving UGC subdomains"""
        try:
            cursor = self.db.connection.cursor(dictionary=True)
            query = """
                SELECT r.id, r.source, r.target, r.type, r.link_text, r.link_url
                FROM relationships r
                JOIN domains d1 ON r.source = d1.domain_name
                JOIN domains d2 ON r.target = d2.domain_name
                WHERE self.is_ugc_subdomain(d1.domain_name) OR self.is_ugc_subdomain(d2.domain_name)
            """
            cursor.execute(query)
            relationships = cursor.fetchall()
            
            return relationships
            
        except Exception as e:
            logger.error(f"Error getting UGC relationships: {e}")
            return []
        finally:
            if cursor:
                cursor.close()
    
    def get_ugc_relationships_alt(self):
        """Alternative method to get relationships involving UGC subdomains"""
        cursor = None
        try:
            if not self.db.connection:
                logger.error("Database connection not available")
                return []
            
            cursor = self.db.connection.cursor(dictionary=True)
            query = """
                SELECT r.id, r.source_domain_id, r.target_domain_id, r.relationship_type, 
                       r.link_text, r.link_url,
                       d1.domain_name as source_domain, d2.domain_name as target_domain
                FROM relationships r
                JOIN domains d1 ON r.source_domain_id = d1.id
                JOIN domains d2 ON r.target_domain_id = d2.id
            """
            cursor.execute(query)
            relationships = cursor.fetchall()
            
            ugc_relationships = []
            for rel in relationships:
                if self.is_ugc_subdomain(rel['source_domain']) or self.is_ugc_subdomain(rel['target_domain']):
                    ugc_relationships.append(rel)
            
            return ugc_relationships
            
        except Exception as e:
            logger.error(f"Error getting UGC relationships: {e}")
            return []
        finally:
            if cursor:
                cursor.close()
    
    def show_statistics(self):
        """Show statistics about UGC domains in the database"""
        logger.info("Analyzing UGC domains in database...")
        
        # Get UGC domains
        ugc_domains = self.get_ugc_domains()
        logger.info(f"Found {len(ugc_domains)} UGC domains in domains table")
        
        # Get UGC queue items
        ugc_queue_items = self.get_ugc_queue_items()
        logger.info(f"Found {len(ugc_queue_items)} UGC items in discovery_queue")
        
        # Get UGC relationships
        ugc_relationships = self.get_ugc_relationships_alt()
        logger.info(f"Found {len(ugc_relationships)} relationships involving UGC domains")
        
        # Show some examples
        if ugc_domains:
            logger.info("Example UGC domains found:")
            for domain in ugc_domains[:5]:  # Show first 5
                logger.info(f"  - {domain['domain_name']} (ID: {domain['id']})")
            if len(ugc_domains) > 5:
                logger.info(f"  ... and {len(ugc_domains) - 5} more")
        
        if ugc_queue_items:
            logger.info("Example UGC queue items found:")
            for item in ugc_queue_items[:5]:  # Show first 5
                logger.info(f"  - {item['domain_name']} (ID: {item['id']})")
            if len(ugc_queue_items) > 5:
                logger.info(f"  ... and {len(ugc_queue_items) - 5} more")
        
        if ugc_relationships:
            logger.info("Example UGC relationships found:")
            for rel in ugc_relationships[:5]:  # Show first 5
                logger.info(f"  - {rel['source']} -> {rel['target']} (ID: {rel['id']})")
            if len(ugc_relationships) > 5:
                logger.info(f"  ... and {len(ugc_relationships) - 5} more")
        
        return len(ugc_domains), len(ugc_queue_items), len(ugc_relationships)
    
    def cleanup_domains(self, dry_run=False):
        """Remove UGC domains from the domains table"""
        ugc_domains = self.get_ugc_domains()
        
        if not ugc_domains:
            logger.info("No UGC domains found in domains table")
            return 0
        
        logger.info(f"{'Would remove' if dry_run else 'Removing'} {len(ugc_domains)} UGC domains from domains table")
        
        if dry_run:
            for domain in ugc_domains[:5]:
                logger.info(f"  Would remove: {domain['domain_name']} (ID: {domain['id']})")
            if len(ugc_domains) > 5:
                logger.info(f"  ... and {len(ugc_domains) - 5} more")
            return len(ugc_domains)
        
        cursor = None
        try:
            if not self.db.connection:
                logger.error("Database connection not available")
                return 0
            
            cursor = self.db.connection.cursor()
            
            # Get domain IDs to remove
            domain_ids = [str(domain['id']) for domain in ugc_domains]
            
            # Delete domains
            query = f"DELETE FROM domains WHERE id IN ({','.join(domain_ids)})"
            cursor.execute(query)
            
            deleted_count = cursor.rowcount
            self.db.connection.commit()
            
            logger.info(f"Successfully removed {deleted_count} UGC domains from domains table")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error removing UGC domains: {e}")
            if self.db.connection:
                self.db.connection.rollback()
            return 0
        finally:
            if cursor:
                cursor.close()
    
    def cleanup_queue(self, dry_run=False):
        """Remove UGC items from the discovery_queue"""
        ugc_queue_items = self.get_ugc_queue_items()
        
        if not ugc_queue_items:
            logger.info("No UGC items found in discovery_queue")
            return 0
        
        logger.info(f"{'Would remove' if dry_run else 'Removing'} {len(ugc_queue_items)} UGC items from discovery_queue")
        
        if dry_run:
            for item in ugc_queue_items[:5]:
                logger.info(f"  Would remove: {item['domain_name']} (ID: {item['id']})")
            if len(ugc_queue_items) > 5:
                logger.info(f"  ... and {len(ugc_queue_items) - 5} more")
            return len(ugc_queue_items)
        
        try:
            cursor = self.db.connection.cursor()
            
            # Get queue item IDs to remove
            item_ids = [str(item['id']) for item in ugc_queue_items]
            
            # Delete queue items
            query = f"DELETE FROM discovery_queue WHERE id IN ({','.join(item_ids)})"
            cursor.execute(query)
            
            deleted_count = cursor.rowcount
            self.db.connection.commit()
            
            logger.info(f"Successfully removed {deleted_count} UGC items from discovery_queue")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error removing UGC queue items: {e}")
            self.db.connection.rollback()
            return 0
        finally:
            if cursor:
                cursor.close()
    
    def cleanup_relationships(self, dry_run=False):
        """Remove relationships involving UGC domains"""
        ugc_relationships = self.get_ugc_relationships_alt()
        
        if not ugc_relationships:
            logger.info("No UGC relationships found")
            return 0
        
        logger.info(f"{'Would remove' if dry_run else 'Removing'} {len(ugc_relationships)} UGC relationships")
        
        if dry_run:
            for rel in ugc_relationships[:5]:
                logger.info(f"  Would remove: {rel['source_domain']} -> {rel['target_domain']} (ID: {rel['id']})")
            if len(ugc_relationships) > 5:
                logger.info(f"  ... and {len(ugc_relationships) - 5} more")
            return len(ugc_relationships)
        
        try:
            cursor = self.db.connection.cursor()
            
            # Get relationship IDs to remove
            rel_ids = [str(rel['id']) for rel in ugc_relationships]
            
            # Delete relationships
            query = f"DELETE FROM relationships WHERE id IN ({','.join(rel_ids)})"
            cursor.execute(query)
            
            deleted_count = cursor.rowcount
            self.db.connection.commit()
            
            logger.info(f"Successfully removed {deleted_count} UGC relationships")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error removing UGC relationships: {e}")
            self.db.connection.rollback()
            return 0
        finally:
            if cursor:
                cursor.close()
    
    def cleanup_all(self, dry_run=False):
        """Clean up all UGC domains and related data"""
        logger.info(f"{'DRY RUN' if dry_run else 'CLEANUP'} - Removing UGC domains (itch.io and github.io subdomains)")
        logger.info("=" * 60)
        
        # Show statistics first
        domain_count, queue_count, rel_count = self.show_statistics()
        
        if domain_count == 0 and queue_count == 0 and rel_count == 0:
            logger.info("No UGC domains found. Nothing to clean up.")
            return
        
        # Clean up in order: relationships first, then queue, then domains
        rel_deleted = self.cleanup_relationships(dry_run)
        queue_deleted = self.cleanup_queue(dry_run)
        domain_deleted = self.cleanup_domains(dry_run)
        
        logger.info("=" * 60)
        logger.info(f"Cleanup {'simulation' if dry_run else 'completed'}:")
        logger.info(f"  - Relationships removed: {rel_deleted}")
        logger.info(f"  - Queue items removed: {queue_deleted}")
        logger.info(f"  - Domains removed: {domain_deleted}")
        logger.info(f"  - Total items {'would be' if dry_run else 'were'} removed: {rel_deleted + queue_deleted + domain_deleted}")
    
    def close(self):
        """Close database connection"""
        self.db.close()


def main():
    parser = argparse.ArgumentParser(description='Clean up UGC domains (itch.io and github.io subdomains) from database')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    parser.add_argument('--stats-only', action='store_true', help='Only show statistics, no cleanup')
    parser.add_argument('--force', action='store_true', help='Force cleanup without confirmation (use with caution)')
    
    args = parser.parse_args()
    
    if args.stats_only:
        # Only show statistics
        cleanup = UGCCleanup()
        try:
            cleanup.show_statistics()
        finally:
            cleanup.close()
        return
    
    if not args.dry_run and not args.force:
        # Ask for confirmation
        print("WARNING: This will permanently remove all itch.io and github.io subdomains from the database!")
        print("This includes domains, queue items, and relationships.")
        response = input("Are you sure you want to continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Cleanup cancelled.")
            return
    
    cleanup = UGCCleanup()
    try:
        cleanup.cleanup_all(dry_run=args.dry_run)
    finally:
        cleanup.close()


if __name__ == "__main__":
    main() 