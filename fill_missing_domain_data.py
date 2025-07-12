#!/usr/bin/env python3
"""
Script to fill missing domain information for all domains in the database.
This script will:
1. Get all domains from the domains table
2. Check which domains have missing information
3. Collect missing data for each domain
4. Update the database with the collected information

Excludes screenshot_path as requested.
"""

import logging
import time
import signal
import sys
from datetime import datetime
from domain_collector import DomainCollector
from database import DatabaseManager
from config import COLLECTION_CONFIG, DATA_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DomainDataFiller:
    def __init__(self):
        """Initialize the domain data filler"""
        self.db = DatabaseManager()
        self.collector = DomainCollector()
        self.shutdown_requested = False
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.shutdown_requested = True
    
    def get_all_domains(self):
        """Get all domains from the database"""
        cursor = None
        try:
            cursor = self.db.connection.cursor(dictionary=True)
            query = "SELECT id, domain_name FROM domains ORDER BY domain_name"
            cursor.execute(query)
            domains = cursor.fetchall()
            logger.info(f"Found {len(domains)} domains in database")
            return domains
        except Exception as e:
            logger.error(f"Error getting domains: {e}")
            return []
        finally:
            if cursor:
                cursor.close()
    
    def get_domain_current_data(self, domain_name):
        """Get current domain data from database"""
        cursor = None
        try:
            cursor = self.db.connection.cursor(dictionary=True)
            query = """
                SELECT title, description, favicon_url, created_date, expiry_date,
                       registrar, nameservers, asn, asn_description, ssl_valid,
                       ssl_expiry, country, ip_address, latitude, longitude,
                       category, tags
                FROM domains 
                WHERE domain_name = %s
            """
            cursor.execute(query, (domain_name,))
            result = cursor.fetchone()
            return result
        except Exception as e:
            logger.error(f"Error getting domain data for {domain_name}: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
    
    def identify_missing_fields(self, domain_data):
        """Identify which fields are missing from domain data"""
        if not domain_data:
            return ['title', 'description', 'favicon_url', 'created_date', 'expiry_date',
                   'registrar', 'nameservers', 'asn', 'asn_description', 'ssl_valid',
                   'ssl_expiry', 'country', 'ip_address', 'latitude', 'longitude',
                   'category', 'tags']
        
        missing_fields = []
        field_mapping = {
            'title': 'title',
            'description': 'description', 
            'favicon_url': 'favicon_url',
            'created_date': 'created_date',
            'expiry_date': 'expiry_date',
            'registrar': 'registrar',
            'nameservers': 'nameservers',
            'asn': 'asn',
            'asn_description': 'asn_description',
            'ssl_valid': 'ssl_valid',
            'ssl_expiry': 'ssl_expiry',
            'country': 'country',
            'ip_address': 'ip_address',
            'latitude': 'latitude',
            'longitude': 'longitude',
            'category': 'category',
            'tags': 'tags'
        }
        
        for field_name, db_field in field_mapping.items():
            if domain_data.get(db_field) is None:
                missing_fields.append(field_name)
        
        return missing_fields
    
    def collect_missing_data(self, domain_name, missing_fields):
        """Collect missing data for a domain"""
        logger.info(f"Collecting missing data for {domain_name}: {missing_fields}")
        
        collected_data = {'domain_name': domain_name}
        
        try:
            # Collect web data (title, description, favicon)
            if any(field in missing_fields for field in ['title', 'description', 'favicon_url']):
                web_data = self.collector._collect_web_data(domain_name)
                if web_data:
                    collected_data.update(web_data)
            
            # Collect WHOIS data
            if any(field in missing_fields for field in ['created_date', 'expiry_date', 'registrar']):
                whois_data = self.collector._collect_whois_data(domain_name)
                if whois_data:
                    collected_data.update(whois_data)
            
            # Collect DNS data (nameservers, ASN)
            if any(field in missing_fields for field in ['nameservers', 'asn', 'asn_description']):
                dns_data = self.collector._collect_dns_data(domain_name)
                if dns_data:
                    collected_data.update(dns_data)
            
            # Collect SSL data
            if any(field in missing_fields for field in ['ssl_valid', 'ssl_expiry']):
                ssl_data = self.collector._collect_ssl_data(domain_name)
                if ssl_data:
                    collected_data.update(ssl_data)
            
            # Collect geolocation data
            if any(field in missing_fields for field in ['country', 'ip_address', 'latitude', 'longitude']):
                geo_data = self.collector._collect_geolocation_data(domain_name)
                if geo_data:
                    collected_data.update(geo_data)
            
            # Add delay between requests to be respectful
            time.sleep(COLLECTION_CONFIG.get('request_delay', 1))
            
            return collected_data
            
        except Exception as e:
            logger.error(f"Error collecting data for {domain_name}: {e}")
            return collected_data
    
    def update_domain_data(self, domain_name, new_data):
        """Update domain data in database"""
        try:
            # Use the existing insert_domain method which handles upserts
            domain_id = self.db.insert_domain(new_data)
            logger.info(f"Updated domain {domain_name} with ID: {domain_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating domain {domain_name}: {e}")
            return False
    
    def process_domains(self, max_domains=None, dry_run=False):
        """Process all domains and fill missing data"""
        domains = self.get_all_domains()
        
        if max_domains:
            domains = domains[:max_domains]
        
        logger.info(f"Processing {len(domains)} domains{' (dry run)' if dry_run else ''}")
        
        processed_count = 0
        updated_count = 0
        error_count = 0
        
        for domain in domains:
            if self.shutdown_requested:
                logger.info("Shutdown requested, stopping processing")
                break
            
            domain_name = domain['domain_name']
            domain_id = domain['id']
            
            logger.info(f"Processing domain {processed_count + 1}/{len(domains)}: {domain_name}")
            
            try:
                # Get current domain data
                current_data = self.get_domain_current_data(domain_name)
                
                # Identify missing fields
                missing_fields = self.identify_missing_fields(current_data)
                
                if not missing_fields:
                    logger.info(f"Domain {domain_name} already has complete data, skipping")
                    processed_count += 1
                    continue
                
                logger.info(f"Domain {domain_name} missing fields: {missing_fields}")
                
                if dry_run:
                    logger.info(f"DRY RUN: Would collect missing data for {domain_name}")
                    processed_count += 1
                    continue
                
                # Collect missing data
                new_data = self.collect_missing_data(domain_name, missing_fields)
                
                # Update database
                if self.update_domain_data(domain_name, new_data):
                    updated_count += 1
                    logger.info(f"Successfully updated {domain_name}")
                else:
                    error_count += 1
                    logger.error(f"Failed to update {domain_name}")
                
                processed_count += 1
                
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing domain {domain_name}: {e}")
                processed_count += 1
        
        logger.info(f"Processing complete:")
        logger.info(f"  Total domains: {len(domains)}")
        logger.info(f"  Processed: {processed_count}")
        logger.info(f"  Updated: {updated_count}")
        logger.info(f"  Errors: {error_count}")
        
        return processed_count, updated_count, error_count
    
    def close(self):
        """Clean up resources"""
        if self.collector:
            self.collector.close()
        if self.db:
            self.db.close()

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Fill missing domain data for all domains in database')
    parser.add_argument('--max-domains', type=int, help='Maximum number of domains to process')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    filler = DomainDataFiller()
    
    try:
        start_time = datetime.now()
        logger.info(f"Starting domain data filling process at {start_time}")
        
        processed, updated, errors = filler.process_domains(
            max_domains=args.max_domains,
            dry_run=args.dry_run
        )
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"Process completed in {duration}")
        logger.info(f"Results: {processed} processed, {updated} updated, {errors} errors")
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        filler.close()

if __name__ == "__main__":
    main() 