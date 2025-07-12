import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime, timedelta
from config import DB_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.connect()
        self.create_tables()
    
    def connect(self):
        """Establish database connection"""
        try:
            # Add connection pool settings for better multi-process handling
            connection_config = DB_CONFIG.copy()
            connection_config.update({
                'pool_name': 'crawler_pool',
                'pool_size': 5,
                'pool_reset_session': True,
                'autocommit': False,
                'charset': 'utf8mb4',
                'use_unicode': True,
                'get_warnings': True,
                'raise_on_warnings': False,
                'connection_timeout': 60,
            })
            
            self.connection = mysql.connector.connect(**connection_config)
            logger.info("Database connection established successfully")
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            raise
    
    def create_tables(self):
        """Create necessary tables if they don't exist"""
        try:
            cursor = self.connection.cursor()
            
            # Suppress warnings for table creation (tables may already exist)
            cursor.execute("SET SESSION sql_notes = 0")
            cursor.execute("SET SESSION sql_warnings = 0")
            
            # Domains table with enhanced fields
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS domains (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    domain_name VARCHAR(255) UNIQUE NOT NULL,
                    title VARCHAR(500),
                    description TEXT,
                    favicon_url VARCHAR(500),
                    created_date DATE,
                    expiry_date DATE,
                    registrar VARCHAR(255),
                    nameservers TEXT,
                    asn VARCHAR(50),
                    asn_description VARCHAR(500),
                    ssl_valid BOOLEAN,
                    ssl_expiry DATE,
                    country VARCHAR(100),
                    ip_address VARCHAR(45),
                    latitude DECIMAL(10, 8),
                    longitude DECIMAL(11, 8),
                    screenshot_path VARCHAR(500),
                    category VARCHAR(100),
                    tags TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_domain_name (domain_name),
                    INDEX idx_category (category),
                    INDEX idx_created_date (created_date),
                    INDEX idx_asn (asn),
                    INDEX idx_registrar (registrar)
                )
            """)
            
            # Relationships table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS relationships (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    source_domain_id INT,
                    target_domain_id INT,
                    relationship_type ENUM('link', 'redirect', 'subdomain', 'related') DEFAULT 'link',
                    link_text VARCHAR(500),
                    link_url VARCHAR(500),
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (source_domain_id) REFERENCES domains(id) ON DELETE CASCADE,
                    FOREIGN KEY (target_domain_id) REFERENCES domains(id) ON DELETE CASCADE,
                    UNIQUE KEY unique_relationship (source_domain_id, target_domain_id, relationship_type),
                    INDEX idx_source_domain (source_domain_id),
                    INDEX idx_target_domain (target_domain_id),
                    INDEX idx_relationship_type (relationship_type)
                )
            """)
            
            # Discovery queue table for auto-discovered URLs
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS discovery_queue (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    url VARCHAR(500) NOT NULL,
                    domain_name VARCHAR(255),
                    source_domain_id INT,
                    priority INT DEFAULT 1,
                    status ENUM('pending', 'processing', 'completed', 'failed', 'skipped') DEFAULT 'pending',
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    queued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP NULL,
                    error_message TEXT,
                    depth INT DEFAULT 0,
                    FOREIGN KEY (source_domain_id) REFERENCES domains(id) ON DELETE SET NULL,
                    INDEX idx_status (status),
                    INDEX idx_priority (priority),
                    INDEX idx_domain_name (domain_name),
                    INDEX idx_discovered_at (discovered_at),
                    UNIQUE KEY unique_url (url)
                )
            """)
            
            # Collection logs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS collection_logs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    domain_name VARCHAR(255),
                    status ENUM('pending', 'processing', 'completed', 'failed') DEFAULT 'pending',
                    error_message TEXT,
                    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processing_time DECIMAL(10, 3),
                    relationships_found INT DEFAULT 0,
                    urls_discovered INT DEFAULT 0,
                    INDEX idx_status (status),
                    INDEX idx_collected_at (collected_at)
                )
            """)
            
            # URL processing history table to track processed URLs
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS url_processing_history (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    url VARCHAR(500) NOT NULL,
                    domain_name VARCHAR(255),
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status ENUM('success', 'failed', 'skipped') DEFAULT 'success',
                    links_found INT DEFAULT 0,
                    INDEX idx_url (url),
                    INDEX idx_domain_name (domain_name),
                    INDEX idx_processed_at (processed_at),
                    UNIQUE KEY unique_url_history (url)
                )
            """)
            
            self.connection.commit()
            logger.info("Database tables created successfully")
            
            # Restore warning settings
            cursor.execute("SET SESSION sql_notes = 1")
            cursor.execute("SET SESSION sql_warnings = 1")
            
        except Error as e:
            logger.error(f"Error creating tables: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def insert_domain(self, domain_data):
        """Insert or update domain information"""
        try:
            cursor = self.connection.cursor()
            
            query = """
                INSERT INTO domains (
                    domain_name, title, description, favicon_url, created_date, 
                    expiry_date, registrar, nameservers, asn, asn_description,
                    ssl_valid, ssl_expiry, country, ip_address, latitude, 
                    longitude, screenshot_path, category, tags
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    description = VALUES(description),
                    favicon_url = VALUES(favicon_url),
                    created_date = VALUES(created_date),
                    expiry_date = VALUES(expiry_date),
                    registrar = VALUES(registrar),
                    nameservers = VALUES(nameservers),
                    asn = VALUES(asn),
                    asn_description = VALUES(asn_description),
                    ssl_valid = VALUES(ssl_valid),
                    ssl_expiry = VALUES(ssl_expiry),
                    country = VALUES(country),
                    ip_address = VALUES(ip_address),
                    latitude = VALUES(latitude),
                    longitude = VALUES(longitude),
                    screenshot_path = VALUES(screenshot_path),
                    category = VALUES(category),
                    tags = VALUES(tags),
                    updated_at = CURRENT_TIMESTAMP
            """
            
            cursor.execute(query, (
                domain_data.get('domain_name'),
                domain_data.get('title'),
                domain_data.get('description'),
                domain_data.get('favicon_url'),
                domain_data.get('created_date'),
                domain_data.get('expiry_date'),
                domain_data.get('registrar'),
                domain_data.get('nameservers'),
                domain_data.get('asn'),
                domain_data.get('asn_description'),
                domain_data.get('ssl_valid'),
                domain_data.get('ssl_expiry'),
                domain_data.get('country'),
                domain_data.get('ip_address'),
                domain_data.get('latitude'),
                domain_data.get('longitude'),
                domain_data.get('screenshot_path'),
                domain_data.get('category'),
                domain_data.get('tags')
            ))
            
            # If lastrowid is 0, it means the row already existed and was updated
            # We need to fetch the actual ID
            if cursor.lastrowid == 0:
                cursor.execute("SELECT id FROM domains WHERE domain_name = %s", (domain_data.get('domain_name'),))
                result = cursor.fetchone()
                domain_id = result[0] if result else None
            else:
                domain_id = cursor.lastrowid
            
            self.connection.commit()
            logger.info(f"Domain {domain_data.get('domain_name')} inserted/updated with ID: {domain_id}")
            return domain_id
            
        except Error as e:
            logger.error(f"Error inserting domain: {e}")
            self.connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
    
    def insert_relationship(self, source_domain_id, target_domain_id, relationship_data):
        """Insert relationship between domains"""
        try:
            cursor = self.connection.cursor()
            
            query = """
                INSERT INTO relationships (
                    source_domain_id, target_domain_id, relationship_type, 
                    link_text, link_url
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    link_text = VALUES(link_text),
                    link_url = VALUES(link_url)
            """
            
            cursor.execute(query, (
                source_domain_id,
                target_domain_id,
                relationship_data.get('type', 'link'),
                relationship_data.get('link_text'),
                relationship_data.get('link_url')
            ))
            
            self.connection.commit()
            
        except Error as e:
            logger.error(f"Error inserting relationship: {e}")
            self.connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
    
    def add_to_discovery_queue(self, url, domain_name, source_domain_id=None, depth=0, priority=1):
        """Add URL to discovery queue"""
        try:
            cursor = self.connection.cursor()
            
            query = """
                INSERT INTO discovery_queue (
                    url, domain_name, source_domain_id, depth, priority
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    priority = GREATEST(discovery_queue.priority, VALUES(priority)),
                    depth = LEAST(discovery_queue.depth, VALUES(depth))
            """
            
            cursor.execute(query, (url, domain_name, source_domain_id, depth, priority))
            self.connection.commit()
            
        except Error as e:
            logger.error(f"Error adding to discovery queue: {e}")
            self.connection.rollback()
        finally:
            if cursor:
                cursor.close()
    
    def get_next_from_queue(self, limit=10):
        """Get next URLs from discovery queue with atomic marking"""
        cursor = None
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Ensure connection is active
                if not self.ensure_connection():
                    logger.error("Cannot establish database connection")
                    return []
                
                # Ensure no active transaction before starting
                if self.connection.in_transaction:
                    logger.warning("Transaction already in progress, rolling back")
                    self.connection.rollback()
                
                cursor = self.connection.cursor(dictionary=True)
                
                # Start transaction
                self.connection.start_transaction()
                
                # First, get the items we want to process
                select_query = """
                    SELECT id, url, domain_name, source_domain_id, depth, priority
                    FROM discovery_queue 
                    WHERE status = 'pending'
                    ORDER BY priority DESC, discovered_at ASC
                    LIMIT %s
                    FOR UPDATE
                """
                
                cursor.execute(select_query, (limit,))
                results = cursor.fetchall()
                
                if results:
                    # Mark these specific items as processing
                    ids = [str(r['id']) for r in results]
                    update_query = f"""
                        UPDATE discovery_queue 
                        SET status = 'processing', processed_at = CURRENT_TIMESTAMP
                        WHERE id IN ({','.join(ids)})
                    """
                    cursor.execute(update_query)
                    self.connection.commit()
                    return results
                else:
                    self.connection.commit()
                    return []
                
            except Error as e:
                retry_count += 1
                logger.warning(f"Error getting from queue (attempt {retry_count}/{max_retries}): {e}")
                
                # Clean up transaction state
                if self.connection.in_transaction:
                    try:
                        self.connection.rollback()
                    except:
                        pass
                
                # If this is the last retry, log as error and return empty
                if retry_count >= max_retries:
                    logger.error(f"Failed to get from queue after {max_retries} attempts: {e}")
                    return []
                
                # Wait a bit before retrying
                import time
                time.sleep(0.1 * retry_count)  # Exponential backoff
                
            finally:
                if cursor:
                    cursor.close()
        
        return []
    
    def mark_queue_item_completed(self, queue_id, success=True, error_message=None):
        """Mark queue item as completed or failed"""
        try:
            cursor = self.connection.cursor()
            
            status = 'completed' if success else 'failed'
            query = """
                UPDATE discovery_queue 
                SET status = %s, processed_at = CURRENT_TIMESTAMP, error_message = %s
                WHERE id = %s
            """
            
            cursor.execute(query, (status, error_message, queue_id))
            self.connection.commit()
            
        except Error as e:
            logger.error(f"Error marking queue item: {e}")
            self.connection.rollback()
        finally:
            if cursor:
                cursor.close()
    
    def mark_queue_item_skipped(self, queue_id, reason=None):
        """Mark queue item as skipped (for business logic reasons)"""
        try:
            cursor = self.connection.cursor()
            
            query = """
                UPDATE discovery_queue 
                SET status = 'skipped', processed_at = CURRENT_TIMESTAMP, error_message = %s
                WHERE id = %s
            """
            
            cursor.execute(query, (reason, queue_id))
            self.connection.commit()
            
        except Error as e:
            logger.error(f"Error marking queue item as skipped: {e}")
            self.connection.rollback()
        finally:
            if cursor:
                cursor.close()
    
    def mark_queue_item_interrupted(self, queue_id, reason="Processing interrupted"):
        """Mark queue item as interrupted (for external interruptions)"""
        try:
            cursor = self.connection.cursor()
            
            query = """
                UPDATE discovery_queue 
                SET status = 'pending', processed_at = NULL, error_message = %s
                WHERE id = %s
            """
            
            cursor.execute(query, (reason, queue_id))
            self.connection.commit()
            
        except Error as e:
            logger.error(f"Error marking queue item as interrupted: {e}")
            self.connection.rollback()
        finally:
            if cursor:
                cursor.close()
    
    def is_url_in_queue(self, url, exclude_id=None):
        """Check if URL is already in the discovery queue"""
        try:
            cursor = self.connection.cursor()
            if exclude_id:
                cursor.execute("SELECT id FROM discovery_queue WHERE url = %s AND status IN ('pending', 'processing') AND id != %s", (url, exclude_id))
            else:
                cursor.execute("SELECT id FROM discovery_queue WHERE url = %s AND status IN ('pending', 'processing')", (url,))
            result = cursor.fetchone()
            return result is not None
        except Error as e:
            logger.error(f"Error checking if URL is in queue: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
    
    def is_url_already_processed(self, url):
        """Check if URL has already been processed"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT id FROM url_processing_history WHERE url = %s", (url,))
            result = cursor.fetchone()
            return result is not None
        except Error as e:
            logger.error(f"Error checking URL processing history: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
    
    def record_url_processing(self, url, domain_name, status='success', links_found=0):
        """Record URL processing in history"""
        try:
            cursor = self.connection.cursor()
            
            query = """
                INSERT INTO url_processing_history (
                    url, domain_name, status, links_found
                ) VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    processed_at = CURRENT_TIMESTAMP,
                    status = VALUES(status),
                    links_found = VALUES(links_found)
            """
            
            cursor.execute(query, (url, domain_name, status, links_found))
            self.connection.commit()
            
        except Error as e:
            logger.error(f"Error recording URL processing: {e}")
            self.connection.rollback()
        finally:
            if cursor:
                cursor.close()
    
    def get_domain_processing_count(self, domain_name):
        """Get count of URLs processed for a domain"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM url_processing_history WHERE domain_name = %s", (domain_name,))
            result = cursor.fetchone()
            return result[0] if result else 0
        except Error as e:
            logger.error(f"Error getting domain processing count: {e}")
            return 0
        finally:
            if cursor:
                cursor.close()
    
    def get_domain_id(self, domain_name):
        """Get domain ID by domain name"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT id FROM domains WHERE domain_name = %s", (domain_name,))
            result = cursor.fetchone()
            return result[0] if result else None
        except Error as e:
            logger.error(f"Error getting domain ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
    
    def update_collection_log(self, domain_name, status, error_message=None, processing_time=None, relationships_found=0, urls_discovered=0, url=None, agent_name=None):
        """Update collection log with URL and agent information"""
        try:
            cursor = self.connection.cursor()
            
            query = """
                INSERT INTO collection_logs (
                    domain_name, status, error_message, processing_time, relationships_found, urls_discovered, url, agent_name
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(query, (domain_name, status, error_message, processing_time, relationships_found, urls_discovered, url, agent_name))
            self.connection.commit()
            
        except Error as e:
            logger.error(f"Error updating collection log: {e}")
            self.connection.rollback()
        finally:
            if cursor:
                cursor.close()
    
    def get_queue_stats(self):
        """Get queue statistics"""
        try:
            cursor = self.connection.cursor(dictionary=True)
            
            query = """
                SELECT 
                    status,
                    COUNT(*) as count
                FROM discovery_queue 
                GROUP BY status
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            stats = {}
            for row in results:
                stats[row['status']] = row['count']
            
            return stats
            
        except Error as e:
            logger.error(f"Error getting queue stats: {e}")
            return {}
        finally:
            if cursor:
                cursor.close()
    
    def cleanup_agent_processing_items(self, agent_name, timeout_minutes=30):
        """
        Clean up processing items that belong to a specific agent and are stuck for too long.
        This helps when agents crash or are restarted without proper cleanup.
        """
        try:
            cursor = self.connection.cursor(dictionary=True)
            
            # Find items stuck in processing status for this agent
            timeout_seconds = timeout_minutes * 60
            cutoff_time = datetime.now() - timedelta(seconds=timeout_seconds)
            
            query = """
                SELECT id, url, domain_name, processed_at
                FROM discovery_queue 
                WHERE status = 'processing' 
                AND processed_at < %s
                ORDER BY processed_at ASC
            """
            
            cursor.execute(query, (cutoff_time,))
            stuck_items = cursor.fetchall()
            
            if not stuck_items:
                logger.info(f"No items found stuck in processing for more than {timeout_minutes} minutes")
                return 0
            
            # Reset stuck items to pending status
            stuck_ids = [str(item['id']) for item in stuck_items]
            update_query = f"""
                UPDATE discovery_queue 
                SET status = 'pending', 
                    processed_at = NULL, 
                    error_message = 'Reset from stuck processing status (agent: {agent_name})'
                WHERE id IN ({','.join(stuck_ids)})
            """
            
            cursor.execute(update_query)
            self.connection.commit()
            
            logger.info(f"Cleaned up {len(stuck_items)} stuck processing items for agent {agent_name}")
            return len(stuck_items)
            
        except Error as e:
            logger.error(f"Error cleaning up agent processing items: {e}")
            return 0
        finally:
            if cursor:
                cursor.close()
    
    def is_domain_data_complete(self, domain_name):
        """Check if domain already has complete data (all required fields)"""
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute("""
                SELECT title, description, created_date, registrar, nameservers, 
                       ssl_valid, country, ip_address, latitude, longitude
                FROM domains 
                WHERE domain_name = %s
            """, (domain_name,))
            result = cursor.fetchone()
            
            if not result:
                return False
            
            # Check if we have the essential data fields
            essential_fields = ['title', 'description', 'ip_address']
            return all(result.get(field) is not None for field in essential_fields)
            
        except Error as e:
            logger.error(f"Error checking domain data completeness: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
    
    def close(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            try:
                # Rollback any active transaction before closing
                if self.connection.in_transaction:
                    logger.warning("Rolling back active transaction before closing")
                    self.connection.rollback()
                self.connection.close()
                logger.info("Database connection closed")
            except Error as e:
                logger.error(f"Error closing database connection: {e}")
    
    def cleanup_stuck_transactions(self):
        """Clean up any stuck transactions"""
        try:
            if self.connection and self.connection.in_transaction:
                logger.warning("Cleaning up stuck transaction")
                self.connection.rollback()
                return True
            return False
        except Error as e:
            logger.error(f"Error cleaning up stuck transaction: {e}")
            return False
    
    def ensure_connection(self):
        """Ensure database connection is active and reconnect if needed"""
        try:
            if not self.connection or not self.connection.is_connected():
                logger.warning("Database connection lost, reconnecting...")
                self.connect()
                return True
            return True
        except Error as e:
            logger.error(f"Error ensuring database connection: {e}")
            return False 