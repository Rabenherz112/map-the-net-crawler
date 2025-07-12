# Domain Relationship Visualization Tool

A comprehensive domain relationship visualization tool with a data collection system that crawls domains, collects metadata (WHOIS, SSL, geolocation, screenshots), discovers relationships by crawling links, and stores data in a MySQL database. The system supports parallel and queue-based processing for scalability.

## Features

### Core Functionality
- **Domain Data Collection**: Comprehensive metadata collection including WHOIS, DNS, SSL certificates, geolocation, and screenshots
- **Relationship Discovery**: Automated link discovery and relationship mapping between domains
- **Queue-Based Processing**: Scalable processing with configurable depth limits and rate limiting
- **Database Storage**: MySQL-based storage with proper indexing and relationship tracking

### Data Collection Capabilities
- **Web Data**: Title, description, favicon extraction
- **WHOIS Data**: Domain registration information (creation date, expiry, registrar)
- **DNS Data**: IP addresses, nameservers, ASN information
- **SSL Data**: Certificate validity, expiry dates
- **Geolocation**: Country, latitude/longitude based on IP
- **Screenshots**: Visual snapshots of web pages
- **Relationship Data**: Link text, URLs, and relationship types

### Scalability Features
- **Parallel Processing**: Support for concurrent domain processing
- **Queue Management**: Intelligent queue processing with duplicate prevention
- **Depth Control**: Configurable crawl depth limits
- **Rate Limiting**: Respectful crawling with configurable delays
- **Resource Management**: Proper cleanup and error recovery

## Installation

### Prerequisites
- Python 3.8+
- MySQL 8.0+
- System `whois` command (for WHOIS data collection)

### Setup
1. Clone the repository:
```bash
git clone <repository-url>
cd map-the-net
```

2. Create virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Install system WHOIS command:
```bash
# Ubuntu/Debian
sudo apt-get install whois

# CentOS/RHEL
sudo yum install whois

# macOS
brew install whois
```

5. Configure database:
```bash
# Copy environment template
cp env_example.txt .env

# Edit .env with your database credentials
# Then run database setup
mysql -u your_user -p your_database < setup_database.sql
```

## Usage

### Basic Usage

1. **Add seed domains to queue**:
```bash
python3 queue_processor.py --add-seeds example.com another-domain.com
```

2. **Process the queue**:
```bash
python3 queue_processor.py --max-items 50 --max-depth 3
```

3. **Continuous processing**:
```bash
python3 queue_processor.py --continuous --max-depth 2
```

### Advanced Usage

**Parallel Processing**:
```bash
python3 parallel_collector.py --domains domains.txt --workers 4
```

**Database Management**:
```bash
# Wipe database (use with caution!)
python3 wipe_database.py --force

# Test specific functionality
python3 test_improvements.py
```

### Utility Tools

The crawler includes several utility tools for maintenance and troubleshooting:

#### Queue Management

**`cleanup_stuck_queue.py`** - Clean up URLs stuck in "processing" status:

```bash
# Show current queue statistics
python cleanup_stuck_queue.py --stats-only

# See what would be cleaned up (dry run)
python cleanup_stuck_queue.py --dry-run --timeout 30

# Clean up items stuck for more than 30 minutes
python cleanup_stuck_queue.py --timeout 30

# Clean up items stuck for more than 10 minutes (more aggressive)
python cleanup_stuck_queue.py --timeout 10
```

**Options:**
- `--timeout MINUTES`: Items stuck longer than this will be reset (default: 30)
- `--dry-run`: Show what would be done without making changes
- `--stats-only`: Only show queue statistics without cleaning up

#### Database Management

**`wipe_database.py`** - Completely wipe the database (use with caution):

```bash
# Wipe database (requires --force flag)
python wipe_database.py --force
```

**Options:**
- `--force`: Required flag to confirm database wipe

**`cleanup_ugc_domains.py`** - Remove itch.io and github.io subdomains from database:

```bash
# Show statistics only
python cleanup_ugc_domains.py --stats-only

# See what would be cleaned up (dry run)
python cleanup_ugc_domains.py --dry-run

# Clean up UGC domains (requires confirmation)
python cleanup_ugc_domains.py

# Force cleanup without confirmation
python cleanup_ugc_domains.py --force
```

**Options:**
- `--stats-only`: Only show statistics without cleaning up
- `--dry-run`: Show what would be done without making changes
- `--force`: Force cleanup without confirmation (use with caution)

**`archive_collection_logs.py`** - Archive old collection logs to manage database size:

```bash
# Show log statistics only
python archive_collection_logs.py --stats-only

# See what would be archived (dry run)
python archive_collection_logs.py --dry-run --days 30

# Archive logs older than 30 days (with CSV export)
python archive_collection_logs.py --days 30

# Archive only failed logs older than 7 days
python archive_collection_logs.py --status failed --days 7

# Archive without CSV export
python archive_collection_logs.py --no-export --days 60

# Force archiving without confirmation
python archive_collection_logs.py --force --days 30
```

**Options:**
- `--days N`: Archive logs older than N days (default: 30)
- `--status STATUS`: Only archive logs with specific status (pending/processing/completed/failed)
- `--dry-run`: Show what would be done without making changes
- `--stats-only`: Only show statistics without archiving
- `--no-export`: Skip CSV export of archived logs
- `--force`: Force archiving without confirmation (use with caution)

**CSV Export Location:**
Archived logs are exported to `resources/collection_logs_archive/` with timestamped filenames like `collection_logs_archive_20240115_143022.csv`.

### Configuration

Create a `.env` file or copy it from `env_example.txt` and set the following variables:

#### Database Configuration
| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | MySQL host address | `localhost` |
| `DB_PORT` | MySQL port | `3306` |
| `DB_NAME` | Database name | `map_the_net` |
| `DB_USER` | Database username | `your_username` |
| `DB_PASSWORD` | Database password | `your_password` |

#### Collection Configuration
| Variable | Description | Default |
|----------|-------------|---------|
| `COLLECTION_TIMEOUT` | Request timeout in seconds | `30` |
| `COLLECTION_REQUEST_DELAY` | Delay between requests in seconds | `1` |
| `COLLECTION_MAX_LINKS_PER_PAGE` | Maximum links to extract per page | `50` |
| `COLLECTION_MAX_URLS_PER_DOMAIN` | Maximum URLs to process per domain | `10` |
| `COLLECTION_MAX_DEPTH` | Maximum crawl depth | `3` |
| `COLLECTION_MAX_ITEMS` | Maximum items to process per batch | `10` |
| `COLLECTION_SKIP_ALREADY_PROCESSED` | Skip already processed URLs | `true` |
| `COLLECTION_HTTP_USER_AGENT` | HTTP User-Agent string | `WorldMapper/1.0` |
| `COLLECTION_INTERNAL_AGENT_NAME` | Internal agent identifier | `hostname-pid` |
| `COLLECTION_RESPECT_ROBOTS_TXT` | Respect robots.txt | `true` |
| `COLLECTION_PARALLEL_WORKERS` | Number of parallel workers | `1` |

#### Data Collection Configuration
| Variable | Description | Default |
|----------|-------------|---------|
| `DATA_COLLECT_WHOIS` | Collect WHOIS data | `true` |
| `DATA_COLLECT_SSL` | Collect SSL certificate data | `true` |
| `DATA_COLLECT_GEOLOCATION` | Collect geolocation data | `true` |
| `DATA_COLLECT_SCREENSHOTS` | Take screenshots | `false` |
| `DATA_COLLECT_IPINFO_FALLBACK` | Use ipinfo.io fallback for geolocation | `true` |
| `DATA_COLLECT_IPINFO_TOKEN` | ipinfo.io API token (optional) | `` |
| `MAXMIND_DB_PATH` | Path to MaxMind GeoLite2 database | `./GeoLite2-City.mmdb` |
| `SCREENSHOT_DIR` | Screenshot storage directory | `./resources/screenshots` |

#### Auto-Update Configuration
| Variable | Description | Default |
|----------|-------------|---------|
| `AUTO_UPDATE_ENABLED` | Enable auto-update | `true` |
| `AUTO_UPDATE_REPO_URL` | Repository URL for updates | `https://github.com/Rabenherz112/map-the-net-crawler.git` |
| `AUTO_UPDATE_CHECK_INTERVAL` | Update check interval in seconds | `21600` (6h) |
| `AUTO_UPDATE_ONLY_ON_RELEASE` | Only update on releases | `false` |
| `AUTO_UPDATE_RELEASE_KEYWORDS` | Release keywords (comma-separated) | `CW-PUSH,ALL-PUSH,PUSH` |
| `AUTO_UPDATE_AUTH_TOKEN` | GitHub auth token (optional) | `` |
| `AUTO_UPDATE_INCLUDE_PRERELEASES` | Include pre-releases | `false` |
| `AUTO_UPDATE_SHUTDOWN_TIMEOUT` | Graceful shutdown timeout in seconds | `120` |

## Database Schema

### Core Tables
- **domains**: Domain metadata and collected data
- **relationships**: Links and relationships between domains
- **discovery_queue**: URL queue for processing
- **url_processing_history**: Processing history and statistics
- **collection_logs**: Collection status and timing

### Key Relationships
- Domains can have multiple relationships (source → target)
- Queue items track processing depth and status
- Processing history prevents duplicate work

## Troubleshooting

### Common Issues

1. **WHOIS Command Not Found**:
   ```bash
   # Install system whois command
   sudo apt-get install whois  # Ubuntu/Debian
   ```

2. **Database Connection Issues**:
   - Verify MySQL is running
   - Check credentials in `.env` file
   - Ensure database exists and user has permissions

3. **Rate Limiting**:
   - Increase `request_delay` in config
   - Reduce `max_items` for slower processing

4. **Memory Issues**:
   - Reduce `max_links_per_page`
   - Lower `max_depth` for shallower crawling

5. **Stuck Queue Items**:
   - Use `cleanup_stuck_queue.py` to reset stuck items
   - Reduce `COLLECTION_MAX_ITEMS` to prevent batch reservation issues
   - Check for crawler crashes or interruptions
   - Monitor queue statistics regularly

### Debugging

Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

Test specific components:
```bash
python3 test_improvements.py
python3 test_new_logic.py example.com
```

## Performance Tips

1. **Database Optimization**:
   - Use SSD storage for MySQL
   - Increase MySQL buffer pool size
   - Add indexes on frequently queried columns

2. **Network Optimization**:
   - Use faster internet connection
   - Consider using proxies for large-scale crawling
   - Implement proper rate limiting

3. **Resource Management**:
   - Monitor memory usage during large crawls
   - Use appropriate `max_items` and `max_depth` values
   - Implement graceful shutdown for long-running processes

## License

This project is licensed under the GNU AGPL v3.0. See the [LICENSE](./LICENSE) file for details.

## Auto-Update Feature

The crawler can automatically check for updates and apply them, with options to control update behavior.

### Configuration Options

- `AUTO_UPDATE_ENABLED` (default: `true`): Enable or disable auto-update.
- `AUTO_UPDATE_REPO_URL` (default: your repo URL): The repository to check for updates.
- `AUTO_UPDATE_CHECK_INTERVAL` (default: 21600): How often (in seconds) to check for updates.
- `AUTO_UPDATE_ONLY_ON_RELEASE` (default: `false`): Only update on new releases.
- `AUTO_UPDATE_RELEASE_KEYWORDS` (default: empty): Only update if release name contains one of these keywords (comma-separated).
- `AUTO_UPDATE_AUTH_TOKEN` (optional): Auth token for private repos.

### How it Works

- On startup and at regular intervals, the crawler checks for updates.
- If an update is available (and matches config), it will gracefully shut down, update, and restart with the same parameters (excluding `--add-seeds`).
- Supports git, release, and file-based deployments.

See `src/data-crawler/auto_update.py` for implementation details.
