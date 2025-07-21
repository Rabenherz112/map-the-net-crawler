import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
from collections import defaultdict
from database import DatabaseManager
import argparse


def main():
    parser = argparse.ArgumentParser(description="Show crawler statistics.")
    parser.add_argument('--top', type=int, default=10, help='Number of top domains to show')
    parser.add_argument('--domain', type=str, help='Domain to show ranking for (by incoming relationships)')
    args = parser.parse_args()
    TOP_N = args.top
    SEARCH_DOMAIN = args.domain.lower() if args.domain else None

    db = DatabaseManager()
    try:
        cursor = db.connection.cursor()
        print("==== Crawler Statistics ====")

        # 1. Total domains discovered
        cursor.execute("SELECT COUNT(*) FROM domains")
        total_domains = cursor.fetchone()[0]
        print(f"Total domains discovered: {total_domains}")

        # 2. Total URLs in the database
        cursor.execute("SELECT COUNT(*) FROM discovery_queue")
        total_urls = cursor.fetchone()[0]
        print(f"Total URLs in discovery_queue: {total_urls}")

        # 3. URLs processed vs total
        cursor.execute("SELECT COUNT(*) FROM url_processing_history")
        processed_urls = cursor.fetchone()[0]
        print(f"URLs processed: {processed_urls} / {total_urls}")

        # 4. Top N domains by number of incoming relationships (excluding subdomain relationships)
        cursor.execute(
            """
            SELECT d.domain_name, COUNT(r.id) as rel_count
            FROM relationships r
            JOIN domains d ON r.target_domain_id = d.id
            WHERE r.relationship_type != 'subdomain'
            GROUP BY r.target_domain_id
            ORDER BY rel_count DESC
            """
        )
        all_ranked = cursor.fetchall()
        print(f"\nTop {TOP_N} domains by incoming relationships (excluding subdomains):")
        for i, (domain, count) in enumerate(all_ranked[:TOP_N], 1):
            print(f"  {i}. {domain}: {count}")

        # If searching for a domain's ranking
        if SEARCH_DOMAIN:
            found = False
            for i, (domain, count) in enumerate(all_ranked, 1):
                if domain.lower() == SEARCH_DOMAIN:
                    print(f"\nDomain '{SEARCH_DOMAIN}' is ranked #{i} with {count} incoming relationships (excluding subdomains).")
                    found = True
                    break
            if not found:
                print(f"\nDomain '{SEARCH_DOMAIN}' not found in the relationships ranking.")

        # 5. Processed links in the last 24 hours (from discovery_queue)
        cursor.execute(
            """
            SELECT COUNT(*) FROM discovery_queue
            WHERE status = 'completed' AND processed_at >= %s
            """,
            (datetime.now() - timedelta(days=1),)
        )
        processed_24h = cursor.fetchone()[0]
        print(f"\nLinks processed in the last 24h: {processed_24h}")

        # 6. Agent statistics (from collection_logs)
        cursor.execute("SELECT agent_name, COUNT(*) FROM collection_logs GROUP BY agent_name")
        agent_counts = cursor.fetchall()
        total_logs = sum(count for _, count in agent_counts)
        print("\nAgent statistics (by collection_logs):")
        for agent, count in sorted(agent_counts, key=lambda x: x[1], reverse=True):
            percent = (count / total_logs * 100) if total_logs else 0
            print(f"  {agent or '[unknown]'}: {count} ({percent:.1f}%)")

        # 7. Other useful stats:
        # Number of failed queue items
        cursor.execute("SELECT COUNT(*) FROM discovery_queue WHERE status = 'failed'")
        failed_count = cursor.fetchone()[0]
        print(f"\nFailed queue items: {failed_count}")

        # Number of pending/processing/skipped queue items
        cursor.execute("SELECT status, COUNT(*) FROM discovery_queue WHERE status IN ('pending', 'processing', 'skipped') GROUP BY status")
        status_counts = dict(cursor.fetchall())
        print("Queue items by status:")
        for status in ['pending', 'processing', 'skipped']:
            print(f"  {status}: {status_counts.get(status, 0)}")

        # Most common relationship type
        cursor.execute("SELECT relationship_type, COUNT(*) FROM relationships GROUP BY relationship_type ORDER BY COUNT(*) DESC")
        print("\nMost common relationship types:")
        for rel_type, count in cursor.fetchall():
            print(f"  {rel_type}: {count}")

        # Average processing time per collection (from collection_logs)
        cursor.execute("SELECT AVG(processing_time) FROM collection_logs WHERE processing_time IS NOT NULL")
        avg_time = cursor.fetchone()[0]
        print(f"\nAverage processing time per collection: {avg_time:.2f} seconds" if avg_time is not None else "No processing time data.")

        # Number of unique domains processed in the last 24 hours (from url_processing_history)
        cursor.execute(
            """
            SELECT COUNT(DISTINCT domain_name) FROM url_processing_history
            WHERE processed_at >= %s
            """,
            (datetime.now() - timedelta(days=1),)
        )
        unique_domains_24h = cursor.fetchone()[0]
        print(f"\nUnique domains processed in last 24h: {unique_domains_24h}")

        # Number of new domains discovered in the last 24 hours (from domains.created_at)
        cursor.execute(
            """
            SELECT COUNT(*) FROM domains
            WHERE created_at >= %s
            """,
            (datetime.now() - timedelta(days=1),)
        )
        new_domains_24h = cursor.fetchone()[0]
        print(f"New domains discovered in last 24h: {new_domains_24h}")

    except Error as e:
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    main() 