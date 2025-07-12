import multiprocessing
import time
import logging
import argparse
import signal
import sys
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from domain_collector import DomainCollector
from config import COLLECTION_CONFIG, AUTO_UPDATE_CONFIG
from version import __version__
from auto_update import AutoUpdate, graceful_restart_callback

# Configure logging to show worker ID
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ParallelQueueProcessor:
    def __init__(self, worker_id=None, force_shutdown_after=3):
        self.worker_id = worker_id or os.getpid()
        self.collector = DomainCollector()
        self.shutdown_requested = False
        self.signal_count = 0
        self.force_shutdown_after = force_shutdown_after
        
        # Create logger with worker ID
        self.logger = logging.getLogger(f"Worker-{self.worker_id}")
        
        # Set up signal handlers for graceful shutdown (all processes)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully with forced shutdown"""
        self.signal_count += 1
        self.logger.info(f"Received signal {signum} (count: {self.signal_count}), initiating graceful shutdown...")
        
        if self.signal_count >= self.force_shutdown_after:
            self.logger.warning(f"Force shutdown after {self.signal_count} signals!")
            sys.exit(1)
        
        self.shutdown_requested = True
    
    def process_batch(self, batch_size, max_depth, write_discoveries=True, shutdown_check=None):
        """Process a batch of domains from the queue"""
        if shutdown_check is None:
            shutdown_check = lambda: self.shutdown_requested
        
        processed_count = 0
        discoveries_count = 0
        
        try:
            # Get domains from queue
            domains = self.collector.db.get_next_from_queue(batch_size)
            
            if not domains:
                self.logger.info("No domains in queue to process")
                return 0, 0
            
            self.logger.info(f"Processing batch of {len(domains)} domains")
            
            for domain_data in domains:
                if shutdown_check():
                    self.logger.info("Shutdown requested, stopping batch processing")
                    break
                
                try:
                    domain_name = domain_data['domain_name']
                    depth = domain_data.get('depth', 0)
                    source_domain_id = domain_data.get('source_domain_id')
                    
                    # Skip if depth exceeds max_depth
                    if depth > max_depth:
                        self.logger.info(f"Skipping {domain_name} (depth {depth} > {max_depth})")
                        continue
                    
                    # Collect domain data with timeout
                    try:
                        import threading
                        import queue
                        
                        result_queue = queue.Queue()
                        exception_queue = queue.Queue()
                        
                        def collect_with_timeout():
                            try:
                                result = self.collector.collect_domain_data(
                                    domain_name, 
                                    depth=depth, 
                                    url=domain_data.get('url'),
                                    shutdown_check=shutdown_check
                                )
                                result_queue.put(result)
                            except Exception as e:
                                exception_queue.put(e)
                        
                        # Start collection in a thread
                        collection_thread = threading.Thread(target=collect_with_timeout)
                        collection_thread.daemon = True
                        collection_thread.start()
                        
                        # Wait for result with timeout
                        timeout_seconds = 300  # 5 minutes timeout
                        collection_thread.join(timeout=timeout_seconds)
                        
                        if collection_thread.is_alive():
                            self.logger.warning(f"Domain collection for {domain_name} timed out after {timeout_seconds} seconds")
                            raise TimeoutError(f"Domain collection timed out after {timeout_seconds} seconds")
                        
                        if not exception_queue.empty():
                            raise exception_queue.get()
                        
                        domain_id, relationships = result_queue.get()
                        
                    except KeyboardInterrupt:
                        self.logger.info("Domain collection interrupted by user")
                        raise
                    except TimeoutError:
                        self.logger.error(f"Domain collection timed out for {domain_name}")
                        raise
                    except Exception as e:
                        if shutdown_check():
                            self.logger.info("Domain collection interrupted due to shutdown request")
                            raise KeyboardInterrupt()
                        raise
                    
                    # Mark as completed
                    self.collector.db.mark_queue_item_completed(domain_data['id'], success=True)
                    
                    processed_count += 1
                    
                    # Add discovered URLs to queue if enabled
                    if write_discoveries and relationships:
                        discovered_urls = []
                        for rel in relationships:
                            if rel.get('target_domain_name'):
                                discovered_urls.append({
                                    'url': rel.get('target_url', f"http://{rel['target_domain_name']}"),
                                    'domain_name': rel['target_domain_name'],
                                    'source_domain_id': domain_id,
                                    'depth': depth + 1
                                })
                        
                        if discovered_urls:
                            self.collector.add_discovered_urls_to_queue(discovered_urls, depth + 1)
                            discoveries_count += len(discovered_urls)
                            self.logger.info(f"Added {len(discovered_urls)} discovered URLs to queue")
                    
                    # Add delay between requests with shutdown check
                    for _ in range(int(COLLECTION_CONFIG['request_delay'])):
                        if shutdown_check():
                            break
                        time.sleep(1)
                    
                except Exception as e:
                    self.logger.error(f"Error processing {domain_data.get('domain_name', 'unknown')}: {e}")
                    # Mark as failed
                    self.collector.db.mark_queue_item_completed(domain_data['id'], success=False, error_message=str(e))
            
            self.logger.info(f"Batch completed - processed {processed_count}, discoveries {discoveries_count}")
            return processed_count, discoveries_count
            
        except Exception as e:
            self.logger.error(f"Batch processing failed: {e}")
            return 0, 0
    
    def run_continuous(self, batch_size, max_depth, write_discoveries=True):
        """Run continuous processing with batches"""
        self.logger.info(f"Starting continuous processing (batch_size={batch_size}, max_depth={max_depth})")
        
        total_processed = 0
        total_discoveries = 0
        
        try:
            while not self.shutdown_requested:
                # Get queue statistics
                stats = self.collector.db.get_queue_stats()
                self.logger.info(f"Queue stats: {stats}")
                
                # Process batch
                processed, discoveries = self.process_batch(
                    batch_size, 
                    max_depth, 
                    write_discoveries,
                    lambda: self.shutdown_requested
                )
                
                total_processed += processed
                total_discoveries += discoveries
                
                if processed == 0:
                    # No work to do, wait a bit
                    self.logger.info("No work available, waiting 30 seconds...")
                    for _ in range(30):
                        if self.shutdown_requested:
                            break
                        time.sleep(1)
                        # Check for shutdown more frequently
                        if self.shutdown_requested:
                            break
                
        except KeyboardInterrupt:
            self.logger.info("Interrupted by user")
        except Exception as e:
            self.logger.error(f"Continuous processing failed: {e}")
        finally:
            self.logger.info(f"Shutdown complete - total processed: {total_processed}, total discoveries: {total_discoveries}")
            self.collector.close()
    
    def add_seed_domains(self, domains, priority=1):
        """Add seed domains to the queue"""
        self.logger.info(f"Adding {len(domains)} seed domains to queue")
        
        for domain in domains:
            try:
                self.collector.db.add_to_discovery_queue(
                    url=f"http://{domain}",
                    domain_name=domain,
                    source_domain_id=None,
                    depth=0,
                    priority=priority
                )
                self.logger.info(f"Added {domain} to queue")
            except Exception as e:
                self.logger.error(f"Error adding {domain} to queue: {e}")


def worker_process(worker_id, batch_size, max_depth, write_discoveries, continuous):
    """Worker process function"""
    processor = ParallelQueueProcessor(worker_id)
    try:
        if continuous:
            processor.run_continuous(batch_size, max_depth, write_discoveries)
        else:
            processor.process_batch(batch_size, max_depth, write_discoveries)
    except KeyboardInterrupt:
        logger.info(f"Worker {worker_id} interrupted")
    except Exception as e:
        logger.error(f"Worker {worker_id} failed: {e}")
    finally:
        try:
            processor.collector.close()
        except Exception as e:
            logger.error(f"Worker {worker_id} error during cleanup: {e}")


def run_parallel_processing(num_workers, batch_size, max_depth, write_discoveries=True, continuous=False):
    """Run parallel queue processing"""
    logger.info(f"Starting parallel processing with {num_workers} workers")
    logger.info(f"Configuration: batch_size={batch_size}, max_depth={max_depth}, write_discoveries={write_discoveries}, continuous={continuous}")
    
    # Start worker processes
    processes = []
    shutdown_requested = False
    
    def signal_handler(signum, frame):
        nonlocal shutdown_requested
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        shutdown_requested = True
        # Terminate all worker processes
        for p in processes:
            if p.is_alive():
                p.terminate()
                logger.info(f"Terminated worker process {p.pid}")
    
    # Set up signal handlers for main process
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        for i in range(num_workers):
            p = multiprocessing.Process(
                target=worker_process,
                args=(i, batch_size, max_depth, write_discoveries, continuous)
            )
            p.start()
            processes.append(p)
            logger.info(f"Started worker {i}")
        
        # Wait for all processes to finish
        for i, p in enumerate(processes):
            p.join()
            logger.info(f"Worker {i} finished")
            
    except KeyboardInterrupt:
        logger.info("Main process interrupted, terminating workers...")
        for p in processes:
            if p.is_alive():
                p.terminate()
        # Wait for processes to terminate
        for p in processes:
            p.join(timeout=5)
            if p.is_alive():
                logger.warning(f"Force killing worker process {p.pid}")
                p.kill()
    except Exception as e:
        logger.error(f"Error in parallel processing: {e}")
        # Terminate all processes on error
        for p in processes:
            if p.is_alive():
                p.terminate()
    finally:
        # Ensure all processes are terminated
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=2)
                if p.is_alive():
                    p.kill()
        logger.info("All worker processes terminated")


def main():
    print(f"[WebtheNet] Parallel Data Crawler Version: {__version__}")
    # Start auto-update checker
    auto_updater = AutoUpdate(AUTO_UPDATE_CONFIG, __version__, graceful_restart_callback)
    auto_updater.start_periodic_check()

    parser = argparse.ArgumentParser(description='Process domain discovery queue in parallel')
    parser.add_argument('--workers', type=int, default=COLLECTION_CONFIG.get('parallel_workers', 4), 
                       help='Number of worker processes')
    parser.add_argument('--batch-size', type=int, default=10, 
                       help='Number of domains to process per batch per worker')
    parser.add_argument('--max-depth', type=int, default=COLLECTION_CONFIG['max_depth'], 
                       help='Maximum crawl depth')
    parser.add_argument('--continuous', action='store_true', 
                       help='Run continuously')
    parser.add_argument('--no-discoveries', action='store_true', 
                       help='Do not write discovered URLs back to queue')
    parser.add_argument('--add-seeds', nargs='+', 
                       help='Add seed domains to queue')
    parser.add_argument('--force-shutdown-after', type=int, default=3, 
                       help='Force shutdown after N signals')
    
    args = parser.parse_args()
    
    # Add seed domains if provided
    if args.add_seeds:
        processor = ParallelQueueProcessor()
        processor.add_seed_domains(args.add_seeds)
        processor.collector.close()
    
    # Run parallel processing
    run_parallel_processing(
        num_workers=args.workers,
        batch_size=args.batch_size,
        max_depth=args.max_depth,
        write_discoveries=not args.no_discoveries,
        continuous=args.continuous
    )


if __name__ == "__main__":
    main()