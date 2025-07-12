"""
AutoUpdate module for checking, downloading, and applying updates to the data-crawler.
Supports git, release, and file-based deployments, with configurable options.
"""

import os
import sys
import subprocess
import threading
import time
import logging
import requests
from typing import Optional
import tempfile
import shutil
import zipfile
import glob

# List of files/folders to protect during update
PROTECTED_FILES = {'.env', '.venv', 'local_settings.py'}

# Capture original working directory, script path, and args at import time
ORIGINAL_CWD = os.getcwd()
ORIGINAL_SCRIPT = os.path.abspath(sys.argv[0])
ORIGINAL_ARGS = sys.argv[1:]

class AutoUpdate:
    """
    AutoUpdate handles checking for, downloading, and applying updates to the data-crawler.
    Supports git, release, and file-based deployments, with configurable options.
    """
    def __init__(self, config, current_version, restart_callback):
        """
        :param config: Configuration dict (AUTO_UPDATE_CONFIG)
        :param current_version: Current version string
        :param restart_callback: Function to call for restarting the crawler
        """
        self.config = config
        self.current_version = current_version
        self.restart_callback = restart_callback
        self.logger = logging.getLogger("AutoUpdate")
        self._stop_event = threading.Event()

    def start_periodic_check(self):
        """
        Start a background thread to periodically check for updates.
        """
        t = threading.Thread(target=self._periodic_check, daemon=True)
        t.start()

    def _periodic_check(self):
        interval = self.config.get('check_interval', 21600)
        while not self._stop_event.is_set():
            try:
                self.check_for_update()
            except Exception as e:
                self.logger.error(f"Auto-update check failed: {e}")
            self._stop_event.wait(interval)

    def stop(self):
        self._stop_event.set()

    def check_for_update(self):
        """
        Check if an update is available and apply it if so.
        """
        if not self.config.get('enabled', True):
            self.logger.debug("Auto-update is disabled.")
            return
        if self._is_git_repo():
            self._check_git_update()
        else:
            self._check_github_release_update()

    def _is_git_repo(self):
        return os.path.isdir('.git')

    def _check_git_update(self):
        self.logger.info("Checking for git updates...")
        try:
            # Check for local changes
            status = subprocess.check_output(['git', 'status', '--porcelain']).decode().strip()
            if status:
                self.logger.warning("Auto-update skipped: local changes detected. Please commit, stash, or discard changes to enable auto-update.")
                return
            # Fetch remote
            fetch_cmd = ['git', 'fetch']
            pull_cmd = ['git', 'pull']
            remote_name = 'origin'
            original_url = None
            token = self.config.get('auth_token')
            if token:
                # Get current remote URL
                original_url = subprocess.check_output(['git', 'remote', 'get-url', remote_name]).decode().strip()
                self.logger.info(f"Original remote URL: {original_url}")
                # Insert token into URL
                if original_url.startswith('https://'):
                    url_parts = original_url.split('https://', 1)[1]
                    if '@' in url_parts:
                        url_parts = url_parts.split('@', 1)[1]  # Remove any existing userinfo
                    token_url = f'https://{token}@{url_parts}'
                    # Set remote URL with token
                    subprocess.run(['git', 'remote', 'set-url', remote_name, token_url], check=True)
                    self.logger.info("Set remote URL with token for private repo fetch.")
            try:
                subprocess.run(fetch_cmd, check=True)
                # Compare local and remote HEAD
                local = subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip()
                remote = subprocess.check_output(['git', 'rev-parse', '@{u}']).strip()
                if local != remote:
                    self.logger.info("Update available via git. Applying update...")
                    subprocess.run(pull_cmd, check=True)
                    self.logger.info("Update pulled. Restarting...")
                    self._restart()
                else:
                    self.logger.info("No git update available.")
            finally:
                # Restore original remote URL if it was changed
                if token and original_url:
                    subprocess.run(['git', 'remote', 'set-url', remote_name, original_url], check=True)
                    self.logger.info("Restored original remote URL after fetch.")
        except Exception as e:
            self.logger.error(f"Git update check failed: {e}")

    def _apply_git_update(self):
        try:
            subprocess.run(['git', 'pull'], check=True)
            self.logger.info("Update pulled. Restarting...")
            self._restart()
        except Exception as e:
            self.logger.error(f"Failed to apply git update: {e}")

    def _check_github_release_update(self):
        self.logger.info("Checking for GitHub release updates...")
        repo_url = self.config.get('repo_url')
        if not repo_url or 'github.com' not in repo_url:
            self.logger.warning("Release update only supported for GitHub repos.")
            return
        repo_path = repo_url.split('github.com/')[-1].replace('.git', '')
        headers = {}
        if self.config.get('auth_token'):
            headers['Authorization'] = f"token {self.config['auth_token']}"
        include_prereleases = self.config.get('include_prereleases', False)
        if include_prereleases:
            # Fetch all releases and pick the latest (including pre-releases)
            api_url = f'https://api.github.com/repos/{repo_path}/releases'
            resp = requests.get(api_url, headers=headers, timeout=10)
            if resp.status_code != 200:
                self.logger.warning(f"Failed to fetch releases: {resp.status_code}")
                return
            releases = resp.json()
            if not releases:
                self.logger.info("No releases found.")
                return
            # Pick the latest release (by published_at)
            releases = sorted(releases, key=lambda r: r.get('published_at', ''), reverse=True)
            release = releases[0]
        else:
            # Only use the latest stable release
            api_url = f'https://api.github.com/repos/{repo_path}/releases/latest'
            resp = requests.get(api_url, headers=headers, timeout=10)
            if resp.status_code != 200:
                self.logger.warning(f"Failed to fetch releases: {resp.status_code}")
                return
            release = resp.json()
        release_name = release.get('name', '')
        release_tag = release.get('tag_name', '')
        keywords = self.config.get('release_keywords', [])
        if self.config.get('only_on_release', False):
            if not any(kw in release_name for kw in keywords) and keywords:
                self.logger.info(f"Latest release '{release_name}' does not match keywords {keywords}. Skipping update.")
                return
        # Compare version (normalize v prefix)
        def _normalize_version(ver):
            return ver.lstrip('vV') if ver else ''
        if release_tag and _normalize_version(release_tag) != _normalize_version(self.current_version):
            self.logger.info(f"Update available via release: {release_tag}. Applying update...")
            assets = release.get('assets', [])
            asset_url = None
            for asset in assets:
                if asset['name'].endswith('.zip'):
                    asset_url = asset['browser_download_url']
                    break
            if not asset_url:
                for asset in assets:
                    if asset['name'].endswith('.tar.gz'):
                        asset_url = asset['browser_download_url']
                        break
            if not asset_url:
                asset_url = release.get('zipball_url') or release.get('tarball_url')
                if asset_url:
                    self.logger.info("No uploaded asset found, using zipball/tarball from GitHub release.")
            if not asset_url:
                self.logger.warning("No suitable release asset (.zip, .tar.gz, or zipball/tarball) found. Skipping update.")
                return
            try:
                self.logger.info(f"Downloading release asset: {asset_url}")
                with requests.get(asset_url, stream=True, headers=headers, timeout=30) as r:
                    r.raise_for_status()
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tmp_file:
                        for chunk in r.iter_content(chunk_size=8192):
                            tmp_file.write(chunk)
                        tmp_path = tmp_file.name
                self.logger.info(f"Extracting asset {tmp_path} to a temporary directory...")
                with tempfile.TemporaryDirectory() as tmpdir:
                    with zipfile.ZipFile(tmp_path, 'r') as zip_ref:
                        zip_ref.extractall(tmpdir)
                    # Find the extracted subdirectory (should be only one)
                    extracted_dirs = [os.path.join(tmpdir, d) for d in os.listdir(tmpdir) if os.path.isdir(os.path.join(tmpdir, d))]
                    if not extracted_dirs:
                        self.logger.error("No directory found in extracted zipball. Update aborted.")
                        return
                    extracted_root = extracted_dirs[0]
                    # Always sync to the project root (parent of src)
                    sync_target = get_project_root()
                    self.logger.info(f"Syncing files from {extracted_root} to {sync_target} (protected: {PROTECTED_FILES})...")
                    for item in os.listdir(extracted_root):
                        if item in PROTECTED_FILES:
                            self.logger.info(f"Skipping protected file/folder: {item}")
                            continue
                        s = os.path.join(extracted_root, item)
                        d = os.path.join(sync_target, item)
                        if os.path.isdir(s):
                            if os.path.exists(d):
                                shutil.rmtree(d)
                            shutil.copytree(s, d)
                        else:
                            shutil.copy2(s, d)
                    self.logger.info(f"Update files copied to {sync_target}.")
                os.remove(tmp_path)
                self.logger.info("Update applied from release asset. Restarting...")
                self._restart()
            except Exception as e:
                self.logger.error(f"Failed to download or extract release asset: {e}")
        else:
            self.logger.info("No release update available.")

    def _restart(self):
        # Soft shutdown and restart with same args, excluding --add-seeds
        args = [a for a in sys.argv if not a.startswith('--add-seeds')]
        self.logger.info(f"Restarting with args: {args}")
        # Flush logs, cleanup, etc. if needed
        self.restart_callback(args)

# Graceful restart callback that uses existing shutdown mechanism
def graceful_restart_callback(args):
    """
    Graceful restart callback that triggers the existing shutdown mechanism
    instead of immediately terminating the process.
    """
    import signal
    import os
    import threading
    import time
    import atexit
    
    # Flag to track if we're in restart mode
    restart_mode = threading.Event()
    
    def restart_after_shutdown():
        """Restart the process after shutdown completes"""
        # Wait for shutdown to complete with a configurable timeout
        # Default: 120 seconds (2 minutes) for very long operations
        shutdown_timeout = int(os.getenv('AUTO_UPDATE_SHUTDOWN_TIMEOUT', '120'))
        print(f"[AutoUpdate] Waiting {shutdown_timeout} seconds for graceful shutdown...")
        
        time.sleep(shutdown_timeout)
        
        if restart_mode.is_set():
            print("[AutoUpdate] Graceful shutdown period completed, restarting...")
            print(f"[AutoUpdate] Restarting: cwd={ORIGINAL_CWD}, script={ORIGINAL_SCRIPT}, args={ORIGINAL_ARGS}")
            os.chdir(ORIGINAL_CWD)
            os.execv(sys.executable, [sys.executable, ORIGINAL_SCRIPT] + ORIGINAL_ARGS)
    
    def cleanup_on_exit():
        """Called when the process is about to exit"""
        if restart_mode.is_set():
            print("[AutoUpdate] Process exiting, restart will be handled by background thread")
            
            # Clean up any stuck processing items for this agent
            try:
                from database import DatabaseManager
                from config import COLLECTION_CONFIG
                
                agent_name = COLLECTION_CONFIG.get('internal_agent_name', 'unknown')
                db = DatabaseManager()
                db.connect()
                
                # Clean up items stuck in processing for more than 5 minutes
                cleaned_count = db.cleanup_agent_processing_items(agent_name, timeout_minutes=5)
                if cleaned_count > 0:
                    print(f"[AutoUpdate] Cleaned up {cleaned_count} stuck processing items for agent {agent_name}")
                
                db.close()
            except Exception as e:
                print(f"[AutoUpdate] Error during cleanup: {e}")
    
    # Register cleanup function
    atexit.register(cleanup_on_exit)
    
    # Set restart mode flag
    restart_mode.set()
    
    # Send SIGINT to trigger graceful shutdown (same as Ctrl+C)
    print("[AutoUpdate] Triggering graceful shutdown for restart...")
    os.kill(os.getpid(), signal.SIGINT)
    
    # Start restart thread with configurable timeout
    restart_thread = threading.Thread(target=restart_after_shutdown, daemon=True)
    restart_thread.start()

# Original restart callback (kept for backward compatibility)
def default_restart_callback(args):
    # Robust restart: use original working directory, script path, and args
    import os
    print(f"[AutoUpdate] Restarting: cwd={ORIGINAL_CWD}, script={ORIGINAL_SCRIPT}, args={ORIGINAL_ARGS}")
    os.chdir(ORIGINAL_CWD)
    os.execv(sys.executable, [sys.executable, ORIGINAL_SCRIPT] + ORIGINAL_ARGS)

def get_project_root():
    # Since this is now an extra repo for the crawler, just use the current directory
    return os.path.abspath(os.path.dirname(__file__))
