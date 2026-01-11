#!/usr/bin/env python3
"""
Daemon script for running the viral-clips-pipeline scheduler.
Runs as a long-running background process.

Usage:
    python daemon.py                    # Start with default schedule
    python daemon.py --aggressive       # Start with aggressive schedule
    python daemon.py --run-now          # Run full pipeline once and exit
"""

import argparse
import logging
import signal
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import settings
from scheduler.jobs import PipelineScheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("daemon")


class PipelineDaemon:
    """Manages the pipeline scheduler as a daemon process."""

    def __init__(self, aggressive: bool = False, mega: bool = False):
        """Initialize daemon."""
        self.scheduler = PipelineScheduler()
        self.aggressive = aggressive
        self.mega = mega
        self._running = False

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self._running = False
        self.scheduler.stop()

    def start(self):
        """Start the daemon."""
        logger.info("=" * 60)
        logger.info("VIRAL CLIPS PIPELINE DAEMON")
        logger.info("=" * 60)

        # Ensure directories exist
        settings.ensure_directories()

        # Configure schedule
        if self.mega:
            logger.info("Using MEGA-COMPILATION schedule")
            self.scheduler.configure_mega_compilation_schedule()
        elif self.aggressive:
            logger.info("Using AGGRESSIVE schedule")
            self.scheduler.configure_aggressive_schedule()
        else:
            logger.info("Using DEFAULT schedule")
            self.scheduler.configure_default_schedule()

        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Start scheduler first (needed for next_run_time to be available)
        self.scheduler.start()
        self._running = True

        # Print job schedule (after scheduler starts)
        logger.info("-" * 60)
        logger.info("Scheduled jobs:")
        for job in self.scheduler.get_jobs():
            next_run = job.next_run_time.strftime("%H:%M:%S") if job.next_run_time else "N/A"
            logger.info(f"  - {job.name}: next run at {next_run}")
        logger.info("-" * 60)

        logger.info("Daemon started. Press Ctrl+C to stop.")

        # Main loop
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            pass

        logger.info("Daemon stopped.")

    def run_once(self):
        """Run the full pipeline once and exit."""
        logger.info("=" * 60)
        logger.info("RUNNING FULL PIPELINE (ONE-SHOT)")
        logger.info("=" * 60)

        settings.ensure_directories()

        try:
            self.scheduler.job_full_pipeline()
            logger.info("Pipeline run complete.")
            return 0
        except Exception as e:
            logger.error(f"Pipeline run failed: {e}")
            return 1

    def run_mega_once(self):
        """Run the mega-compilation pipeline once and exit."""
        logger.info("=" * 60)
        logger.info("RUNNING MEGA-COMPILATION PIPELINE (ONE-SHOT)")
        logger.info("=" * 60)

        settings.ensure_directories()

        try:
            self.scheduler.job_mega_compilation_pipeline()
            logger.info("Mega-compilation pipeline run complete.")
            return 0
        except Exception as e:
            logger.error(f"Mega-compilation pipeline run failed: {e}")
            return 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Viral Clips Pipeline Daemon",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python daemon.py                    Start with default schedule (individual clips)
    python daemon.py --aggressive       Start with aggressive schedule
    python daemon.py --mega             Start with mega-compilation schedule
    python daemon.py --run-now          Run full pipeline once and exit
    python daemon.py --run-mega         Run mega-compilation pipeline once and exit
    python daemon.py --status           Show current pipeline status
        """,
    )

    parser.add_argument(
        "--aggressive",
        action="store_true",
        help="Use aggressive schedule (faster, higher throughput)",
    )
    parser.add_argument(
        "--mega",
        action="store_true",
        help="Use mega-compilation schedule (source compilations pipeline)",
    )
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Run full pipeline once and exit",
    )
    parser.add_argument(
        "--run-mega",
        action="store_true",
        help="Run mega-compilation pipeline once and exit",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show current pipeline status and exit",
    )
    parser.add_argument(
        "--discover",
        action="store_true",
        help="Run only the discovery job and exit",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Run only the download job and exit",
    )
    parser.add_argument(
        "--classify",
        action="store_true",
        help="Run only the classification job and exit",
    )
    parser.add_argument(
        "--group",
        action="store_true",
        help="Run only the grouping job and exit",
    )
    parser.add_argument(
        "--render",
        action="store_true",
        help="Run only the render job and exit",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Run only the upload processing job and exit",
    )

    args = parser.parse_args()

    daemon = PipelineDaemon(aggressive=args.aggressive, mega=args.mega)

    # Handle single-job runs
    if args.status:
        show_status()
        return 0

    if args.run_now:
        return daemon.run_once()

    if args.run_mega:
        return daemon.run_mega_once()

    if args.discover:
        logger.info("Running discovery job...")
        daemon.scheduler.job_discover_content()
        return 0

    if args.download:
        logger.info("Running download job...")
        daemon.scheduler.job_download_videos()
        return 0

    if args.classify:
        logger.info("Running classification job...")
        daemon.scheduler.job_classify_videos()
        return 0

    if args.group:
        logger.info("Running grouping job...")
        daemon.scheduler.job_create_compilations()
        return 0

    if args.render:
        logger.info("Running render job...")
        daemon.scheduler.job_render_compilations()
        return 0

    if args.upload:
        logger.info("Running upload job...")
        daemon.scheduler.job_route_uploads()
        daemon.scheduler.job_process_uploads()
        return 0

    # Default: run as daemon
    daemon.start()
    return 0


def show_status():
    """Show current pipeline status."""
    from core.database import Database
    from core.models import VideoStatus, CompilationStatus, UploadStatus

    db = Database(settings.DATABASE_PATH)

    print("\n" + "=" * 60)
    print("PIPELINE STATUS")
    print("=" * 60)

    # Video stats
    video_counts = db.count_videos_by_status()
    print("\nVideos:")
    for status in VideoStatus:
        count = video_counts.get(status.value, 0)
        print(f"  {status.value:15} {count:5}")

    # Compilation stats
    comp_counts = db.count_compilations_by_status()
    print("\nCompilations:")
    for status in CompilationStatus:
        count = comp_counts.get(status.value, 0)
        print(f"  {status.value:15} {count:5}")

    # Account stats
    from services.account_manager import AccountManager
    am = AccountManager(db)
    stats = am.get_all_stats()
    print("\nAccounts:")
    print(f"  Total: {stats['total_accounts']}")
    for platform, counts in stats.get("by_platform", {}).items():
        print(f"  {platform}: {counts['active']} active, {counts['with_creds']} with credentials")

    # Upload queue
    from services.upload_router import UploadRouter
    router = UploadRouter(db, am)
    upload_stats = router.get_upload_stats()
    print("\nUpload Queue:")
    print(f"  Pending:   {upload_stats['pending']}")
    print(f"  Uploading: {upload_stats['uploading']}")
    print(f"  Success:   {upload_stats['success']}")
    print(f"  Failed:    {upload_stats['failed']}")

    print("\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    sys.exit(main())
