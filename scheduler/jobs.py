"""
Scheduled jobs for automated pipeline operation.
Uses APScheduler for job management.
"""

import logging
from datetime import datetime, time
from typing import Optional, Tuple

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from core.database import Database
from core.models import (
    VideoStatus, CompilationStatus, Platform, UploadStatus,
    Upload, Account, Compilation, ContentStrategy
)
from config.settings import settings
from services.discovery import DiscoveryService
from services.downloader import DownloaderService
from services.classifier import ClassifierService
from services.grouper import GrouperService
from services.stitcher import StitcherService
from services.upload_router import UploadRouter
from services.account_manager import AccountManager
from services.youtube_uploader import YouTubeUploader
from services.tiktok_uploader import TikTokUploader

logger = logging.getLogger(__name__)


class PipelineScheduler:
    """Manages scheduled pipeline jobs."""

    def __init__(self, db_path: Optional[str] = None):
        """Initialize scheduler with database connection."""
        self.db = Database(db_path or settings.DATABASE_PATH)
        self.scheduler = BackgroundScheduler()

        # Initialize services
        self._discovery: Optional[DiscoveryService] = None
        self._downloader: Optional[DownloaderService] = None
        self._classifier: Optional[ClassifierService] = None
        self._grouper: Optional[GrouperService] = None
        self._stitcher: Optional[StitcherService] = None
        self._account_manager: Optional[AccountManager] = None
        self._upload_router: Optional[UploadRouter] = None
        self._youtube_uploader: Optional[YouTubeUploader] = None
        self._tiktok_uploader: Optional[TikTokUploader] = None

        # Auto-create default account from .env if no accounts exist
        self._ensure_default_account()

    # =========================================================================
    # Lazy Service Initialization
    # =========================================================================

    @property
    def discovery(self) -> DiscoveryService:
        if self._discovery is None:
            self._discovery = DiscoveryService(self.db)
        return self._discovery

    @property
    def downloader(self) -> DownloaderService:
        if self._downloader is None:
            self._downloader = DownloaderService(self.db)
        return self._downloader

    @property
    def classifier(self) -> ClassifierService:
        if self._classifier is None:
            self._classifier = ClassifierService(self.db)
        return self._classifier

    @property
    def grouper(self) -> GrouperService:
        if self._grouper is None:
            self._grouper = GrouperService(self.db)
        return self._grouper

    @property
    def stitcher(self) -> StitcherService:
        if self._stitcher is None:
            self._stitcher = StitcherService(self.db)
        return self._stitcher

    @property
    def account_manager(self) -> AccountManager:
        if self._account_manager is None:
            self._account_manager = AccountManager(self.db)
        return self._account_manager

    @property
    def upload_router(self) -> UploadRouter:
        if self._upload_router is None:
            self._upload_router = UploadRouter(self.db, self.account_manager)
        return self._upload_router

    @property
    def youtube_uploader(self) -> YouTubeUploader:
        if self._youtube_uploader is None:
            self._youtube_uploader = YouTubeUploader(self.db, self.account_manager)
        return self._youtube_uploader

    @property
    def tiktok_uploader(self) -> TikTokUploader:
        if self._tiktok_uploader is None:
            self._tiktok_uploader = TikTokUploader(self.db, self.account_manager)
        return self._tiktok_uploader

    # =========================================================================
    # Account Initialization
    # =========================================================================

    def _ensure_default_account(self) -> None:
        """
        Automatically create a default YouTube account from .env credentials
        if no YouTube accounts exist in the database.
        """
        try:
            # Check if any YouTube accounts exist
            existing_accounts = self.db.get_accounts_by_platform(Platform.YOUTUBE, active_only=False)
            
            if existing_accounts:
                # Accounts already exist, no need to create
                return

            # Check if .env credentials are available
            if not (settings.YOUTUBE_CLIENT_ID and 
                    settings.YOUTUBE_CLIENT_SECRET and 
                    settings.YOUTUBE_REFRESH_TOKEN):
                logger.info("No YouTube accounts found and .env credentials not set - skipping auto-account creation")
                return

            # Create default account
            logger.info("No YouTube accounts found - creating default account from .env credentials")
            account = self.account_manager.create_account(
                platform=Platform.YOUTUBE,
                name="Default YouTube Account",
                strategy=ContentStrategy.MIXED,
                daily_limit=6,
            )

            # Store credentials from .env
            credentials = {
                "client_id": settings.YOUTUBE_CLIENT_ID,
                "client_secret": settings.YOUTUBE_CLIENT_SECRET,
                "refresh_token": settings.YOUTUBE_REFRESH_TOKEN,
            }

            if self.account_manager.set_credentials(account.id, credentials):
                logger.info(f"Successfully created default YouTube account: {account.id}")
            else:
                logger.error("Failed to store credentials for default account")

        except Exception as e:
            logger.error(f"Failed to create default account: {e}")

    # =========================================================================
    # Job Definitions
    # =========================================================================

    def job_discover_content(self) -> None:
        """Discover new content from TikTok."""
        logger.info("Running discovery job...")
        try:
            videos, skipped = self.discovery.discover_trending(limit=50)
            logger.info(f"Discovery complete: {len(videos)} new, {skipped} skipped")
        except Exception as e:
            logger.error(f"Discovery job failed: {e}")

    def job_download_videos(self) -> None:
        """Download discovered videos."""
        logger.info("Running download job...")
        try:
            success, fail = self.downloader.download_discovered(limit=20)
            logger.info(f"Download complete: {success} succeeded, {fail} failed")
        except Exception as e:
            logger.error(f"Download job failed: {e}")

    def job_classify_videos(self) -> None:
        """Classify downloaded videos."""
        logger.info("Running classification job...")
        try:
            classified, skipped, failed = self.classifier.classify_downloaded(limit=30)
            logger.info(
                f"Classification complete: {classified} classified, "
                f"{skipped} skipped, {failed} failed"
            )
        except Exception as e:
            logger.error(f"Classification job failed: {e}")

    def job_create_compilations(self) -> None:
        """Create compilations from classified videos."""
        logger.info("Running grouping job...")
        try:
            compilations = self.grouper.create_compilations(max_compilations=2)
            logger.info(f"Created {len(compilations)} compilations")
        except Exception as e:
            logger.error(f"Grouping job failed: {e}")

    def job_render_compilations(self) -> None:
        """Render pending compilations."""
        logger.info("Running render job...")
        try:
            success, fail = self.stitcher.render_pending(limit=2)
            logger.info(f"Render complete: {success} succeeded, {fail} failed")

            # Auto-approve rendered compilations if they were pre-approved
            self._auto_approve_rendered()
        except Exception as e:
            logger.error(f"Render job failed: {e}")

    def _auto_approve_rendered(self) -> None:
        """Auto-approve compilations that were marked for auto-approval."""
        review_compilations = self.db.get_compilations_by_status(
            CompilationStatus.REVIEW
        )

        for compilation in review_compilations:
            if compilation.auto_approved:
                compilation.status = CompilationStatus.APPROVED
                self.db.update_compilation(compilation)
                logger.info(
                    f"Auto-approved compilation {compilation.id} "
                    f"(confidence: {compilation.confidence_score:.2f})"
                )

    def job_route_uploads(self) -> None:
        """Route approved compilations to platform accounts."""
        logger.info("Running upload routing job...")
        try:
            uploads = self.upload_router.route_approved_compilations()
            logger.info(f"Created {len(uploads)} upload jobs")
        except Exception as e:
            logger.error(f"Upload routing job failed: {e}")

    def job_process_uploads(self) -> None:
        """Process pending uploads - one upload per run (every 6 hours)."""
        logger.info("Running upload processing job...")

        # Process one upload total (prioritize YouTube, then TikTok)
        # This ensures only one short is uploaded every 6 hours
        result = self.upload_router.get_next_upload(Platform.YOUTUBE)
        if result:
            self._process_youtube_upload(result)
        else:
            # Try TikTok if no YouTube upload available
            result = self.upload_router.get_next_upload(Platform.TIKTOK)
            if result:
                self._process_tiktok_upload(result)

    def _process_youtube_upload(self, result: Tuple[Upload, Account, Compilation]) -> None:
        """Process a single YouTube upload."""
        upload, account, compilation = result
        
        try:
            self.upload_router.mark_upload_started(upload)

            try:
                video_id = self.youtube_uploader.upload(
                    upload, account, compilation
                )

                if video_id:
                    self.upload_router.mark_upload_success(upload, video_id)

                    # Update compilation status
                    compilation.status = CompilationStatus.UPLOADED
                    compilation.youtube_id = video_id
                    self.db.update_compilation(compilation)
                    logger.info(f"Successfully uploaded YouTube video: {video_id}")
                else:
                    self.upload_router.mark_upload_failed(upload, "Upload returned no video ID")

            except Exception as e:
                self.upload_router.mark_upload_failed(upload, str(e))
                self.account_manager.record_error(account.id, str(e))

        except Exception as e:
            logger.error(f"YouTube upload processing failed: {e}")

    def _process_tiktok_upload(self, result: Tuple[Upload, Account, Compilation]) -> None:
        """Process a single TikTok upload."""
        upload, account, compilation = result
        
        try:
            self.upload_router.mark_upload_started(upload)

            try:
                video_id = self.tiktok_uploader.upload(
                    upload, account, compilation
                )

                if video_id:
                    self.upload_router.mark_upload_success(upload, video_id)
                    logger.info(f"Successfully uploaded TikTok video: {video_id}")
                else:
                    self.upload_router.mark_upload_failed(upload, "Upload returned no video ID")

            except Exception as e:
                self.upload_router.mark_upload_failed(upload, str(e))
                self.account_manager.record_error(account.id, str(e))

        except Exception as e:
            logger.error(f"TikTok upload processing failed: {e}")

    def job_reset_daily_limits(self) -> None:
        """Reset daily upload limits for all accounts."""
        logger.info("Resetting daily upload limits...")
        try:
            self.account_manager.reset_daily_limits()
        except Exception as e:
            logger.error(f"Reset daily limits failed: {e}")

    def job_retry_failed_uploads(self) -> None:
        """Retry failed uploads."""
        logger.info("Retrying failed uploads...")
        try:
            count = self.upload_router.retry_failed_uploads(max_retries=3)
            if count > 0:
                logger.info(f"Re-queued {count} failed uploads")
        except Exception as e:
            logger.error(f"Retry failed uploads job failed: {e}")

    def job_full_pipeline(self) -> None:
        """
        Run the full pipeline in sequence.
        Useful for manual triggering or less frequent scheduling.
        """
        logger.info("Running full pipeline...")

        self.job_discover_content()
        self.job_download_videos()
        self.job_classify_videos()
        self.job_create_compilations()
        self.job_render_compilations()
        self.job_route_uploads()
        self.job_process_uploads()

        logger.info("Full pipeline complete")

    # =========================================================================
    # Mega-Compilation Jobs (Source Compilations Pipeline)
    # =========================================================================

    def job_discover_source_compilations(self) -> None:
        """Discover existing TikTok compilation videos."""
        logger.info("Running source compilation discovery job...")
        try:
            compilations, skipped = self.discovery.discover_compilations(limit=30)
            logger.info(
                f"Source compilation discovery complete: {len(compilations)} new, "
                f"{skipped} skipped"
            )
        except Exception as e:
            logger.error(f"Source compilation discovery job failed: {e}")

    def job_download_source_compilations(self) -> None:
        """Download discovered source compilations."""
        logger.info("Running source compilation download job...")
        try:
            # Get discovered source compilations that need downloading
            source_comps = self.db.get_source_compilations(
                status=VideoStatus.DISCOVERED,
                limit=10
            )

            if not source_comps:
                logger.info("No source compilations to download")
                return

            success = 0
            fail = 0
            for video in source_comps:
                if self.downloader.download(video):
                    success += 1
                else:
                    fail += 1

            logger.info(
                f"Source compilation download complete: {success} succeeded, "
                f"{fail} failed"
            )
        except Exception as e:
            logger.error(f"Source compilation download job failed: {e}")

    def job_create_mega_compilations(self) -> None:
        """Create mega-compilations from downloaded source compilations."""
        logger.info("Running mega-compilation creation job...")
        try:
            compilations = self.grouper.create_mega_compilations(
                max_compilations=2,
                num_sources_per=4
            )
            logger.info(f"Created {len(compilations)} mega-compilations")
        except Exception as e:
            logger.error(f"Mega-compilation creation job failed: {e}")

    def job_mega_compilation_pipeline(self) -> None:
        """
        Run the full mega-compilation pipeline in sequence.
        Discovers existing compilations, downloads, groups, renders, and routes.
        """
        logger.info("Running mega-compilation pipeline...")

        self.job_discover_source_compilations()
        self.job_download_source_compilations()
        self.job_create_mega_compilations()
        self.job_render_compilations()  # Reuse existing render job
        self.job_route_uploads()
        self.job_process_uploads()

        logger.info("Mega-compilation pipeline complete")

    # =========================================================================
    # Scheduler Configuration
    # =========================================================================

    def configure_default_schedule(self) -> None:
        """Configure the default job schedule (optimized for 6-hour upload cycles)."""

        # Discovery: Every 6 hours (aligned with upload cycle, saves API costs)
        self.scheduler.add_job(
            self.job_discover_content,
            IntervalTrigger(hours=6),
            id="discover_content",
            name="Discover new content",
            replace_existing=True,
        )

        # Download: Every 2 hours (processes discovered content efficiently)
        self.scheduler.add_job(
            self.job_download_videos,
            IntervalTrigger(hours=2),
            id="download_videos",
            name="Download videos",
            replace_existing=True,
        )

        # Classify: Every 2 hours (processes downloaded content)
        self.scheduler.add_job(
            self.job_classify_videos,
            IntervalTrigger(hours=2),
            id="classify_videos",
            name="Classify videos",
            replace_existing=True,
        )

        # Create compilations: Every 2 hours (creates compilations from classified videos)
        self.scheduler.add_job(
            self.job_create_compilations,
            IntervalTrigger(hours=2),
            id="create_compilations",
            name="Create compilations",
            replace_existing=True,
        )

        # Render: Every 2 hours (renders pending compilations)
        self.scheduler.add_job(
            self.job_render_compilations,
            IntervalTrigger(hours=2),
            id="render_compilations",
            name="Render compilations",
            replace_existing=True,
        )

        # Route uploads: Every 1 hour (routes approved compilations to accounts)
        self.scheduler.add_job(
            self.job_route_uploads,
            IntervalTrigger(hours=1),
            id="route_uploads",
            name="Route uploads to accounts",
            replace_existing=True,
        )

        # Process uploads: Every 6 hours (4 times per day)
        self.scheduler.add_job(
            self.job_process_uploads,
            IntervalTrigger(hours=6),
            id="process_uploads",
            name="Process pending uploads",
            replace_existing=True,
        )

        # Retry failed: Every 2 hours
        self.scheduler.add_job(
            self.job_retry_failed_uploads,
            IntervalTrigger(hours=2),
            id="retry_uploads",
            name="Retry failed uploads",
            replace_existing=True,
        )

        # Reset daily limits: At midnight
        self.scheduler.add_job(
            self.job_reset_daily_limits,
            CronTrigger(hour=0, minute=0),
            id="reset_limits",
            name="Reset daily upload limits",
            replace_existing=True,
        )

        logger.info("Configured default schedule with 9 jobs")

    def configure_aggressive_schedule(self) -> None:
        """
        Configure a more aggressive schedule for faster content throughput.
        Use when you have many accounts and want to maximize output.
        """
        # Discovery: Every 2 hours
        self.scheduler.add_job(
            self.job_discover_content,
            IntervalTrigger(hours=2),
            id="discover_content",
            name="Discover new content",
            replace_existing=True,
        )

        # Download: Every 15 minutes
        self.scheduler.add_job(
            self.job_download_videos,
            IntervalTrigger(minutes=15),
            id="download_videos",
            name="Download videos",
            replace_existing=True,
        )

        # Classify: Every 15 minutes
        self.scheduler.add_job(
            self.job_classify_videos,
            IntervalTrigger(minutes=15),
            id="classify_videos",
            name="Classify videos",
            replace_existing=True,
        )

        # Create compilations: Every 30 minutes
        self.scheduler.add_job(
            self.job_create_compilations,
            IntervalTrigger(minutes=30),
            id="create_compilations",
            name="Create compilations",
            replace_existing=True,
        )

        # Render: Every 30 minutes
        self.scheduler.add_job(
            self.job_render_compilations,
            IntervalTrigger(minutes=30),
            id="render_compilations",
            name="Render compilations",
            replace_existing=True,
        )

        # Route uploads: Every 15 minutes
        self.scheduler.add_job(
            self.job_route_uploads,
            IntervalTrigger(minutes=15),
            id="route_uploads",
            name="Route uploads to accounts",
            replace_existing=True,
        )

        # Process uploads: Every 6 hours (4 times per day)
        self.scheduler.add_job(
            self.job_process_uploads,
            IntervalTrigger(hours=6),
            id="process_uploads",
            name="Process pending uploads",
            replace_existing=True,
        )

        # Retry failed: Every hour
        self.scheduler.add_job(
            self.job_retry_failed_uploads,
            IntervalTrigger(hours=1),
            id="retry_uploads",
            name="Retry failed uploads",
            replace_existing=True,
        )

        # Reset daily limits: At midnight
        self.scheduler.add_job(
            self.job_reset_daily_limits,
            CronTrigger(hour=0, minute=0),
            id="reset_limits",
            name="Reset daily upload limits",
            replace_existing=True,
        )

        logger.info("Configured aggressive schedule with 9 jobs")

    def configure_mega_compilation_schedule(self) -> None:
        """
        Configure schedule for mega-compilation pipeline only.
        This discovers existing TikTok compilations and stitches them together.
        """
        # Discover source compilations: Every 6 hours
        self.scheduler.add_job(
            self.job_discover_source_compilations,
            IntervalTrigger(hours=6),
            id="discover_source_compilations",
            name="Discover source compilations",
            replace_existing=True,
        )

        # Download source compilations: Every 2 hours
        self.scheduler.add_job(
            self.job_download_source_compilations,
            IntervalTrigger(hours=2),
            id="download_source_compilations",
            name="Download source compilations",
            replace_existing=True,
        )

        # Create mega-compilations: Every 2 hours (more efficient processing)
        self.scheduler.add_job(
            self.job_create_mega_compilations,
            IntervalTrigger(hours=2),
            id="create_mega_compilations",
            name="Create mega-compilations",
            replace_existing=True,
        )

        # Render: Every 2 hours
        self.scheduler.add_job(
            self.job_render_compilations,
            IntervalTrigger(hours=2),
            id="render_compilations",
            name="Render compilations",
            replace_existing=True,
        )

        # Route uploads: Every hour
        self.scheduler.add_job(
            self.job_route_uploads,
            IntervalTrigger(hours=1),
            id="route_uploads",
            name="Route uploads to accounts",
            replace_existing=True,
        )

        # Process uploads: Every 6 hours (4 times per day)
        self.scheduler.add_job(
            self.job_process_uploads,
            IntervalTrigger(hours=6),
            id="process_uploads",
            name="Process pending uploads",
            replace_existing=True,
        )

        # Retry failed: Every 2 hours
        self.scheduler.add_job(
            self.job_retry_failed_uploads,
            IntervalTrigger(hours=2),
            id="retry_uploads",
            name="Retry failed uploads",
            replace_existing=True,
        )

        # Reset daily limits: At midnight
        self.scheduler.add_job(
            self.job_reset_daily_limits,
            CronTrigger(hour=0, minute=0),
            id="reset_limits",
            name="Reset daily upload limits",
            replace_existing=True,
        )

        logger.info("Configured mega-compilation schedule with 8 jobs")

    def start(self) -> None:
        """Start the scheduler."""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("Scheduler started")

    def stop(self) -> None:
        """Stop the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
            logger.info("Scheduler stopped")

    def get_jobs(self) -> list:
        """Get list of scheduled jobs."""
        return self.scheduler.get_jobs()

    def run_job_now(self, job_id: str) -> bool:
        """Run a specific job immediately."""
        job = self.scheduler.get_job(job_id)
        if job:
            job.func()
            return True
        return False

    def get_status(self) -> dict:
        """Get scheduler status."""
        return {
            "running": self.scheduler.running,
            "jobs": [
                {
                    "id": job.id,
                    "name": job.name,
                    "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                }
                for job in self.scheduler.get_jobs()
            ],
        }
