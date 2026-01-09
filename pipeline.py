"""
Pipeline orchestrator for viral-clips-pipeline.
Coordinates all services to run the full pipeline.
"""

import logging
from typing import Optional, Tuple, List

from core.database import Database
from core.models import Compilation, CompilationStatus
from config.settings import settings

from services.discovery import DiscoveryService
from services.downloader import DownloaderService
from services.classifier import ClassifierService
from services.grouper import GrouperService
from services.captioner import CaptionerService
from services.stitcher import StitcherService
from services.uploader import UploaderService

logger = logging.getLogger(__name__)


class Pipeline:
    """Orchestrates the viral clips pipeline."""

    def __init__(self, db: Optional[Database] = None):
        """Initialize pipeline with database connection."""
        settings.ensure_directories()

        self.db = db or Database(settings.DATABASE_PATH)

        # Lazy-load services
        self._discovery = None
        self._downloader = None
        self._classifier = None
        self._grouper = None
        self._captioner = None
        self._stitcher = None
        self._uploader = None

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
    def captioner(self) -> CaptionerService:
        if self._captioner is None:
            self._captioner = CaptionerService(self.db)
        return self._captioner

    @property
    def stitcher(self) -> StitcherService:
        if self._stitcher is None:
            self._stitcher = StitcherService(self.db)
        return self._stitcher

    @property
    def uploader(self) -> UploaderService:
        if self._uploader is None:
            self._uploader = UploaderService(self.db)
        return self._uploader

    def discover(
        self,
        limit: int = 50,
        hashtag: Optional[str] = None,
        download: bool = True,
        progress_callback: Optional[callable] = None,
    ) -> dict:
        """
        Run discovery phase: fetch videos and optionally download.
        Returns stats dict.
        """
        logger.info(f"Starting discovery (limit={limit}, hashtag={hashtag})")

        if hashtag:
            videos, skipped = self.discovery.discover_by_hashtag(hashtag, limit)
        else:
            videos, skipped = self.discovery.discover_default(limit)

        stats = {
            "discovered": len(videos),
            "skipped_duplicates": skipped,
            "downloaded": 0,
            "download_failed": 0,
        }

        if download and videos:
            success, fail = self.downloader.download_batch(videos, progress_callback)
            stats["downloaded"] = success
            stats["download_failed"] = fail

        logger.info(f"Discovery complete: {stats}")
        return stats

    def classify(
        self,
        limit: Optional[int] = None,
        progress_callback: Optional[callable] = None,
    ) -> dict:
        """
        Run classification phase on downloaded videos.
        Returns stats dict.
        """
        logger.info(f"Starting classification (limit={limit})")

        classified, skipped, failed = self.classifier.classify_downloaded(
            limit, progress_callback=progress_callback
        )

        stats = {
            "classified": classified,
            "skipped": skipped,
            "failed": failed,
        }

        logger.info(f"Classification complete: {stats}")
        return stats

    def group(
        self,
        max_compilations: int = 3,
        clips_per_compilation: Optional[int] = None,
    ) -> List[Compilation]:
        """
        Run grouping phase to create compilations.
        Returns list of created compilations.
        """
        logger.info(f"Starting grouping (max_compilations={max_compilations})")

        compilations = self.grouper.create_compilations(
            max_compilations, clips_per_compilation
        )

        logger.info(f"Grouping complete: created {len(compilations)} compilations")
        return compilations

    def caption(
        self,
        compilation_id: Optional[str] = None,
    ) -> int:
        """
        Generate captions for pending compilations.
        Returns number of compilations captioned.
        """
        logger.info("Starting caption generation")

        if compilation_id:
            compilation = self.db.get_compilation(compilation_id)
            if compilation:
                self.captioner.generate_and_update(compilation)
                return 1
            return 0

        pending = self.db.get_compilations_by_status(CompilationStatus.PENDING)
        count = 0

        for compilation in pending:
            if not compilation.hook:  # Not yet captioned
                if self.captioner.generate_and_update(compilation):
                    count += 1

        logger.info(f"Caption generation complete: {count} compilations")
        return count

    def stitch(
        self,
        compilation_id: Optional[str] = None,
        progress_callback: Optional[callable] = None,
    ) -> Tuple[int, int]:
        """
        Run stitching phase to render compilations.
        Returns (success_count, fail_count).
        """
        logger.info("Starting stitching")

        if compilation_id:
            compilation = self.db.get_compilation(compilation_id)
            if compilation:
                # Generate captions if not present
                if not compilation.hook:
                    self.captioner.generate_and_update(compilation)
                    compilation = self.db.get_compilation(compilation_id)

                success = self.stitcher.render(compilation, progress_callback)
                return (1, 0) if success else (0, 1)
            return 0, 0

        # Generate captions for pending compilations first
        self.caption()

        success, fail = self.stitcher.render_pending(
            progress_callback=progress_callback
        )

        logger.info(f"Stitching complete: {success} succeeded, {fail} failed")
        return success, fail

    def run(
        self,
        discover_limit: int = 50,
        max_compilations: int = 3,
        hashtag: Optional[str] = None,
        progress_callback: Optional[callable] = None,
    ) -> dict:
        """
        Run the full pipeline: discover -> classify -> group -> stitch.
        Stops at review stage (does not upload).
        Returns stats dict.
        """
        logger.info("Starting full pipeline run")

        stats = {
            "discovery": {},
            "classification": {},
            "grouping": 0,
            "stitching": {"success": 0, "failed": 0},
        }

        # Discovery + Download
        stats["discovery"] = self.discover(
            limit=discover_limit,
            hashtag=hashtag,
            download=True,
            progress_callback=progress_callback,
        )

        # Classification
        stats["classification"] = self.classify(progress_callback=progress_callback)

        # Grouping
        compilations = self.group(max_compilations)
        stats["grouping"] = len(compilations)

        # Stitching
        if compilations:
            success, fail = self.stitch(progress_callback=progress_callback)
            stats["stitching"] = {"success": success, "failed": fail}

        logger.info(f"Pipeline run complete: {stats}")
        return stats

    def approve(self, compilation_id: str) -> bool:
        """Approve a compilation for upload."""
        compilation = self.db.get_compilation(compilation_id)
        if not compilation:
            logger.warning(f"Compilation {compilation_id} not found")
            return False

        if compilation.status != CompilationStatus.REVIEW:
            logger.warning(
                f"Can only approve compilations in REVIEW status "
                f"(current: {compilation.status})"
            )
            return False

        compilation.status = CompilationStatus.APPROVED
        self.db.update_compilation(compilation)
        logger.info(f"Approved compilation {compilation_id}")
        return True

    def reject(self, compilation_id: str, delete_file: bool = True) -> bool:
        """Reject a compilation."""
        compilation = self.db.get_compilation(compilation_id)
        if not compilation:
            logger.warning(f"Compilation {compilation_id} not found")
            return False

        if compilation.status not in (
            CompilationStatus.REVIEW,
            CompilationStatus.APPROVED,
        ):
            logger.warning(
                f"Can only reject compilations in REVIEW or APPROVED status "
                f"(current: {compilation.status})"
            )
            return False

        compilation.status = CompilationStatus.REJECTED
        self.db.update_compilation(compilation)

        if delete_file and compilation.output_path:
            import os
            try:
                os.remove(compilation.output_path)
                logger.info(f"Deleted output file: {compilation.output_path}")
            except OSError:
                pass

        logger.info(f"Rejected compilation {compilation_id}")
        return True

    def upload(
        self,
        compilation_id: str,
        privacy: str = "private",
        progress_callback: Optional[callable] = None,
    ) -> Optional[str]:
        """Upload an approved compilation to YouTube."""
        compilation = self.db.get_compilation(compilation_id)
        if not compilation:
            logger.warning(f"Compilation {compilation_id} not found")
            return None

        return self.uploader.upload(compilation, privacy, progress_callback)

    def get_status(self) -> dict:
        """Get pipeline status and statistics."""
        return self.db.get_stats()

    def reset(self) -> None:
        """Reset the database (clear all data)."""
        logger.warning("Resetting database - all data will be deleted")
        self.db.reset_database()
