"""
Reddit Story Narration Pipeline Orchestrator.
Coordinates the full flow from discovery to video composition.
"""

import logging
from typing import List, Optional, Tuple

from config.settings import settings
from core.database import Database
from core.models import RedditPost, RedditVideo, RedditPostStatus, RedditVideoStatus
from services.reddit_scraper import RedditScraperService
from services.reddit_tts import RedditTTSService
from services.reddit_composer import RedditComposerService

logger = logging.getLogger(__name__)


class RedditPipeline:
    """Orchestrates the Reddit story narration pipeline."""

    def __init__(self, db: Database = None):
        """Initialize pipeline with optional database."""
        self._db = db
        self._scraper = None
        self._tts = None
        self._composer = None

    @property
    def db(self) -> Database:
        """Lazy-load database connection."""
        if self._db is None:
            settings.ensure_directories()
            self._db = Database(settings.DATABASE_PATH)
        return self._db

    @property
    def scraper(self) -> RedditScraperService:
        """Lazy-load scraper service."""
        if self._scraper is None:
            self._scraper = RedditScraperService(self.db)
        return self._scraper

    @property
    def tts(self) -> RedditTTSService:
        """Lazy-load TTS service."""
        if self._tts is None:
            self._tts = RedditTTSService(self.db)
        return self._tts

    @property
    def composer(self) -> RedditComposerService:
        """Lazy-load composer service."""
        if self._composer is None:
            self._composer = RedditComposerService(self.db)
        return self._composer

    def discover(
        self,
        subreddit: Optional[str] = None,
        limit: int = 10,
    ) -> Tuple[List[RedditPost], int]:
        """Discover Reddit posts for narration.

        Args:
            subreddit: Specific subreddit to scrape (None = use config)
            limit: Maximum posts to discover

        Returns:
            Tuple of (discovered posts, skipped count)
        """
        logger.info("=" * 60)
        logger.info("REDDIT DISCOVERY")
        logger.info("=" * 60)

        if subreddit:
            return self.scraper.discover_from_subreddit(subreddit, limit)
        else:
            return self.scraper.discover_from_config(limit_per_subreddit=limit)

    def generate_audio(self, limit: Optional[int] = None) -> Tuple[int, int]:
        """Generate TTS audio for pending posts.

        Args:
            limit: Maximum posts to process

        Returns:
            Tuple of (success_count, fail_count)
        """
        logger.info("=" * 60)
        logger.info("TTS GENERATION")
        logger.info("=" * 60)

        return self.tts.process_pending(limit)

    def compose_videos(self, limit: Optional[int] = None) -> Tuple[int, int]:
        """Compose videos from posts with audio.

        Args:
            limit: Maximum posts to process

        Returns:
            Tuple of (success_count, fail_count)
        """
        logger.info("=" * 60)
        logger.info("VIDEO COMPOSITION")
        logger.info("=" * 60)

        return self.composer.compose_pending(limit)

    def run_full_pipeline(
        self,
        subreddit: Optional[str] = None,
        discover_limit: int = 10,
        videos_to_create: int = 3,
    ) -> dict:
        """Run the complete pipeline: discover -> TTS -> compose.

        Args:
            subreddit: Specific subreddit to scrape (None = use config)
            discover_limit: Maximum posts to discover per subreddit
            videos_to_create: Maximum videos to create

        Returns:
            Dict with pipeline results
        """
        logger.info("=" * 60)
        logger.info("REDDIT STORY NARRATION PIPELINE")
        logger.info("=" * 60)

        results = {
            "discovered": 0,
            "skipped": 0,
            "audio_success": 0,
            "audio_failed": 0,
            "videos_success": 0,
            "videos_failed": 0,
        }

        # Step 1: Discover posts
        try:
            posts, skipped = self.discover(subreddit, discover_limit)
            results["discovered"] = len(posts)
            results["skipped"] = skipped
        except Exception as e:
            logger.error(f"Discovery failed: {e}")

        # Step 2: Generate TTS audio
        try:
            audio_success, audio_failed = self.generate_audio(limit=videos_to_create)
            results["audio_success"] = audio_success
            results["audio_failed"] = audio_failed
        except Exception as e:
            logger.error(f"TTS generation failed: {e}")

        # Step 3: Compose videos
        try:
            videos_success, videos_failed = self.compose_videos(limit=videos_to_create)
            results["videos_success"] = videos_success
            results["videos_failed"] = videos_failed
        except Exception as e:
            logger.error(f"Video composition failed: {e}")

        # Summary
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info("-" * 60)
        logger.info(f"Posts discovered: {results['discovered']}")
        logger.info(f"Posts skipped: {results['skipped']}")
        logger.info(f"Audio generated: {results['audio_success']}")
        logger.info(f"Audio failed: {results['audio_failed']}")
        logger.info(f"Videos created: {results['videos_success']}")
        logger.info(f"Videos failed: {results['videos_failed']}")
        logger.info("=" * 60)

        return results

    def get_status(self) -> dict:
        """Get pipeline status and statistics."""
        return self.db.get_reddit_stats()

    def get_pending_posts(self) -> List[RedditPost]:
        """Get posts pending TTS generation."""
        return self.db.get_reddit_posts_by_status(RedditPostStatus.DISCOVERED)

    def get_audio_ready_posts(self) -> List[RedditPost]:
        """Get posts with audio ready for composition."""
        return self.db.get_reddit_posts_by_status(RedditPostStatus.AUDIO_READY)

    def get_videos_for_review(self) -> List[RedditVideo]:
        """Get videos ready for review."""
        return self.db.get_reddit_videos_by_status(RedditVideoStatus.REVIEW)

    def get_approved_videos(self) -> List[RedditVideo]:
        """Get videos approved for upload."""
        return self.db.get_reddit_videos_by_status(RedditVideoStatus.APPROVED)

    def approve_video(self, video_id: str) -> bool:
        """Approve a video for upload."""
        return self.composer.approve_video(video_id)

    def reject_video(self, video_id: str) -> bool:
        """Reject a video."""
        return self.composer.reject_video(video_id)

    def list_posts(
        self,
        status: Optional[str] = None,
        limit: int = 20,
    ) -> List[RedditPost]:
        """List Reddit posts with optional status filter.

        Args:
            status: Status filter (discovered, audio_ready, composed, etc.)
            limit: Maximum posts to return

        Returns:
            List of RedditPost objects
        """
        if status:
            try:
                status_enum = RedditPostStatus(status)
                return self.db.get_reddit_posts_by_status(status_enum, limit)
            except ValueError:
                logger.warning(f"Invalid status: {status}")
                return []
        else:
            # Return all posts (combine all statuses)
            all_posts = []
            for s in RedditPostStatus:
                posts = self.db.get_reddit_posts_by_status(s, limit)
                all_posts.extend(posts)
            return all_posts[:limit]

    def list_videos(
        self,
        status: Optional[str] = None,
        limit: int = 20,
    ) -> List[RedditVideo]:
        """List Reddit videos with optional status filter.

        Args:
            status: Status filter (pending, review, approved, etc.)
            limit: Maximum videos to return

        Returns:
            List of RedditVideo objects
        """
        if status:
            try:
                status_enum = RedditVideoStatus(status)
                return self.db.get_reddit_videos_by_status(status_enum, limit)
            except ValueError:
                logger.warning(f"Invalid status: {status}")
                return []
        else:
            return self.db.get_all_reddit_videos()[:limit]
