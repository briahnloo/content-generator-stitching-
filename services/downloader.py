"""
Downloader service for fetching TikTok videos via yt-dlp.
"""

import json
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Tuple, Optional

from core.models import Video, VideoStatus
from core.database import Database
from config.settings import settings

logger = logging.getLogger(__name__)


class DownloaderService:
    """Downloads TikTok videos using yt-dlp."""

    def __init__(self, db: Database):
        """Initialize downloader service."""
        self.db = db
        self.download_dir = settings.DOWNLOAD_DIR
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def _get_video_metadata(self, file_path: Path) -> Tuple[float, int, int]:
        """
        Get video metadata using ffprobe.
        Returns (duration, width, height).
        """
        try:
            cmd = [
                "ffprobe",
                "-v", "quiet",
                "-print_format", "json",
                "-show_format",
                "-show_streams",
                str(file_path),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            if result.returncode != 0:
                logger.warning(f"ffprobe failed for {file_path}")
                return 0.0, 0, 0

            data = json.loads(result.stdout)

            # Get duration from format
            duration = float(data.get("format", {}).get("duration", 0))

            # Get dimensions from video stream
            width, height = 0, 0
            for stream in data.get("streams", []):
                if stream.get("codec_type") == "video":
                    width = stream.get("width", 0)
                    height = stream.get("height", 0)
                    break

            return duration, width, height

        except subprocess.TimeoutExpired:
            logger.warning(f"ffprobe timed out for {file_path}")
            return 0.0, 0, 0
        except Exception as e:
            logger.warning(f"Failed to get video metadata: {e}")
            return 0.0, 0, 0

    def download(self, video: Video) -> bool:
        """
        Download a single video.
        Returns True on success, False on failure.
        """
        output_path = self.download_dir / f"{video.id}.mp4"

        # Skip if already downloaded
        if output_path.exists() and output_path.stat().st_size > 0:
            logger.info(f"Video {video.id} already downloaded")
            duration, width, height = self._get_video_metadata(output_path)
            video.local_path = str(output_path)
            video.duration = duration
            video.width = width
            video.height = height
            video.status = VideoStatus.DOWNLOADED
            self.db.update_video(video)
            return True

        # Build yt-dlp command
        cmd = [
            "yt-dlp",
            "-f", "best[ext=mp4]/best",
            "--merge-output-format", "mp4",
            "--no-playlist",
            "--no-warnings",
            "--quiet",
            "-o", str(output_path),
            video.url,
        ]

        try:
            logger.info(f"Downloading video {video.id} from {video.url}")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,  # 2 minute timeout
            )

            if result.returncode != 0:
                error_msg = result.stderr.strip() or "Unknown download error"
                logger.warning(f"Download failed for {video.id}: {error_msg}")
                video.retry_count += 1
                video.error = error_msg

                if video.retry_count >= settings.MAX_DOWNLOAD_RETRIES:
                    video.status = VideoStatus.FAILED
                    logger.error(f"Video {video.id} failed after {video.retry_count} retries")

                self.db.update_video(video)
                return False

            # Verify file exists
            if not output_path.exists() or output_path.stat().st_size == 0:
                logger.warning(f"Downloaded file missing or empty for {video.id}")
                video.retry_count += 1
                video.error = "Downloaded file missing or empty"

                if video.retry_count >= settings.MAX_DOWNLOAD_RETRIES:
                    video.status = VideoStatus.FAILED

                self.db.update_video(video)
                return False

            # Get metadata
            duration, width, height = self._get_video_metadata(output_path)

            # Update video record
            video.local_path = str(output_path)
            video.duration = duration
            video.width = width
            video.height = height
            video.status = VideoStatus.DOWNLOADED
            video.error = ""
            self.db.update_video(video)

            logger.info(f"Downloaded video {video.id} ({duration:.1f}s, {width}x{height})")
            return True

        except subprocess.TimeoutExpired:
            logger.warning(f"Download timed out for {video.id}")
            video.retry_count += 1
            video.error = "Download timed out"

            if video.retry_count >= settings.MAX_DOWNLOAD_RETRIES:
                video.status = VideoStatus.FAILED

            self.db.update_video(video)
            return False

        except Exception as e:
            logger.error(f"Download exception for {video.id}: {e}")
            video.retry_count += 1
            video.error = str(e)

            if video.retry_count >= settings.MAX_DOWNLOAD_RETRIES:
                video.status = VideoStatus.FAILED

            self.db.update_video(video)
            return False

    def download_batch(
        self, videos: list[Video], progress_callback: Optional[callable] = None
    ) -> Tuple[int, int]:
        """
        Download multiple videos.
        Returns (success_count, fail_count).
        """
        success = 0
        fail = 0

        for i, video in enumerate(videos):
            if progress_callback:
                progress_callback(i + 1, len(videos), video)

            if self.download(video):
                success += 1
            else:
                fail += 1

        logger.info(f"Batch download complete: {success} succeeded, {fail} failed")
        return success, fail

    def download_discovered(
        self, limit: Optional[int] = None, progress_callback: Optional[callable] = None
    ) -> Tuple[int, int]:
        """
        Download all videos with DISCOVERED status.
        Returns (success_count, fail_count).
        """
        videos = self.db.get_videos_by_status(VideoStatus.DISCOVERED, limit)
        logger.info(f"Found {len(videos)} videos to download")

        if not videos:
            return 0, 0

        return self.download_batch(videos, progress_callback)

    def retry_failed(
        self, progress_callback: Optional[callable] = None
    ) -> Tuple[int, int]:
        """
        Retry downloading failed videos that haven't exceeded retry limit.
        Returns (success_count, fail_count).
        """
        failed_videos = self.db.get_videos_by_status(VideoStatus.FAILED)
        retryable = [v for v in failed_videos if v.retry_count < settings.MAX_DOWNLOAD_RETRIES]

        logger.info(f"Found {len(retryable)} failed videos to retry")

        if not retryable:
            return 0, 0

        return self.download_batch(retryable, progress_callback)
