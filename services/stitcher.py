"""
Stitcher service for rendering compilations with FFmpeg.
"""

import logging
import os
import random
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from core.models import Compilation, Video, CompilationStatus, VideoStatus
from core.database import Database
from config.settings import settings, categories_config

logger = logging.getLogger(__name__)


class StitcherService:
    """Renders compilation videos using FFmpeg."""

    def __init__(self, db: Database):
        """Initialize stitcher service."""
        self.db = db
        self.output_dir = settings.OUTPUT_DIR
        self.review_dir = settings.REVIEW_DIR
        self.music_dir = settings.MUSIC_DIR
        self.width = settings.VIDEO_WIDTH
        self.height = settings.VIDEO_HEIGHT
        self.fps = settings.FPS
        self.max_clip_duration = settings.MAX_CLIP_DURATION

        # Ensure directories exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.review_dir.mkdir(parents=True, exist_ok=True)

    def _check_ffmpeg(self) -> bool:
        """Check if FFmpeg is available."""
        try:
            subprocess.run(
                ["ffmpeg", "-version"],
                capture_output=True,
                check=True,
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False

    def _get_music_track(self, category: str) -> Optional[Path]:
        """Select a background music track based on category mood."""
        category_config = categories_config.get_category(category)
        mood = category_config.get("mood", "upbeat")

        # Get tracks for mood
        mood_tracks = categories_config.get_music_for_mood(mood)

        # Find available tracks
        available = []
        for track_name in mood_tracks:
            track_path = self.music_dir / track_name
            if track_path.exists():
                available.append(track_path)

        # Fallback to any available track
        if not available:
            for track_path in self.music_dir.glob("*.mp3"):
                available.append(track_path)

        if not available:
            logger.warning("No music tracks available")
            return None

        return random.choice(available)

    def _escape_text(self, text: str) -> str:
        """Escape text for FFmpeg drawtext filter."""
        # Escape special characters for FFmpeg
        text = text.replace("\\", "\\\\")
        text = text.replace("'", "'\\''")
        text = text.replace(":", "\\:")
        text = text.replace("%", "\\%")
        return text

    def _process_clip(
        self,
        video: Video,
        output_path: Path,
        temp_dir: Path,
    ) -> bool:
        """
        Process a single clip: scale, crop, limit duration.
        Returns True on success.
        """
        if not video.local_path or not Path(video.local_path).exists():
            logger.error(f"Video file not found: {video.local_path}")
            return False

        # Build filter chain (no captions - raw video only)
        filters = [
            # Scale to fit while maintaining aspect ratio
            f"scale={self.width}:{self.height}:force_original_aspect_ratio=increase",
            # Center crop to exact dimensions
            f"crop={self.width}:{self.height}",
            # Set frame rate
            f"fps={self.fps}",
        ]

        filter_string = ",".join(filters)

        # Determine clip duration
        duration = min(video.duration, self.max_clip_duration) if video.duration > 0 else self.max_clip_duration

        cmd = [
            "ffmpeg",
            "-y",
            "-i", video.local_path,
            "-vf", filter_string,
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "23",
            "-c:a", "aac",
            "-b:a", "128k",
            "-t", str(duration),
            "-movflags", "+faststart",
            str(output_path),
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,
            )

            if result.returncode != 0:
                logger.error(f"FFmpeg clip processing failed: {result.stderr}")
                return False

            return output_path.exists()

        except subprocess.TimeoutExpired:
            logger.error(f"Clip processing timed out for {video.id}")
            return False
        except Exception as e:
            logger.error(f"Clip processing error: {e}")
            return False

    def _create_title_card(
        self,
        text: str,
        output_path: Path,
        duration: float = 2.0,
    ) -> bool:
        """Create a title card with text."""
        escaped_text = self._escape_text(text)

        cmd = [
            "ffmpeg",
            "-y",
            "-f", "lavfi",
            "-i", f"color=c=black:s={self.width}x{self.height}:d={duration}:r={self.fps}",
            "-f", "lavfi",
            "-i", f"anullsrc=channel_layout=stereo:sample_rate=44100",
            "-vf", (
                f"drawtext=text='{escaped_text}':"
                f"fontsize=64:"
                f"fontcolor=white:"
                f"x=(w-text_w)/2:"
                f"y=(h-text_h)/2"
            ),
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "23",
            "-c:a", "aac",
            "-t", str(duration),
            str(output_path),
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )

            if result.returncode != 0:
                logger.error(f"Title card creation failed: {result.stderr}")
                return False

            return output_path.exists()

        except Exception as e:
            logger.error(f"Title card error: {e}")
            return False

    def _concatenate_clips(
        self,
        clip_paths: List[Path],
        output_path: Path,
        music_path: Optional[Path] = None,
    ) -> bool:
        """Concatenate clips with optional background music."""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False
        ) as concat_file:
            for clip_path in clip_paths:
                concat_file.write(f"file '{clip_path}'\n")
            concat_path = concat_file.name

        try:
            if music_path and music_path.exists():
                # Concatenate with music mixing
                cmd = [
                    "ffmpeg",
                    "-y",
                    "-f", "concat",
                    "-safe", "0",
                    "-i", concat_path,
                    "-i", str(music_path),
                    "-filter_complex",
                    "[0:a]volume=1.0[v];[1:a]volume=0.25[m];[v][m]amix=inputs=2:duration=first:dropout_transition=2",
                    "-c:v", "copy",
                    "-c:a", "aac",
                    "-b:a", "192k",
                    "-shortest",
                    str(output_path),
                ]
            else:
                # Concatenate without music
                cmd = [
                    "ffmpeg",
                    "-y",
                    "-f", "concat",
                    "-safe", "0",
                    "-i", concat_path,
                    "-c:v", "copy",
                    "-c:a", "aac",
                    str(output_path),
                ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,
            )

            if result.returncode != 0:
                logger.error(f"Concatenation failed: {result.stderr}")
                return False

            return output_path.exists()

        except Exception as e:
            logger.error(f"Concatenation error: {e}")
            return False
        finally:
            os.unlink(concat_path)

    def render(
        self,
        compilation: Compilation,
        progress_callback: Optional[callable] = None,
    ) -> bool:
        """
        Render a compilation video.
        Returns True on success.
        """
        if not self._check_ffmpeg():
            logger.error("FFmpeg not found")
            compilation.error = "FFmpeg not found"
            compilation.status = CompilationStatus.PENDING
            self.db.update_compilation(compilation)
            return False

        # Update status
        compilation.status = CompilationStatus.RENDERING
        self.db.update_compilation(compilation)

        videos = self.db.get_videos_for_compilation(compilation.id)
        if not videos:
            logger.error(f"No videos found for compilation {compilation.id}")
            compilation.error = "No videos found"
            compilation.status = CompilationStatus.PENDING
            self.db.update_compilation(compilation)
            return False

        # Create temp directory for intermediate files
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            clip_paths = []
            total_steps = len(videos) + 1  # clips + concat (no title/end cards)

            try:
                # Process each clip (no black screen hook - start with content immediately)
                for i, video in enumerate(videos):
                    if progress_callback:
                        progress_callback(i + 1, total_steps, f"Processing clip {i+1}")

                    clip_path = temp_path / f"clip_{i:03d}.mp4"

                    if self._process_clip(video, clip_path, temp_path):
                        clip_paths.append(clip_path)
                    else:
                        logger.warning(f"Failed to process clip {i} ({video.id})")

                # No end card - keep it clean and fast-paced

                if len(clip_paths) < 1:
                    logger.error("Not enough clips processed successfully")
                    compilation.error = "Not enough clips processed"
                    compilation.status = CompilationStatus.PENDING
                    self.db.update_compilation(compilation)
                    return False

                # Get music track
                music_path = self._get_music_track(compilation.category)
                if music_path:
                    compilation.music_track = music_path.name

                # Concatenate
                if progress_callback:
                    progress_callback(total_steps, total_steps, "Concatenating clips")

                # Add timestamp prefix for chronological sorting (YYYYMMDD-HHMMSS)
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                output_path = self.review_dir / f"{timestamp}-{compilation.id}.mp4"

                if not self._concatenate_clips(clip_paths, output_path, music_path):
                    logger.error("Failed to concatenate clips")
                    compilation.error = "Concatenation failed"
                    compilation.status = CompilationStatus.PENDING
                    self.db.update_compilation(compilation)
                    return False

                # Get final duration
                try:
                    cmd = [
                        "ffprobe", "-v", "quiet",
                        "-show_entries", "format=duration",
                        "-of", "default=noprint_wrappers=1:nokey=1",
                        str(output_path),
                    ]
                    result = subprocess.run(cmd, capture_output=True, text=True)
                    compilation.duration = float(result.stdout.strip())
                except Exception:
                    compilation.duration = 0.0

                # Update compilation
                compilation.output_path = str(output_path)
                compilation.status = CompilationStatus.REVIEW
                compilation.error = ""
                self.db.update_compilation(compilation)

                # Update video statuses
                for video in videos:
                    video.status = VideoStatus.USED
                    self.db.update_video(video)

                logger.info(
                    f"Rendered compilation {compilation.id}: "
                    f"{compilation.duration:.1f}s at {output_path}"
                )
                return True

            except Exception as e:
                logger.error(f"Render failed for {compilation.id}: {e}")
                compilation.error = str(e)
                compilation.status = CompilationStatus.PENDING
                self.db.update_compilation(compilation)
                return False

    def render_pending(
        self,
        limit: Optional[int] = None,
        progress_callback: Optional[callable] = None,
    ) -> tuple[int, int]:
        """
        Render all pending compilations.
        Returns (success_count, fail_count).
        """
        pending = self.db.get_compilations_by_status(CompilationStatus.PENDING, limit)
        logger.info(f"Found {len(pending)} pending compilations to render")

        success = 0
        fail = 0

        for i, compilation in enumerate(pending):
            logger.info(f"Rendering compilation {i+1}/{len(pending)}: {compilation.id}")

            if self.render(compilation, progress_callback):
                success += 1
            else:
                fail += 1

        logger.info(f"Render complete: {success} succeeded, {fail} failed")
        return success, fail
