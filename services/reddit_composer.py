"""
Video composition service for Reddit story narration.
Combines background video, TTS audio, and synchronized captions using FFmpeg.
"""

import json
import logging
import random
import subprocess
import uuid
from pathlib import Path
from typing import List, Optional, Tuple

from config.settings import settings, reddit_config
from core.database import Database
from core.models import RedditPost, RedditVideo, RedditPostStatus, RedditVideoStatus

logger = logging.getLogger(__name__)


class RedditComposerService:
    """Composes Reddit narration videos with synchronized captions."""

    def __init__(self, db: Database):
        """Initialize composer with database connection."""
        self.db = db
        self.output_dir = settings.REDDIT_OUTPUT_DIR
        self.backgrounds_dir = settings.BACKGROUNDS_DIR

    def _get_random_background(self) -> Optional[Path]:
        """Get a random background video file."""
        video_extensions = [".mp4", ".mov", ".avi", ".mkv"]
        backgrounds = []

        for ext in video_extensions:
            backgrounds.extend(self.backgrounds_dir.glob(f"*{ext}"))

        if not backgrounds:
            logger.error(f"No background videos found in {self.backgrounds_dir}")
            return None

        return random.choice(backgrounds)

    def _get_audio_duration(self, audio_path: str) -> float:
        """Get duration of audio file using ffprobe."""
        try:
            result = subprocess.run(
                [
                    "ffprobe",
                    "-v", "error",
                    "-show_entries", "format=duration",
                    "-of", "default=noprint_wrappers=1:nokey=1",
                    audio_path,
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            return float(result.stdout.strip())
        except Exception as e:
            logger.error(f"Failed to get audio duration: {e}")
            return 0.0

    def _group_words_into_captions(
        self,
        word_timings: List[dict],
        words_per_caption: int = 4,
    ) -> List[dict]:
        """Group word timings into caption segments.

        Args:
            word_timings: List of {word, start, end} dicts
            words_per_caption: Number of words per caption

        Returns:
            List of {text, start, end} caption dicts
        """
        if not word_timings:
            return []

        captions = []
        current_words = []
        current_start = None

        for timing in word_timings:
            if current_start is None:
                current_start = timing["start"]

            current_words.append(timing["word"])

            if len(current_words) >= words_per_caption:
                captions.append({
                    "text": " ".join(current_words),
                    "start": current_start,
                    "end": timing["end"],
                })
                current_words = []
                current_start = None

        # Add remaining words
        if current_words:
            captions.append({
                "text": " ".join(current_words),
                "start": current_start,
                "end": word_timings[-1]["end"],
            })

        return captions

    def _generate_subtitle_file(
        self,
        captions: List[dict],
        output_path: Path,
    ) -> bool:
        """Generate ASS subtitle file for captions.

        Args:
            captions: List of {text, start, end} dicts
            output_path: Path to save .ass file

        Returns:
            True if successful
        """
        video_config = reddit_config.get_video_config()
        font_size = video_config.get("font_size", 48)
        font_color = video_config.get("font_color", "white")
        stroke_color = video_config.get("stroke_color", "black")
        stroke_width = video_config.get("stroke_width", 2)

        # Convert colors to ASS format (BGR)
        color_map = {
            "white": "&HFFFFFF",
            "black": "&H000000",
            "yellow": "&H00FFFF",
            "red": "&H0000FF",
        }
        primary_color = color_map.get(font_color, "&HFFFFFF")
        outline_color = color_map.get(stroke_color, "&H000000")

        # ASS header
        ass_content = f"""[Script Info]
Title: Reddit Story
ScriptType: v4.00+
PlayResX: {settings.VIDEO_WIDTH}
PlayResY: {settings.VIDEO_HEIGHT}
WrapStyle: 0

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,Arial,{font_size},{primary_color},&H000000FF,{outline_color},&H00000000,1,0,0,0,100,100,0,0,1,{stroke_width},0,5,50,50,100,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""

        # Add dialogue lines
        for caption in captions:
            start_time = self._seconds_to_ass_time(caption["start"])
            end_time = self._seconds_to_ass_time(caption["end"])
            text = caption["text"].replace("\n", "\\N")

            ass_content += f"Dialogue: 0,{start_time},{end_time},Default,,0,0,0,,{text}\n"

        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(ass_content)
            return True
        except Exception as e:
            logger.error(f"Failed to write subtitle file: {e}")
            return False

    def _seconds_to_ass_time(self, seconds: float) -> str:
        """Convert seconds to ASS time format (H:MM:SS.CC)."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        centisecs = int((seconds % 1) * 100)
        return f"{hours}:{minutes:02d}:{secs:02d}.{centisecs:02d}"

    def compose_video(
        self,
        post: RedditPost,
    ) -> Tuple[bool, str]:
        """Compose a video for a Reddit post.

        Args:
            post: RedditPost with audio_path and word_timings

        Returns:
            Tuple of (success, output_path)
        """
        if not post.audio_path or not Path(post.audio_path).exists():
            logger.error(f"Audio file not found for post {post.id}")
            return False, ""

        # Get background video
        background = self._get_random_background()
        if not background:
            return False, ""

        # Get audio duration
        audio_duration = self._get_audio_duration(post.audio_path)
        if audio_duration <= 0:
            logger.error(f"Invalid audio duration for post {post.id}")
            return False, ""

        # Group words into captions
        video_config = reddit_config.get_video_config()
        words_per_caption = video_config.get("words_per_caption", 4)
        captions = self._group_words_into_captions(post.word_timings, words_per_caption)

        # Generate subtitle file
        subtitle_path = self.output_dir / f"{post.id}.ass"
        if not self._generate_subtitle_file(captions, subtitle_path):
            return False, ""

        # Output video path
        output_path = self.output_dir / f"{post.id}.mp4"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Build FFmpeg command
        try:
            # Escape subtitle path for FFmpeg filter (handle backslashes and quotes)
            # FFmpeg ass filter requires the path to be quoted
            subtitle_path_str = str(subtitle_path).replace("\\", "\\\\").replace("'", "\\'")
            subtitle_path_escaped = f"'{subtitle_path_str}'"
            
            # Split filter chain: first scale/crop, then apply subtitles
            # Use -stream_loop -1 to loop background video, then trim to audio duration
            filter_complex = (
                f"[0:v]scale={settings.VIDEO_WIDTH}:{settings.VIDEO_HEIGHT}:force_original_aspect_ratio=increase,"
                f"crop={settings.VIDEO_WIDTH}:{settings.VIDEO_HEIGHT},"
                f"setpts=PTS-STARTPTS[v_scaled];"
                f"[v_scaled]ass={subtitle_path_escaped}[v]"
            )

            cmd = [
                "ffmpeg",
                "-y",  # Overwrite output
                "-stream_loop", "-1",  # Loop background video indefinitely
                "-i", str(background),
                "-i", post.audio_path,
                "-filter_complex", filter_complex,
                "-map", "[v]",
                "-map", "1:a",
                "-c:v", "libx264",
                "-preset", "medium",
                "-crf", "23",
                "-r", str(settings.FPS),  # Set output frame rate
                "-c:a", "aac",
                "-b:a", "128k",
                "-shortest",  # Stop when shortest stream (audio) ends
                "-pix_fmt", "yuv420p",  # Ensure compatibility
                str(output_path),
            ]

            logger.info(f"Composing video for post {post.id}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

            if result.returncode != 0:
                logger.error(f"FFmpeg error: {result.stderr}")
                return False, ""

            # Clean up subtitle file
            try:
                subtitle_path.unlink()
            except Exception:
                pass

            logger.info(f"Video composed: {output_path}")
            return True, str(output_path)

        except subprocess.TimeoutExpired:
            logger.error(f"FFmpeg timed out for post {post.id}")
            return False, ""
        except Exception as e:
            logger.error(f"Video composition failed: {e}")
            return False, ""

    def compose_and_update(self, post: RedditPost) -> Optional[RedditVideo]:
        """Compose video and create RedditVideo record.

        Args:
            post: RedditPost with audio ready

        Returns:
            RedditVideo if successful, None otherwise
        """
        success, output_path = self.compose_video(post)

        if not success:
            post.status = RedditPostStatus.FAILED
            post.error = "Video composition failed"
            self.db.update_reddit_post(post)
            return None

        # Get video duration
        duration = self._get_audio_duration(post.audio_path)

        # Create RedditVideo record
        video = RedditVideo(
            id=str(uuid.uuid4())[:12],
            post_id=post.id,
            title=self._generate_title(post),
            description=self._generate_description(post),
            duration=duration,
            output_path=output_path,
            background_used=str(self._get_random_background()),
            status=RedditVideoStatus.REVIEW,
        )

        # Save video and update post
        self.db.insert_reddit_video(video)
        post.video_id = video.id
        post.status = RedditPostStatus.COMPOSED
        self.db.update_reddit_post(post)

        logger.info(f"Created video {video.id} for post {post.id}")
        return video

    def _generate_title(self, post: RedditPost) -> str:
        """Generate a video title from post."""
        # Clean up title and truncate
        title = post.title
        if len(title) > 80:
            title = title[:77] + "..."
        return f"r/{post.subreddit}: {title}"

    def _generate_description(self, post: RedditPost) -> str:
        """Generate a video description."""
        return (
            f"Story from r/{post.subreddit}\n\n"
            f"Original post by u/{post.author}\n\n"
            f"#reddit #redditstories #{post.subreddit.lower()}"
        )

    def compose_pending(self, limit: Optional[int] = None) -> Tuple[int, int]:
        """Compose videos for all posts with audio ready.

        Args:
            limit: Maximum posts to process

        Returns:
            Tuple of (success_count, fail_count)
        """
        posts = self.db.get_reddit_posts_by_status(RedditPostStatus.AUDIO_READY, limit)

        if not posts:
            logger.info("No posts ready for video composition")
            return 0, 0

        logger.info(f"Composing videos for {len(posts)} posts")

        success_count = 0
        fail_count = 0

        for post in posts:
            video = self.compose_and_update(post)
            if video:
                success_count += 1
            else:
                fail_count += 1

        logger.info(f"Composition complete: {success_count} success, {fail_count} failed")
        return success_count, fail_count

    def get_videos_for_review(self, limit: Optional[int] = None) -> List[RedditVideo]:
        """Get videos ready for review."""
        return self.db.get_reddit_videos_by_status(RedditVideoStatus.REVIEW, limit)

    def approve_video(self, video_id: str) -> bool:
        """Approve a video for upload."""
        video = self.db.get_reddit_video(video_id)
        if not video:
            logger.error(f"Video not found: {video_id}")
            return False

        video.status = RedditVideoStatus.APPROVED
        self.db.update_reddit_video(video)
        logger.info(f"Approved video {video_id}")
        return True

    def reject_video(self, video_id: str) -> bool:
        """Reject a video."""
        video = self.db.get_reddit_video(video_id)
        if not video:
            logger.error(f"Video not found: {video_id}")
            return False

        video.status = RedditVideoStatus.REJECTED
        self.db.update_reddit_video(video)
        logger.info(f"Rejected video {video_id}")
        return True
