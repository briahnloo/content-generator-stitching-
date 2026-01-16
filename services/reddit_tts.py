"""
Text-to-Speech service for Reddit story narration.
Uses Edge TTS for free, high-quality speech synthesis with word timings.
"""

import asyncio
import json
import logging
import re
from pathlib import Path
from typing import List, Optional, Tuple

import edge_tts

from config.settings import settings, reddit_config
from core.database import Database
from core.models import RedditPost, RedditPostStatus

logger = logging.getLogger(__name__)


class RedditTTSService:
    """Generates TTS audio with word-level timings for Reddit posts."""

    def __init__(self, db: Database):
        """Initialize TTS service with database connection."""
        self.db = db
        self.audio_dir = settings.REDDIT_AUDIO_DIR

    def _clean_text(self, text: str) -> str:
        """Clean text for TTS processing."""
        # Remove markdown formatting
        text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)  # Bold
        text = re.sub(r'\*(.+?)\*', r'\1', text)      # Italic
        text = re.sub(r'\_\_(.+?)\_\_', r'\1', text)  # Underline
        text = re.sub(r'\_(.+?)\_', r'\1', text)      # Italic underscore
        text = re.sub(r'\~\~(.+?)\~\~', r'\1', text)  # Strikethrough

        # Remove Reddit-specific formatting
        text = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', text)  # Links
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        text = re.sub(r'&nbsp;', ' ', text)

        # Clean up common abbreviations for better pronunciation
        text = re.sub(r'\bAITA\b', 'Am I the asshole', text, flags=re.IGNORECASE)
        text = re.sub(r'\bTIFU\b', 'Today I fucked up', text, flags=re.IGNORECASE)
        text = re.sub(r'\bTL;?DR\b', 'Too long, didnt read', text, flags=re.IGNORECASE)
        text = re.sub(r'\bOP\b', 'original poster', text, flags=re.IGNORECASE)
        text = re.sub(r'\bMIL\b', 'mother in law', text, flags=re.IGNORECASE)
        text = re.sub(r'\bFIL\b', 'father in law', text, flags=re.IGNORECASE)
        text = re.sub(r'\bSIL\b', 'sister in law', text, flags=re.IGNORECASE)
        text = re.sub(r'\bBIL\b', 'brother in law', text, flags=re.IGNORECASE)
        text = re.sub(r'\bSO\b', 'significant other', text, flags=re.IGNORECASE)
        text = re.sub(r'\bGF\b', 'girlfriend', text, flags=re.IGNORECASE)
        text = re.sub(r'\bBF\b', 'boyfriend', text, flags=re.IGNORECASE)

        # Remove edit markers
        text = re.sub(r'Edit\s*\d*\s*:', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Update\s*\d*\s*:', '', text, flags=re.IGNORECASE)

        # Clean up whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        text = text.strip()

        return text

    async def _generate_audio_async(
        self,
        text: str,
        output_path: Path,
        voice: str,
    ) -> Tuple[bool, List[dict]]:
        """Generate audio and extract word timings asynchronously.

        Returns:
            Tuple of (success, word_timings)
        """
        word_timings = []

        try:
            # Create TTS communicate object
            tts_config = reddit_config.get_tts_config()
            rate = tts_config.get("rate", "+0%")
            pitch = tts_config.get("pitch", "+0Hz")

            communicate = edge_tts.Communicate(text, voice, rate=rate, pitch=pitch)

            # Collect audio data and word timings
            audio_data = b""

            async for chunk in communicate.stream():
                if chunk["type"] == "audio":
                    audio_data += chunk["data"]
                elif chunk["type"] == "WordBoundary":
                    # Extract word timing information
                    word_timings.append({
                        "word": chunk["text"],
                        "start": chunk["offset"] / 10_000_000,  # Convert to seconds
                        "end": (chunk["offset"] + chunk["duration"]) / 10_000_000,
                    })

            # Write audio to file
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "wb") as f:
                f.write(audio_data)

            logger.info(f"Generated audio: {output_path} ({len(word_timings)} words)")
            return True, word_timings

        except Exception as e:
            logger.error(f"TTS generation failed: {e}")
            return False, []

    def generate_audio(
        self,
        post: RedditPost,
        voice: Optional[str] = None,
    ) -> Tuple[bool, str, List[dict]]:
        """Generate TTS audio for a Reddit post.

        Args:
            post: RedditPost to generate audio for
            voice: Edge TTS voice to use (default from config)

        Returns:
            Tuple of (success, audio_path, word_timings)
        """
        # Use default voice if not specified
        if voice is None:
            voice = reddit_config.get_default_voice()

        # Clean text for TTS
        full_text = post.full_text
        cleaned_text = self._clean_text(full_text)

        if not cleaned_text:
            logger.error(f"No text to convert for post {post.id}")
            return False, "", []

        # Generate output path
        output_path = self.audio_dir / f"{post.id}.mp3"

        # Run async generation
        try:
            success, word_timings = asyncio.run(
                self._generate_audio_async(cleaned_text, output_path, voice)
            )
        except Exception as e:
            logger.error(f"Failed to generate audio for post {post.id}: {e}")
            return False, "", []

        if success:
            return True, str(output_path), word_timings
        return False, "", []

    def generate_and_update(
        self,
        post: RedditPost,
        voice: Optional[str] = None,
    ) -> bool:
        """Generate TTS audio and update post in database.

        Args:
            post: RedditPost to process
            voice: Edge TTS voice to use

        Returns:
            True if successful
        """
        success, audio_path, word_timings = self.generate_audio(post, voice)

        if success:
            post.audio_path = audio_path
            post.word_timings = word_timings
            post.status = RedditPostStatus.AUDIO_READY
            self.db.update_reddit_post(post)
            logger.info(f"Updated post {post.id} with audio")
            return True
        else:
            post.status = RedditPostStatus.FAILED
            post.error = "TTS generation failed"
            self.db.update_reddit_post(post)
            return False

    def process_pending(self, limit: Optional[int] = None) -> Tuple[int, int]:
        """Process all pending posts for TTS generation.

        Args:
            limit: Maximum posts to process

        Returns:
            Tuple of (success_count, fail_count)
        """
        posts = self.db.get_reddit_posts_by_status(RedditPostStatus.DISCOVERED, limit)

        if not posts:
            logger.info("No pending posts for TTS generation")
            return 0, 0

        logger.info(f"Processing {len(posts)} posts for TTS generation")

        success_count = 0
        fail_count = 0

        for post in posts:
            if self.generate_and_update(post):
                success_count += 1
            else:
                fail_count += 1

        logger.info(f"TTS generation complete: {success_count} success, {fail_count} failed")
        return success_count, fail_count

    def get_audio_ready_posts(self, limit: Optional[int] = None) -> List[RedditPost]:
        """Get posts that have audio ready for video composition."""
        return self.db.get_reddit_posts_by_status(RedditPostStatus.AUDIO_READY, limit)

    def get_available_voices(self) -> dict:
        """Get available TTS voices from config."""
        return reddit_config.get_tts_config().get("voices", {})
