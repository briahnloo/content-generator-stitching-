"""
Captioner service for generating text overlays.
Uses simple countdown format: "Top Viral Moments: 5, 4, 3, 2, 1"
"""

import logging
from typing import List

from core.models import Compilation, Video
from core.database import Database
from config.settings import categories_config

logger = logging.getLogger(__name__)


class CaptionerService:
    """Generates simple countdown captions for compilations."""

    def __init__(self, db: Database):
        """Initialize captioner service."""
        self.db = db

    def generate_captions(self, compilation: Compilation) -> dict:
        """
        Generate simple countdown captions for a compilation.
        Returns dict with hook, clip_captions, transitions, end_card.

        Format: First clip shows "Top Viral Moments: 5, 4, 3, 2, 1"
        No per-clip captions, no end card.
        """
        videos = self.db.get_videos_for_compilation(compilation.id)
        num_clips = len(videos)

        if not videos:
            logger.warning(f"No videos found for compilation {compilation.id}")
            return {
                "hook": "",
                "clip_captions": [],
                "transitions": [],
                "end_card": "",
            }

        # Generate countdown format: "category moments:\n5:\n4:\n3:\n2:\n1:"
        category_config = categories_config.get_category(compilation.category)
        category_name = category_config.get("name", compilation.category.title()).lower()
        
        countdown_lines = [f"{category_name} moments:"] + [f"{i}:" for i in range(num_clips, 0, -1)]
        first_clip_caption = "\n".join(countdown_lines)

        # Only first clip gets the caption, rest are empty
        clip_captions = [first_clip_caption] + [""] * (num_clips - 1)

        logger.info(f"Generated countdown caption for compilation {compilation.id}: {first_clip_caption}")

        return {
            "hook": "",  # No hook (removed black screen)
            "clip_captions": clip_captions,
            "transitions": [""] * num_clips,
            "end_card": "",  # No end card
        }

    def generate_and_update(self, compilation: Compilation) -> bool:
        """
        Generate captions and update the compilation in the database.
        Also updates individual video captions.
        Returns True on success.
        """
        try:
            captions = self.generate_captions(compilation)

            # Update compilation
            compilation.hook = captions["hook"]
            compilation.clip_captions = captions["clip_captions"]
            compilation.transitions = captions["transitions"]
            compilation.end_card = captions["end_card"]

            self.db.update_compilation(compilation)

            # Update individual video captions
            videos = self.db.get_videos_for_compilation(compilation.id)
            for video, caption in zip(videos, captions["clip_captions"]):
                video.caption = caption
                self.db.update_video(video)

            logger.info(f"Updated captions for compilation {compilation.id}")
            return True

        except Exception as e:
            compilation.error = str(e)
            self.db.update_compilation(compilation)
            logger.error(f"Failed to generate captions for {compilation.id}: {e}")
            return False

    def generate_description(self, compilation: Compilation) -> str:
        """Generate a YouTube description for the compilation."""
        category_config = categories_config.get_category(compilation.category)
        category_name = category_config.get("name", compilation.category.title())

        description_parts = [
            compilation.title,
            "",
            f"A compilation of the best {category_name.lower()} videos!",
            "",
        ]

        if compilation.credits_text:
            description_parts.extend([
                "Featured creators:",
                compilation.credits_text,
                "",
            ])

        description_parts.extend([
            "---",
            "Like and subscribe for more compilations!",
            "",
            "#shorts #viral #compilation",
        ])

        # Add category-specific hashtags
        hashtags = category_config.get("hashtags", [])
        if hashtags:
            description_parts.append(" ".join(hashtags[:5]))

        return "\n".join(description_parts)
