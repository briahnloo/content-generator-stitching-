"""
Grouper service for clustering classified videos into compilations.
"""

import logging
import uuid
from typing import List, Optional, Dict

from core.models import Video, Compilation, VideoStatus, CompilationStatus
from core.database import Database
from config.settings import settings, categories_config

logger = logging.getLogger(__name__)


class GrouperService:
    """Groups classified videos into compilations by category."""

    def __init__(self, db: Database):
        """Initialize grouper service."""
        self.db = db
        self.min_clips = settings.MIN_CLIPS_PER_COMPILATION
        self.max_clips = settings.MAX_CLIPS_PER_COMPILATION

    def _get_next_part_number(self, category: str) -> int:
        """Get the next part number for a category's compilations."""
        compilations = self.db.get_compilations_by_status(CompilationStatus.UPLOADED)
        compilations += self.db.get_compilations_by_status(CompilationStatus.APPROVED)
        compilations += self.db.get_compilations_by_status(CompilationStatus.REVIEW)
        compilations += self.db.get_compilations_by_status(CompilationStatus.PENDING)

        category_count = sum(1 for c in compilations if c.category == category)
        return category_count + 1

    def get_groupable_categories(self) -> Dict[str, int]:
        """
        Get categories that have enough videos for a compilation.
        Returns dict of category -> available video count.
        """
        groupable = {}

        for category in categories_config.get_category_names():
            videos = self.db.get_videos_by_category(
                category,
                status=VideoStatus.CLASSIFIED,
                unassigned_only=True,
            )

            if len(videos) >= self.min_clips:
                groupable[category] = len(videos)

        return groupable

    def create_compilation(
        self, category: str, num_clips: Optional[int] = None
    ) -> Optional[Compilation]:
        """
        Create a compilation from available videos in a category.
        Returns the created Compilation or None if not enough videos.
        """
        # Get available videos
        videos = self.db.get_videos_by_category(
            category,
            status=VideoStatus.CLASSIFIED,
            unassigned_only=True,
        )

        if len(videos) < self.min_clips:
            logger.warning(
                f"Not enough videos for {category} compilation "
                f"({len(videos)} < {self.min_clips})"
            )
            return None

        # Determine clip count
        if num_clips is None:
            num_clips = min(len(videos), self.max_clips)
        else:
            num_clips = min(num_clips, len(videos), self.max_clips)
            num_clips = max(num_clips, self.min_clips)

        # Select top videos by engagement (already sorted by DB query)
        selected_videos = videos[:num_clips]

        # Generate compilation ID and title
        compilation_id = str(uuid.uuid4())[:12]
        part_number = self._get_next_part_number(category)
        title = categories_config.get_compilation_title(
            category, num_clips, part_number
        )

        # Build credits text
        authors = list(set(v.author for v in selected_videos if v.author))
        credits_text = ", ".join(f"@{a}" for a in authors)

        # Create compilation
        compilation = Compilation(
            id=compilation_id,
            category=category,
            title=title,
            video_ids=[v.id for v in selected_videos],
            credits_text=credits_text,
            status=CompilationStatus.PENDING,
        )

        # Insert compilation
        if not self.db.insert_compilation(compilation):
            logger.error(f"Failed to insert compilation {compilation_id}")
            return None

        # Update videos with compilation assignment
        for order, video in enumerate(selected_videos):
            video.compilation_id = compilation_id
            video.clip_order = order
            video.status = VideoStatus.GROUPED
            self.db.update_video(video)

        logger.info(
            f"Created compilation {compilation_id}: '{title}' with {num_clips} clips"
        )
        return compilation

    def create_compilations(
        self, max_compilations: int = 3, num_clips_per: Optional[int] = None
    ) -> List[Compilation]:
        """
        Create multiple compilations from available videos.
        Prioritizes categories with the most available videos.
        Returns list of created Compilations.
        """
        created = []
        groupable = self.get_groupable_categories()

        if not groupable:
            logger.info("No categories have enough videos for compilations")
            return created

        # Sort by available count (descending)
        sorted_categories = sorted(
            groupable.items(), key=lambda x: x[1], reverse=True
        )

        for category, count in sorted_categories:
            if len(created) >= max_compilations:
                break

            logger.info(f"Creating compilation for {category} ({count} available)")
            compilation = self.create_compilation(category, num_clips_per)

            if compilation:
                created.append(compilation)
                # Refresh groupable counts
                groupable = self.get_groupable_categories()

        logger.info(f"Created {len(created)} compilations")
        return created

    def ungroup_compilation(self, compilation_id: str) -> bool:
        """
        Remove a compilation and return its videos to CLASSIFIED status.
        Returns True on success.
        """
        compilation = self.db.get_compilation(compilation_id)
        if not compilation:
            logger.warning(f"Compilation {compilation_id} not found")
            return False

        # Only allow ungrouping pending compilations
        if compilation.status not in (
            CompilationStatus.PENDING,
            CompilationStatus.REJECTED,
        ):
            logger.warning(
                f"Cannot ungroup compilation with status {compilation.status}"
            )
            return False

        # Delete compilation (database layer handles video reassignment)
        self.db.delete_compilation(compilation_id)
        logger.info(f"Ungrouped compilation {compilation_id}")
        return True
