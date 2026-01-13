"""
Enhanced grouper service for clustering classified videos into coherent compilations.
Groups by subcategory for thematic consistency.
Prioritizes videos with high visual_independence scores.
"""

import logging
import uuid
from datetime import datetime
from typing import List, Optional, Dict, Tuple

from core.models import Video, Compilation, VideoStatus, CompilationStatus
from core.database import Database
from config.settings import settings, categories_config

logger = logging.getLogger(__name__)

# Default confidence threshold for auto-approval
DEFAULT_AUTO_APPROVE_THRESHOLD = 0.75

# Minimum compilation score for videos to be included
DEFAULT_MIN_COMPILATION_SCORE = 0.6

# Minimum visual independence for videos to be included in compilations
DEFAULT_MIN_VISUAL_INDEPENDENCE = 0.6


class GrouperService:
    """Groups classified videos into thematically coherent compilations."""

    def __init__(
        self,
        db: Database,
        auto_approve_threshold: Optional[float] = None,
        min_compilation_score: float = DEFAULT_MIN_COMPILATION_SCORE,
        min_visual_independence: float = DEFAULT_MIN_VISUAL_INDEPENDENCE,
    ):
        """
        Initialize grouper service.

        Args:
            db: Database instance
            auto_approve_threshold: Confidence threshold for auto-approval.
                Set to None to disable auto-approval.
            min_compilation_score: Minimum compilation score for videos.
            min_visual_independence: Minimum visual_independence for videos.
        """
        self.db = db
        self.min_clips = settings.MIN_CLIPS_PER_COMPILATION
        self.max_clips = settings.MAX_CLIPS_PER_COMPILATION
        self.min_compilation_score = min_compilation_score
        self.min_visual_independence = min_visual_independence

        # Get threshold from settings or use default
        if auto_approve_threshold is None:
            self.auto_approve_threshold = getattr(
                settings, 'AUTO_APPROVE_THRESHOLD', DEFAULT_AUTO_APPROVE_THRESHOLD
            )
        else:
            self.auto_approve_threshold = auto_approve_threshold

    def _calculate_confidence_score(self, videos: List[Video]) -> float:
        """Calculate average confidence score for a set of videos."""
        if not videos:
            return 0.0
        confidences = [v.category_confidence for v in videos if v.category_confidence > 0]
        return sum(confidences) / len(confidences) if confidences else 0.0

    def _calculate_compilation_quality(self, videos: List[Video]) -> float:
        """Calculate average compilation score for a set of videos."""
        if not videos:
            return 0.0
        scores = [v.compilation_score for v in videos if v.compilation_score > 0]
        return sum(scores) / len(scores) if scores else 0.0

    def _calculate_visual_independence(self, videos: List[Video]) -> float:
        """Calculate average visual_independence score for a set of videos."""
        if not videos:
            return 0.0
        scores = [v.visual_independence for v in videos if v.visual_independence > 0]
        return sum(scores) / len(scores) if scores else 0.0

    def _should_auto_approve(
        self,
        confidence_score: float,
        compilation_quality: float,
        visual_independence: float,
    ) -> bool:
        """
        Determine if a compilation should be auto-approved.
        Now requires high visual_independence as well.
        """
        if self.auto_approve_threshold is None:
            return False
        # All three metrics must meet threshold for auto-approval
        return (
            confidence_score >= self.auto_approve_threshold and
            compilation_quality >= self.auto_approve_threshold and
            visual_independence >= self.auto_approve_threshold
        )

    def _get_next_part_number(self, category: str, subcategory: str = "") -> int:
        """Get the next part number for a category's compilations."""
        compilations = self.db.get_compilations_by_status(CompilationStatus.UPLOADED)
        compilations += self.db.get_compilations_by_status(CompilationStatus.APPROVED)
        compilations += self.db.get_compilations_by_status(CompilationStatus.REVIEW)
        compilations += self.db.get_compilations_by_status(CompilationStatus.PENDING)

        # Count compilations with matching category (optionally subcategory in title)
        category_count = sum(1 for c in compilations if c.category == category)
        return category_count + 1

    def _filter_quality_videos(self, videos: List[Video]) -> List[Video]:
        """
        Filter videos that meet quality thresholds.
        Handles legacy videos (score=0) by including them with lower priority.
        """
        quality_videos = []
        for v in videos:
            # Legacy videos (scores=0) are included but will be sorted lower
            is_legacy = v.compilation_score == 0 and v.visual_independence == 0

            if is_legacy:
                # Include legacy videos for backwards compatibility
                quality_videos.append(v)
            elif (v.compilation_score >= self.min_compilation_score and
                  v.visual_independence >= self.min_visual_independence):
                # New videos must meet both thresholds
                quality_videos.append(v)

        return quality_videos

    def get_groupable_subcategories(self) -> Dict[str, Dict[str, int]]:
        """
        Get subcategories that have enough quality videos for a compilation.
        Returns dict of category -> subcategory -> video count.
        """
        groupable = {}

        for category in categories_config.get_category_names():
            subcategories = self.db.get_available_subcategories(
                category,
                min_videos=self.min_clips,
                status=VideoStatus.CLASSIFIED,
            )

            if subcategories:
                # Re-filter with visual_independence threshold
                filtered_subcats = {}
                for subcat, count in subcategories.items():
                    videos = self.db.get_videos_by_subcategory(
                        category, subcat,
                        status=VideoStatus.CLASSIFIED,
                        unassigned_only=True,
                    )
                    quality_videos = self._filter_quality_videos(videos)
                    if len(quality_videos) >= self.min_clips:
                        filtered_subcats[subcat] = len(quality_videos)

                if filtered_subcats:
                    groupable[category] = filtered_subcats

        return groupable

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

            # Filter by quality thresholds
            quality_videos = self._filter_quality_videos(videos)

            if len(quality_videos) >= self.min_clips:
                groupable[category] = len(quality_videos)

        return groupable

    def _select_best_videos(
        self,
        videos: List[Video],
        num_clips: int,
    ) -> List[Video]:
        """
        Select the best videos for a compilation.
        Prioritizes by likes (most to least).
        """
        # Filter by quality thresholds
        quality_videos = self._filter_quality_videos(videos)

        # Sort by likes descending
        sorted_videos = sorted(
            quality_videos,
            key=lambda v: v.likes,
            reverse=True
        )

        return sorted_videos[:num_clips]

    def create_compilation_by_subcategory(
        self,
        category: str,
        subcategory: str,
        num_clips: Optional[int] = None,
    ) -> Optional[Compilation]:
        """
        Create a compilation from videos in a specific subcategory.
        This creates thematically coherent compilations.
        """
        # Get available videos for this subcategory
        videos = self.db.get_videos_by_subcategory(
            category,
            subcategory,
            status=VideoStatus.CLASSIFIED,
            unassigned_only=True,
        )

        # Filter by quality thresholds
        quality_videos = self._filter_quality_videos(videos)

        if len(quality_videos) < self.min_clips:
            logger.warning(
                f"Not enough quality videos for {category}/{subcategory} compilation "
                f"({len(quality_videos)} < {self.min_clips})"
            )
            return None

        # Determine clip count
        if num_clips is None:
            num_clips = min(len(quality_videos), self.max_clips)
        else:
            num_clips = min(num_clips, len(quality_videos), self.max_clips)
            num_clips = max(num_clips, self.min_clips)

        # Select best videos
        selected_videos = self._select_best_videos(quality_videos, num_clips)

        # Generate compilation ID and title
        compilation_id = str(uuid.uuid4())[:12]
        part_number = self._get_next_part_number(category, subcategory)

        # Get subcategory-specific title or use category title
        subcat_config = categories_config.get_subcategory(category, subcategory)
        subcat_name = subcat_config.get("name", subcategory.title())

        title = f"{subcat_name} Compilation #{part_number}"

        # Build credits text
        authors = list(set(v.author for v in selected_videos if v.author))
        credits_text = ", ".join(f"@{a}" for a in authors)

        # Calculate scores for auto-approval
        confidence_score = self._calculate_confidence_score(selected_videos)
        compilation_quality = self._calculate_compilation_quality(selected_videos)
        visual_independence = self._calculate_visual_independence(selected_videos)
        should_auto_approve = self._should_auto_approve(
            confidence_score, compilation_quality, visual_independence
        )

        # Determine initial status
        initial_status = CompilationStatus.PENDING
        if should_auto_approve:
            logger.info(
                f"Compilation {compilation_id} auto-approved "
                f"(confidence: {confidence_score:.2f}, quality: {compilation_quality:.2f}, "
                f"visual: {visual_independence:.2f})"
            )

        # Create compilation
        compilation = Compilation(
            id=compilation_id,
            category=category,
            title=title,
            video_ids=[v.id for v in selected_videos],
            credits_text=credits_text,
            status=initial_status,
            confidence_score=confidence_score,
            auto_approved=should_auto_approve,
        )

        # Store subcategory and visual independence in description for reference
        compilation.description = (
            f"Subcategory: {subcategory} | "
            f"Avg visual independence: {visual_independence:.2f}"
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
            f"Created {category}/{subcategory} compilation {compilation_id}: "
            f"'{title}' with {num_clips} clips "
            f"(quality: {compilation_quality:.2f}, visual: {visual_independence:.2f})"
        )
        return compilation

    def create_compilation(
        self, category: str, num_clips: Optional[int] = None
    ) -> Optional[Compilation]:
        """
        Create a compilation from available videos in a category.
        Tries to create by subcategory first for better coherence.
        Falls back to mixed category if no single subcategory has enough videos.
        """
        # First, try to create a subcategory-specific compilation
        subcategories = self.db.get_available_subcategories(
            category,
            min_videos=self.min_clips,
            status=VideoStatus.CLASSIFIED,
        )

        if subcategories:
            # Check which subcategories have enough quality videos
            best_subcat = None
            best_count = 0

            for subcat, _ in subcategories.items():
                videos = self.db.get_videos_by_subcategory(
                    category, subcat,
                    status=VideoStatus.CLASSIFIED,
                    unassigned_only=True,
                )
                quality_videos = self._filter_quality_videos(videos)
                if len(quality_videos) >= self.min_clips and len(quality_videos) > best_count:
                    best_subcat = subcat
                    best_count = len(quality_videos)

            if best_subcat:
                return self.create_compilation_by_subcategory(
                    category, best_subcat, num_clips
                )

        # Fallback: Create mixed compilation from all subcategories
        videos = self.db.get_videos_by_category(
            category,
            status=VideoStatus.CLASSIFIED,
            unassigned_only=True,
        )

        # Filter by quality thresholds
        quality_videos = self._filter_quality_videos(videos)

        if len(quality_videos) < self.min_clips:
            logger.warning(
                f"Not enough quality videos for {category} compilation "
                f"({len(quality_videos)} < {self.min_clips})"
            )
            return None

        # Determine clip count
        if num_clips is None:
            num_clips = min(len(quality_videos), self.max_clips)
        else:
            num_clips = min(num_clips, len(quality_videos), self.max_clips)
            num_clips = max(num_clips, self.min_clips)

        # Select best videos
        selected_videos = self._select_best_videos(quality_videos, num_clips)

        # Generate compilation ID and title
        compilation_id = str(uuid.uuid4())[:12]
        part_number = self._get_next_part_number(category)
        title = categories_config.get_compilation_title(
            category, num_clips, part_number
        )

        # Build credits text
        authors = list(set(v.author for v in selected_videos if v.author))
        credits_text = ", ".join(f"@{a}" for a in authors)

        # Calculate scores for auto-approval
        confidence_score = self._calculate_confidence_score(selected_videos)
        compilation_quality = self._calculate_compilation_quality(selected_videos)
        visual_independence = self._calculate_visual_independence(selected_videos)
        should_auto_approve = self._should_auto_approve(
            confidence_score, compilation_quality, visual_independence
        )

        # Determine initial status
        initial_status = CompilationStatus.PENDING
        if should_auto_approve:
            logger.info(
                f"Compilation {compilation_id} auto-approved "
                f"(confidence: {confidence_score:.2f}, quality: {compilation_quality:.2f}, "
                f"visual: {visual_independence:.2f})"
            )

        # Create compilation
        compilation = Compilation(
            id=compilation_id,
            category=category,
            title=title,
            video_ids=[v.id for v in selected_videos],
            credits_text=credits_text,
            status=initial_status,
            confidence_score=confidence_score,
            auto_approved=should_auto_approve,
        )

        # Note subcategories used and visual independence
        subcats = set(v.subcategory for v in selected_videos if v.subcategory)
        compilation.description = (
            f"Mixed: {', '.join(subcats) if subcats else 'none'} | "
            f"Avg visual independence: {visual_independence:.2f}"
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
            f"Created mixed {category} compilation {compilation_id}: "
            f"'{title}' with {num_clips} clips "
            f"(visual independence: {visual_independence:.2f})"
        )
        return compilation

    def create_compilations(
        self, max_compilations: int = 3, num_clips_per: Optional[int] = None
    ) -> List[Compilation]:
        """
        Create multiple compilations from available videos.
        Prioritizes subcategory-specific compilations for coherence.
        Returns list of created Compilations.
        """
        created = []

        # First, try subcategory-specific compilations
        subcategory_groups = self.get_groupable_subcategories()

        # Flatten and sort by video count
        all_groups = []
        for category, subcats in subcategory_groups.items():
            for subcategory, count in subcats.items():
                all_groups.append((category, subcategory, count))

        # Sort by count (most videos first)
        all_groups.sort(key=lambda x: x[2], reverse=True)

        # Create subcategory compilations first
        for category, subcategory, count in all_groups:
            if len(created) >= max_compilations:
                break

            logger.info(
                f"Creating {category}/{subcategory} compilation ({count} quality videos available)"
            )
            compilation = self.create_compilation_by_subcategory(
                category, subcategory, num_clips_per
            )

            if compilation:
                created.append(compilation)

        # If we still have room, try mixed category compilations
        if len(created) < max_compilations:
            groupable = self.get_groupable_categories()
            sorted_categories = sorted(
                groupable.items(), key=lambda x: x[1], reverse=True
            )

            for category, count in sorted_categories:
                if len(created) >= max_compilations:
                    break

                # Skip if we already created a compilation for this category
                if any(c.category == category for c in created):
                    continue

                logger.info(f"Creating mixed {category} compilation ({count} quality videos available)")
                compilation = self.create_compilation(category, num_clips_per)

                if compilation:
                    created.append(compilation)

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

    # =========================================================================
    # Source Compilation Grouping (for stitching existing compilations)
    # =========================================================================

    def get_groupable_source_compilations(self) -> Dict[str, int]:
        """
        Get source compilations available for mega-compilation grouping.
        Returns dict of compilation_type -> count.
        """
        from core.models import VideoStatus

        source_comps = self.db.get_source_compilations(status=VideoStatus.DOWNLOADED)
        by_type = {}

        for video in source_comps:
            comp_type = video.compilation_type or "mixed"
            by_type[comp_type] = by_type.get(comp_type, 0) + 1

        return by_type

    def _calculate_source_score(self, v: Video) -> float:
        """Calculate ranking score for source compilation selection.

        Weights engagement, quality ratio, duration, and recency.
        """
        # Engagement (normalized to 0-1 based on typical viral ranges)
        engagement_norm = min(v.engagement_score / 50000, 1.0)

        # Quality ratio: likes/plays indicates content quality
        quality_ratio = (v.likes / v.plays) if v.plays > 0 else 0

        # Duration penalty: soft preference for target duration
        target = settings.MEGA_RANK_TARGET_DURATION
        duration_penalty = abs(v.duration - target) / target if v.duration else 0.5

        # Recency bonus: prefer fresher content
        days_old = (datetime.now() - v.created_at).days
        recency_window = settings.MEGA_RANK_RECENCY_DAYS
        recency_bonus = max(0, 1 - (days_old / recency_window))

        # Weighted combination
        score = (
            engagement_norm * settings.MEGA_RANK_ENGAGEMENT_WEIGHT
            + quality_ratio * settings.MEGA_RANK_QUALITY_WEIGHT
            - duration_penalty * settings.MEGA_RANK_DURATION_PENALTY
            + recency_bonus * settings.MEGA_RANK_RECENCY_BONUS
        )

        return score

    def create_mega_compilation(
        self,
        compilation_type: str = None,
        num_sources: Optional[int] = None,
    ) -> Optional[Compilation]:
        """
        Create a mega-compilation by grouping multiple source compilations.

        Args:
            compilation_type: Type of compilations to group (fails, comedy, etc.)
                            If None, mixes all types.
            num_sources: Number of source compilations to include.
                        Defaults to 3-5.

        Returns:
            Created Compilation or None if not enough sources.
        """
        from core.models import VideoStatus

        # Get available source compilations
        source_comps = self.db.get_source_compilations(status=VideoStatus.DOWNLOADED)

        if compilation_type:
            source_comps = [
                v for v in source_comps
                if (v.compilation_type or "mixed") == compilation_type
            ]

        if len(source_comps) < 2:
            logger.warning(
                f"Not enough source compilations for mega-compilation "
                f"({len(source_comps)} < 2)"
            )
            return None

        # Determine how many to use
        if num_sources is None:
            num_sources = min(len(source_comps), 5)
        else:
            num_sources = min(num_sources, len(source_comps))

        # Sort by weighted score (engagement, quality, duration, recency)
        sorted_sources = sorted(
            source_comps,
            key=self._calculate_source_score,
            reverse=True
        )

        selected = sorted_sources[:num_sources]

        # Calculate total duration
        total_duration = sum(v.duration or 0 for v in selected)

        # Generate compilation ID and title
        compilation_id = str(uuid.uuid4())[:12]
        type_name = (compilation_type or "mixed").title()
        part_number = self._get_next_part_number(compilation_type or "mega")

        title = f"Ultimate {type_name} Compilation #{part_number}"

        # Build credits text
        authors = list(set(v.author for v in selected if v.author))
        credits_text = ", ".join(f"@{a}" for a in authors[:10])  # Limit to 10 authors

        # Create compilation
        compilation = Compilation(
            id=compilation_id,
            category=compilation_type or "mega",
            title=title,
            video_ids=[v.id for v in selected],
            credits_text=credits_text,
            status=CompilationStatus.PENDING,
            confidence_score=0.9,  # Source compilations are pre-vetted
            auto_approved=True,  # Auto-approve mega-compilations (from pre-vetted sources)
        )

        compilation.description = (
            f"Mega-compilation from {len(selected)} sources | "
            f"Total duration: {total_duration:.0f}s | "
            f"Types: {', '.join(set(v.compilation_type or 'mixed' for v in selected))}"
        )

        # Insert compilation
        if not self.db.insert_compilation(compilation):
            logger.error(f"Failed to insert mega-compilation {compilation_id}")
            return None

        # Update source videos with compilation assignment
        for order, video in enumerate(selected):
            video.compilation_id = compilation_id
            video.clip_order = order
            video.status = VideoStatus.GROUPED
            self.db.update_video(video)

        logger.info(
            f"Created mega-compilation {compilation_id}: '{title}' "
            f"with {len(selected)} sources ({total_duration:.0f}s total)"
        )
        return compilation

    def create_mega_compilations(
        self,
        max_compilations: int = 2,
        num_sources_per: Optional[int] = None,
    ) -> List[Compilation]:
        """
        Create multiple mega-compilations from source compilations.
        Groups by type when possible.
        """
        created = []
        available = self.get_groupable_source_compilations()

        if not available:
            logger.warning("No downloaded source compilations available")
            return []

        # Sort types by availability
        sorted_types = sorted(available.items(), key=lambda x: x[1], reverse=True)

        for comp_type, count in sorted_types:
            if len(created) >= max_compilations:
                break

            if count >= 2:  # Need at least 2 to make a mega-compilation
                logger.info(f"Creating {comp_type} mega-compilation ({count} sources available)")
                compilation = self.create_mega_compilation(comp_type, num_sources_per)
                if compilation:
                    created.append(compilation)

        # If we still need more and have mixed sources available
        if len(created) < max_compilations:
            remaining = self.db.get_source_compilations(status=VideoStatus.DOWNLOADED)
            remaining = [v for v in remaining if v.compilation_id is None]

            if len(remaining) >= 2:
                logger.info(f"Creating mixed mega-compilation ({len(remaining)} sources available)")
                compilation = self.create_mega_compilation(None, num_sources_per)
                if compilation:
                    created.append(compilation)

        logger.info(f"Created {len(created)} mega-compilations")
        return created

    def get_compilation_stats(self) -> Dict:
        """Get statistics about available videos for compilation."""
        stats = {
            "by_category": {},
            "by_subcategory": {},
            "total_available": 0,
            "avg_visual_independence": {},
        }

        for category in categories_config.get_category_names():
            videos = self.db.get_videos_by_category(
                category,
                status=VideoStatus.CLASSIFIED,
                unassigned_only=True,
            )

            quality_videos = self._filter_quality_videos(videos)
            stats["by_category"][category] = len(quality_videos)
            stats["total_available"] += len(quality_videos)

            # Calculate average visual independence for category
            if quality_videos:
                visual_scores = [v.visual_independence for v in quality_videos if v.visual_independence > 0]
                if visual_scores:
                    stats["avg_visual_independence"][category] = sum(visual_scores) / len(visual_scores)

            # Break down by subcategory
            subcats = {}
            for video in quality_videos:
                subcat = video.subcategory or "uncategorized"
                subcats[subcat] = subcats.get(subcat, 0) + 1

            if subcats:
                stats["by_subcategory"][category] = subcats

        return stats
