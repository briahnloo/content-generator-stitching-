"""
Discovery service for fetching trending TikTok videos via Apify.
Uses the clockworks/tiktok-scraper actor.
Supports both individual clip discovery and existing compilation discovery.
"""

import hashlib
import logging
import random
import re
from datetime import date
from typing import List, Optional, Tuple

from apify_client import ApifyClient

from core.models import Video, VideoStatus
from core.database import Database
from config.settings import settings, categories_config

logger = logging.getLogger(__name__)


class DiscoveryService:
    """Fetches trending TikTok video metadata via Apify."""

    ACTOR_ID = "clockworks/tiktok-scraper"

    def __init__(self, db: Database):
        """Initialize discovery service."""
        self.db = db
        self._client: Optional[ApifyClient] = None

    @property
    def client(self) -> ApifyClient:
        """Lazy-load Apify client."""
        if self._client is None:
            if not settings.APIFY_API_TOKEN:
                raise ValueError("APIFY_API_TOKEN is required for discovery")
            self._client = ApifyClient(settings.APIFY_API_TOKEN)
        return self._client

    def _generate_video_id(self, tiktok_id: str, url: str) -> str:
        """Generate a unique internal ID for a video."""
        content = f"{tiktok_id}:{url}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def _parse_video_data(self, item: dict) -> Optional[Video]:
        """Parse Apify response item into Video model."""
        try:
            tiktok_id = item.get("id", "")
            if not tiktok_id:
                return None

            # Skip if already exists
            if self.db.tiktok_id_exists(tiktok_id):
                return None

            url = item.get("webVideoUrl", "") or item.get("videoUrl", "")
            if not url:
                return None

            # Extract metadata
            author_meta = item.get("authorMeta", {})
            author = author_meta.get("name", "") or author_meta.get("nickName", "")

            stats = item.get("stats", {}) or item.get("videoMeta", {})
            plays = stats.get("playCount", 0) or stats.get("plays", 0)
            likes = stats.get("diggCount", 0) or stats.get("likes", 0)
            shares = stats.get("shareCount", 0) or stats.get("shares", 0)

            # Extract hashtags
            hashtags = []
            for tag in item.get("hashtags", []):
                if isinstance(tag, dict):
                    hashtags.append(tag.get("name", ""))
                elif isinstance(tag, str):
                    hashtags.append(tag)
            hashtags = [f"#{h}" if not h.startswith("#") else h for h in hashtags if h]

            video_id = self._generate_video_id(tiktok_id, url)

            return Video(
                id=video_id,
                tiktok_id=tiktok_id,
                url=url,
                description=item.get("text", "") or item.get("description", ""),
                author=author,
                hashtags=hashtags,
                plays=plays,
                likes=likes,
                shares=shares,
                status=VideoStatus.DISCOVERED,
            )
        except Exception as e:
            logger.warning(f"Failed to parse video data: {e}")
            return None

    def _run_actor(self, run_input: dict) -> List[dict]:
        """Execute Apify actor and return results."""
        logger.info(f"Running Apify actor with input: {run_input}")

        try:
            run = self.client.actor(self.ACTOR_ID).call(run_input=run_input)
            items = list(self.client.dataset(run["defaultDatasetId"]).iterate_items())
            logger.info(f"Apify returned {len(items)} items")
            return items
        except Exception as e:
            logger.error(f"Apify actor failed: {e}")
            raise

    def discover_by_hashtag(
        self, hashtag: str, limit: int = 30
    ) -> Tuple[List[Video], int]:
        """
        Fetch videos for a specific hashtag.
        Returns (new_videos, skipped_duplicates).
        """
        # Clean hashtag
        hashtag = hashtag.lstrip("#").strip()

        run_input = {
            "hashtags": [hashtag],
            "resultsPerPage": min(limit, 100),
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False,
        }

        items = self._run_actor(run_input)

        videos = []
        skipped = 0

        for item in items:
            video = self._parse_video_data(item)
            if video:
                if self.db.insert_video(video):
                    videos.append(video)
                else:
                    skipped += 1
            else:
                skipped += 1

        logger.info(f"Discovered {len(videos)} new videos, skipped {skipped}")
        return videos, skipped

    def discover_trending(self, limit: int = 30) -> Tuple[List[Video], int]:
        """
        Fetch trending videos.
        Returns (new_videos, skipped_duplicates).
        """
        run_input = {
            "resultsPerPage": min(limit, 100),
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False,
        }

        items = self._run_actor(run_input)

        videos = []
        skipped = 0

        for item in items:
            video = self._parse_video_data(item)
            if video:
                if self.db.insert_video(video):
                    videos.append(video)
                else:
                    skipped += 1
            else:
                skipped += 1

        logger.info(f"Discovered {len(videos)} new trending videos, skipped {skipped}")
        return videos, skipped

    def discover_from_hashtags(
        self, hashtags: List[str], limit_per: int = 20
    ) -> Tuple[List[Video], int]:
        """
        Fetch videos from multiple hashtags.
        Returns (new_videos, total_skipped).
        """
        all_videos = []
        total_skipped = 0

        for hashtag in hashtags:
            logger.info(f"Discovering videos for #{hashtag}")
            videos, skipped = self.discover_by_hashtag(hashtag, limit_per)
            all_videos.extend(videos)
            total_skipped += skipped

        logger.info(
            f"Total discovered: {len(all_videos)} new videos, {total_skipped} skipped"
        )
        return all_videos, total_skipped

    def discover_default(self, limit: int = 50) -> Tuple[List[Video], int]:
        """
        Discover videos using default hashtags from config.
        Returns (new_videos, total_skipped).
        """
        hashtags = settings.DISCOVERY_HASHTAGS
        limit_per = max(1, limit // len(hashtags)) if hashtags else limit
        return self.discover_from_hashtags(hashtags, limit_per)

    # =========================================================================
    # Compilation Discovery - Finding existing compilations to stitch
    # =========================================================================

    def _is_talking_head_content(self, video: Video) -> bool:
        """
        Detect single-speaker/talking head content via metadata.
        These videos don't work well in compilations.
        """
        text = f"{video.description} {' '.join(video.hashtags)}".lower()
        author_lower = video.author.lower()

        # Get rejection config
        config = categories_config.get_talking_head_rejection()

        # Check author patterns
        for pattern in config.get("author_patterns", []):
            if pattern.lower() in author_lower:
                logger.debug(f"Rejected {video.id}: talking head author pattern '{pattern}'")
                return True

        # Check description patterns
        for pattern in config.get("description_patterns", []):
            if pattern.lower() in text:
                logger.debug(f"Rejected {video.id}: talking head description pattern '{pattern}'")
                return True

        # Check hashtags
        video_tags_lower = [t.lower() for t in video.hashtags]
        for tag in config.get("hashtags", []):
            if tag.lower() in video_tags_lower or tag.lower().lstrip("#") in [t.lstrip("#") for t in video_tags_lower]:
                logger.debug(f"Rejected {video.id}: talking head hashtag '{tag}'")
                return True

        return False

    def _is_likely_compilation(self, video: Video) -> Tuple[bool, str]:
        """
        Check if a video is likely an existing compilation based on metadata.
        Returns (is_compilation, detected_type).
        """
        text = f"{video.description} {' '.join(video.hashtags)}".lower()
        author_lower = video.author.lower()

        # Get patterns from config
        description_patterns = categories_config.get_compilation_description_patterns()
        author_patterns = categories_config.get_compilation_author_patterns()

        # Check description patterns
        for pattern in description_patterns:
            try:
                if re.search(pattern, text, re.IGNORECASE):
                    # Determine compilation type (check specific types before generic)
                    if any(kw in text for kw in ["animal", "pet", "dog", "cat", "puppy", "kitten", "derp", "pets"]):
                        return True, "animals"
                    elif any(kw in text for kw in ["baby", "babies", "kid", "kids", "toddler", "child", "infant"]):
                        return True, "babies"
                    elif any(kw in text for kw in ["fail", "fails", "wcgw", "gone wrong", "instant regret", "karma"]):
                        return True, "fails"
                    elif any(kw in text for kw in ["funny", "comedy", "laugh", "hilarious"]):
                        return True, "comedy"
                    elif any(kw in text for kw in ["satisfying", "asmr", "relaxing"]):
                        return True, "satisfying"
                    else:
                        return True, "mixed"
            except re.error:
                continue

        # Check author patterns (compilation-focused accounts)
        for pattern in author_patterns:
            if pattern in author_lower:
                return True, "mixed"

        # Check for countdown indicators in description
        countdown_patterns = [
            r'\b[5-9]\s*[.\-)\]]\s*[4-8]\s*[.\-)\]]\s*[3-7]',  # "5. 4. 3." or "5-4-3"
            r'#\d+\s*[-–]\s*#\d+',  # "#5 - #1"
            r'top\s*\d+',           # "top 10"
        ]
        for pattern in countdown_patterns:
            if re.search(pattern, text):
                return True, "mixed"

        return False, ""

    def _parse_compilation_video(self, item: dict) -> Optional[Video]:
        """
        Parse Apify response item into Video model, with compilation-specific handling.
        Includes duration extraction and compilation marking.
        """
        try:
            tiktok_id = item.get("id", "")
            if not tiktok_id:
                return None

            # Skip if already exists
            if self.db.tiktok_id_exists(tiktok_id):
                return None

            url = item.get("webVideoUrl", "") or item.get("videoUrl", "")
            if not url:
                return None

            # Extract duration - critical for compilation detection
            video_meta = item.get("videoMeta", {})
            duration = video_meta.get("duration", 0)

            # Get duration limits from config
            duration_limits = categories_config.get_compilation_duration_limits()

            # Skip videos that are too short (not compilations) or too long
            if duration < duration_limits["min"]:
                logger.debug(f"Skipping video {tiktok_id}: too short ({duration}s < {duration_limits['min']}s)")
                return None
            if duration > duration_limits["max"]:
                logger.debug(f"Skipping video {tiktok_id}: too long ({duration}s > {duration_limits['max']}s)")
                return None

            # Extract metadata
            author_meta = item.get("authorMeta", {})
            author = author_meta.get("name", "") or author_meta.get("nickName", "")

            stats = item.get("stats", {}) or item.get("videoMeta", {})
            plays = stats.get("playCount", 0) or stats.get("plays", 0)
            likes = stats.get("diggCount", 0) or stats.get("likes", 0)
            shares = stats.get("shareCount", 0) or stats.get("shares", 0)

            # Extract hashtags
            hashtags = []
            for tag in item.get("hashtags", []):
                if isinstance(tag, dict):
                    hashtags.append(tag.get("name", ""))
                elif isinstance(tag, str):
                    hashtags.append(tag)
            hashtags = [f"#{h}" if not h.startswith("#") else h for h in hashtags if h]

            video_id = self._generate_video_id(tiktok_id, url)

            video = Video(
                id=video_id,
                tiktok_id=tiktok_id,
                url=url,
                description=item.get("text", "") or item.get("description", ""),
                author=author,
                hashtags=hashtags,
                plays=plays,
                likes=likes,
                shares=shares,
                duration=duration,
                status=VideoStatus.DISCOVERED,
            )

            # Filter out talking head / single-speaker content
            if self._is_talking_head_content(video):
                logger.debug(f"Skipping talking head video: {video.id}")
                return None

            # Check if this is likely a compilation
            is_compilation, comp_type = self._is_likely_compilation(video)
            if is_compilation:
                video.is_source_compilation = True
                video.compilation_type = comp_type
                # Estimate clip count based on duration (rough: 10-15s per clip)
                video.source_clip_count = max(3, int(duration / 12))
                logger.debug(
                    f"Detected compilation: {video.id} ({comp_type}, ~{video.source_clip_count} clips, {duration}s)"
                )
                return video
            else:
                # Not a compilation, skip it
                logger.debug(f"Skipping non-compilation video: {video.id}")
                return None

        except Exception as e:
            logger.warning(f"Failed to parse compilation video data: {e}")
            return None

    def discover_compilations(
        self, limit: int = 30, hashtags: Optional[List[str]] = None, max_hashtags: int = 8
    ) -> Tuple[List[Video], int]:
        """
        Discover existing compilation videos on TikTok.
        Uses batched API calls for efficiency (1 call instead of N calls).

        Args:
            limit: Maximum number of compilations to discover
            hashtags: Optional list of hashtags to search. If None, uses config defaults.
            max_hashtags: Maximum hashtags to search per call (rotates daily for variety)

        Returns:
            (new_compilations, skipped_count)
        """
        # Get compilation hashtags from config if not provided
        if hashtags is None:
            hashtags = categories_config.get_compilation_hashtags()

        if not hashtags:
            logger.warning("No compilation hashtags configured")
            return [], 0

        # Clean hashtags
        clean_hashtags = [h.lstrip("#").strip() for h in hashtags]

        # OPTIMIZATION: Rotate through hashtags daily for variety
        # Use date as seed so same hashtags are used within a day (consistent results)
        # but different hashtags on different days (variety)
        if len(clean_hashtags) > max_hashtags:
            seed = date.today().toordinal()
            rng = random.Random(seed)
            clean_hashtags = rng.sample(clean_hashtags, max_hashtags)
            logger.info(f"Rotating hashtags: selected {max_hashtags} for today")

        # OPTIMIZATION: Batch all hashtags into a single API call
        logger.info(f"Discovering compilations from {len(clean_hashtags)} hashtags (batched): {clean_hashtags}")

        run_input = {
            "hashtags": clean_hashtags,
            "resultsPerPage": min(limit * 3, 100),  # Fetch extra to account for filtering
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False,
        }

        try:
            items = self._run_actor(run_input)
        except Exception as e:
            logger.error(f"Failed to discover compilations: {e}")
            return [], 0

        all_compilations = []
        total_skipped = 0

        for item in items:
            if len(all_compilations) >= limit:
                break

            video = self._parse_compilation_video(item)
            if video:
                if self.db.insert_video(video):
                    all_compilations.append(video)
                    logger.info(
                        f"Found compilation: {video.id} - {video.compilation_type} "
                        f"(~{video.source_clip_count} clips, {video.duration:.0f}s)"
                    )
                else:
                    total_skipped += 1
            else:
                total_skipped += 1

        logger.info(
            f"Discovered {len(all_compilations)} compilations, skipped {total_skipped}"
        )
        return all_compilations, total_skipped

    def discover_compilations_by_type(
        self, compilation_type: str, limit: int = 20
    ) -> Tuple[List[Video], int]:
        """
        Discover compilations of a specific type (fails, comedy, satisfying).

        Args:
            compilation_type: Type of compilation to find ("fails", "comedy", "satisfying")
            limit: Maximum number to discover

        Returns:
            (new_compilations, skipped_count)
        """
        # Type-specific hashtags (trimmed for efficiency)
        type_hashtags = {
            "fails": ["failscompilation", "epicfails", "failarmy", "instantkarma"],
            "comedy": ["funnycompilation", "trynottolaugh", "funniestmoments"],
            "animals": ["funnyanimals", "funnypets", "animalsoftiktok"],
            "babies": ["funnybaby", "funnykids", "babiesoftiktok"],
            "satisfying": ["satisfyingcompilation", "oddlysatisfying"],
        }

        hashtags = type_hashtags.get(compilation_type, [])
        if not hashtags:
            logger.warning(f"Unknown compilation type: {compilation_type}")
            return [], 0

        # Pass max_hashtags equal to list length to use all type-specific hashtags
        return self.discover_compilations(limit=limit, hashtags=hashtags, max_hashtags=len(hashtags))
