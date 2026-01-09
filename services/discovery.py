"""
Discovery service for fetching trending TikTok videos via Apify.
Uses the clockworks/tiktok-scraper actor.
"""

import hashlib
import logging
from typing import List, Optional, Tuple

from apify_client import ApifyClient

from core.models import Video, VideoStatus
from core.database import Database
from config.settings import settings

logger = logging.getLogger(__name__)


class DiscoveryService:
    """Fetches trending TikTok video metadata via Apify."""

    ACTOR_ID = "clockworks/tiktok-scraper"

    # Blacklist keywords for content we don't want (dance, thirst traps, ads)
    BLACKLIST_KEYWORDS = [
        # Dance content
        "dance", "dancing", "dancer", "choreography", "choreo",
        "twerk", "twerking", "shuffle", "shuffling",
        "tutorial dance", "dance challenge", "dancechallenge",
        # Thirst trap / model content
        "thirst", "thirsttrap", "hot girl", "hotgirl", "baddie",
        "model", "modeling", "photoshoot", "bikini", "swimsuit",
        "outfit check", "outfitcheck", "ootd", "grwm", "getreadywithme",
        "fit check", "fitcheck", "glow up", "glowup",
        # Advertisement / promotional
        "ad", "sponsored", "sponsor", "promo", "promotion",
        "discount", "sale", "buy now", "link in bio", "linkinbio",
        "shop now", "shopnow", "use code", "usecode", "coupon",
        "affiliate", "partnership", "collab", "brand deal",
        "review", "unboxing", "haul",
        # Music / lip sync focused
        "lip sync", "lipsync", "duet", "singing", "singer",
        "cover song", "coversong",
        # Lifestyle / beauty focused
        "makeup", "skincare", "beauty", "fashion", "style",
        "aesthetic", "vlog", "dayinmylife", "routine",
    ]

    # Blacklist hashtags (without #)
    BLACKLIST_HASHTAGS = [
        "dance", "dancing", "dancer", "choreography",
        "twerk", "shuffle", "dancechallenge",
        "thirsttrap", "hotgirl", "baddie", "model",
        "ootd", "grwm", "fitcheck", "outfitcheck",
        "ad", "sponsored", "promo", "linkinbio",
        "makeup", "skincare", "beauty", "fashion",
        "aesthetic", "fyp", "foryou",  # fyp/foryou are too generic
        "lipsync", "duet", "singing",
    ]

    def __init__(self, db: Database):
        """Initialize discovery service."""
        self.db = db
        self._client: Optional[ApifyClient] = None

    @property
    def client(self) -> ApifyClient:
        """Lazy-load Apify client."""
        if self._client is None:
            token = settings.APIFY_API_TOKEN
            if not token:
                raise ValueError(
                    "APIFY_API_TOKEN is required for discovery. "
                    "Please set it in your .env file in the project root."
                )
            # Strip whitespace in case it was added accidentally
            token = token.strip()
            if not token:
                raise ValueError(
                    "APIFY_API_TOKEN appears to be empty or whitespace. "
                    "Please check your .env file."
                )
            logger.debug(f"Initializing Apify client with token (length: {len(token)})")
            self._client = ApifyClient(token)
        return self._client

    def _is_blacklisted(self, description: str, hashtags: List[str]) -> bool:
        """
        Check if video content matches blacklist criteria.
        Returns True if the video should be filtered out.
        """
        # Normalize description to lowercase
        desc_lower = description.lower() if description else ""

        # Check description for blacklisted keywords
        for keyword in self.BLACKLIST_KEYWORDS:
            if keyword.lower() in desc_lower:
                logger.debug(f"Blacklisted by keyword: '{keyword}'")
                return True

        # Check hashtags against blacklist
        for tag in hashtags:
            tag_clean = tag.lstrip("#").lower()
            if tag_clean in self.BLACKLIST_HASHTAGS:
                logger.debug(f"Blacklisted by hashtag: '#{tag_clean}'")
                return True

        return False

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

            # Get description for blacklist check
            description = item.get("text", "") or item.get("description", "")

            # Filter out blacklisted content (dance, ads, thirst traps)
            if self._is_blacklisted(description, hashtags):
                logger.debug(f"Filtered out blacklisted video: {tiktok_id}")
                return None

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
            # Verify token is set before making the call
            token = settings.APIFY_API_TOKEN
            if not token or not token.strip():
                raise ValueError(
                    "APIFY_API_TOKEN is not set or is empty. "
                    "Please check your .env file in the project root."
                )
            
            # Get the actor client
            actor_client = self.client.actor(self.ACTOR_ID)
            
            # Try to call the actor
            run = actor_client.call(run_input=run_input)
            items = list(self.client.dataset(run["defaultDatasetId"]).iterate_items())
            logger.info(f"Apify returned {len(items)} items")
            return items
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Apify actor failed: {error_msg}")
            
            # Provide helpful error messages
            if "not found" in error_msg.lower() or "not valid" in error_msg.lower():
                logger.error(
                    "Authentication error detected. Please verify:\n"
                    "1. Your APIFY_API_TOKEN in .env is correct\n"
                    "2. The token hasn't expired or been revoked\n"
                    "3. The .env file is in the project root directory\n"
                    "4. There are no quotes or extra spaces around the token\n"
                    "5. Run 'python test_apify_token.py' to verify token validity"
                )
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
        Fetch trending videos focused on fails and comedy content.
        The Apify actor requires at least one input type (hashtags, postURLs, etc.)
        so we use fails/comedy hashtags to get viral funny content.
        Returns (new_videos, skipped_duplicates).
        """
        # Use fails/comedy hashtags to get funny viral content
        # Avoiding generic tags like fyp/foryou which return too much dance content
        comedy_fails_hashtags = [
            "fail", "fails", "epicfail",
            "funny", "funnyvideos", "comedy",
            "trynottolaugh", "memes", "humor",
            "instant_regret", "whatcouldgowrong",
        ]
        run_input = {
            "hashtags": comedy_fails_hashtags,
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
        Discover trending videos (most viral/liked).
        Uses trending feed instead of hashtags for higher quality videos.
        Returns (new_videos, total_skipped).
        """
        logger.info(f"Discovering trending videos (limit={limit})")
        return self.discover_trending(limit)
