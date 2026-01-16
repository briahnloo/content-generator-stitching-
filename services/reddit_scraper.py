"""
Reddit scraper service for viral-clips-pipeline.
Discovers text stories from configured subreddits using PRAW or public JSON API.
"""

import logging
import re
import uuid
import requests
from datetime import datetime
from typing import List, Optional, Tuple, Union

try:
    import praw
    from praw.models import Submission
    PRAW_AVAILABLE = True
except ImportError:
    PRAW_AVAILABLE = False
    Submission = None

from config.settings import settings, reddit_config
from core.database import Database
from core.models import RedditPost, RedditPostStatus

logger = logging.getLogger(__name__)


class RedditScraperService:
    """Scrapes Reddit for text stories suitable for narration."""

    def __init__(self, db: Database):
        """Initialize scraper with database connection."""
        self.db = db
        self._reddit: Optional[praw.Reddit] = None
        self._use_public_api = False

    @property
    def reddit(self) -> Optional[praw.Reddit]:
        """Lazy-load Reddit API client (falls back to public API if no credentials)."""
        if self._reddit is None and PRAW_AVAILABLE:
            if settings.REDDIT_CLIENT_ID and settings.REDDIT_CLIENT_SECRET:
                try:
                    self._reddit = praw.Reddit(
                        client_id=settings.REDDIT_CLIENT_ID,
                        client_secret=settings.REDDIT_CLIENT_SECRET,
                        user_agent=settings.REDDIT_USER_AGENT,
                    )
                    self._use_public_api = False
                except Exception as e:
                    logger.warning(f"Failed to initialize PRAW, falling back to public API: {e}")
                    self._use_public_api = True
            else:
                self._use_public_api = True
        elif not PRAW_AVAILABLE:
            self._use_public_api = True
        return self._reddit

    def _estimate_duration(self, word_count: int) -> float:
        """Estimate narration duration in seconds based on word count."""
        words_per_minute = settings.REDDIT_WORDS_PER_MINUTE
        return (word_count / words_per_minute) * 60

    def _count_words(self, text: str) -> int:
        """Count words in text."""
        return len(text.split())

    def _fetch_public_json(self, subreddit_name: str, sort: str = "hot", limit: int = 100) -> List[dict]:
        """Fetch posts from Reddit's public JSON API."""
        base_url = f"https://www.reddit.com/r/{subreddit_name}"
        
        if sort == "hot":
            url = f"{base_url}/hot.json"
        elif sort == "top":
            url = f"{base_url}/top.json?t=week"
        elif sort == "new":
            url = f"{base_url}/new.json"
        else:
            url = f"{base_url}/hot.json"
        
        url += f"?limit={min(limit * 2, 100)}"  # Reddit API max is 100
        
        try:
            headers = {"User-Agent": settings.REDDIT_USER_AGENT}
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            posts = []
            for child in data.get("data", {}).get("children", []):
                post_data = child.get("data", {})
                posts.append(post_data)
            
            return posts
        except Exception as e:
            logger.error(f"Error fetching from public API: {e}")
            return []

    def _post_data_to_submission(self, post_data: dict):
        """Convert JSON post data to a submission-like object."""
        class SubmissionLike:
            def __init__(self, data):
                self.id = data.get("id", "")
                self.title = data.get("title", "")
                self.selftext = data.get("selftext", "")
                self.score = data.get("score", 0)
                self.upvote_ratio = data.get("upvote_ratio", 0.0)
                self.num_comments = data.get("num_comments", 0)
                self.created_utc = data.get("created_utc", 0)
                self.author = data.get("author", "[deleted]")
                self.subreddit = type("Subreddit", (), {"display_name": data.get("subreddit", "")})()
        
        return SubmissionLike(post_data)

    def _is_valid_post(
        self,
        submission: Union[Submission, object],
        min_upvotes: int,
        min_words: int,
        max_words: int,
    ) -> Tuple[bool, str]:
        """Check if a post meets filtering criteria.

        Returns (is_valid, rejection_reason).
        """
        # Check upvotes
        if submission.score < min_upvotes:
            return False, f"low upvotes ({submission.score} < {min_upvotes})"

        # Check upvote ratio
        min_ratio = reddit_config.get_filtering_config().get("min_upvote_ratio", 0.7)
        if submission.upvote_ratio < min_ratio:
            return False, f"low upvote ratio ({submission.upvote_ratio:.2f} < {min_ratio})"

        # Check if post has text content
        if not submission.selftext or submission.selftext == "[removed]" or submission.selftext == "[deleted]":
            return False, "no text content"

        # Get full text (title + body)
        full_text = f"{submission.title}\n\n{submission.selftext}"
        word_count = self._count_words(full_text)

        # Check word count
        if word_count < min_words:
            return False, f"too short ({word_count} < {min_words} words)"
        if word_count > max_words:
            return False, f"too long ({word_count} > {max_words} words)"

        # Check for blocked words
        blocked_words = reddit_config.get_blocked_words()
        text_lower = full_text.lower()
        for word in blocked_words:
            if word.lower() in text_lower:
                return False, f"contains blocked word: {word}"

        # Check if already in database
        if self.db.reddit_id_exists(submission.id):
            return False, "already in database"

        return True, ""

    def _submission_to_post(self, submission: Union[Submission, object]) -> RedditPost:
        """Convert PRAW submission to RedditPost model."""
        full_text = f"{submission.title}\n\n{submission.selftext}"
        word_count = self._count_words(full_text)
        estimated_duration = self._estimate_duration(word_count)

        return RedditPost(
            id=str(uuid.uuid4())[:12],
            reddit_id=submission.id,
            subreddit=submission.subreddit.display_name,
            title=submission.title,
            body=submission.selftext,
            author=str(submission.author) if submission.author else "[deleted]",
            upvotes=submission.score,
            upvote_ratio=submission.upvote_ratio,
            num_comments=submission.num_comments,
            word_count=word_count,
            estimated_duration=estimated_duration,
            status=RedditPostStatus.DISCOVERED,
            reddit_created_at=datetime.fromtimestamp(submission.created_utc),
        )

    def discover_from_subreddit(
        self,
        subreddit_name: str,
        limit: int = 50,
        min_upvotes: Optional[int] = None,
        min_words: Optional[int] = None,
        max_words: Optional[int] = None,
        sort: str = "hot",
    ) -> Tuple[List[RedditPost], int]:
        """Discover posts from a specific subreddit.

        Args:
            subreddit_name: Name of subreddit (without r/)
            limit: Maximum posts to fetch
            min_upvotes: Minimum upvotes required
            min_words: Minimum word count
            max_words: Maximum word count
            sort: Sort method (hot, top, new)

        Returns:
            Tuple of (discovered posts, skipped count)
        """
        # Get config for this subreddit if available
        config_key = subreddit_name.lower().replace("_", "")
        subreddit_config = reddit_config.get_subreddit(config_key)

        # Use provided values or fall back to config or defaults
        min_upvotes = min_upvotes or subreddit_config.get("min_upvotes", settings.REDDIT_MIN_UPVOTES)
        min_words = min_words or subreddit_config.get("min_words", settings.REDDIT_MIN_WORDS)
        max_words = max_words or subreddit_config.get("max_words", settings.REDDIT_MAX_WORDS)
        sort = sort or subreddit_config.get("sort", "hot")

        logger.info(f"Discovering posts from r/{subreddit_name} (limit={limit}, sort={sort})")

        discovered = []
        skipped = 0

        try:
            # Use public API if PRAW not available or no credentials
            if self._use_public_api or not self.reddit:
                logger.info(f"Using Reddit public JSON API (no credentials required)")
                post_data_list = self._fetch_public_json(subreddit_name, sort, limit * 2)
                submissions = [self._post_data_to_submission(p) for p in post_data_list]
            else:
                # Use PRAW
                subreddit = self.reddit.subreddit(subreddit_name)
                if sort == "hot":
                    submissions = subreddit.hot(limit=limit * 2)  # Fetch extra to account for filtering
                elif sort == "top":
                    submissions = subreddit.top(limit=limit * 2, time_filter="week")
                elif sort == "new":
                    submissions = subreddit.new(limit=limit * 2)
                else:
                    submissions = subreddit.hot(limit=limit * 2)

            for submission in submissions:
                if len(discovered) >= limit:
                    break

                is_valid, reason = self._is_valid_post(
                    submission, min_upvotes, min_words, max_words
                )

                if not is_valid:
                    skipped += 1
                    logger.debug(f"Skipped: {submission.id} - {reason}")
                    continue

                post = self._submission_to_post(submission)

                # Save to database
                if self.db.insert_reddit_post(post):
                    discovered.append(post)
                    logger.info(
                        f"Discovered: r/{post.subreddit} - {post.title[:50]}... "
                        f"({post.word_count} words, ~{post.estimated_duration:.0f}s)"
                    )
                else:
                    skipped += 1

        except Exception as e:
            logger.error(f"Error discovering from r/{subreddit_name}: {e}")
            raise

        logger.info(f"Discovered {len(discovered)} posts, skipped {skipped}")
        return discovered, skipped

    def discover_from_config(
        self,
        limit_per_subreddit: int = 10,
    ) -> Tuple[List[RedditPost], int]:
        """Discover posts from all configured subreddits.

        Args:
            limit_per_subreddit: Maximum posts per subreddit

        Returns:
            Tuple of (all discovered posts, total skipped count)
        """
        all_discovered = []
        total_skipped = 0

        subreddits = reddit_config.get_subreddits()

        for key, config in subreddits.items():
            subreddit_name = config.get("subreddit", key)
            try:
                discovered, skipped = self.discover_from_subreddit(
                    subreddit_name=subreddit_name,
                    limit=limit_per_subreddit,
                    min_upvotes=config.get("min_upvotes"),
                    min_words=config.get("min_words"),
                    max_words=config.get("max_words"),
                    sort=config.get("sort", "hot"),
                )
                all_discovered.extend(discovered)
                total_skipped += skipped
            except Exception as e:
                logger.error(f"Failed to discover from r/{subreddit_name}: {e}")
                continue

        logger.info(
            f"Total discovery: {len(all_discovered)} posts from "
            f"{len(subreddits)} subreddits, {total_skipped} skipped"
        )
        return all_discovered, total_skipped

    def get_pending_posts(self, limit: Optional[int] = None) -> List[RedditPost]:
        """Get posts that are ready for TTS generation."""
        return self.db.get_reddit_posts_by_status(RedditPostStatus.DISCOVERED, limit)

    def get_stats(self) -> dict:
        """Get discovery statistics."""
        return self.db.get_reddit_stats()
