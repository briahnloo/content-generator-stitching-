"""
Classifier service for categorizing videos using GPT-4o-mini.
"""

import json
import logging
from typing import Tuple, Optional, List

from openai import OpenAI

from core.models import Video, VideoStatus
from core.database import Database
from config.settings import settings, categories_config

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a strict content classifier for short-form viral video compilations. Your job is to identify ONLY genuinely funny fails or comedy content.

ACCEPTED CATEGORIES (only use these):
1. fails - accidents, mishaps, things going wrong, instant regret, people falling, crashes, fails
2. comedy - funny skits, humor, pranks, jokes, memes, funny reactions, comedic moments

REJECTED CONTENT (use "reject" category for these):
- Dancing or choreography of any kind
- People posing, modeling, or showing off outfits
- Thirst traps or attractive people just existing on camera
- Advertisements, product promotions, sponsored content
- Lip syncing or singing
- Beauty, makeup, skincare, fashion content
- Lifestyle vlogs, routines, "day in my life"
- Relationship content, couples content
- Motivational or inspirational content
- Music videos or performances

Respond with JSON only:
{"category": "fails|comedy|reject", "confidence": 0.0-1.0, "reasoning": "brief explanation"}

Rules:
- ONLY accept content that is genuinely FUNNY or shows a FAIL
- Use "reject" for anything that doesn't fit fails or comedy
- Be STRICT - when in doubt, reject
- Dancing = ALWAYS reject, no exceptions
- Ads/promos = ALWAYS reject
- People just looking attractive = ALWAYS reject
- confidence 0.7+ for clear fails/comedy
- confidence 0.3-0.7 for moderate
- Use "reject" with high confidence for unwanted content"""


class ClassifierService:
    """Classifies videos into fails/comedy categories, rejecting other content."""

    # Only accept these categories for compilations
    ACCEPTED_CATEGORIES = {"fails", "comedy"}

    def __init__(self, db: Database):
        """Initialize classifier service."""
        self.db = db
        self._client: Optional[OpenAI] = None
        self.model = settings.OPENAI_MODEL

    @property
    def client(self) -> OpenAI:
        """Lazy-load OpenAI client."""
        if self._client is None:
            if not settings.OPENAI_API_KEY:
                raise ValueError("OPENAI_API_KEY is required for classification")
            self._client = OpenAI(api_key=settings.OPENAI_API_KEY)
        return self._client

    def _build_user_prompt(self, video: Video) -> str:
        """Build the user prompt for classification."""
        parts = []

        if video.description:
            parts.append(f"Description: {video.description[:500]}")

        if video.hashtags:
            parts.append(f"Hashtags: {' '.join(video.hashtags[:20])}")

        if video.author:
            parts.append(f"Author: @{video.author}")

        return "\n".join(parts) if parts else "No description available"

    def _parse_response(self, response_text: str) -> Tuple[str, float, str]:
        """Parse GPT response into category, confidence, reasoning."""
        try:
            # Clean up response (remove markdown code blocks if present)
            text = response_text.strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()

            data = json.loads(text)

            category = data.get("category", "").lower().strip()
            confidence = float(data.get("confidence", 0))
            reasoning = data.get("reasoning", "")

            # Handle reject category
            if category == "reject":
                return "reject", confidence, reasoning

            # Validate category - only accept fails or comedy
            if category not in self.ACCEPTED_CATEGORIES:
                logger.warning(f"Invalid category '{category}', treating as reject")
                return "reject", 0.8, f"Invalid category: {category}"

            # Clamp confidence
            confidence = max(0.0, min(1.0, confidence))

            return category, confidence, reasoning

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"Failed to parse classification response: {e}")
            return "reject", 0.5, f"Parse error: {e}"

    def classify(self, video: Video) -> Tuple[str, float, str]:
        """
        Classify a single video.
        Returns (category, confidence, reasoning).
        """
        user_prompt = self._build_user_prompt(video)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.3,
                max_tokens=150,
            )

            response_text = response.choices[0].message.content
            category, confidence, reasoning = self._parse_response(response_text)

            logger.debug(
                f"Classified {video.id}: {category} ({confidence:.2f}) - {reasoning}"
            )

            return category, confidence, reasoning

        except Exception as e:
            logger.error(f"Classification failed for {video.id}: {e}")
            raise

    def classify_and_update(
        self, video: Video, min_confidence: float = None
    ) -> bool:
        """
        Classify a video and update the database.
        Returns True if classified as fails/comedy with sufficient confidence.
        Returns False if rejected or low confidence.
        """
        if min_confidence is None:
            min_confidence = settings.MIN_CLASSIFICATION_CONFIDENCE

        try:
            category, confidence, reasoning = self.classify(video)

            video.category = category
            video.category_confidence = confidence
            video.classification_reasoning = reasoning

            # Reject category = skip the video
            if category == "reject":
                video.status = VideoStatus.SKIPPED
                logger.info(f"Video {video.id} rejected: {reasoning}")
                video.error = ""
                self.db.update_video(video)
                return False

            # Low confidence = skip
            if confidence < min_confidence:
                video.status = VideoStatus.SKIPPED
                logger.info(f"Video {video.id} skipped (low confidence: {confidence:.2f})")
                video.error = ""
                self.db.update_video(video)
                return False

            # Accepted: fails or comedy with good confidence
            video.status = VideoStatus.CLASSIFIED
            logger.info(f"Video {video.id} classified as {category} ({confidence:.2f})")
            video.error = ""
            self.db.update_video(video)
            return True

        except Exception as e:
            video.error = str(e)
            video.status = VideoStatus.FAILED
            self.db.update_video(video)
            return False

    def classify_batch(
        self,
        videos: List[Video],
        min_confidence: float = None,
        progress_callback: Optional[callable] = None,
    ) -> Tuple[int, int, int]:
        """
        Classify multiple videos.
        Returns (classified_count, skipped_count, failed_count).
        """
        classified = 0
        skipped = 0
        failed = 0

        for i, video in enumerate(videos):
            if progress_callback:
                progress_callback(i + 1, len(videos), video)

            try:
                if self.classify_and_update(video, min_confidence):
                    classified += 1
                else:
                    if video.status == VideoStatus.SKIPPED:
                        skipped += 1
                    else:
                        failed += 1
            except Exception:
                failed += 1

        logger.info(
            f"Batch classification: {classified} classified, {skipped} skipped, {failed} failed"
        )
        return classified, skipped, failed

    def classify_downloaded(
        self,
        limit: Optional[int] = None,
        min_confidence: float = None,
        progress_callback: Optional[callable] = None,
    ) -> Tuple[int, int, int]:
        """
        Classify all videos with DOWNLOADED status.
        Returns (classified_count, skipped_count, failed_count).
        """
        videos = self.db.get_videos_by_status(VideoStatus.DOWNLOADED, limit)
        logger.info(f"Found {len(videos)} videos to classify")

        if not videos:
            return 0, 0, 0

        return self.classify_batch(videos, min_confidence, progress_callback)
