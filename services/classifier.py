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

SYSTEM_PROMPT = """You are a content classifier for short-form videos. Categorize videos into exactly one of these categories:

1. fails - accidents, mishaps, things going wrong
2. satisfying - oddly satisfying, ASMR, smooth/perfect actions
3. wholesome - heartwarming, feel-good, emotional positive moments
4. comedy - funny skits, humor, pranks, jokes
5. skills - impressive talents, amazing abilities, pro-level performance
6. animals - pets, wildlife, cute animal moments
7. food - cooking, eating, recipes, food content
8. drama - confrontations, arguments, public freakouts, exposed moments

Analyze the video description and hashtags to determine the most appropriate category.

Respond with JSON only:
{"category": "category_name", "confidence": 0.0-1.0, "reasoning": "brief explanation"}

Rules:
- confidence should be 0.0-1.0 based on how certain you are
- Use 0.7+ for clear matches
- Use 0.3-0.7 for moderate confidence
- Use <0.3 only if genuinely ambiguous
- If multiple categories could apply, pick the strongest match"""


class ClassifierService:
    """Classifies videos into categories using GPT-4o-mini."""

    def __init__(self, db: Database):
        """Initialize classifier service."""
        self.db = db
        self._client: Optional[OpenAI] = None
        self.model = settings.OPENAI_MODEL
        self.valid_categories = set(categories_config.get_category_names())

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

            # Validate category
            if category not in self.valid_categories:
                logger.warning(f"Invalid category '{category}', defaulting to comedy")
                category = "comedy"
                confidence = 0.2

            # Clamp confidence
            confidence = max(0.0, min(1.0, confidence))

            return category, confidence, reasoning

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"Failed to parse classification response: {e}")
            return "comedy", 0.1, "Parse error"

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
        Returns True if classified (confidence >= threshold), False if skipped.
        """
        if min_confidence is None:
            min_confidence = settings.MIN_CLASSIFICATION_CONFIDENCE

        try:
            category, confidence, reasoning = self.classify(video)

            video.category = category
            video.category_confidence = confidence
            video.classification_reasoning = reasoning

            if confidence >= min_confidence:
                video.status = VideoStatus.CLASSIFIED
                logger.info(f"Video {video.id} classified as {category} ({confidence:.2f})")
            else:
                video.status = VideoStatus.SKIPPED
                logger.info(f"Video {video.id} skipped (low confidence: {confidence:.2f})")

            video.error = ""
            self.db.update_video(video)

            return confidence >= min_confidence

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
