"""
Enhanced classifier service for categorizing videos using GPT-4o-mini.
Three-stage classification: trend pre-filter, hard reject, then LLM with visual_independence scoring.
GOAL: Only accept INSTANT VISUAL MOMENTS that work without audio/context.
"""

import json
import logging
import re
from typing import Tuple, Optional, List

from openai import OpenAI

from core.models import Video, VideoStatus
from core.database import Database
from config.settings import settings, categories_config

logger = logging.getLogger(__name__)


# Updated prompt: EXPLICITLY anti-narrative, requires visual independence
CLASSIFICATION_PROMPT = """You are an expert classifier for short-form COMPILATION videos. Your job is to find clips for rapid-fire "Try Not to Laugh" compilations where each clip is 5-15 seconds.

⚠️ CRITICAL: These clips will be:
- Played back-to-back with other clips
- Stripped of original audio (music overlay added)
- Watched by viewers with NO context about the original

═══════════════════════════════════════════════════════════════
🎯 WHAT WE WANT: INSTANT VISUAL MOMENTS
═══════════════════════════════════════════════════════════════

The PERFECT clip is funny THE INSTANT you see it:
- Someone falls → immediately funny (ACCEPT)
- Cat knocks something over → immediately funny (ACCEPT)
- Failed trick → the moment of failure is funny (ACCEPT)
- Surprised face → expression tells the whole story (ACCEPT)
- Object unexpectedly breaks → visual payoff (ACCEPT)

Categories:
1. FAILS (physical | skill | social)
   - physical: Falls, trips, crashes, collisions - VISUAL accidents
   - skill: Failed tricks, sports fails, DIY disasters - SEE the failure
   - social: Embarrassing caught-on-camera moments - must be VISUALLY obvious

2. COMEDY (physical | reaction | animal)
   - physical: Slapstick, getting bonked, funny accidents
   - reaction: Funny FACIAL EXPRESSIONS (not reactions to other content)
   - animal: Pets/animals doing visually funny things

═══════════════════════════════════════════════════════════════
🚫 HARD REJECT - NARRATIVE/TREND CONTENT
═══════════════════════════════════════════════════════════════

These formats NEVER work in compilations because they need context/audio:

❌ "Telling my X that Y" - reaction bait, needs dialogue
❌ "POV: you are..." - requires buying into the premise
❌ "Watch till the end" - means there's buildup required
❌ "Part 1/2/3" - serialized content
❌ Skits with dialogue - punchline is in what they SAY
❌ Reactions to other content - "reacting to...", duets, stitches
❌ Relationship drama - "caught my bf", confrontations
❌ Story time content - "so basically what happened..."
❌ Sound-dependent jokes - "the sound he made", trending audio
❌ Text overlay is the joke - if you need to read text to get it
❌ Green screen/commentary - person talking about something

Also always reject:
❌ Dancing, choreography, thirst traps, posing
❌ Lip sync, singing, ASMR, mukbang
❌ Ads, sponsored, tutorials, vlogs
❌ Motivational, inspirational content
❌ Beauty/makeup/skincare content

═══════════════════════════════════════════════════════════════
🔇 THE MUTE TEST (visual_independence score)
═══════════════════════════════════════════════════════════════

Imagine watching this clip on MUTE with no captions:
- 1.0: Instantly funny, no audio needed at all (physical fail, animal being derpy)
- 0.8: Very funny visually, audio adds little (surprised face, object breaking)
- 0.6: Funny but audio helps somewhat (verbal reaction visible but not essential)
- 0.4: Need to hear something to fully get it
- 0.2: Mostly depends on audio/dialogue
- 0.0: Completely audio-dependent (talking, singing, sound effect IS the joke)

For compilations, we need visual_independence >= 0.7

═══════════════════════════════════════════════════════════════
📊 COMPILATION SCORE
═══════════════════════════════════════════════════════════════

Rate how well this works in a rapid-fire compilation:
- 0.9-1.0: Perfect. Funny in first 2 seconds. No setup needed.
- 0.7-0.9: Great. Clear visual payoff, works standalone.
- 0.5-0.7: Decent but needs a moment to understand.
- 0.3-0.5: Too slow or needs context.
- 0.0-0.3: Doesn't work in compilation format.

═══════════════════════════════════════════════════════════════

Response format (JSON only, no other text):
{
  "category": "fails|comedy|reject",
  "subcategory": "physical|skill|social|reaction|animal|none",
  "confidence": 0.0-1.0,
  "compilation_score": 0.0-1.0,
  "visual_independence": 0.0-1.0,
  "reasoning": "brief explanation",
  "rejection_reason": "if rejected, why specifically"
}"""


class ClassifierService:
    """Enhanced classifier with three-stage filtering: trend patterns, hard reject, LLM."""

    ACCEPTED_CATEGORIES = {"fails", "comedy"}

    # Subcategories by category
    SUBCATEGORIES = {
        "fails": ["physical", "skill", "social"],
        "comedy": ["physical", "reaction", "animal"]
    }

    def __init__(self, db: Database):
        """Initialize classifier service."""
        self.db = db
        self._client: Optional[OpenAI] = None
        self.model = settings.OPENAI_MODEL

        # Load rejection patterns from config
        self.hard_reject_keywords = categories_config.get_hard_reject_keywords()
        self.soft_reject_keywords = categories_config.get_soft_reject_keywords()
        self.trend_patterns = categories_config.get_trend_patterns()
        self.narrative_keywords = categories_config.get_narrative_keywords()
        self.narrative_hashtags = categories_config.get_narrative_hashtags()

    @property
    def client(self) -> OpenAI:
        """Lazy-load OpenAI client."""
        if self._client is None:
            if not settings.OPENAI_API_KEY:
                raise ValueError("OPENAI_API_KEY is required for classification")
            self._client = OpenAI(api_key=settings.OPENAI_API_KEY)
        return self._client

    def _check_trend_patterns(self, text: str) -> Tuple[bool, str]:
        """
        Check if text matches any trend/narrative patterns.
        Returns (is_trend, matched_pattern).
        """
        text_lower = text.lower()

        # Check regex patterns
        for pattern in self.trend_patterns:
            try:
                if re.search(pattern, text_lower, re.IGNORECASE):
                    return True, f"Trend pattern: {pattern}"
            except re.error:
                continue

        # Check narrative keywords
        for keyword in self.narrative_keywords:
            if keyword.lower() in text_lower:
                return True, f"Narrative keyword: {keyword}"

        return False, ""

    def _check_narrative_hashtags(self, hashtags: List[str]) -> Tuple[bool, str]:
        """
        Check if hashtags indicate narrative/trend content.
        Returns (is_narrative, matched_hashtag).
        """
        hashtags_lower = [h.lower() for h in hashtags]

        for narrative_tag in self.narrative_hashtags:
            if narrative_tag.lower() in hashtags_lower:
                return True, f"Narrative hashtag: {narrative_tag}"

        return False, ""

    def _pre_filter(self, video: Video) -> Tuple[bool, str]:
        """
        Three-stage pre-filter before LLM classification.
        Returns (should_reject, reason).

        Stage 1: Trend/narrative pattern detection
        Stage 2: Hard reject keywords
        Stage 3: Spam detection
        """
        text_to_check = f"{video.description} {' '.join(video.hashtags)}"
        text_lower = text_to_check.lower()

        # Stage 1: Check trend/narrative patterns (MOST IMPORTANT)
        is_trend, trend_reason = self._check_trend_patterns(video.description)
        if is_trend:
            return True, trend_reason

        # Check narrative hashtags
        is_narrative, hashtag_reason = self._check_narrative_hashtags(video.hashtags)
        if is_narrative:
            return True, hashtag_reason

        # Stage 2: Check hard reject keywords
        for keyword in self.hard_reject_keywords:
            if keyword.lower() in text_lower:
                return True, f"Hard reject keyword: {keyword}"

        # Stage 3: Check for common TikTok spam patterns
        spam_patterns = [
            r"follow\s*for\s*more",
            r"link\s*in\s*bio",
            r"shop\s*now",
            r"use\s*code",
            r"dm\s*for",
            r"collab\s*\?",
            r"rate\s*me",
            r"hot\s*or\s*not",
            r"duet\s*this",
            r"stitch\s*with",
        ]

        for pattern in spam_patterns:
            if re.search(pattern, text_lower):
                return True, f"Spam pattern: {pattern}"

        # Check for excessive hashtag count (likely spam or algorithm gaming)
        if len(video.hashtags) > 15:
            return True, "Too many hashtags (likely spam)"

        # Check for low-effort descriptions that suggest trend content
        low_effort_exact = [
            "part 1", "part 2", "part 3",
            "wait for it", "watch till end",
            "pov", "storytime", "story time",
        ]
        for phrase in low_effort_exact:
            if phrase in text_lower:
                return True, f"Low-effort trend phrase: {phrase}"

        return False, ""

    def _build_user_prompt(self, video: Video) -> str:
        """Build the user prompt for classification."""
        parts = []

        if video.description:
            # Clean description
            desc = video.description[:500].strip()
            parts.append(f"Description: {desc}")

        if video.hashtags:
            # Take relevant hashtags (not generic viral tags)
            generic_tags = {"#fyp", "#foryou", "#foryoupage", "#viral", "#xyzbca", "#trending", "#blowthisup"}
            relevant_tags = [
                tag for tag in video.hashtags[:15]
                if tag.lower() not in generic_tags
            ]
            if relevant_tags:
                parts.append(f"Hashtags: {' '.join(relevant_tags)}")

        if video.author:
            parts.append(f"Author: @{video.author}")

        # Add duration context (shorter = better for compilations)
        if video.duration > 0:
            if video.duration <= 10:
                parts.append(f"Duration: {video.duration:.1f}s (short - good for compilation)")
            elif video.duration <= 20:
                parts.append(f"Duration: {video.duration:.1f}s (medium length)")
            else:
                parts.append(f"Duration: {video.duration:.1f}s (long - may have buildup)")

        return "\n".join(parts) if parts else "No description available"

    def _parse_response(self, response_text: str) -> dict:
        """Parse GPT response into structured result."""
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
            subcategory = data.get("subcategory", "").lower().strip()
            confidence = float(data.get("confidence", 0))
            compilation_score = float(data.get("compilation_score", 0))
            visual_independence = float(data.get("visual_independence", 0))
            reasoning = data.get("reasoning", "")
            rejection_reason = data.get("rejection_reason", "")

            # Clamp values
            confidence = max(0.0, min(1.0, confidence))
            compilation_score = max(0.0, min(1.0, compilation_score))
            visual_independence = max(0.0, min(1.0, visual_independence))

            # Validate category
            if category not in self.ACCEPTED_CATEGORIES and category != "reject":
                category = "reject"
                rejection_reason = f"Invalid category: {data.get('category', 'unknown')}"

            # Validate subcategory
            if category in self.ACCEPTED_CATEGORIES:
                valid_subs = self.SUBCATEGORIES.get(category, [])
                if subcategory not in valid_subs:
                    subcategory = valid_subs[0] if valid_subs else ""

            return {
                "category": category,
                "subcategory": subcategory if category != "reject" else "",
                "confidence": confidence,
                "compilation_score": compilation_score,
                "visual_independence": visual_independence,
                "reasoning": reasoning,
                "rejection_reason": rejection_reason,
            }

        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            logger.warning(f"Failed to parse classification response: {e}")
            return {
                "category": "reject",
                "subcategory": "",
                "confidence": 0.5,
                "compilation_score": 0.0,
                "visual_independence": 0.0,
                "reasoning": f"Parse error: {e}",
                "rejection_reason": "Could not parse LLM response",
            }

    def classify(self, video: Video) -> dict:
        """
        Classify a single video with three-stage filtering.
        Returns dict with category, subcategory, confidence, compilation_score,
        visual_independence, reasoning.
        """
        # Stage 1 & 2: Pre-filter (trend patterns + hard reject)
        should_reject, reject_reason = self._pre_filter(video)
        if should_reject:
            logger.debug(f"Pre-filtered {video.id}: {reject_reason}")
            return {
                "category": "reject",
                "subcategory": "",
                "confidence": 0.95,  # High confidence in pre-filter rejections
                "compilation_score": 0.0,
                "visual_independence": 0.0,
                "reasoning": f"Pre-filter rejection: {reject_reason}",
                "rejection_reason": reject_reason,
            }

        # Stage 3: LLM classification with visual_independence scoring
        user_prompt = self._build_user_prompt(video)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": CLASSIFICATION_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.1,  # Very low temperature for consistent classification
                max_tokens=300,
            )

            response_text = response.choices[0].message.content
            result = self._parse_response(response_text)

            logger.debug(
                f"Classified {video.id}: {result['category']}/{result['subcategory']} "
                f"(conf: {result['confidence']:.2f}, comp: {result['compilation_score']:.2f}, "
                f"visual: {result['visual_independence']:.2f})"
            )

            return result

        except Exception as e:
            logger.error(f"Classification failed for {video.id}: {e}")
            raise

    def classify_and_update(
        self,
        video: Video,
        min_confidence: float = None,
        min_compilation_score: float = 0.6,
        min_visual_independence: float = 0.6
    ) -> bool:
        """
        Classify a video and update the database.
        Returns True if classified as fails/comedy with sufficient scores.
        Returns False if rejected or low scores.

        New: Also checks visual_independence threshold.
        """
        if min_confidence is None:
            min_confidence = settings.MIN_CLASSIFICATION_CONFIDENCE

        try:
            result = self.classify(video)

            # Update video with classification results
            video.category = result["category"]
            video.subcategory = result["subcategory"]
            video.category_confidence = result["confidence"]
            video.compilation_score = result["compilation_score"]
            video.visual_independence = result["visual_independence"]

            # Build reasoning string
            reasoning_parts = [result["reasoning"]]
            if result["rejection_reason"]:
                reasoning_parts.append(f"Rejection: {result['rejection_reason']}")
            video.classification_reasoning = " | ".join(filter(None, reasoning_parts))

            # Reject category = skip the video
            if result["category"] == "reject":
                video.status = VideoStatus.SKIPPED
                logger.info(
                    f"Video {video.id} rejected: {result.get('rejection_reason', result['reasoning'])}"
                )
                video.error = ""
                self.db.update_video(video)
                return False

            # Low confidence = skip
            if result["confidence"] < min_confidence:
                video.status = VideoStatus.SKIPPED
                logger.info(f"Video {video.id} skipped (low confidence: {result['confidence']:.2f})")
                video.error = ""
                self.db.update_video(video)
                return False

            # Low compilation score = skip (not suitable for compilations)
            if result["compilation_score"] < min_compilation_score:
                video.status = VideoStatus.SKIPPED
                logger.info(
                    f"Video {video.id} skipped (low compilation score: {result['compilation_score']:.2f})"
                )
                video.error = ""
                self.db.update_video(video)
                return False

            # Low visual independence = skip (needs audio/context)
            if result["visual_independence"] < min_visual_independence:
                video.status = VideoStatus.SKIPPED
                logger.info(
                    f"Video {video.id} skipped (low visual independence: {result['visual_independence']:.2f})"
                )
                video.error = ""
                self.db.update_video(video)
                return False

            # Accepted: fails or comedy with good scores
            video.status = VideoStatus.CLASSIFIED
            logger.info(
                f"Video {video.id} classified as {result['category']}/{result['subcategory']} "
                f"(conf: {result['confidence']:.2f}, comp: {result['compilation_score']:.2f}, "
                f"visual: {result['visual_independence']:.2f})"
            )
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
        min_compilation_score: float = 0.6,
        min_visual_independence: float = 0.6,
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
                if self.classify_and_update(
                    video, min_confidence, min_compilation_score, min_visual_independence
                ):
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
        min_compilation_score: float = 0.6,
        min_visual_independence: float = 0.6,
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

        return self.classify_batch(
            videos, min_confidence, min_compilation_score, min_visual_independence, progress_callback
        )

    def reclassify_low_confidence(
        self,
        threshold: float = 0.5,
        limit: Optional[int] = None,
    ) -> Tuple[int, int]:
        """
        Re-classify videos that were previously classified with low confidence.
        Returns (reclassified_count, unchanged_count).
        """
        # Get classified videos with low confidence
        videos = self.db.get_videos_by_status(VideoStatus.CLASSIFIED, limit)
        low_confidence = [v for v in videos if v.category_confidence < threshold]

        logger.info(f"Found {len(low_confidence)} low-confidence videos to reclassify")

        reclassified = 0
        unchanged = 0

        for video in low_confidence:
            old_category = video.category
            old_subcategory = video.subcategory

            result = self.classify(video)

            if result["category"] != old_category or result["subcategory"] != old_subcategory:
                video.category = result["category"]
                video.subcategory = result["subcategory"]
                video.category_confidence = result["confidence"]
                video.compilation_score = result["compilation_score"]
                video.visual_independence = result["visual_independence"]
                video.classification_reasoning = result["reasoning"]

                if result["category"] == "reject":
                    video.status = VideoStatus.SKIPPED

                self.db.update_video(video)
                reclassified += 1
            else:
                unchanged += 1

        logger.info(f"Reclassification: {reclassified} changed, {unchanged} unchanged")
        return reclassified, unchanged

    def reclassify_low_visual_independence(
        self,
        threshold: float = 0.6,
        limit: Optional[int] = None,
    ) -> Tuple[int, int]:
        """
        Re-classify videos that have low visual_independence scores.
        These may have been classified before the visual_independence metric was added.
        Returns (reclassified_count, unchanged_count).
        """
        videos = self.db.get_videos_by_status(VideoStatus.CLASSIFIED, limit)
        low_visual = [v for v in videos if v.visual_independence < threshold]

        logger.info(f"Found {len(low_visual)} low visual independence videos to reclassify")

        reclassified = 0
        unchanged = 0

        for video in low_visual:
            result = self.classify(video)

            video.category = result["category"]
            video.subcategory = result["subcategory"]
            video.category_confidence = result["confidence"]
            video.compilation_score = result["compilation_score"]
            video.visual_independence = result["visual_independence"]
            video.classification_reasoning = result["reasoning"]

            # If visual independence is still too low, skip the video
            if result["visual_independence"] < threshold:
                video.status = VideoStatus.SKIPPED
                reclassified += 1
            elif result["category"] == "reject":
                video.status = VideoStatus.SKIPPED
                reclassified += 1
            else:
                unchanged += 1

            self.db.update_video(video)

        logger.info(f"Visual independence reclassification: {reclassified} changed, {unchanged} unchanged")
        return reclassified, unchanged

    # =========================================================================
    # Compilation Classification - Verify and score source compilations
    # =========================================================================

    def classify_compilation(self, video: Video) -> dict:
        """
        Classify a video that's been flagged as a potential source compilation.
        Verifies it's actually a compilation and scores its quality.

        Returns dict with:
        - is_compilation: bool
        - compilation_type: fails|comedy|satisfying|mixed
        - quality_score: 0-1
        - estimated_clips: int
        - has_countdown: bool
        - reasoning: str
        """
        prompt = """You are evaluating if this TikTok video is an EXISTING COMPILATION suitable for re-use.

A COMPILATION is a video with multiple distinct clips/moments edited together by the original creator.

WHAT MAKES A GOOD SOURCE COMPILATION:
✓ Multiple distinct clips edited together (3+ clips)
✓ Consistent theme (all fails, all funny moments, etc.)
✓ Clean transitions between clips
✓ Good visual quality
✓ May have countdown numbers (5, 4, 3, 2, 1)
✓ Duration 45-180 seconds ideal

REJECT THESE:
✗ Single-moment videos (not compilations)
✗ Slideshows or photo compilations
✗ Reaction videos watching compilations
✗ Low quality or heavily watermarked
✗ Talking/commentary videos
✗ Music videos or dance compilations

Analyze this video metadata and determine:
1. Is it actually a compilation? (multiple clips edited together)
2. What type? (fails, comedy, satisfying, mixed)
3. Quality score (0-1) for re-use
4. Estimated number of clips
5. Does it have visible countdown/numbers?

Response JSON only:
{
  "is_compilation": true/false,
  "compilation_type": "fails|comedy|satisfying|mixed|none",
  "quality_score": 0.0-1.0,
  "estimated_clips": 3-20,
  "has_countdown": true/false,
  "reasoning": "brief explanation"
}"""

        user_prompt = self._build_user_prompt(video)
        user_prompt += f"\nDuration: {video.duration:.0f} seconds"

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.1,
                max_tokens=200,
            )

            response_text = response.choices[0].message.content
            return self._parse_compilation_response(response_text)

        except Exception as e:
            logger.error(f"Compilation classification failed for {video.id}: {e}")
            return {
                "is_compilation": False,
                "compilation_type": "none",
                "quality_score": 0.0,
                "estimated_clips": 0,
                "has_countdown": False,
                "reasoning": f"Error: {e}",
            }

    def _parse_compilation_response(self, response_text: str) -> dict:
        """Parse GPT response for compilation classification."""
        try:
            text = response_text.strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()

            data = json.loads(text)

            return {
                "is_compilation": bool(data.get("is_compilation", False)),
                "compilation_type": data.get("compilation_type", "none").lower(),
                "quality_score": max(0.0, min(1.0, float(data.get("quality_score", 0)))),
                "estimated_clips": max(0, int(data.get("estimated_clips", 0))),
                "has_countdown": bool(data.get("has_countdown", False)),
                "reasoning": data.get("reasoning", ""),
            }

        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            logger.warning(f"Failed to parse compilation response: {e}")
            return {
                "is_compilation": False,
                "compilation_type": "none",
                "quality_score": 0.0,
                "estimated_clips": 0,
                "has_countdown": False,
                "reasoning": f"Parse error: {e}",
            }

    def classify_compilation_and_update(
        self, video: Video, min_quality_score: float = 0.6
    ) -> bool:
        """
        Classify a source compilation video and update the database.
        Returns True if verified as a good compilation.
        """
        result = self.classify_compilation(video)

        # Update video with classification results
        video.is_source_compilation = result["is_compilation"]
        video.compilation_type = result["compilation_type"]
        video.source_clip_count = result["estimated_clips"]
        video.compilation_score = result["quality_score"]
        video.classification_reasoning = result["reasoning"]

        if not result["is_compilation"]:
            video.status = VideoStatus.SKIPPED
            video.is_source_compilation = False
            logger.info(f"Video {video.id} is not a compilation: {result['reasoning']}")
            self.db.update_video(video)
            return False

        if result["quality_score"] < min_quality_score:
            video.status = VideoStatus.SKIPPED
            logger.info(
                f"Compilation {video.id} quality too low: {result['quality_score']:.2f}"
            )
            self.db.update_video(video)
            return False

        # Good compilation - mark as classified
        video.status = VideoStatus.CLASSIFIED
        video.category = result["compilation_type"]
        logger.info(
            f"Verified compilation {video.id}: {result['compilation_type']} "
            f"(~{result['estimated_clips']} clips, quality: {result['quality_score']:.2f})"
        )
        self.db.update_video(video)
        return True

    def classify_source_compilations(
        self,
        limit: Optional[int] = None,
        min_quality_score: float = 0.6,
        progress_callback: Optional[callable] = None,
    ) -> Tuple[int, int, int]:
        """
        Classify all discovered source compilations.
        Returns (verified_count, rejected_count, failed_count).
        """
        # Get videos that are flagged as source compilations but not yet classified
        videos = self.db.get_source_compilations(
            status=VideoStatus.DISCOVERED,
            limit=limit
        )

        logger.info(f"Found {len(videos)} source compilations to classify")

        verified = 0
        rejected = 0
        failed = 0

        for i, video in enumerate(videos):
            if progress_callback:
                progress_callback(i + 1, len(videos), video)

            try:
                if self.classify_compilation_and_update(video, min_quality_score):
                    verified += 1
                else:
                    rejected += 1
            except Exception as e:
                logger.error(f"Failed to classify compilation {video.id}: {e}")
                video.status = VideoStatus.FAILED
                video.error = str(e)
                self.db.update_video(video)
                failed += 1

        logger.info(
            f"Compilation classification: {verified} verified, {rejected} rejected, {failed} failed"
        )
        return verified, rejected, failed
