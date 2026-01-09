"""
Captioner service for generating text overlays using GPT-4o-mini.
"""

import json
import logging
from typing import Optional, List

from openai import OpenAI

from core.models import Compilation, Video
from core.database import Database
from config.settings import settings, categories_config

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a viral video caption writer. Generate engaging text overlays for a compilation video.

Your captions should be:
- Short and punchy (2-8 words max per caption)
- Engaging and attention-grabbing
- Match the energy of the category
- Use emojis sparingly but effectively
- Encourage watching to the end

Respond with JSON only:
{
  "hook": "Opening hook (2-5 words) to grab attention",
  "clip_captions": ["Caption for clip 1", "Caption for clip 2", ...],
  "transitions": ["Optional transition text", "", "Another transition", ...],
  "end_card": "Follow call-to-action (3-6 words)"
}

Rules:
- hook: Very short, creates curiosity or excitement
- clip_captions: One per video, describes what's happening or adds humor
- transitions: Can be empty strings, used between notable clips
- end_card: Encourage engagement (follow, like, share)"""


class CaptionerService:
    """Generates captions for compilations using GPT-4o-mini."""

    def __init__(self, db: Database):
        """Initialize captioner service."""
        self.db = db
        self._client: Optional[OpenAI] = None
        self.model = settings.OPENAI_MODEL

    @property
    def client(self) -> OpenAI:
        """Lazy-load OpenAI client."""
        if self._client is None:
            if not settings.OPENAI_API_KEY:
                raise ValueError("OPENAI_API_KEY is required for captioning")
            self._client = OpenAI(api_key=settings.OPENAI_API_KEY)
        return self._client

    def _build_user_prompt(
        self, compilation: Compilation, videos: List[Video]
    ) -> str:
        """Build the user prompt for caption generation."""
        category_config = categories_config.get_category(compilation.category)
        category_name = category_config.get("name", compilation.category.title())

        clip_descriptions = []
        for i, video in enumerate(videos):
            desc = video.description[:100] if video.description else "No description"
            clip_descriptions.append(f"Clip {i+1}: {desc}")

        return f"""Generate captions for a {category_name} compilation titled: "{compilation.title}"

Number of clips: {len(videos)}

Clip descriptions:
{chr(10).join(clip_descriptions)}

Generate engaging captions that match the {compilation.category} category style."""

    def _parse_response(
        self, response_text: str, num_clips: int
    ) -> dict:
        """Parse GPT response into caption structure."""
        try:
            # Clean up response
            text = response_text.strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()

            data = json.loads(text)

            # Validate and normalize
            hook = str(data.get("hook", "Check this out"))[:50]
            end_card = str(data.get("end_card", "Follow for more"))[:50]

            # Ensure correct number of clip captions
            clip_captions = data.get("clip_captions", [])
            if not isinstance(clip_captions, list):
                clip_captions = []

            # Pad or trim to match clip count
            while len(clip_captions) < num_clips:
                clip_captions.append("")
            clip_captions = clip_captions[:num_clips]

            # Normalize transitions
            transitions = data.get("transitions", [])
            if not isinstance(transitions, list):
                transitions = []

            # Pad transitions to match clips
            while len(transitions) < num_clips:
                transitions.append("")
            transitions = transitions[:num_clips]

            return {
                "hook": hook,
                "clip_captions": clip_captions,
                "transitions": transitions,
                "end_card": end_card,
            }

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"Failed to parse caption response: {e}")
            # Return defaults
            return {
                "hook": "Watch this",
                "clip_captions": [""] * num_clips,
                "transitions": [""] * num_clips,
                "end_card": "Follow for more",
            }

    def generate_captions(
        self, compilation: Compilation
    ) -> dict:
        """
        Generate captions for a compilation.
        Returns dict with hook, clip_captions, transitions, end_card.
        """
        videos = self.db.get_videos_for_compilation(compilation.id)

        if not videos:
            logger.warning(f"No videos found for compilation {compilation.id}")
            return {
                "hook": "",
                "clip_captions": [],
                "transitions": [],
                "end_card": "",
            }

        user_prompt = self._build_user_prompt(compilation, videos)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.7,
                max_tokens=500,
            )

            response_text = response.choices[0].message.content
            captions = self._parse_response(response_text, len(videos))

            logger.info(f"Generated captions for compilation {compilation.id}")
            return captions

        except Exception as e:
            logger.error(f"Caption generation failed for {compilation.id}: {e}")
            raise

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
