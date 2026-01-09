"""
Data models for viral-clips-pipeline.
Defines Video, Compilation dataclasses and status enums.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional
import json


class VideoStatus(Enum):
    """Video processing status."""
    DISCOVERED = "discovered"  # Metadata fetched from TikTok
    DOWNLOADED = "downloaded"  # Video file saved locally
    CLASSIFIED = "classified"  # Category assigned by GPT
    GROUPED = "grouped"        # Assigned to a compilation
    USED = "used"              # In a rendered compilation
    FAILED = "failed"          # Processing failed
    SKIPPED = "skipped"        # Low confidence classification


class CompilationStatus(Enum):
    """Compilation processing status."""
    PENDING = "pending"        # Clips grouped, not yet rendered
    RENDERING = "rendering"    # FFmpeg in progress
    REVIEW = "review"          # Ready for manual review
    APPROVED = "approved"      # Approved, ready to upload
    UPLOADED = "uploaded"      # On YouTube
    REJECTED = "rejected"      # Manually rejected


@dataclass
class Video:
    """Represents a TikTok video in the pipeline."""

    # Identifiers
    id: str                              # Internal hash/UUID
    tiktok_id: str                       # Original TikTok video ID
    url: str                             # TikTok video URL

    # Metadata
    description: str = ""                # Video description/caption
    author: str = ""                     # Creator username
    hashtags: List[str] = field(default_factory=list)

    # Engagement metrics
    plays: int = 0
    likes: int = 0
    shares: int = 0

    # Processing state
    status: VideoStatus = VideoStatus.DISCOVERED
    local_path: str = ""                 # Path to downloaded .mp4
    duration: float = 0.0                # Duration in seconds
    width: int = 0
    height: int = 0

    # Classification
    category: str = ""                   # One of 8 categories
    category_confidence: float = 0.0     # 0-1 confidence score
    classification_reasoning: str = ""   # GPT's reasoning

    # Compilation assignment
    compilation_id: str = ""             # Which compilation this belongs to
    clip_order: int = 0                  # Position in compilation
    caption: str = ""                    # Text overlay for this clip

    # Error tracking
    error: str = ""                      # Last error message
    retry_count: int = 0                 # Number of download retries

    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)

    @property
    def engagement_score(self) -> int:
        """Calculate engagement score for ranking."""
        return self.likes + (self.shares * 2)

    @property
    def hashtags_json(self) -> str:
        """Serialize hashtags to JSON string."""
        return json.dumps(self.hashtags)

    @classmethod
    def from_db_row(cls, row: dict) -> "Video":
        """Create Video from database row."""
        hashtags = json.loads(row.get("hashtags", "[]"))
        status = VideoStatus(row.get("status", "discovered"))
        created_at = row.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.now()

        return cls(
            id=row["id"],
            tiktok_id=row.get("tiktok_id", ""),
            url=row.get("url", ""),
            description=row.get("description", ""),
            author=row.get("author", ""),
            hashtags=hashtags,
            plays=row.get("plays", 0),
            likes=row.get("likes", 0),
            shares=row.get("shares", 0),
            status=status,
            local_path=row.get("local_path", ""),
            duration=row.get("duration", 0.0),
            width=row.get("width", 0),
            height=row.get("height", 0),
            category=row.get("category", ""),
            category_confidence=row.get("category_confidence", 0.0),
            classification_reasoning=row.get("classification_reasoning", ""),
            compilation_id=row.get("compilation_id", ""),
            clip_order=row.get("clip_order", 0),
            caption=row.get("caption", ""),
            error=row.get("error", ""),
            retry_count=row.get("retry_count", 0),
            created_at=created_at,
        )

    def to_db_dict(self) -> dict:
        """Convert to dictionary for database storage."""
        return {
            "id": self.id,
            "tiktok_id": self.tiktok_id,
            "url": self.url,
            "description": self.description,
            "author": self.author,
            "hashtags": self.hashtags_json,
            "plays": self.plays,
            "likes": self.likes,
            "shares": self.shares,
            "status": self.status.value,
            "local_path": self.local_path,
            "duration": self.duration,
            "width": self.width,
            "height": self.height,
            "category": self.category,
            "category_confidence": self.category_confidence,
            "classification_reasoning": self.classification_reasoning,
            "compilation_id": self.compilation_id,
            "clip_order": self.clip_order,
            "caption": self.caption,
            "error": self.error,
            "retry_count": self.retry_count,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class Compilation:
    """Represents a compilation of videos."""

    id: str                              # UUID
    category: str                        # Category of all clips
    title: str = ""                      # Generated title
    description: str = ""                # Auto-generated description
    video_ids: List[str] = field(default_factory=list)  # Ordered list of clip IDs

    # Processing state
    status: CompilationStatus = CompilationStatus.PENDING
    output_path: str = ""                # Path to rendered video
    duration: float = 0.0                # Total duration
    music_track: str = ""                # Background music file used

    # Upload info
    youtube_id: str = ""                 # YouTube video ID after upload
    credits_text: str = ""               # "@user1, @user2, ..."

    # Captions (stored as JSON)
    hook: str = ""                       # Opening hook text
    clip_captions: List[str] = field(default_factory=list)
    transitions: List[str] = field(default_factory=list)
    end_card: str = ""                   # Closing text

    # Error tracking
    error: str = ""

    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)

    @property
    def video_ids_json(self) -> str:
        """Serialize video_ids to JSON string."""
        return json.dumps(self.video_ids)

    @property
    def clip_captions_json(self) -> str:
        """Serialize clip_captions to JSON string."""
        return json.dumps(self.clip_captions)

    @property
    def transitions_json(self) -> str:
        """Serialize transitions to JSON string."""
        return json.dumps(self.transitions)

    @classmethod
    def from_db_row(cls, row: dict) -> "Compilation":
        """Create Compilation from database row."""
        video_ids = json.loads(row.get("video_ids", "[]"))
        clip_captions = json.loads(row.get("clip_captions", "[]"))
        transitions = json.loads(row.get("transitions", "[]"))
        status = CompilationStatus(row.get("status", "pending"))
        created_at = row.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.now()

        return cls(
            id=row["id"],
            category=row.get("category", ""),
            title=row.get("title", ""),
            description=row.get("description", ""),
            video_ids=video_ids,
            status=status,
            output_path=row.get("output_path", ""),
            duration=row.get("duration", 0.0),
            music_track=row.get("music_track", ""),
            youtube_id=row.get("youtube_id", ""),
            credits_text=row.get("credits_text", ""),
            hook=row.get("hook", ""),
            clip_captions=clip_captions,
            transitions=transitions,
            end_card=row.get("end_card", ""),
            error=row.get("error", ""),
            created_at=created_at,
        )

    def to_db_dict(self) -> dict:
        """Convert to dictionary for database storage."""
        return {
            "id": self.id,
            "category": self.category,
            "title": self.title,
            "description": self.description,
            "video_ids": self.video_ids_json,
            "status": self.status.value,
            "output_path": self.output_path,
            "duration": self.duration,
            "music_track": self.music_track,
            "youtube_id": self.youtube_id,
            "credits_text": self.credits_text,
            "hook": self.hook,
            "clip_captions": self.clip_captions_json,
            "transitions": self.transitions_json,
            "end_card": self.end_card,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
        }
