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


class Platform(Enum):
    """Supported upload platforms."""
    YOUTUBE = "youtube"
    TIKTOK = "tiktok"


class ContentStrategy(Enum):
    """Content strategy for accounts."""
    FAILS = "fails"
    COMEDY = "comedy"
    MIXED = "mixed"


class UploadStatus(Enum):
    """Upload job status."""
    PENDING = "pending"
    UPLOADING = "uploading"
    SUCCESS = "success"
    FAILED = "failed"


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

    # Auto-approval
    auto_approved: bool = False          # Whether auto-approved by confidence
    confidence_score: float = 0.0        # Average classification confidence

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
            auto_approved=bool(row.get("auto_approved", False)),
            confidence_score=row.get("confidence_score", 0.0),
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
            "auto_approved": int(self.auto_approved),
            "confidence_score": self.confidence_score,
            "hook": self.hook,
            "clip_captions": self.clip_captions_json,
            "transitions": self.transitions_json,
            "end_card": self.end_card,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class Account:
    """Represents a platform account for uploading."""

    id: str                              # UUID
    platform: Platform                   # youtube | tiktok
    name: str                            # Human-readable name
    handle: str = ""                     # @username

    # Content strategy
    content_strategy: ContentStrategy = ContentStrategy.MIXED

    # Credentials (encrypted JSON)
    credentials_encrypted: str = ""

    # Rate limiting
    daily_upload_limit: int = 3
    uploads_today: int = 0
    last_upload_at: Optional[datetime] = None

    # State
    is_active: bool = True
    error: str = ""

    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)

    @classmethod
    def from_db_row(cls, row: dict) -> "Account":
        """Create Account from database row."""
        platform = Platform(row.get("platform", "youtube"))
        content_strategy = ContentStrategy(row.get("content_strategy", "mixed"))

        created_at = row.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.now()

        last_upload_at = row.get("last_upload_at")
        if isinstance(last_upload_at, str) and last_upload_at:
            last_upload_at = datetime.fromisoformat(last_upload_at)
        else:
            last_upload_at = None

        return cls(
            id=row["id"],
            platform=platform,
            name=row.get("name", ""),
            handle=row.get("handle", ""),
            content_strategy=content_strategy,
            credentials_encrypted=row.get("credentials_encrypted", ""),
            daily_upload_limit=row.get("daily_upload_limit", 3),
            uploads_today=row.get("uploads_today", 0),
            last_upload_at=last_upload_at,
            is_active=bool(row.get("is_active", True)),
            error=row.get("error", ""),
            created_at=created_at,
        )

    def to_db_dict(self) -> dict:
        """Convert to dictionary for database storage."""
        return {
            "id": self.id,
            "platform": self.platform.value,
            "name": self.name,
            "handle": self.handle,
            "content_strategy": self.content_strategy.value,
            "credentials_encrypted": self.credentials_encrypted,
            "daily_upload_limit": self.daily_upload_limit,
            "uploads_today": self.uploads_today,
            "last_upload_at": self.last_upload_at.isoformat() if self.last_upload_at else None,
            "is_active": int(self.is_active),
            "error": self.error,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class Upload:
    """Represents an upload job for a compilation to a platform account."""

    id: str                              # UUID
    compilation_id: str                  # Foreign key to compilations
    account_id: str                      # Foreign key to accounts
    platform: Platform                   # youtube | tiktok

    # Upload state
    status: UploadStatus = UploadStatus.PENDING
    platform_video_id: str = ""          # YouTube/TikTok video ID
    privacy: str = "private"             # public | private | unlisted

    # Scheduling
    scheduled_at: Optional[datetime] = None
    uploaded_at: Optional[datetime] = None

    # Error tracking
    error: str = ""
    retry_count: int = 0

    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)

    @classmethod
    def from_db_row(cls, row: dict) -> "Upload":
        """Create Upload from database row."""
        platform = Platform(row.get("platform", "youtube"))
        status = UploadStatus(row.get("status", "pending"))

        created_at = row.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.now()

        scheduled_at = row.get("scheduled_at")
        if isinstance(scheduled_at, str) and scheduled_at:
            scheduled_at = datetime.fromisoformat(scheduled_at)
        else:
            scheduled_at = None

        uploaded_at = row.get("uploaded_at")
        if isinstance(uploaded_at, str) and uploaded_at:
            uploaded_at = datetime.fromisoformat(uploaded_at)
        else:
            uploaded_at = None

        return cls(
            id=row["id"],
            compilation_id=row.get("compilation_id", ""),
            account_id=row.get("account_id", ""),
            platform=platform,
            status=status,
            platform_video_id=row.get("platform_video_id", ""),
            privacy=row.get("privacy", "private"),
            scheduled_at=scheduled_at,
            uploaded_at=uploaded_at,
            error=row.get("error", ""),
            retry_count=row.get("retry_count", 0),
            created_at=created_at,
        )

    def to_db_dict(self) -> dict:
        """Convert to dictionary for database storage."""
        return {
            "id": self.id,
            "compilation_id": self.compilation_id,
            "account_id": self.account_id,
            "platform": self.platform.value,
            "status": self.status.value,
            "platform_video_id": self.platform_video_id,
            "privacy": self.privacy,
            "scheduled_at": self.scheduled_at.isoformat() if self.scheduled_at else None,
            "uploaded_at": self.uploaded_at.isoformat() if self.uploaded_at else None,
            "error": self.error,
            "retry_count": self.retry_count,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class RoutingRule:
    """Defines content routing rules for accounts."""

    id: str                              # UUID
    account_id: str                      # Foreign key to accounts
    category: str                        # fails | comedy
    min_confidence: float = 0.7          # Minimum confidence to route
    priority: int = 1                    # Higher = preferred

    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)

    @classmethod
    def from_db_row(cls, row: dict) -> "RoutingRule":
        """Create RoutingRule from database row."""
        created_at = row.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.now()

        return cls(
            id=row["id"],
            account_id=row.get("account_id", ""),
            category=row.get("category", ""),
            min_confidence=row.get("min_confidence", 0.7),
            priority=row.get("priority", 1),
            created_at=created_at,
        )

    def to_db_dict(self) -> dict:
        """Convert to dictionary for database storage."""
        return {
            "id": self.id,
            "account_id": self.account_id,
            "category": self.category,
            "min_confidence": self.min_confidence,
            "priority": self.priority,
            "created_at": self.created_at.isoformat(),
        }
