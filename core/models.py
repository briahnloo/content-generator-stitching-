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
    category: str = ""                   # fails | comedy
    subcategory: str = ""                # physical | reaction | prank | verbal | social | skill
    category_confidence: float = 0.0     # 0-1 confidence score
    classification_reasoning: str = ""   # GPT's reasoning
    compilation_score: float = 0.0       # 0-1 suitability for compilation
    visual_independence: float = 0.0     # 0-1 funny without audio/context

    # Source compilation tracking (for videos that are already compilations)
    is_source_compilation: bool = False  # True if this is an existing compilation we're sourcing
    source_clip_count: int = 0           # Estimated number of clips in the source compilation
    compilation_type: str = ""           # fails | comedy | satisfying | mixed

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
            subcategory=row.get("subcategory", ""),
            category_confidence=row.get("category_confidence", 0.0),
            classification_reasoning=row.get("classification_reasoning", ""),
            compilation_score=row.get("compilation_score", 0.0),
            visual_independence=row.get("visual_independence", 0.0),
            is_source_compilation=bool(row.get("is_source_compilation", False)),
            source_clip_count=row.get("source_clip_count", 0),
            compilation_type=row.get("compilation_type", ""),
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
            "subcategory": self.subcategory,
            "category_confidence": self.category_confidence,
            "classification_reasoning": self.classification_reasoning,
            "compilation_score": self.compilation_score,
            "visual_independence": self.visual_independence,
            "is_source_compilation": int(self.is_source_compilation),
            "source_clip_count": self.source_clip_count,
            "compilation_type": self.compilation_type,
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
    daily_upload_limit: int = 6
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
            daily_upload_limit=row.get("daily_upload_limit", 6),
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


# =============================================================================
# Reddit Story Narration Models
# =============================================================================

class RedditPostStatus(Enum):
    """Reddit post processing status."""
    DISCOVERED = "discovered"      # Post scraped from Reddit
    AUDIO_READY = "audio_ready"    # TTS audio generated
    COMPOSED = "composed"          # Video composed
    UPLOADED = "uploaded"          # Uploaded to platform
    FAILED = "failed"              # Processing failed


class RedditVideoStatus(Enum):
    """Reddit video processing status."""
    PENDING = "pending"            # Not yet rendered
    REVIEW = "review"              # Ready for manual review
    APPROVED = "approved"          # Approved for upload
    UPLOADED = "uploaded"          # Uploaded to platform
    REJECTED = "rejected"          # Manually rejected


@dataclass
class RedditPost:
    """Represents a Reddit story post for narration."""

    # Identifiers
    id: str                              # Internal UUID
    reddit_id: str                       # Reddit post ID

    # Content
    subreddit: str = ""                  # Subreddit name
    title: str = ""                      # Post title
    body: str = ""                       # Post body text
    author: str = ""                     # Reddit username

    # Engagement metrics
    upvotes: int = 0
    upvote_ratio: float = 0.0
    num_comments: int = 0

    # Content analysis
    word_count: int = 0
    estimated_duration: float = 0.0      # Estimated narration duration in seconds

    # Processing state
    status: RedditPostStatus = RedditPostStatus.DISCOVERED
    audio_path: str = ""                 # Path to generated TTS audio
    word_timings: List[dict] = field(default_factory=list)  # [{word, start, end}, ...]

    # Link to generated video
    video_id: str = ""                   # Foreign key to reddit_videos

    # Error tracking
    error: str = ""

    # Timestamps
    reddit_created_at: Optional[datetime] = None
    created_at: datetime = field(default_factory=datetime.now)

    @property
    def word_timings_json(self) -> str:
        """Serialize word_timings to JSON string."""
        return json.dumps(self.word_timings)

    @property
    def full_text(self) -> str:
        """Get full text for TTS (title + body)."""
        return f"{self.title}\n\n{self.body}"

    @classmethod
    def from_db_row(cls, row: dict) -> "RedditPost":
        """Create RedditPost from database row."""
        word_timings = json.loads(row.get("word_timings", "[]"))
        status = RedditPostStatus(row.get("status", "discovered"))

        created_at = row.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.now()

        reddit_created_at = row.get("reddit_created_at")
        if isinstance(reddit_created_at, str) and reddit_created_at:
            reddit_created_at = datetime.fromisoformat(reddit_created_at)
        else:
            reddit_created_at = None

        return cls(
            id=row["id"],
            reddit_id=row.get("reddit_id", ""),
            subreddit=row.get("subreddit", ""),
            title=row.get("title", ""),
            body=row.get("body", ""),
            author=row.get("author", ""),
            upvotes=row.get("upvotes", 0),
            upvote_ratio=row.get("upvote_ratio", 0.0),
            num_comments=row.get("num_comments", 0),
            word_count=row.get("word_count", 0),
            estimated_duration=row.get("estimated_duration", 0.0),
            status=status,
            audio_path=row.get("audio_path", ""),
            word_timings=word_timings,
            video_id=row.get("video_id", ""),
            error=row.get("error", ""),
            reddit_created_at=reddit_created_at,
            created_at=created_at,
        )

    def to_db_dict(self) -> dict:
        """Convert to dictionary for database storage."""
        return {
            "id": self.id,
            "reddit_id": self.reddit_id,
            "subreddit": self.subreddit,
            "title": self.title,
            "body": self.body,
            "author": self.author,
            "upvotes": self.upvotes,
            "upvote_ratio": self.upvote_ratio,
            "num_comments": self.num_comments,
            "word_count": self.word_count,
            "estimated_duration": self.estimated_duration,
            "status": self.status.value,
            "audio_path": self.audio_path,
            "word_timings": self.word_timings_json,
            "video_id": self.video_id,
            "error": self.error,
            "reddit_created_at": self.reddit_created_at.isoformat() if self.reddit_created_at else None,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class RedditVideo:
    """Represents a composed Reddit narration video."""

    id: str                              # UUID
    post_id: str                         # Foreign key to reddit_posts

    # Content
    title: str = ""                      # Video title
    description: str = ""                # Video description

    # Video properties
    duration: float = 0.0                # Video duration in seconds
    output_path: str = ""                # Path to rendered video
    background_used: str = ""            # Background video file used

    # Processing state
    status: RedditVideoStatus = RedditVideoStatus.PENDING

    # Upload info
    youtube_id: str = ""                 # YouTube video ID after upload
    tiktok_id: str = ""                  # TikTok video ID after upload

    # Error tracking
    error: str = ""

    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)

    @classmethod
    def from_db_row(cls, row: dict) -> "RedditVideo":
        """Create RedditVideo from database row."""
        status = RedditVideoStatus(row.get("status", "pending"))

        created_at = row.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.now()

        return cls(
            id=row["id"],
            post_id=row.get("post_id", ""),
            title=row.get("title", ""),
            description=row.get("description", ""),
            duration=row.get("duration", 0.0),
            output_path=row.get("output_path", ""),
            background_used=row.get("background_used", ""),
            status=status,
            youtube_id=row.get("youtube_id", ""),
            tiktok_id=row.get("tiktok_id", ""),
            error=row.get("error", ""),
            created_at=created_at,
        )

    def to_db_dict(self) -> dict:
        """Convert to dictionary for database storage."""
        return {
            "id": self.id,
            "post_id": self.post_id,
            "title": self.title,
            "description": self.description,
            "duration": self.duration,
            "output_path": self.output_path,
            "background_used": self.background_used,
            "status": self.status.value,
            "youtube_id": self.youtube_id,
            "tiktok_id": self.tiktok_id,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
        }
