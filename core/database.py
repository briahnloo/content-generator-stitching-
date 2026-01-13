"""
SQLite database layer for viral-clips-pipeline.
Provides CRUD operations for Video and Compilation models.
"""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import List, Optional, Generator

from .models import (
    Video, Compilation, VideoStatus, CompilationStatus,
    Account, Upload, RoutingRule, Platform, ContentStrategy, UploadStatus,
    RedditPost, RedditVideo, RedditPostStatus, RedditVideoStatus
)


class Database:
    """SQLite database manager for the pipeline."""

    def __init__(self, db_path: Path):
        """Initialize database connection."""
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def _get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_schema(self) -> None:
        """Initialize database schema."""
        with self._get_connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS videos (
                    id TEXT PRIMARY KEY,
                    tiktok_id TEXT UNIQUE,
                    url TEXT,
                    description TEXT,
                    author TEXT,
                    hashtags TEXT,
                    plays INTEGER DEFAULT 0,
                    likes INTEGER DEFAULT 0,
                    shares INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'discovered',
                    local_path TEXT,
                    duration REAL DEFAULT 0,
                    width INTEGER DEFAULT 0,
                    height INTEGER DEFAULT 0,
                    category TEXT,
                    subcategory TEXT,
                    category_confidence REAL DEFAULT 0,
                    classification_reasoning TEXT,
                    compilation_score REAL DEFAULT 0,
                    visual_independence REAL DEFAULT 0,
                    compilation_id TEXT,
                    clip_order INTEGER DEFAULT 0,
                    caption TEXT,
                    error TEXT,
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS compilations (
                    id TEXT PRIMARY KEY,
                    category TEXT,
                    title TEXT,
                    description TEXT,
                    video_ids TEXT,
                    status TEXT DEFAULT 'pending',
                    output_path TEXT,
                    duration REAL DEFAULT 0,
                    music_track TEXT,
                    youtube_id TEXT,
                    credits_text TEXT,
                    auto_approved INTEGER DEFAULT 0,
                    confidence_score REAL DEFAULT 0,
                    hook TEXT,
                    clip_captions TEXT,
                    transitions TEXT,
                    end_card TEXT,
                    error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS accounts (
                    id TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    name TEXT NOT NULL,
                    handle TEXT,
                    content_strategy TEXT DEFAULT 'mixed',
                    credentials_encrypted TEXT,
                    daily_upload_limit INTEGER DEFAULT 6,
                    uploads_today INTEGER DEFAULT 0,
                    last_upload_at TIMESTAMP,
                    is_active INTEGER DEFAULT 1,
                    error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS uploads (
                    id TEXT PRIMARY KEY,
                    compilation_id TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    platform_video_id TEXT,
                    privacy TEXT DEFAULT 'private',
                    scheduled_at TIMESTAMP,
                    uploaded_at TIMESTAMP,
                    error TEXT,
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (compilation_id) REFERENCES compilations(id),
                    FOREIGN KEY (account_id) REFERENCES accounts(id)
                );

                CREATE TABLE IF NOT EXISTS routing_rules (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    category TEXT NOT NULL,
                    min_confidence REAL DEFAULT 0.7,
                    priority INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (account_id) REFERENCES accounts(id)
                );

                -- Reddit Story Narration tables
                CREATE TABLE IF NOT EXISTS reddit_posts (
                    id TEXT PRIMARY KEY,
                    reddit_id TEXT UNIQUE,
                    subreddit TEXT,
                    title TEXT,
                    body TEXT,
                    author TEXT,
                    upvotes INTEGER DEFAULT 0,
                    upvote_ratio REAL DEFAULT 0,
                    num_comments INTEGER DEFAULT 0,
                    word_count INTEGER DEFAULT 0,
                    estimated_duration REAL DEFAULT 0,
                    status TEXT DEFAULT 'discovered',
                    audio_path TEXT,
                    word_timings TEXT,
                    video_id TEXT,
                    error TEXT,
                    reddit_created_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS reddit_videos (
                    id TEXT PRIMARY KEY,
                    post_id TEXT NOT NULL,
                    title TEXT,
                    description TEXT,
                    duration REAL DEFAULT 0,
                    output_path TEXT,
                    background_used TEXT,
                    status TEXT DEFAULT 'pending',
                    youtube_id TEXT,
                    tiktok_id TEXT,
                    error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (post_id) REFERENCES reddit_posts(id)
                );

                CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status);
                CREATE INDEX IF NOT EXISTS idx_videos_category ON videos(category);
                CREATE INDEX IF NOT EXISTS idx_videos_tiktok_id ON videos(tiktok_id);
                CREATE INDEX IF NOT EXISTS idx_videos_compilation_id ON videos(compilation_id);
                CREATE INDEX IF NOT EXISTS idx_compilations_status ON compilations(status);
                CREATE INDEX IF NOT EXISTS idx_compilations_category ON compilations(category);
                CREATE INDEX IF NOT EXISTS idx_accounts_platform ON accounts(platform);
                CREATE INDEX IF NOT EXISTS idx_accounts_active ON accounts(is_active);
                CREATE INDEX IF NOT EXISTS idx_uploads_status ON uploads(status);
                CREATE INDEX IF NOT EXISTS idx_uploads_account ON uploads(account_id);
                CREATE INDEX IF NOT EXISTS idx_uploads_compilation ON uploads(compilation_id);
                CREATE INDEX IF NOT EXISTS idx_routing_account ON routing_rules(account_id);
                CREATE INDEX IF NOT EXISTS idx_routing_category ON routing_rules(category);
                CREATE INDEX IF NOT EXISTS idx_reddit_posts_status ON reddit_posts(status);
                CREATE INDEX IF NOT EXISTS idx_reddit_posts_subreddit ON reddit_posts(subreddit);
                CREATE INDEX IF NOT EXISTS idx_reddit_posts_reddit_id ON reddit_posts(reddit_id);
                CREATE INDEX IF NOT EXISTS idx_reddit_videos_status ON reddit_videos(status);
                CREATE INDEX IF NOT EXISTS idx_reddit_videos_post_id ON reddit_videos(post_id);
            """)

            # Migration: Add new columns to existing tables if they don't exist
            self._migrate_schema(conn)

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        """Add new columns to existing tables for backwards compatibility."""
        # Get existing columns in compilations table
        cursor = conn.execute("PRAGMA table_info(compilations)")
        compilation_columns = {row[1] for row in cursor.fetchall()}

        # Add auto_approved if missing
        if "auto_approved" not in compilation_columns:
            conn.execute("ALTER TABLE compilations ADD COLUMN auto_approved INTEGER DEFAULT 0")

        # Add confidence_score if missing
        if "confidence_score" not in compilation_columns:
            conn.execute("ALTER TABLE compilations ADD COLUMN confidence_score REAL DEFAULT 0")

        # Get existing columns in videos table
        cursor = conn.execute("PRAGMA table_info(videos)")
        video_columns = {row[1] for row in cursor.fetchall()}

        # Add subcategory if missing
        if "subcategory" not in video_columns:
            conn.execute("ALTER TABLE videos ADD COLUMN subcategory TEXT DEFAULT ''")

        # Add compilation_score if missing
        if "compilation_score" not in video_columns:
            conn.execute("ALTER TABLE videos ADD COLUMN compilation_score REAL DEFAULT 0")

        # Add visual_independence if missing
        if "visual_independence" not in video_columns:
            conn.execute("ALTER TABLE videos ADD COLUMN visual_independence REAL DEFAULT 0")

        # Add source compilation tracking fields
        if "is_source_compilation" not in video_columns:
            conn.execute("ALTER TABLE videos ADD COLUMN is_source_compilation INTEGER DEFAULT 0")

        if "source_clip_count" not in video_columns:
            conn.execute("ALTER TABLE videos ADD COLUMN source_clip_count INTEGER DEFAULT 0")

        if "compilation_type" not in video_columns:
            conn.execute("ALTER TABLE videos ADD COLUMN compilation_type TEXT DEFAULT ''")

        # Create index on subcategory (after column exists)
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_subcategory ON videos(subcategory)")
        except sqlite3.OperationalError:
            pass  # Index already exists or column missing

        # Create index on is_source_compilation for efficient querying
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_source_compilation ON videos(is_source_compilation)")
        except sqlite3.OperationalError:
            pass

    # =========================================================================
    # Video CRUD Operations
    # =========================================================================

    def insert_video(self, video: Video) -> bool:
        """Insert a new video record. Returns False if duplicate."""
        with self._get_connection() as conn:
            try:
                data = video.to_db_dict()
                columns = ", ".join(data.keys())
                placeholders = ", ".join(["?" for _ in data])
                conn.execute(
                    f"INSERT INTO videos ({columns}) VALUES ({placeholders})",
                    list(data.values())
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def update_video(self, video: Video) -> None:
        """Update an existing video record."""
        with self._get_connection() as conn:
            data = video.to_db_dict()
            video_id = data.pop("id")
            set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
            conn.execute(
                f"UPDATE videos SET {set_clause} WHERE id = ?",
                list(data.values()) + [video_id]
            )

    def get_video(self, video_id: str) -> Optional[Video]:
        """Get a video by ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM videos WHERE id = ?", (video_id,)
            ).fetchone()
            return Video.from_db_row(dict(row)) if row else None

    def get_video_by_tiktok_id(self, tiktok_id: str) -> Optional[Video]:
        """Get a video by TikTok ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM videos WHERE tiktok_id = ?", (tiktok_id,)
            ).fetchone()
            return Video.from_db_row(dict(row)) if row else None

    def get_videos_by_status(
        self, status: VideoStatus, limit: Optional[int] = None
    ) -> List[Video]:
        """Get videos by status."""
        with self._get_connection() as conn:
            query = "SELECT * FROM videos WHERE status = ? ORDER BY created_at DESC"
            if limit:
                query += f" LIMIT {limit}"
            rows = conn.execute(query, (status.value,)).fetchall()
            return [Video.from_db_row(dict(row)) for row in rows]

    def get_videos_by_category(
        self,
        category: str,
        status: Optional[VideoStatus] = None,
        unassigned_only: bool = False,
    ) -> List[Video]:
        """Get videos by category with optional filters."""
        with self._get_connection() as conn:
            query = "SELECT * FROM videos WHERE category = ?"
            params = [category]

            if status:
                query += " AND status = ?"
                params.append(status.value)

            if unassigned_only:
                query += " AND (compilation_id IS NULL OR compilation_id = '')"

            query += " ORDER BY (likes + shares * 2) DESC"
            rows = conn.execute(query, params).fetchall()
            return [Video.from_db_row(dict(row)) for row in rows]

    def get_videos_by_subcategory(
        self,
        category: str,
        subcategory: str,
        status: Optional[VideoStatus] = None,
        unassigned_only: bool = False,
    ) -> List[Video]:
        """Get videos by category and subcategory with optional filters."""
        with self._get_connection() as conn:
            query = "SELECT * FROM videos WHERE category = ? AND subcategory = ?"
            params = [category, subcategory]

            if status:
                query += " AND status = ?"
                params.append(status.value)

            if unassigned_only:
                query += " AND (compilation_id IS NULL OR compilation_id = '')"

            # Sort by compilation_score first, then engagement
            query += " ORDER BY compilation_score DESC, (likes + shares * 2) DESC"
            rows = conn.execute(query, params).fetchall()
            return [Video.from_db_row(dict(row)) for row in rows]

    def get_available_subcategories(
        self,
        category: str,
        min_videos: int = 5,
        status: VideoStatus = VideoStatus.CLASSIFIED,
    ) -> dict:
        """Get subcategories with enough videos for compilation."""
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT subcategory, COUNT(*) as count FROM videos
                   WHERE category = ? AND status = ? AND subcategory != ''
                   AND (compilation_id IS NULL OR compilation_id = '')
                   GROUP BY subcategory
                   HAVING count >= ?
                   ORDER BY count DESC""",
                (category, status.value, min_videos)
            ).fetchall()
            return {row["subcategory"]: row["count"] for row in rows}

    def get_videos_for_compilation(self, compilation_id: str) -> List[Video]:
        """Get all videos assigned to a compilation, ordered by clip_order."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM videos WHERE compilation_id = ? ORDER BY clip_order",
                (compilation_id,)
            ).fetchall()
            return [Video.from_db_row(dict(row)) for row in rows]

    def get_source_compilations(
        self,
        status: Optional[VideoStatus] = None,
        compilation_type: Optional[str] = None,
        unassigned_only: bool = False,
        limit: Optional[int] = None,
    ) -> List[Video]:
        """Get videos that are source compilations (existing compilations from TikTok)."""
        with self._get_connection() as conn:
            query = "SELECT * FROM videos WHERE is_source_compilation = 1"
            params = []

            if status:
                query += " AND status = ?"
                params.append(status.value)

            if compilation_type:
                query += " AND compilation_type = ?"
                params.append(compilation_type)

            if unassigned_only:
                query += " AND (compilation_id IS NULL OR compilation_id = '')"

            # Sort by quality and duration (longer = more content)
            query += " ORDER BY compilation_score DESC, duration DESC"

            if limit:
                query += f" LIMIT {limit}"

            rows = conn.execute(query, params).fetchall()
            return [Video.from_db_row(dict(row)) for row in rows]

    def count_videos_by_status(self) -> dict:
        """Get count of videos for each status."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) as count FROM videos GROUP BY status"
            ).fetchall()
            return {row["status"]: row["count"] for row in rows}

    def count_videos_by_category(self, status: Optional[VideoStatus] = None) -> dict:
        """Get count of videos for each category."""
        with self._get_connection() as conn:
            if status:
                rows = conn.execute(
                    """SELECT category, COUNT(*) as count FROM videos
                       WHERE status = ? AND category != '' GROUP BY category""",
                    (status.value,)
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT category, COUNT(*) as count FROM videos
                       WHERE category != '' GROUP BY category"""
                ).fetchall()
            return {row["category"]: row["count"] for row in rows}

    def tiktok_id_exists(self, tiktok_id: str) -> bool:
        """Check if a TikTok ID already exists in the database."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM videos WHERE tiktok_id = ? LIMIT 1", (tiktok_id,)
            ).fetchone()
            return row is not None

    def delete_video(self, video_id: str) -> None:
        """Delete a video record."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM videos WHERE id = ?", (video_id,))

    # =========================================================================
    # Compilation CRUD Operations
    # =========================================================================

    def insert_compilation(self, compilation: Compilation) -> bool:
        """Insert a new compilation record."""
        with self._get_connection() as conn:
            try:
                data = compilation.to_db_dict()
                columns = ", ".join(data.keys())
                placeholders = ", ".join(["?" for _ in data])
                conn.execute(
                    f"INSERT INTO compilations ({columns}) VALUES ({placeholders})",
                    list(data.values())
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def update_compilation(self, compilation: Compilation) -> None:
        """Update an existing compilation record."""
        with self._get_connection() as conn:
            data = compilation.to_db_dict()
            comp_id = data.pop("id")
            set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
            conn.execute(
                f"UPDATE compilations SET {set_clause} WHERE id = ?",
                list(data.values()) + [comp_id]
            )

    def get_compilation(self, compilation_id: str) -> Optional[Compilation]:
        """Get a compilation by ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM compilations WHERE id = ?", (compilation_id,)
            ).fetchone()
            return Compilation.from_db_row(dict(row)) if row else None

    def get_compilations_by_status(
        self, status: CompilationStatus, limit: Optional[int] = None
    ) -> List[Compilation]:
        """Get compilations by status."""
        with self._get_connection() as conn:
            query = "SELECT * FROM compilations WHERE status = ? ORDER BY created_at DESC"
            if limit:
                query += f" LIMIT {limit}"
            rows = conn.execute(query, (status.value,)).fetchall()
            return [Compilation.from_db_row(dict(row)) for row in rows]

    def get_all_compilations(self) -> List[Compilation]:
        """Get all compilations."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM compilations ORDER BY created_at DESC"
            ).fetchall()
            return [Compilation.from_db_row(dict(row)) for row in rows]

    def count_compilations_by_status(self) -> dict:
        """Get count of compilations for each status."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) as count FROM compilations GROUP BY status"
            ).fetchall()
            return {row["status"]: row["count"] for row in rows}

    def delete_compilation(self, compilation_id: str) -> None:
        """Delete a compilation and unassign its videos."""
        with self._get_connection() as conn:
            # Unassign videos
            conn.execute(
                """UPDATE videos
                   SET compilation_id = '', clip_order = 0, status = 'classified'
                   WHERE compilation_id = ?""",
                (compilation_id,)
            )
            # Delete compilation
            conn.execute("DELETE FROM compilations WHERE id = ?", (compilation_id,))

    # =========================================================================
    # Utility Operations
    # =========================================================================

    def reset_database(self) -> None:
        """Clear all data from the database."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM videos")
            conn.execute("DELETE FROM compilations")

    def get_stats(self) -> dict:
        """Get overall pipeline statistics."""
        video_status_counts = self.count_videos_by_status()
        compilation_status_counts = self.count_compilations_by_status()
        category_counts = self.count_videos_by_category(VideoStatus.CLASSIFIED)

        with self._get_connection() as conn:
            total_videos = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
            total_compilations = conn.execute("SELECT COUNT(*) FROM compilations").fetchone()[0]

        return {
            "total_videos": total_videos,
            "total_compilations": total_compilations,
            "videos_by_status": video_status_counts,
            "compilations_by_status": compilation_status_counts,
            "videos_by_category": category_counts,
        }

    # =========================================================================
    # Account CRUD Operations
    # =========================================================================

    def insert_account(self, account: Account) -> bool:
        """Insert a new account record."""
        with self._get_connection() as conn:
            try:
                data = account.to_db_dict()
                columns = ", ".join(data.keys())
                placeholders = ", ".join(["?" for _ in data])
                conn.execute(
                    f"INSERT INTO accounts ({columns}) VALUES ({placeholders})",
                    list(data.values())
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def update_account(self, account: Account) -> None:
        """Update an existing account record."""
        with self._get_connection() as conn:
            data = account.to_db_dict()
            account_id = data.pop("id")
            set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
            conn.execute(
                f"UPDATE accounts SET {set_clause} WHERE id = ?",
                list(data.values()) + [account_id]
            )

    def get_account(self, account_id: str) -> Optional[Account]:
        """Get an account by ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM accounts WHERE id = ?", (account_id,)
            ).fetchone()
            return Account.from_db_row(dict(row)) if row else None

    def get_accounts_by_platform(
        self, platform: Platform, active_only: bool = True
    ) -> List[Account]:
        """Get accounts by platform."""
        with self._get_connection() as conn:
            if active_only:
                rows = conn.execute(
                    "SELECT * FROM accounts WHERE platform = ? AND is_active = 1 ORDER BY name",
                    (platform.value,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM accounts WHERE platform = ? ORDER BY name",
                    (platform.value,)
                ).fetchall()
            return [Account.from_db_row(dict(row)) for row in rows]

    def get_accounts_by_strategy(
        self, strategy: ContentStrategy, platform: Optional[Platform] = None
    ) -> List[Account]:
        """Get accounts by content strategy."""
        with self._get_connection() as conn:
            if platform:
                rows = conn.execute(
                    """SELECT * FROM accounts
                       WHERE content_strategy = ? AND platform = ? AND is_active = 1
                       ORDER BY name""",
                    (strategy.value, platform.value)
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT * FROM accounts
                       WHERE content_strategy = ? AND is_active = 1
                       ORDER BY name""",
                    (strategy.value,)
                ).fetchall()
            return [Account.from_db_row(dict(row)) for row in rows]

    def get_all_accounts(self, active_only: bool = True) -> List[Account]:
        """Get all accounts."""
        with self._get_connection() as conn:
            if active_only:
                rows = conn.execute(
                    "SELECT * FROM accounts WHERE is_active = 1 ORDER BY platform, name"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM accounts ORDER BY platform, name"
                ).fetchall()
            return [Account.from_db_row(dict(row)) for row in rows]

    def delete_account(self, account_id: str) -> None:
        """Delete an account and its routing rules."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM routing_rules WHERE account_id = ?", (account_id,))
            conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))

    def reset_daily_upload_counts(self) -> None:
        """Reset uploads_today to 0 for all accounts (call daily)."""
        with self._get_connection() as conn:
            conn.execute("UPDATE accounts SET uploads_today = 0")

    def increment_upload_count(self, account_id: str) -> None:
        """Increment the uploads_today counter for an account."""
        with self._get_connection() as conn:
            conn.execute(
                """UPDATE accounts
                   SET uploads_today = uploads_today + 1, last_upload_at = CURRENT_TIMESTAMP
                   WHERE id = ?""",
                (account_id,)
            )

    # =========================================================================
    # Upload CRUD Operations
    # =========================================================================

    def insert_upload(self, upload: Upload) -> bool:
        """Insert a new upload record."""
        with self._get_connection() as conn:
            try:
                data = upload.to_db_dict()
                columns = ", ".join(data.keys())
                placeholders = ", ".join(["?" for _ in data])
                conn.execute(
                    f"INSERT INTO uploads ({columns}) VALUES ({placeholders})",
                    list(data.values())
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def update_upload(self, upload: Upload) -> None:
        """Update an existing upload record."""
        with self._get_connection() as conn:
            data = upload.to_db_dict()
            upload_id = data.pop("id")
            set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
            conn.execute(
                f"UPDATE uploads SET {set_clause} WHERE id = ?",
                list(data.values()) + [upload_id]
            )

    def get_upload(self, upload_id: str) -> Optional[Upload]:
        """Get an upload by ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM uploads WHERE id = ?", (upload_id,)
            ).fetchone()
            return Upload.from_db_row(dict(row)) if row else None

    def get_uploads_by_status(
        self, status: UploadStatus, limit: Optional[int] = None
    ) -> List[Upload]:
        """Get uploads by status."""
        with self._get_connection() as conn:
            query = "SELECT * FROM uploads WHERE status = ? ORDER BY created_at"
            if limit:
                query += f" LIMIT {limit}"
            rows = conn.execute(query, (status.value,)).fetchall()
            return [Upload.from_db_row(dict(row)) for row in rows]

    def get_uploads_for_compilation(self, compilation_id: str) -> List[Upload]:
        """Get all uploads for a compilation."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM uploads WHERE compilation_id = ? ORDER BY created_at",
                (compilation_id,)
            ).fetchall()
            return [Upload.from_db_row(dict(row)) for row in rows]

    def get_uploads_for_account(
        self, account_id: str, status: Optional[UploadStatus] = None
    ) -> List[Upload]:
        """Get uploads for an account."""
        with self._get_connection() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM uploads WHERE account_id = ? AND status = ? ORDER BY created_at DESC",
                    (account_id, status.value)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM uploads WHERE account_id = ? ORDER BY created_at DESC",
                    (account_id,)
                ).fetchall()
            return [Upload.from_db_row(dict(row)) for row in rows]

    def get_pending_uploads(self, limit: Optional[int] = None) -> List[Upload]:
        """Get pending uploads ordered by scheduled time."""
        with self._get_connection() as conn:
            query = """SELECT * FROM uploads
                       WHERE status = 'pending'
                       ORDER BY COALESCE(scheduled_at, created_at)"""
            if limit:
                query += f" LIMIT {limit}"
            rows = conn.execute(query).fetchall()
            return [Upload.from_db_row(dict(row)) for row in rows]

    def upload_exists_for_compilation_account(
        self, compilation_id: str, account_id: str
    ) -> bool:
        """Check if an upload already exists for this compilation/account pair."""
        with self._get_connection() as conn:
            row = conn.execute(
                """SELECT 1 FROM uploads
                   WHERE compilation_id = ? AND account_id = ? LIMIT 1""",
                (compilation_id, account_id)
            ).fetchone()
            return row is not None

    def delete_upload(self, upload_id: str) -> None:
        """Delete an upload record."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM uploads WHERE id = ?", (upload_id,))

    # =========================================================================
    # Routing Rule CRUD Operations
    # =========================================================================

    def insert_routing_rule(self, rule: RoutingRule) -> bool:
        """Insert a new routing rule."""
        with self._get_connection() as conn:
            try:
                data = rule.to_db_dict()
                columns = ", ".join(data.keys())
                placeholders = ", ".join(["?" for _ in data])
                conn.execute(
                    f"INSERT INTO routing_rules ({columns}) VALUES ({placeholders})",
                    list(data.values())
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def update_routing_rule(self, rule: RoutingRule) -> None:
        """Update an existing routing rule."""
        with self._get_connection() as conn:
            data = rule.to_db_dict()
            rule_id = data.pop("id")
            set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
            conn.execute(
                f"UPDATE routing_rules SET {set_clause} WHERE id = ?",
                list(data.values()) + [rule_id]
            )

    def get_routing_rule(self, rule_id: str) -> Optional[RoutingRule]:
        """Get a routing rule by ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM routing_rules WHERE id = ?", (rule_id,)
            ).fetchone()
            return RoutingRule.from_db_row(dict(row)) if row else None

    def get_routing_rules_for_account(self, account_id: str) -> List[RoutingRule]:
        """Get all routing rules for an account."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM routing_rules WHERE account_id = ? ORDER BY priority DESC",
                (account_id,)
            ).fetchall()
            return [RoutingRule.from_db_row(dict(row)) for row in rows]

    def get_routing_rules_for_category(self, category: str) -> List[RoutingRule]:
        """Get all routing rules for a category, ordered by priority."""
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT r.* FROM routing_rules r
                   JOIN accounts a ON r.account_id = a.id
                   WHERE r.category = ? AND a.is_active = 1
                   ORDER BY r.priority DESC""",
                (category,)
            ).fetchall()
            return [RoutingRule.from_db_row(dict(row)) for row in rows]

    def get_all_routing_rules(self) -> List[RoutingRule]:
        """Get all routing rules."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM routing_rules ORDER BY account_id, priority DESC"
            ).fetchall()
            return [RoutingRule.from_db_row(dict(row)) for row in rows]

    def delete_routing_rule(self, rule_id: str) -> None:
        """Delete a routing rule."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM routing_rules WHERE id = ?", (rule_id,))

    # =========================================================================
    # Reddit Post CRUD Operations
    # =========================================================================

    def insert_reddit_post(self, post: RedditPost) -> bool:
        """Insert a new Reddit post record. Returns False if duplicate."""
        with self._get_connection() as conn:
            try:
                data = post.to_db_dict()
                columns = ", ".join(data.keys())
                placeholders = ", ".join(["?" for _ in data])
                conn.execute(
                    f"INSERT INTO reddit_posts ({columns}) VALUES ({placeholders})",
                    list(data.values())
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def update_reddit_post(self, post: RedditPost) -> None:
        """Update an existing Reddit post record."""
        with self._get_connection() as conn:
            data = post.to_db_dict()
            post_id = data.pop("id")
            set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
            conn.execute(
                f"UPDATE reddit_posts SET {set_clause} WHERE id = ?",
                list(data.values()) + [post_id]
            )

    def get_reddit_post(self, post_id: str) -> Optional[RedditPost]:
        """Get a Reddit post by ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM reddit_posts WHERE id = ?", (post_id,)
            ).fetchone()
            return RedditPost.from_db_row(dict(row)) if row else None

    def get_reddit_post_by_reddit_id(self, reddit_id: str) -> Optional[RedditPost]:
        """Get a Reddit post by Reddit ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM reddit_posts WHERE reddit_id = ?", (reddit_id,)
            ).fetchone()
            return RedditPost.from_db_row(dict(row)) if row else None

    def get_reddit_posts_by_status(
        self, status: RedditPostStatus, limit: Optional[int] = None
    ) -> List[RedditPost]:
        """Get Reddit posts by status."""
        with self._get_connection() as conn:
            query = "SELECT * FROM reddit_posts WHERE status = ? ORDER BY upvotes DESC"
            if limit:
                query += f" LIMIT {limit}"
            rows = conn.execute(query, (status.value,)).fetchall()
            return [RedditPost.from_db_row(dict(row)) for row in rows]

    def get_reddit_posts_by_subreddit(
        self, subreddit: str, status: Optional[RedditPostStatus] = None
    ) -> List[RedditPost]:
        """Get Reddit posts by subreddit."""
        with self._get_connection() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM reddit_posts WHERE subreddit = ? AND status = ? ORDER BY upvotes DESC",
                    (subreddit, status.value)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM reddit_posts WHERE subreddit = ? ORDER BY upvotes DESC",
                    (subreddit,)
                ).fetchall()
            return [RedditPost.from_db_row(dict(row)) for row in rows]

    def reddit_id_exists(self, reddit_id: str) -> bool:
        """Check if a Reddit ID already exists in the database."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM reddit_posts WHERE reddit_id = ? LIMIT 1", (reddit_id,)
            ).fetchone()
            return row is not None

    def count_reddit_posts_by_status(self) -> dict:
        """Get count of Reddit posts for each status."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) as count FROM reddit_posts GROUP BY status"
            ).fetchall()
            return {row["status"]: row["count"] for row in rows}

    def delete_reddit_post(self, post_id: str) -> None:
        """Delete a Reddit post and its associated video."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM reddit_videos WHERE post_id = ?", (post_id,))
            conn.execute("DELETE FROM reddit_posts WHERE id = ?", (post_id,))

    # =========================================================================
    # Reddit Video CRUD Operations
    # =========================================================================

    def insert_reddit_video(self, video: RedditVideo) -> bool:
        """Insert a new Reddit video record."""
        with self._get_connection() as conn:
            try:
                data = video.to_db_dict()
                columns = ", ".join(data.keys())
                placeholders = ", ".join(["?" for _ in data])
                conn.execute(
                    f"INSERT INTO reddit_videos ({columns}) VALUES ({placeholders})",
                    list(data.values())
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def update_reddit_video(self, video: RedditVideo) -> None:
        """Update an existing Reddit video record."""
        with self._get_connection() as conn:
            data = video.to_db_dict()
            video_id = data.pop("id")
            set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
            conn.execute(
                f"UPDATE reddit_videos SET {set_clause} WHERE id = ?",
                list(data.values()) + [video_id]
            )

    def get_reddit_video(self, video_id: str) -> Optional[RedditVideo]:
        """Get a Reddit video by ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM reddit_videos WHERE id = ?", (video_id,)
            ).fetchone()
            return RedditVideo.from_db_row(dict(row)) if row else None

    def get_reddit_video_by_post_id(self, post_id: str) -> Optional[RedditVideo]:
        """Get a Reddit video by post ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM reddit_videos WHERE post_id = ?", (post_id,)
            ).fetchone()
            return RedditVideo.from_db_row(dict(row)) if row else None

    def get_reddit_videos_by_status(
        self, status: RedditVideoStatus, limit: Optional[int] = None
    ) -> List[RedditVideo]:
        """Get Reddit videos by status."""
        with self._get_connection() as conn:
            query = "SELECT * FROM reddit_videos WHERE status = ? ORDER BY created_at DESC"
            if limit:
                query += f" LIMIT {limit}"
            rows = conn.execute(query, (status.value,)).fetchall()
            return [RedditVideo.from_db_row(dict(row)) for row in rows]

    def get_all_reddit_videos(self) -> List[RedditVideo]:
        """Get all Reddit videos."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM reddit_videos ORDER BY created_at DESC"
            ).fetchall()
            return [RedditVideo.from_db_row(dict(row)) for row in rows]

    def count_reddit_videos_by_status(self) -> dict:
        """Get count of Reddit videos for each status."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) as count FROM reddit_videos GROUP BY status"
            ).fetchall()
            return {row["status"]: row["count"] for row in rows}

    def delete_reddit_video(self, video_id: str) -> None:
        """Delete a Reddit video record."""
        with self._get_connection() as conn:
            # Clear video_id reference in post
            conn.execute(
                "UPDATE reddit_posts SET video_id = '' WHERE video_id = ?",
                (video_id,)
            )
            conn.execute("DELETE FROM reddit_videos WHERE id = ?", (video_id,))

    def get_reddit_stats(self) -> dict:
        """Get Reddit pipeline statistics."""
        post_status_counts = self.count_reddit_posts_by_status()
        video_status_counts = self.count_reddit_videos_by_status()

        with self._get_connection() as conn:
            total_posts = conn.execute("SELECT COUNT(*) FROM reddit_posts").fetchone()[0]
            total_videos = conn.execute("SELECT COUNT(*) FROM reddit_videos").fetchone()[0]

        return {
            "total_posts": total_posts,
            "total_videos": total_videos,
            "posts_by_status": post_status_counts,
            "videos_by_status": video_status_counts,
        }
