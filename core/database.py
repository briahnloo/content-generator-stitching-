"""
SQLite database layer for viral-clips-pipeline.
Provides CRUD operations for Video and Compilation models.
"""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import List, Optional, Generator

from .models import Video, Compilation, VideoStatus, CompilationStatus


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
                    category_confidence REAL DEFAULT 0,
                    classification_reasoning TEXT,
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
                    hook TEXT,
                    clip_captions TEXT,
                    transitions TEXT,
                    end_card TEXT,
                    error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status);
                CREATE INDEX IF NOT EXISTS idx_videos_category ON videos(category);
                CREATE INDEX IF NOT EXISTS idx_videos_tiktok_id ON videos(tiktok_id);
                CREATE INDEX IF NOT EXISTS idx_videos_compilation_id ON videos(compilation_id);
                CREATE INDEX IF NOT EXISTS idx_compilations_status ON compilations(status);
                CREATE INDEX IF NOT EXISTS idx_compilations_category ON compilations(category);
            """)

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

    def get_videos_for_compilation(self, compilation_id: str) -> List[Video]:
        """Get all videos assigned to a compilation, ordered by clip_order."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM videos WHERE compilation_id = ? ORDER BY clip_order",
                (compilation_id,)
            ).fetchall()
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
