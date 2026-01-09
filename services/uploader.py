"""
Uploader service for publishing compilations to YouTube.
Uses OAuth 2.0 for authentication.
"""

import json
import logging
import os
from pathlib import Path
from typing import Optional

from core.models import Compilation, CompilationStatus
from core.database import Database
from config.settings import settings, categories_config

logger = logging.getLogger(__name__)

# YouTube API constants
YOUTUBE_UPLOAD_SCOPE = "https://www.googleapis.com/auth/youtube.upload"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"


class UploaderService:
    """Uploads compilations to YouTube."""

    def __init__(self, db: Database):
        """Initialize uploader service."""
        self.db = db
        self._youtube = None
        self._credentials = None

    def _check_credentials(self) -> bool:
        """Check if YouTube credentials are configured."""
        return bool(
            settings.YOUTUBE_CLIENT_ID
            and settings.YOUTUBE_CLIENT_SECRET
            and settings.YOUTUBE_REFRESH_TOKEN
        )

    def _get_youtube_service(self):
        """Get authenticated YouTube service."""
        if self._youtube is not None:
            return self._youtube

        if not self._check_credentials():
            raise ValueError(
                "YouTube credentials not configured. "
                "Set YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET, and YOUTUBE_REFRESH_TOKEN"
            )

        try:
            from google.oauth2.credentials import Credentials
            from googleapiclient.discovery import build

            self._credentials = Credentials(
                None,
                refresh_token=settings.YOUTUBE_REFRESH_TOKEN,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=settings.YOUTUBE_CLIENT_ID,
                client_secret=settings.YOUTUBE_CLIENT_SECRET,
            )

            self._youtube = build(
                YOUTUBE_API_SERVICE_NAME,
                YOUTUBE_API_VERSION,
                credentials=self._credentials,
            )

            return self._youtube

        except ImportError:
            raise ImportError(
                "YouTube upload requires google-api-python-client and google-auth-oauthlib. "
                "Install with: pip install google-api-python-client google-auth-oauthlib"
            )

    def authenticate(self) -> bool:
        """
        Run OAuth flow to get refresh token.
        Prints the token for manual configuration.
        Returns True on success.
        """
        if not settings.YOUTUBE_CLIENT_ID or not settings.YOUTUBE_CLIENT_SECRET:
            logger.error("YOUTUBE_CLIENT_ID and YOUTUBE_CLIENT_SECRET must be set")
            return False

        try:
            from google_auth_oauthlib.flow import InstalledAppFlow

            client_config = {
                "installed": {
                    "client_id": settings.YOUTUBE_CLIENT_ID,
                    "client_secret": settings.YOUTUBE_CLIENT_SECRET,
                    "redirect_uris": ["http://localhost"],
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                }
            }

            flow = InstalledAppFlow.from_client_config(
                client_config, scopes=[YOUTUBE_UPLOAD_SCOPE]
            )

            credentials = flow.run_local_server(port=8080)

            print("\n" + "=" * 60)
            print("Authentication successful!")
            print("=" * 60)
            print("\nAdd this to your .env file:")
            print(f"\nYOUTUBE_REFRESH_TOKEN={credentials.refresh_token}")
            print("=" * 60 + "\n")

            return True

        except ImportError:
            logger.error(
                "OAuth flow requires google-auth-oauthlib. "
                "Install with: pip install google-auth-oauthlib"
            )
            return False
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return False

    def _build_video_metadata(self, compilation: Compilation) -> dict:
        """Build YouTube video metadata."""
        category_config = categories_config.get_category(compilation.category)

        # Build tags
        tags = [
            "shorts",
            "viral",
            "compilation",
            compilation.category,
        ]
        tags.extend(h.lstrip("#") for h in category_config.get("hashtags", [])[:5])

        # Build description
        from services.captioner import CaptionerService
        captioner = CaptionerService(self.db)
        description = captioner.generate_description(compilation)

        return {
            "snippet": {
                "title": compilation.title[:100],  # YouTube title limit
                "description": description[:5000],  # YouTube description limit
                "tags": tags[:500],  # YouTube tags limit
                "categoryId": "24",  # Entertainment
            },
            "status": {
                "privacyStatus": "private",  # Start as private
                "selfDeclaredMadeForKids": False,
            },
        }

    def upload(
        self,
        compilation: Compilation,
        privacy: str = "private",
        progress_callback: Optional[callable] = None,
    ) -> Optional[str]:
        """
        Upload a compilation to YouTube.
        Returns YouTube video ID on success, None on failure.
        """
        if compilation.status != CompilationStatus.APPROVED:
            logger.warning(
                f"Compilation {compilation.id} is not approved (status: {compilation.status})"
            )
            return None

        if not compilation.output_path or not Path(compilation.output_path).exists():
            logger.error(f"Output file not found: {compilation.output_path}")
            compilation.error = "Output file not found"
            self.db.update_compilation(compilation)
            return None

        try:
            from googleapiclient.http import MediaFileUpload

            youtube = self._get_youtube_service()

            metadata = self._build_video_metadata(compilation)
            metadata["status"]["privacyStatus"] = privacy

            media = MediaFileUpload(
                compilation.output_path,
                chunksize=1024 * 1024,  # 1MB chunks
                resumable=True,
                mimetype="video/mp4",
            )

            request = youtube.videos().insert(
                part="snippet,status",
                body=metadata,
                media_body=media,
            )

            logger.info(f"Uploading compilation {compilation.id} to YouTube")

            response = None
            while response is None:
                status, response = request.next_chunk()
                if status and progress_callback:
                    progress_callback(int(status.progress() * 100))

            video_id = response["id"]
            logger.info(f"Upload complete: https://youtube.com/watch?v={video_id}")

            # Update compilation
            compilation.youtube_id = video_id
            compilation.status = CompilationStatus.UPLOADED
            compilation.error = ""
            self.db.update_compilation(compilation)

            return video_id

        except Exception as e:
            logger.error(f"Upload failed for {compilation.id}: {e}")
            compilation.error = str(e)
            self.db.update_compilation(compilation)
            return None

    def set_public(self, video_id: str) -> bool:
        """
        Set a YouTube video to public.
        Returns True on success.
        """
        try:
            youtube = self._get_youtube_service()

            youtube.videos().update(
                part="status",
                body={
                    "id": video_id,
                    "status": {"privacyStatus": "public"},
                },
            ).execute()

            logger.info(f"Set video {video_id} to public")
            return True

        except Exception as e:
            logger.error(f"Failed to set video public: {e}")
            return False

    def get_video_status(self, video_id: str) -> Optional[dict]:
        """Get the status of a YouTube video."""
        try:
            youtube = self._get_youtube_service()

            response = youtube.videos().list(
                part="status,statistics",
                id=video_id,
            ).execute()

            items = response.get("items", [])
            if items:
                return {
                    "status": items[0].get("status", {}),
                    "statistics": items[0].get("statistics", {}),
                }
            return None

        except Exception as e:
            logger.error(f"Failed to get video status: {e}")
            return None
