"""
YouTube uploader service for publishing videos via YouTube Data API v3.
Uses OAuth 2.0 for authentication with support for multi-account credentials.
"""

import logging
from pathlib import Path
from typing import Optional, Dict

from core.database import Database
from core.models import (
    Compilation, CompilationStatus, Account, Upload, UploadStatus, Platform
)
from config.settings import settings, categories_config
from services.account_manager import AccountManager

logger = logging.getLogger(__name__)

# YouTube API constants
YOUTUBE_UPLOAD_SCOPE = "https://www.googleapis.com/auth/youtube.upload"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"


class YouTubeUploader:
    """Uploads videos to YouTube using OAuth 2.0."""

    def __init__(self, db: Database, account_manager: Optional[AccountManager] = None):
        """Initialize YouTube uploader."""
        self.db = db
        self.account_manager = account_manager or AccountManager(db)
        self._youtube_services: Dict[str, object] = {}  # Cache per account

    def _get_youtube_service(self, account: Account):
        """
        Get authenticated YouTube service for an account.

        Args:
            account: Account with encrypted YouTube credentials

        Returns:
            YouTube API service object
        """
        # Check cache
        if account.id in self._youtube_services:
            return self._youtube_services[account.id]

        # Get credentials
        credentials = self.account_manager.get_credentials(account.id)
        if not credentials:
            raise ValueError(f"No credentials found for account {account.name}")

        required = ["client_id", "client_secret", "refresh_token"]
        for key in required:
            if key not in credentials:
                raise ValueError(f"Missing {key} in credentials for {account.name}")

        try:
            from google.oauth2.credentials import Credentials
            from googleapiclient.discovery import build

            oauth_credentials = Credentials(
                None,
                refresh_token=credentials["refresh_token"],
                token_uri="https://oauth2.googleapis.com/token",
                client_id=credentials["client_id"],
                client_secret=credentials["client_secret"],
            )

            youtube = build(
                YOUTUBE_API_SERVICE_NAME,
                YOUTUBE_API_VERSION,
                credentials=oauth_credentials,
            )

            # Cache the service
            self._youtube_services[account.id] = youtube
            return youtube

        except ImportError:
            raise ImportError(
                "YouTube upload requires google-api-python-client and google-auth-oauthlib. "
                "Install with: pip install google-api-python-client google-auth-oauthlib"
            )

    def _build_video_metadata(
        self,
        compilation: Compilation,
        privacy: str = "public",
    ) -> dict:
        """Build YouTube video metadata from compilation."""
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
        description_parts = [
            compilation.title,
            "",
            "A compilation of the best viral moments!",
            "",
        ]

        if compilation.credits_text:
            description_parts.append(f"Credits: {compilation.credits_text}")
            description_parts.append("")

        # Add hashtags
        hashtag_list = category_config.get("hashtags", [])[:10]
        if hashtag_list:
            description_parts.append(" ".join(hashtag_list))

        description = "\n".join(description_parts)

        return {
            "snippet": {
                "title": compilation.title[:100],  # YouTube title limit
                "description": description[:5000],  # YouTube description limit
                "tags": tags[:500],  # YouTube tags limit
                "categoryId": "24",  # Entertainment
            },
            "status": {
                "privacyStatus": privacy,
                "selfDeclaredMadeForKids": False,
            },
        }

    def upload(
        self,
        upload: Upload,
        account: Account,
        compilation: Compilation,
        progress_callback: Optional[callable] = None,
    ) -> Optional[str]:
        """
        Upload a compilation to YouTube.

        Args:
            upload: Upload job object
            account: YouTube account to upload to
            compilation: Compilation to upload
            progress_callback: Optional callback for progress updates (0-100)

        Returns:
            YouTube video ID on success, None on failure
        """
        if account.platform != Platform.YOUTUBE:
            logger.error(f"Account {account.name} is not a YouTube account")
            return None

        if not compilation.output_path or not Path(compilation.output_path).exists():
            logger.error(f"Output file not found: {compilation.output_path}")
            return None

        try:
            from googleapiclient.http import MediaFileUpload

            youtube = self._get_youtube_service(account)

            metadata = self._build_video_metadata(compilation, upload.privacy)

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

            logger.info(
                f"Uploading compilation {compilation.id} to YouTube "
                f"via {account.name}"
            )

            response = None
            while response is None:
                status, response = request.next_chunk()
                if status and progress_callback:
                    progress_callback(int(status.progress() * 100))

            video_id = response["id"]
            logger.info(
                f"Upload complete: https://youtube.com/watch?v={video_id}"
            )

            return video_id

        except Exception as e:
            logger.error(f"Upload failed: {e}")
            raise

    def set_privacy(
        self,
        account: Account,
        video_id: str,
        privacy: str,
    ) -> bool:
        """
        Update privacy status of a YouTube video.

        Args:
            account: YouTube account that owns the video
            video_id: YouTube video ID
            privacy: "public", "private", or "unlisted"

        Returns:
            True on success
        """
        try:
            youtube = self._get_youtube_service(account)

            youtube.videos().update(
                part="status",
                body={
                    "id": video_id,
                    "status": {"privacyStatus": privacy},
                },
            ).execute()

            logger.info(f"Set video {video_id} to {privacy}")
            return True

        except Exception as e:
            logger.error(f"Failed to set privacy: {e}")
            return False

    def get_video_status(
        self,
        account: Account,
        video_id: str,
    ) -> Optional[Dict]:
        """Get status and statistics of a YouTube video."""
        try:
            youtube = self._get_youtube_service(account)

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

    @staticmethod
    def run_oauth_flow(client_id: str, client_secret: str) -> Optional[str]:
        """
        Run OAuth flow to get refresh token.

        Args:
            client_id: Google OAuth client ID
            client_secret: Google OAuth client secret

        Returns:
            Refresh token on success, None on failure
        """
        try:
            from google_auth_oauthlib.flow import InstalledAppFlow

            client_config = {
                "installed": {
                    "client_id": client_id,
                    "client_secret": client_secret,
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
            print(f"\nRefresh Token: {credentials.refresh_token}")
            print("=" * 60 + "\n")

            return credentials.refresh_token

        except ImportError:
            logger.error(
                "OAuth flow requires google-auth-oauthlib. "
                "Install with: pip install google-auth-oauthlib"
            )
            return None
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return None


# Legacy compatibility: UploaderService alias
class UploaderService(YouTubeUploader):
    """Legacy alias for YouTubeUploader for backwards compatibility."""

    def __init__(self, db: Database):
        """Initialize with legacy interface."""
        super().__init__(db)
        self._legacy_mode = True

    def _check_credentials(self) -> bool:
        """Check if legacy YouTube credentials are configured."""
        return bool(
            settings.YOUTUBE_CLIENT_ID
            and settings.YOUTUBE_CLIENT_SECRET
            and settings.YOUTUBE_REFRESH_TOKEN
        )

    def authenticate(self) -> bool:
        """Run OAuth flow using legacy environment credentials."""
        if not settings.YOUTUBE_CLIENT_ID or not settings.YOUTUBE_CLIENT_SECRET:
            logger.error("YOUTUBE_CLIENT_ID and YOUTUBE_CLIENT_SECRET must be set")
            return False

        result = self.run_oauth_flow(
            settings.YOUTUBE_CLIENT_ID,
            settings.YOUTUBE_CLIENT_SECRET,
        )
        return result is not None

    def upload_legacy(
        self,
        compilation: Compilation,
        privacy: str = "public",
        progress_callback: Optional[callable] = None,
    ) -> Optional[str]:
        """
        Legacy upload method using environment credentials.

        This method is for backwards compatibility with the original interface.
        New code should use the multi-account upload() method.
        """
        if not self._check_credentials():
            raise ValueError(
                "YouTube credentials not configured. "
                "Set YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET, and YOUTUBE_REFRESH_TOKEN"
            )

        if compilation.status != CompilationStatus.APPROVED:
            logger.warning(
                f"Compilation {compilation.id} is not approved "
                f"(status: {compilation.status})"
            )
            return None

        if not compilation.output_path or not Path(compilation.output_path).exists():
            logger.error(f"Output file not found: {compilation.output_path}")
            return None

        try:
            from google.oauth2.credentials import Credentials
            from googleapiclient.discovery import build
            from googleapiclient.http import MediaFileUpload

            credentials = Credentials(
                None,
                refresh_token=settings.YOUTUBE_REFRESH_TOKEN,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=settings.YOUTUBE_CLIENT_ID,
                client_secret=settings.YOUTUBE_CLIENT_SECRET,
            )

            youtube = build(
                YOUTUBE_API_SERVICE_NAME,
                YOUTUBE_API_VERSION,
                credentials=credentials,
            )

            metadata = self._build_video_metadata(compilation, privacy)

            media = MediaFileUpload(
                compilation.output_path,
                chunksize=1024 * 1024,
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
            logger.error(f"Upload failed: {e}")
            compilation.error = str(e)
            self.db.update_compilation(compilation)
            return None
