"""
TikTok uploader service using tiktok-uploader library.
Uses browser cookies for authentication.
"""

import json
import logging
import tempfile
from pathlib import Path
from typing import Optional, Dict, List

from core.database import Database
from core.models import (
    Compilation, Account, Upload, Platform
)
from config.settings import settings, categories_config
from services.account_manager import AccountManager

logger = logging.getLogger(__name__)


class TikTokUploader:
    """Uploads videos to TikTok using browser cookies."""

    def __init__(self, db: Database, account_manager: Optional[AccountManager] = None):
        """Initialize TikTok uploader."""
        self.db = db
        self.account_manager = account_manager or AccountManager(db)

    def _get_cookies_path(self, account: Account) -> Optional[Path]:
        """
        Get or create cookies file for an account.

        Credentials for TikTok should contain:
        - cookies: List of cookie dictionaries from browser
        """
        credentials = self.account_manager.get_credentials(account.id)
        if not credentials:
            logger.error(f"No credentials found for account {account.name}")
            return None

        cookies = credentials.get("cookies")
        if not cookies:
            logger.error(f"No cookies found in credentials for {account.name}")
            return None

        # Write cookies to temp file (tiktok-uploader expects a file path)
        cookies_dir = Path(tempfile.gettempdir()) / "tiktok_cookies"
        cookies_dir.mkdir(exist_ok=True)

        cookies_path = cookies_dir / f"{account.id}_cookies.json"

        try:
            with open(cookies_path, "w") as f:
                json.dump(cookies, f)
            return cookies_path
        except Exception as e:
            logger.error(f"Failed to write cookies file: {e}")
            return None

    def _build_description(self, compilation: Compilation) -> str:
        """Build TikTok description with hashtags."""
        category_config = categories_config.get_category(compilation.category)

        parts = [compilation.title]

        # Add category hashtags
        hashtags = category_config.get("hashtags", [])[:10]
        if hashtags:
            parts.append("")
            parts.append(" ".join(hashtags))

        # Always add viral hashtags
        parts.append("#fyp #viral #foryou #foryoupage")

        return "\n".join(parts)[:2200]  # TikTok description limit

    def upload(
        self,
        upload: Upload,
        account: Account,
        compilation: Compilation,
        progress_callback: Optional[callable] = None,
    ) -> Optional[str]:
        """
        Upload a compilation to TikTok.

        Args:
            upload: Upload job object
            account: TikTok account to upload to
            compilation: Compilation to upload
            progress_callback: Optional callback (not fully supported by tiktok-uploader)

        Returns:
            TikTok video ID on success, None on failure

        Note:
            tiktok-uploader doesn't return video IDs reliably.
            Returns account handle as placeholder on success.
        """
        if account.platform != Platform.TIKTOK:
            logger.error(f"Account {account.name} is not a TikTok account")
            return None

        if not compilation.output_path or not Path(compilation.output_path).exists():
            logger.error(f"Output file not found: {compilation.output_path}")
            return None

        cookies_path = self._get_cookies_path(account)
        if not cookies_path:
            return None

        try:
            from tiktok_uploader.upload import upload_video

            description = self._build_description(compilation)

            logger.info(
                f"Uploading compilation {compilation.id} to TikTok "
                f"via {account.name}"
            )

            if progress_callback:
                progress_callback(10)

            # tiktok-uploader upload
            # Note: This library uses Selenium/browser automation
            upload_video(
                filename=compilation.output_path,
                description=description,
                cookies=str(cookies_path),
                headless=True,  # Run without visible browser
            )

            if progress_callback:
                progress_callback(100)

            # tiktok-uploader doesn't return video ID
            # Use a placeholder that indicates success
            video_id = f"tiktok_{account.handle or account.id}"

            logger.info(f"TikTok upload complete for {account.name}")
            return video_id

        except ImportError:
            logger.error(
                "TikTok upload requires tiktok-uploader. "
                "Install with: pip install tiktok-uploader"
            )
            raise
        except Exception as e:
            logger.error(f"TikTok upload failed: {e}")
            raise

    def upload_with_schedule(
        self,
        upload: Upload,
        account: Account,
        compilation: Compilation,
        schedule_time: Optional[str] = None,
    ) -> Optional[str]:
        """
        Upload a video with optional scheduling.

        Args:
            upload: Upload job
            account: TikTok account
            compilation: Compilation to upload
            schedule_time: Optional schedule time (format depends on tiktok-uploader)

        Returns:
            Video ID placeholder on success
        """
        if account.platform != Platform.TIKTOK:
            logger.error(f"Account {account.name} is not a TikTok account")
            return None

        if not compilation.output_path or not Path(compilation.output_path).exists():
            logger.error(f"Output file not found: {compilation.output_path}")
            return None

        cookies_path = self._get_cookies_path(account)
        if not cookies_path:
            return None

        try:
            from tiktok_uploader.upload import upload_video

            description = self._build_description(compilation)

            logger.info(
                f"Uploading compilation {compilation.id} to TikTok "
                f"via {account.name}"
                + (f" (scheduled: {schedule_time})" if schedule_time else "")
            )

            # Build upload kwargs
            kwargs = {
                "filename": compilation.output_path,
                "description": description,
                "cookies": str(cookies_path),
                "headless": True,
            }

            if schedule_time:
                kwargs["schedule"] = schedule_time

            upload_video(**kwargs)

            video_id = f"tiktok_{account.handle or account.id}"
            logger.info(f"TikTok upload complete for {account.name}")
            return video_id

        except ImportError:
            logger.error(
                "TikTok upload requires tiktok-uploader. "
                "Install with: pip install tiktok-uploader"
            )
            raise
        except Exception as e:
            logger.error(f"TikTok upload failed: {e}")
            raise

    @staticmethod
    def extract_cookies_from_browser(browser: str = "chrome") -> Optional[List[Dict]]:
        """
        Extract TikTok cookies from browser.

        Args:
            browser: Browser to extract from ("chrome", "firefox", "edge")

        Returns:
            List of cookie dictionaries, or None on failure

        Note:
            User must be logged into TikTok in the specified browser.
        """
        try:
            import browser_cookie3

            if browser == "chrome":
                cookie_jar = browser_cookie3.chrome(domain_name=".tiktok.com")
            elif browser == "firefox":
                cookie_jar = browser_cookie3.firefox(domain_name=".tiktok.com")
            elif browser == "edge":
                cookie_jar = browser_cookie3.edge(domain_name=".tiktok.com")
            else:
                logger.error(f"Unsupported browser: {browser}")
                return None

            cookies = []
            for cookie in cookie_jar:
                cookies.append({
                    "name": cookie.name,
                    "value": cookie.value,
                    "domain": cookie.domain,
                    "path": cookie.path,
                    "secure": cookie.secure,
                    "httpOnly": getattr(cookie, "httponly", False),
                    "sameSite": "Lax",
                })

            if not cookies:
                logger.warning("No TikTok cookies found. Make sure you're logged in.")
                return None

            logger.info(f"Extracted {len(cookies)} TikTok cookies from {browser}")
            return cookies

        except ImportError:
            logger.error(
                "Cookie extraction requires browser_cookie3. "
                "Install with: pip install browser_cookie3"
            )
            return None
        except Exception as e:
            logger.error(f"Failed to extract cookies: {e}")
            return None

    @staticmethod
    def validate_cookies(cookies: List[Dict]) -> bool:
        """
        Validate that cookies contain required TikTok auth cookies.

        Args:
            cookies: List of cookie dictionaries

        Returns:
            True if essential cookies are present
        """
        required_names = {"sessionid", "sid_tt", "sid_guard"}
        cookie_names = {c.get("name", "").lower() for c in cookies}

        # Check for at least one session cookie
        has_session = bool(required_names & cookie_names)

        if not has_session:
            logger.warning(
                "Missing required TikTok session cookies. "
                "Make sure you're logged in."
            )

        return has_session

    def test_auth(self, account: Account) -> bool:
        """
        Test if TikTok authentication is working.

        Args:
            account: TikTok account to test

        Returns:
            True if auth appears valid
        """
        credentials = self.account_manager.get_credentials(account.id)
        if not credentials:
            return False

        cookies = credentials.get("cookies")
        if not cookies:
            return False

        return self.validate_cookies(cookies)
