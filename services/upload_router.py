"""
Upload router service for routing compilations to platform accounts.
Handles content matching, rate limiting, and upload queue management.
"""

import logging
import uuid
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple

from core.database import Database
from core.models import (
    Compilation, CompilationStatus, Account, Upload, UploadStatus,
    Platform, ContentStrategy, RoutingRule
)
from services.account_manager import AccountManager

logger = logging.getLogger(__name__)

# Minimum time between uploads to same account (prevents spam)
MIN_UPLOAD_INTERVAL_MINUTES = 30


class UploadRouter:
    """Routes compilations to appropriate platform accounts."""

    def __init__(self, db: Database, account_manager: Optional[AccountManager] = None):
        """Initialize upload router."""
        self.db = db
        self.account_manager = account_manager or AccountManager(db)

    def _category_matches_strategy(self, category: str, strategy: ContentStrategy) -> bool:
        """Check if a content category matches an account's strategy."""
        if strategy == ContentStrategy.MIXED:
            return True
        if strategy == ContentStrategy.FAILS and category == "fails":
            return True
        if strategy == ContentStrategy.COMEDY and category == "comedy":
            return True
        return False

    def _get_matching_accounts(
        self,
        compilation: Compilation,
        platform: Platform,
    ) -> List[Tuple[Account, float]]:
        """
        Get accounts that match a compilation's category.

        Returns list of (Account, priority_score) tuples, sorted by priority.
        Priority is based on:
        - Routing rule priority
        - Routing rule min_confidence match
        - Account availability (uploads today)
        """
        matching = []

        # Get all routing rules for this category
        rules = self.db.get_routing_rules_for_category(compilation.category)

        for rule in rules:
            account = self.db.get_account(rule.account_id)
            if not account:
                continue

            # Must be correct platform
            if account.platform != platform:
                continue

            # Must be active with credentials
            if not account.is_active or not account.credentials_encrypted:
                continue

            # Check confidence threshold
            if compilation.confidence_score < rule.min_confidence:
                continue

            # Check rate limit
            if account.uploads_today >= account.daily_upload_limit:
                continue

            # Calculate priority score
            # Higher rule priority + more remaining capacity = higher score
            remaining_capacity = account.daily_upload_limit - account.uploads_today
            priority_score = (rule.priority * 10) + remaining_capacity

            matching.append((account, priority_score))

        # Sort by priority score (descending)
        matching.sort(key=lambda x: x[1], reverse=True)
        return matching

    def _get_fallback_accounts(
        self,
        compilation: Compilation,
        platform: Platform,
    ) -> List[Account]:
        """
        Get accounts by strategy when no routing rules match.
        Used as fallback when no explicit routing rules exist.
        """
        # Map category to strategy
        if compilation.category == "fails":
            strategies = [ContentStrategy.FAILS, ContentStrategy.MIXED]
        elif compilation.category == "comedy":
            strategies = [ContentStrategy.COMEDY, ContentStrategy.MIXED]
        else:
            strategies = [ContentStrategy.MIXED]

        available = []
        for strategy in strategies:
            accounts = self.account_manager.get_available_accounts(platform, strategy)
            for acc in accounts:
                if acc not in available:
                    available.append(acc)

        # Sort by uploads today (prefer accounts with fewer uploads)
        available.sort(key=lambda a: a.uploads_today)
        return available

    def route_compilation(
        self,
        compilation: Compilation,
        platforms: Optional[List[Platform]] = None,
    ) -> List[Upload]:
        """
        Route a compilation to appropriate accounts on specified platforms.

        Args:
            compilation: Compilation to route
            platforms: List of platforms to route to. Default: [YouTube, TikTok]

        Returns:
            List of created Upload objects
        """
        if platforms is None:
            platforms = [Platform.YOUTUBE, Platform.TIKTOK]

        # Only route approved compilations
        if compilation.status != CompilationStatus.APPROVED:
            logger.warning(
                f"Compilation {compilation.id} is not approved "
                f"(status: {compilation.status})"
            )
            return []

        uploads = []

        for platform in platforms:
            # First try routing rules
            matching = self._get_matching_accounts(compilation, platform)

            if matching:
                account, _ = matching[0]  # Take highest priority
            else:
                # Fallback to strategy-based selection
                fallback = self._get_fallback_accounts(compilation, platform)
                if fallback:
                    account = fallback[0]
                else:
                    logger.warning(
                        f"No available {platform.value} accounts for "
                        f"compilation {compilation.id}"
                    )
                    continue

            # Check if upload already exists for this compilation/account
            if self.db.upload_exists_for_compilation_account(
                compilation.id, account.id
            ):
                logger.debug(
                    f"Upload already exists for {compilation.id} -> {account.name}"
                )
                continue

            # Create upload job
            upload = Upload(
                id=str(uuid.uuid4())[:12],
                compilation_id=compilation.id,
                account_id=account.id,
                platform=platform,
                status=UploadStatus.PENDING,
                privacy="public",  # Auto-approved = public
            )

            if self.db.insert_upload(upload):
                uploads.append(upload)
                logger.info(
                    f"Routed compilation {compilation.id} -> "
                    f"{account.name} ({platform.value})"
                )

        return uploads

    def route_approved_compilations(
        self,
        limit: Optional[int] = None,
    ) -> List[Upload]:
        """
        Route all approved compilations that haven't been routed yet.

        Returns list of created uploads.
        """
        approved = self.db.get_compilations_by_status(
            CompilationStatus.APPROVED, limit
        )

        all_uploads = []
        for compilation in approved:
            # Check if already has pending/successful uploads
            existing = self.db.get_uploads_for_compilation(compilation.id)
            has_pending = any(
                u.status in (UploadStatus.PENDING, UploadStatus.UPLOADING)
                for u in existing
            )

            if has_pending:
                continue

            uploads = self.route_compilation(compilation)
            all_uploads.extend(uploads)

        logger.info(f"Created {len(all_uploads)} upload jobs")
        return all_uploads

    def get_pending_uploads(
        self,
        platform: Optional[Platform] = None,
        limit: Optional[int] = None,
    ) -> List[Upload]:
        """Get pending uploads, optionally filtered by platform."""
        pending = self.db.get_pending_uploads(limit)

        if platform:
            pending = [u for u in pending if u.platform == platform]

        return pending

    def get_next_upload(self, platform: Platform) -> Optional[Tuple[Upload, Account, Compilation]]:
        """
        Get the next upload to process for a platform.

        Returns (Upload, Account, Compilation) tuple or None if no uploads ready.
        """
        pending = self.get_pending_uploads(platform, limit=10)

        for upload in pending:
            account = self.db.get_account(upload.account_id)
            if not account:
                continue

            # Check if account can still upload
            if not self.account_manager.can_upload(account.id):
                continue

            # Check minimum interval since last upload
            # Note: last_upload_at is stored in UTC, so compare with utcnow()
            if account.last_upload_at:
                min_interval = timedelta(minutes=MIN_UPLOAD_INTERVAL_MINUTES)
                if datetime.utcnow() - account.last_upload_at < min_interval:
                    continue

            compilation = self.db.get_compilation(upload.compilation_id)
            if not compilation:
                continue

            return upload, account, compilation

        return None

    def mark_upload_started(self, upload: Upload) -> None:
        """Mark an upload as in progress."""
        upload.status = UploadStatus.UPLOADING
        self.db.update_upload(upload)

    def mark_upload_success(
        self,
        upload: Upload,
        platform_video_id: str,
    ) -> None:
        """Mark an upload as successful."""
        upload.status = UploadStatus.SUCCESS
        upload.platform_video_id = platform_video_id
        upload.uploaded_at = datetime.now()
        upload.error = ""
        self.db.update_upload(upload)

        # Record upload for rate limiting
        self.account_manager.record_upload(upload.account_id)

        logger.info(
            f"Upload {upload.id} successful: {platform_video_id}"
        )

    def mark_upload_failed(self, upload: Upload, error: str) -> None:
        """Mark an upload as failed."""
        upload.status = UploadStatus.FAILED
        upload.error = error
        upload.retry_count += 1
        self.db.update_upload(upload)

        logger.error(f"Upload {upload.id} failed: {error}")

    def retry_failed_uploads(self, max_retries: int = 3) -> int:
        """
        Retry failed uploads that haven't exceeded max retries.

        Returns number of uploads re-queued.
        """
        failed = self.db.get_uploads_by_status(UploadStatus.FAILED)
        retried = 0

        for upload in failed:
            if upload.retry_count < max_retries:
                upload.status = UploadStatus.PENDING
                self.db.update_upload(upload)
                retried += 1
                logger.info(
                    f"Re-queued upload {upload.id} "
                    f"(retry {upload.retry_count + 1}/{max_retries})"
                )

        return retried

    def get_upload_stats(self) -> Dict:
        """Get upload queue statistics."""
        pending = len(self.db.get_uploads_by_status(UploadStatus.PENDING))
        uploading = len(self.db.get_uploads_by_status(UploadStatus.UPLOADING))
        success = len(self.db.get_uploads_by_status(UploadStatus.SUCCESS))
        failed = len(self.db.get_uploads_by_status(UploadStatus.FAILED))

        return {
            "pending": pending,
            "uploading": uploading,
            "success": success,
            "failed": failed,
            "total": pending + uploading + success + failed,
        }
