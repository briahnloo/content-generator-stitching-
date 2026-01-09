"""
Account manager service for handling platform accounts.
Manages credentials with encryption and rate limiting.
"""

import logging
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from core.database import Database
from core.encryption import get_encryption
from core.models import (
    Account, Platform, ContentStrategy, RoutingRule
)

logger = logging.getLogger(__name__)


class AccountManager:
    """Manages platform accounts with encrypted credentials."""

    def __init__(self, db: Database):
        """Initialize account manager."""
        self.db = db
        self.encryption = get_encryption()

    def create_account(
        self,
        platform: Platform,
        name: str,
        strategy: ContentStrategy = ContentStrategy.MIXED,
        handle: str = "",
        daily_limit: int = 3,
    ) -> Account:
        """
        Create a new platform account.

        Args:
            platform: Platform type (youtube/tiktok)
            name: Human-readable account name
            strategy: Content strategy (fails/comedy/mixed)
            handle: Optional @username
            daily_limit: Max uploads per day

        Returns:
            Created Account object
        """
        account = Account(
            id=str(uuid.uuid4())[:12],
            platform=platform,
            name=name,
            handle=handle,
            content_strategy=strategy,
            daily_upload_limit=daily_limit,
        )

        if self.db.insert_account(account):
            logger.info(f"Created {platform.value} account: {name}")
            return account
        else:
            raise ValueError(f"Failed to create account: {name}")

    def get_account(self, account_id: str) -> Optional[Account]:
        """Get an account by ID."""
        return self.db.get_account(account_id)

    def list_accounts(
        self,
        platform: Optional[Platform] = None,
        active_only: bool = True
    ) -> List[Account]:
        """List accounts with optional platform filter."""
        if platform:
            return self.db.get_accounts_by_platform(platform, active_only)
        return self.db.get_all_accounts(active_only)

    def update_account(
        self,
        account_id: str,
        name: Optional[str] = None,
        handle: Optional[str] = None,
        strategy: Optional[ContentStrategy] = None,
        daily_limit: Optional[int] = None,
        is_active: Optional[bool] = None,
    ) -> Optional[Account]:
        """Update account properties."""
        account = self.db.get_account(account_id)
        if not account:
            return None

        if name is not None:
            account.name = name
        if handle is not None:
            account.handle = handle
        if strategy is not None:
            account.content_strategy = strategy
        if daily_limit is not None:
            account.daily_upload_limit = daily_limit
        if is_active is not None:
            account.is_active = is_active

        self.db.update_account(account)
        logger.info(f"Updated account: {account.name}")
        return account

    def delete_account(self, account_id: str) -> bool:
        """Delete an account and its routing rules."""
        account = self.db.get_account(account_id)
        if not account:
            return False

        self.db.delete_account(account_id)
        logger.info(f"Deleted account: {account.name}")
        return True

    def deactivate_account(self, account_id: str) -> bool:
        """Deactivate an account without deleting it."""
        account = self.db.get_account(account_id)
        if not account:
            return False

        account.is_active = False
        self.db.update_account(account)
        logger.info(f"Deactivated account: {account.name}")
        return True

    def activate_account(self, account_id: str) -> bool:
        """Reactivate a deactivated account."""
        account = self.db.get_account(account_id)
        if not account:
            return False

        account.is_active = True
        account.error = ""
        self.db.update_account(account)
        logger.info(f"Activated account: {account.name}")
        return True

    # =========================================================================
    # Credential Management
    # =========================================================================

    def set_credentials(self, account_id: str, credentials: Dict) -> bool:
        """
        Store encrypted credentials for an account.

        Args:
            account_id: Account ID
            credentials: Dictionary of credentials (varies by platform)
                YouTube: {client_id, client_secret, refresh_token}
                TikTok: {session_id, cookies}

        Returns:
            True on success
        """
        account = self.db.get_account(account_id)
        if not account:
            return False

        encrypted = self.encryption.encrypt_dict(credentials)
        account.credentials_encrypted = encrypted
        self.db.update_account(account)
        logger.info(f"Stored credentials for account: {account.name}")
        return True

    def get_credentials(self, account_id: str) -> Optional[Dict]:
        """
        Retrieve decrypted credentials for an account.

        Args:
            account_id: Account ID

        Returns:
            Decrypted credentials dictionary, or None if not found
        """
        account = self.db.get_account(account_id)
        if not account or not account.credentials_encrypted:
            return None

        try:
            return self.encryption.decrypt_dict(account.credentials_encrypted)
        except ValueError as e:
            logger.error(f"Failed to decrypt credentials for {account_id}: {e}")
            return None

    def has_credentials(self, account_id: str) -> bool:
        """Check if an account has credentials stored."""
        account = self.db.get_account(account_id)
        return bool(account and account.credentials_encrypted)

    # =========================================================================
    # Rate Limiting
    # =========================================================================

    def can_upload(self, account_id: str) -> bool:
        """
        Check if account can upload (within daily limit).

        Args:
            account_id: Account ID

        Returns:
            True if upload is allowed
        """
        account = self.db.get_account(account_id)
        if not account:
            return False

        if not account.is_active:
            return False

        if not account.credentials_encrypted:
            return False

        return account.uploads_today < account.daily_upload_limit

    def record_upload(self, account_id: str) -> None:
        """Record an upload for rate limiting."""
        self.db.increment_upload_count(account_id)

    def reset_daily_limits(self) -> None:
        """Reset daily upload counters for all accounts."""
        self.db.reset_daily_upload_counts()
        logger.info("Reset daily upload limits for all accounts")

    def get_available_accounts(
        self,
        platform: Platform,
        strategy: Optional[ContentStrategy] = None
    ) -> List[Account]:
        """
        Get accounts that can still upload today.

        Args:
            platform: Platform filter
            strategy: Optional content strategy filter

        Returns:
            List of accounts with upload capacity
        """
        if strategy:
            accounts = self.db.get_accounts_by_strategy(strategy, platform)
        else:
            accounts = self.db.get_accounts_by_platform(platform)

        return [
            acc for acc in accounts
            if acc.is_active
            and acc.credentials_encrypted
            and acc.uploads_today < acc.daily_upload_limit
        ]

    def get_best_account_for_upload(
        self,
        platform: Platform,
        category: str,
    ) -> Optional[Account]:
        """
        Select the best account for uploading based on strategy and availability.

        Args:
            platform: Target platform
            category: Content category (fails/comedy)

        Returns:
            Best available account, or None if no suitable account found
        """
        # Map category to strategy
        if category == "fails":
            strategies = [ContentStrategy.FAILS, ContentStrategy.MIXED]
        elif category == "comedy":
            strategies = [ContentStrategy.COMEDY, ContentStrategy.MIXED]
        else:
            strategies = [ContentStrategy.MIXED]

        best_account = None
        min_uploads = float('inf')

        for strategy in strategies:
            accounts = self.get_available_accounts(platform, strategy)
            for account in accounts:
                # Prefer accounts with fewer uploads today
                if account.uploads_today < min_uploads:
                    min_uploads = account.uploads_today
                    best_account = account

        return best_account

    # =========================================================================
    # Routing Rules
    # =========================================================================

    def add_routing_rule(
        self,
        account_id: str,
        category: str,
        min_confidence: float = 0.7,
        priority: int = 1,
    ) -> Optional[RoutingRule]:
        """
        Add a routing rule for an account.

        Args:
            account_id: Account ID
            category: Content category (fails/comedy)
            min_confidence: Minimum confidence threshold
            priority: Higher = preferred

        Returns:
            Created RoutingRule, or None on failure
        """
        account = self.db.get_account(account_id)
        if not account:
            return None

        rule = RoutingRule(
            id=str(uuid.uuid4())[:12],
            account_id=account_id,
            category=category,
            min_confidence=min_confidence,
            priority=priority,
        )

        if self.db.insert_routing_rule(rule):
            logger.info(f"Added routing rule: {category} -> {account.name}")
            return rule
        return None

    def get_routing_rules(self, account_id: str) -> List[RoutingRule]:
        """Get all routing rules for an account."""
        return self.db.get_routing_rules_for_account(account_id)

    def delete_routing_rule(self, rule_id: str) -> bool:
        """Delete a routing rule."""
        rule = self.db.get_routing_rule(rule_id)
        if not rule:
            return False

        self.db.delete_routing_rule(rule_id)
        logger.info(f"Deleted routing rule: {rule_id}")
        return True

    # =========================================================================
    # Error Handling
    # =========================================================================

    def record_error(self, account_id: str, error: str) -> None:
        """Record an error for an account."""
        account = self.db.get_account(account_id)
        if account:
            account.error = error
            self.db.update_account(account)
            logger.warning(f"Recorded error for {account.name}: {error}")

    def clear_error(self, account_id: str) -> None:
        """Clear error for an account."""
        account = self.db.get_account(account_id)
        if account:
            account.error = ""
            self.db.update_account(account)

    # =========================================================================
    # Statistics
    # =========================================================================

    def get_account_stats(self, account_id: str) -> Optional[Dict]:
        """Get upload statistics for an account."""
        account = self.db.get_account(account_id)
        if not account:
            return None

        uploads = self.db.get_uploads_for_account(account_id)
        successful = [u for u in uploads if u.status.value == "success"]

        return {
            "account_id": account_id,
            "name": account.name,
            "platform": account.platform.value,
            "strategy": account.content_strategy.value,
            "is_active": account.is_active,
            "uploads_today": account.uploads_today,
            "daily_limit": account.daily_upload_limit,
            "total_uploads": len(uploads),
            "successful_uploads": len(successful),
            "last_upload": account.last_upload_at.isoformat() if account.last_upload_at else None,
            "has_credentials": bool(account.credentials_encrypted),
            "error": account.error or None,
        }

    def get_all_stats(self) -> Dict:
        """Get summary statistics for all accounts."""
        accounts = self.db.get_all_accounts(active_only=False)

        by_platform = {}
        for acc in accounts:
            platform = acc.platform.value
            if platform not in by_platform:
                by_platform[platform] = {"total": 0, "active": 0, "with_creds": 0}
            by_platform[platform]["total"] += 1
            if acc.is_active:
                by_platform[platform]["active"] += 1
            if acc.credentials_encrypted:
                by_platform[platform]["with_creds"] += 1

        return {
            "total_accounts": len(accounts),
            "by_platform": by_platform,
        }
