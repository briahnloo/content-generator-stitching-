"""
Encryption utilities for secure credential storage.
Uses Fernet symmetric encryption.
"""

import base64
import hashlib
import os
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken


class CredentialEncryption:
    """Handles encryption/decryption of sensitive credentials."""

    def __init__(self, key: Optional[str] = None):
        """
        Initialize with encryption key.

        Args:
            key: Base64-encoded Fernet key, or None to generate from env.
        """
        if key:
            self._fernet = Fernet(key.encode() if isinstance(key, str) else key)
        else:
            self._fernet = Fernet(self._get_or_create_key())

    @staticmethod
    def generate_key() -> str:
        """Generate a new Fernet encryption key."""
        return Fernet.generate_key().decode()

    @staticmethod
    def _get_or_create_key() -> bytes:
        """Get key from environment or derive from a passphrase."""
        from config.settings import settings

        key = getattr(settings, 'CREDENTIALS_ENCRYPTION_KEY', None)

        if key:
            # Validate it's a proper Fernet key
            if len(key) == 44 and key.endswith('='):
                return key.encode()
            # Otherwise derive a key from the passphrase
            return CredentialEncryption._derive_key(key)

        # Fallback: derive from a combination of stable system values
        # This is less secure but allows the system to work without explicit key
        fallback = f"viral-clips-{os.getenv('USER', 'default')}"
        return CredentialEncryption._derive_key(fallback)

    @staticmethod
    def _derive_key(passphrase: str) -> bytes:
        """Derive a Fernet-compatible key from a passphrase."""
        # Use SHA256 to get 32 bytes, then base64 encode for Fernet
        digest = hashlib.sha256(passphrase.encode()).digest()
        return base64.urlsafe_b64encode(digest)

    def encrypt(self, plaintext: str) -> str:
        """
        Encrypt a string.

        Args:
            plaintext: The string to encrypt.

        Returns:
            Base64-encoded encrypted string.
        """
        if not plaintext:
            return ""
        encrypted = self._fernet.encrypt(plaintext.encode())
        return encrypted.decode()

    def decrypt(self, ciphertext: str) -> str:
        """
        Decrypt a string.

        Args:
            ciphertext: Base64-encoded encrypted string.

        Returns:
            Decrypted plaintext string.

        Raises:
            InvalidToken: If decryption fails (wrong key or corrupted data).
        """
        if not ciphertext:
            return ""
        try:
            decrypted = self._fernet.decrypt(ciphertext.encode())
            return decrypted.decode()
        except InvalidToken:
            raise ValueError("Failed to decrypt: invalid key or corrupted data")

    def encrypt_dict(self, data: dict) -> str:
        """Encrypt a dictionary as JSON string."""
        import json
        return self.encrypt(json.dumps(data))

    def decrypt_dict(self, ciphertext: str) -> dict:
        """Decrypt a JSON string back to dictionary."""
        import json
        plaintext = self.decrypt(ciphertext)
        return json.loads(plaintext) if plaintext else {}


# Singleton instance
_encryption: Optional[CredentialEncryption] = None


def get_encryption() -> CredentialEncryption:
    """Get the singleton encryption instance."""
    global _encryption
    if _encryption is None:
        _encryption = CredentialEncryption()
    return _encryption
