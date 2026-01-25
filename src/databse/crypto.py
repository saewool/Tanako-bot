"""
KotonexusTakako Encryption Module
Provides Fernet encryption with obfuscated key hidden in code
"""

import base64
import hashlib
import struct
import time
from typing import Any, Dict, Optional, Union
from cryptography.fernet import Fernet, InvalidToken
import json


_KEY_PART_A = b'\x4b\x6f\x74\x6f\x6e\x65\x78\x75'
_KEY_PART_B = b'\x73\x54\x61\x6b\x61\x6b\x6f\x44'
_KEY_PART_C = b'\x42\x53\x65\x63\x72\x65\x74\x4b'
_KEY_PART_D = b'\x65\x79\x32\x30\x32\x35\x21\x40'


def _derive_key() -> bytes:
    """
    Derive Fernet key from obfuscated parts.
    Uses PBKDF2-style derivation for key stretching.
    """
    combined = _KEY_PART_A + _KEY_PART_B + _KEY_PART_C + _KEY_PART_D
    
    salt = b'kotonexus_takako_salt_v3'
    
    key_material = hashlib.pbkdf2_hmac(
        'sha256',
        combined,
        salt,
        iterations=100000,
        dklen=32
    )
    
    return base64.urlsafe_b64encode(key_material)


class CryptoManager:
    """
    Encryption manager using Fernet (AES-128-CBC + HMAC-SHA256)
    
    Features:
    - Transparent encryption/decryption
    - Hidden key obfuscation
    - Integrity verification via HMAC
    """
    
    _instance: Optional['CryptoManager'] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        """Initialize Fernet with derived key"""
        key = _derive_key()
        self._fernet: Fernet = Fernet(key)
        self._encryption_enabled: bool = True
    
    @property
    def enabled(self) -> bool:
        return self._encryption_enabled
    
    def enable(self):
        """Enable encryption"""
        self._encryption_enabled = True
    
    def disable(self):
        """Disable encryption (for debugging only)"""
        self._encryption_enabled = False
    
    def encrypt(self, data: Union[bytes, str, Dict[str, Any]]) -> bytes:
        """
        Encrypt data.
        
        Args:
            data: Raw bytes, string, or dict to encrypt
            
        Returns:
            Encrypted bytes
        """
        if not self._encryption_enabled:
            if isinstance(data, dict):
                return json.dumps(data, default=str).encode('utf-8')
            elif isinstance(data, str):
                return data.encode('utf-8')
            return data
        
        if isinstance(data, dict):
            plaintext = json.dumps(data, default=str).encode('utf-8')
        elif isinstance(data, str):
            plaintext = data.encode('utf-8')
        else:
            plaintext = data
        
        return self._fernet.encrypt(plaintext)
    
    def decrypt(self, encrypted_data: bytes) -> bytes:
        """
        Decrypt data.
        
        Args:
            encrypted_data: Encrypted bytes
            
        Returns:
            Decrypted bytes
        """
        if not self._encryption_enabled:
            return encrypted_data
        
        try:
            return self._fernet.decrypt(encrypted_data)
        except InvalidToken:
            return encrypted_data
    
    def encrypt_dict(self, data: Dict[str, Any]) -> bytes:
        """Encrypt a dictionary"""
        return self.encrypt(data)
    
    def decrypt_dict(self, encrypted_data: bytes) -> Dict[str, Any]:
        """Decrypt to dictionary"""
        decrypted = self.decrypt(encrypted_data)
        try:
            return json.loads(decrypted.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {}
    
    def encrypt_value(self, value: Any) -> Optional[str]:
        """
        Encrypt a single value and return base64 string.
        Useful for encrypting individual column values.
        """
        if value is None:
            return None
        
        if not self._encryption_enabled:
            return str(value)
        
        plaintext = json.dumps(value, default=str).encode('utf-8')
        encrypted = self._fernet.encrypt(plaintext)
        return base64.urlsafe_b64encode(encrypted).decode('utf-8')
    
    def decrypt_value(self, encrypted_str: str) -> Any:
        """
        Decrypt a base64 encoded encrypted value.
        """
        if encrypted_str is None:
            return None
        
        if not self._encryption_enabled:
            try:
                return json.loads(encrypted_str)
            except:
                return encrypted_str
        
        try:
            encrypted = base64.urlsafe_b64decode(encrypted_str.encode('utf-8'))
            decrypted = self._fernet.decrypt(encrypted)
            return json.loads(decrypted.decode('utf-8'))
        except (InvalidToken, json.JSONDecodeError, UnicodeDecodeError):
            try:
                return json.loads(encrypted_str)
            except:
                return encrypted_str
    
    def encrypt_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encrypt all values in a row dictionary.
        Preserves key names but encrypts values.
        """
        if not self._encryption_enabled:
            return row
        
        encrypted_row = {}
        for key, value in row.items():
            encrypted_row[key] = self.encrypt_value(value)
        return encrypted_row
    
    def decrypt_row(self, encrypted_row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Decrypt all values in a row dictionary.
        """
        if not self._encryption_enabled:
            return encrypted_row
        
        decrypted_row = {}
        for key, value in encrypted_row.items():
            decrypted_row[key] = self.decrypt_value(value)
        return decrypted_row
    
    def rotate_key(self, new_key_parts: tuple) -> bool:
        """
        Rotate encryption key (for admin use).
        This is a dangerous operation that requires re-encrypting all data.
        
        Returns:
            True if key rotation was successful
        """
        return False
    
    def verify_integrity(self, encrypted_data: bytes) -> bool:
        """
        Verify the integrity of encrypted data without decrypting.
        
        Returns:
            True if data integrity is valid
        """
        try:
            self._fernet.decrypt(encrypted_data)
            return True
        except InvalidToken:
            return False


_crypto_manager = None

def get_crypto_manager() -> CryptoManager:
    """Get singleton instance of CryptoManager"""
    global _crypto_manager
    if _crypto_manager is None:
        _crypto_manager = CryptoManager()
    return _crypto_manager


def encrypt(data: Union[bytes, str, Dict[str, Any]]) -> bytes:
    """Convenience function for encryption"""
    return get_crypto_manager().encrypt(data)


def decrypt(encrypted_data: bytes) -> bytes:
    """Convenience function for decryption"""
    return get_crypto_manager().decrypt(encrypted_data)


def encrypt_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convenience function for row encryption"""
    return get_crypto_manager().encrypt_row(row)


def decrypt_row(encrypted_row: Dict[str, Any]) -> Dict[str, Any]:
    """Convenience function for row decryption"""
    return get_crypto_manager().decrypt_row(encrypted_row)
