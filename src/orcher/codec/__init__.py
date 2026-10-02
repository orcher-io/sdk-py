"""
Codec module for ORCHER Python SDK

Compression and encryption codecs that transform Payloads.
They wrap the Rust implementations in the shared SDK core, so a payload
encoded here decodes identically in every other Orcher SDK.

Example:
    >>> from orcher.codec import GzipCodec, EncryptionCodec, CodecChain
    >>> from orcher._native import Payload
    >>>
    >>> # Compress a payload
    >>> gzip = GzipCodec()
    >>> payload = Payload.from_json({"key": "value"})
    >>> compressed = gzip.encode(payload)
    >>> original = gzip.decode(compressed)
    >>>
    >>> # Encrypt a payload
    >>> key = EncryptionCodec.generate_key()
    >>> enc = EncryptionCodec(key)
    >>> encrypted = enc.encode(payload)
    >>> decrypted = enc.decode(encrypted)
    >>>
    >>> # Chain codecs: encrypt then compress
    >>> chain = CodecChain().with_encryption(key).with_gzip()
    >>> encoded = chain.encode(payload)
    >>> decoded = chain.decode(encoded)
"""

from orcher._native import CodecChain, EncryptionCodec, GzipCodec

__all__ = ["GzipCodec", "EncryptionCodec", "CodecChain"]
