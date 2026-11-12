"""Strict JSON and canonical base64url primitives for SecureHook v2."""

import base64
import binascii
import json
import re
import unicodedata


class StrictError(ValueError):
    """Raised when input violates the strict encoding rules."""


_B64URL_RE = re.compile(r"^[A-Za-z0-9_-]*$")
_CONTROL_CHARS = frozenset(chr(c) for c in range(0x20)) | frozenset((chr(0x7F),))


def is_nfc_text(value):
    return (
        isinstance(value, str)
        and len(value) > 0
        and unicodedata.is_normalized("NFC", value)
        and not any(
            ch in _CONTROL_CHARS or unicodedata.category(ch) == "Cs"
            for ch in value
        )
    )


class _StrictDecoder(json.JSONDecoder):
    """Rejects duplicate object keys and non-finite numbers at decode time."""

    def __init__(self):
        super().__init__(
            object_pairs_hook=self._object_pairs,
            parse_constant=self._parse_constant,
        )

    @staticmethod
    def _object_pairs(pairs):
        seen = set()
        for key, _ in pairs:
            if key in seen:
                raise StrictError("duplicate JSON object key")
            seen.add(key)
        return dict(pairs)

    @staticmethod
    def _parse_constant(value):
        raise StrictError("non-finite JSON number is not allowed")


def _walk(value):
    if isinstance(value, str):
        if not unicodedata.is_normalized("NFC", value):
            raise StrictError("string is not NFC normalized")
        for ch in value:
            if ch in _CONTROL_CHARS or unicodedata.category(ch) == "Cs":
                raise StrictError("string contains control or surrogate character")
    elif isinstance(value, dict):
        for key, item in value.items():
            if not unicodedata.is_normalized("NFC", key):
                raise StrictError("object key is not NFC normalized")
            if any(
                ch in _CONTROL_CHARS or unicodedata.category(ch) == "Cs"
                for ch in key
            ):
                raise StrictError("object key contains forbidden character")
            _walk(item)
    elif isinstance(value, list):
        for item in value:
            _walk(item)
    elif value is None or isinstance(value, (bool, int, float)):
        return
    else:  # pragma: no cover - json never produces other types
        raise StrictError("unsupported JSON value")


def loads(data):
    """Decode strict UTF-8 JSON; see section 1 of the v2 contract."""
    try:
        if isinstance(data, str):
            raw = data.encode("utf-8")
        elif isinstance(data, (bytes, bytearray)):
            raw = bytes(data)
        else:
            raise StrictError("JSON input must be bytes or text")
        value = json.loads(raw.decode("utf-8", errors="strict"),
                           cls=_StrictDecoder)
    except StrictError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise StrictError("invalid JSON text") from exc
    if not isinstance(value, (dict, list)):
        raise StrictError("JSON document must be an object or array")
    _walk(value)
    return value


def b64url_decode(text):
    """Decode canonical (unpadded, re-encode identical) base64url text."""
    if (
        not isinstance(text, str)
        or not text
        or not _B64URL_RE.fullmatch(text)
    ):
        raise StrictError("not canonical base64url")
    if len(text) % 4 == 1:
        raise StrictError("invalid base64url length")
    padded = text + ("=" * (-len(text) % 4))
    try:
        decoded = base64.urlsafe_b64decode(padded.encode("ascii"))
    except (binascii.Error, ValueError) as exc:
        raise StrictError("invalid base64url data") from exc
    if b64url_encode(decoded) != text:
        raise StrictError("base64url is not canonical")
    return decoded


def b64url_encode(data):
    if not isinstance(data, (bytes, bytearray)):
        raise StrictError("base64url input must be bytes")
    return base64.urlsafe_b64encode(bytes(data)).rstrip(b"=").decode("ascii")


def canonical(value):
    """Serialize with the deterministic form used by the test helpers."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
