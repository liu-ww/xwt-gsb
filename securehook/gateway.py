"""SecureHook v2 gateway: strict offline webhook authorization."""

import hashlib
import hmac
import re
import sqlite3
import threading
from pathlib import Path

from .codec import StrictError, b64url_decode, is_nfc_text, loads

SCHEMA_VERSION = 2
CLOCK_SKEW = 30
MAX_LIFETIME = 300
ALLOWED_METHODS = frozenset(("POST", "PUT", "PATCH", "DELETE"))
HEADER_FIELDS = frozenset(("alg", "kid", "typ"))
PAYLOAD_FIELDS = frozenset((
    "iss", "sub", "aud", "tenant", "iat", "nbf", "exp", "jti", "seq",
    "method", "target", "body_sha256", "key_epoch",
))
REQUEST_FIELDS = frozenset(("method", "target", "body_b64"))
BUNDLE_FIELDS = frozenset(
    ("version", "issuer", "audience", "epoch", "keys")
)
HS_FIELDS = frozenset((
    "kid", "kty", "alg", "material", "epoch",
    "active_from", "retire_at", "tenants",
))
RS_FIELDS = frozenset((
    "kid", "kty", "alg", "n", "e", "epoch",
    "active_from", "retire_at", "tenants",
))
ITEM_FIELDS = frozenset(("request", "token"))
_UNRESERVED = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
)
_SUB_DELIMS = frozenset("!$&'()*+,;=")
_PCHAR = _UNRESERVED | _SUB_DELIMS | frozenset(":@")
_QUERY_CHARS = _PCHAR | frozenset("/?")
_PCT_ESCAPE = re.compile(r"%[0-9A-F]{2}")
_SHA256_DIGESTINFO = bytes.fromhex(
    "3031300d060960864801650304020105000420"
)


class Invalid(ValueError):
    pass


class Replay(ValueError):
    pass


class Sequence(ValueError):
    pass


class Signature(ValueError):
    pass


class Fenced(ValueError):
    pass


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _is_positive_int(value):
    return _is_int(value) and value > 0


def _check_pct_encoding(text, allowed_raw):
    index = 0
    while index < len(text):
        ch = text[index]
        if ch == "%":
            match = _PCT_ESCAPE.match(text, index)
            if match is None:
                raise Invalid("malformed percent encoding")
            decoded = int(text[index + 1:index + 3], 16)
            if chr(decoded) in _UNRESERVED:
                raise Invalid("unreserved characters must not be percent encoded")
            index = match.end()
            continue
        if ch not in allowed_raw:
            raise Invalid("character not allowed in target")
        index += 1


def validate_target(target):
    """Validate the canonical ASCII origin-form target from section 2."""
    if not isinstance(target, str) or not target or not target.isascii():
        raise Invalid("target must be a non-empty ASCII string")
    if target[0] != "/" or target.startswith("//"):
        raise Invalid("target must start with a single slash")
    if "\\" in target or "#" in target:
        raise Invalid("target must not contain backslashes or fragments")
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in target):
        raise Invalid("target must not contain control characters")
    path, separator, query = target.partition("?")
    if "//" in path:
        raise Invalid("target must not contain consecutive slashes")
    segments = path.split("/")
    if segments[0] != "":
        raise Invalid("target must be an absolute path")
    for segment in segments[1:]:
        if segment == "":
            raise Invalid("target must not contain empty path segments")
        if segment in (".", ".."):
            raise Invalid("target must not contain dot segments")
        _check_pct_encoding(segment, _PCHAR)
    if separator:
        items = query.split("&") if query else []
        names = set()
        encoded_items = []
        for item in items:
            if "=" not in item:
                raise Invalid("query items must contain '='")
            name = item.partition("=")[0]
            if name == "":
                raise Invalid("query names must be non-empty")
            if name in names:
                raise Invalid("query names must not be repeated")
            names.add(name)
            _check_pct_encoding(item, _QUERY_CHARS)
            encoded_items.append(item)
        if encoded_items != sorted(encoded_items):
            raise Invalid("query items must be sorted by encoded bytes")
    return target


def _validate_tenants(tenants):
    if not isinstance(tenants, list) or not tenants:
        raise Invalid("tenants must be a non-empty list")
    if any(not is_nfc_text(item) for item in tenants):
        raise Invalid("tenants must be non-empty NFC strings")
    if len(set(tenants)) != len(tenants):
        raise Invalid("tenants must not contain duplicates")
    if tenants != sorted(tenants):
        raise Invalid("tenants must be sorted")


def _validate_time_fields(entry):
    if not _is_positive_int(entry["epoch"]):
        raise Invalid("key epoch must be a positive integer")
    if not _is_int(entry["active_from"]):
        raise Invalid("active_from must be an integer")
    retire_at = entry["retire_at"]
    if retire_at is not None:
        if not _is_int(retire_at) or retire_at <= entry["active_from"]:
            raise Invalid("retire_at must be null or greater than active_from")


def load_bundle(keys):
    """Parse and fully validate a v2 key bundle."""
    if isinstance(keys, (str, Path)):
        data = Path(keys).read_bytes()
    elif isinstance(keys, (bytes, bytearray)):
        data = bytes(keys)
    else:
        raise Invalid("keys must be a path or JSON bytes")
    bundle = loads(data)
    if not isinstance(bundle, dict) or set(bundle) != BUNDLE_FIELDS:
        raise Invalid("key bundle has invalid fields")
    if bundle["version"] != 2:
        raise Invalid("unsupported key bundle version")
    if not _is_positive_int(bundle["epoch"]):
        raise Invalid("bundle epoch must be a positive integer")
    issuer = bundle["issuer"]
    audience = bundle["audience"]
    if not is_nfc_text(issuer) or not is_nfc_text(audience):
        raise Invalid("issuer and audience must be non-empty NFC strings")
    entries = bundle["keys"]
    if not isinstance(entries, list) or not entries:
        raise Invalid("keys must be a non-empty list")
    kids = set()
    prepared = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise Invalid("each key must be an object")
        alg = entry.get("alg")
        if alg == "HS256":
            if set(entry) != HS_FIELDS or entry.get("kty") != "oct":
                raise Invalid("invalid HS256 key entry")
            try:
                material = b64url_decode(entry["material"])
            except StrictError as exc:
                raise Invalid("invalid key material") from exc
            if len(material) < 32:
                raise Invalid("HS256 material must be at least 32 bytes")
            secret = material
            modulus = exponent = None
        elif alg == "RS256":
            if set(entry) != RS_FIELDS or entry.get("kty") != "RSA":
                raise Invalid("invalid RS256 key entry")
            try:
                modulus_bytes = b64url_decode(entry["n"])
                exponent_bytes = b64url_decode(entry["e"])
            except StrictError as exc:
                raise Invalid("invalid RSA key numbers") from exc
            if not modulus_bytes or not exponent_bytes:
                raise Invalid("RSA key numbers must not be empty")
            if modulus_bytes[0] == 0 or exponent_bytes[0] == 0:
                raise Invalid("RSA integers must not have leading zero bytes")
            modulus = int.from_bytes(modulus_bytes, "big")
            exponent = int.from_bytes(exponent_bytes, "big")
            if modulus.bit_length() < 768:
                raise Invalid("RSA modulus must be at least 768 bits")
            if exponent <= 2 or exponent % 2 == 0:
                raise Invalid("RSA exponent must be odd and greater than 2")
            secret = None
        else:
            raise Invalid("unsupported key algorithm")
        kid = entry.get("kid")
        if not is_nfc_text(kid):
            raise Invalid("kid must be a non-empty NFC string")
        if kid in kids:
            raise Invalid("kid values must be unique")
        kids.add(kid)
        _validate_time_fields(entry)
        _validate_tenants(entry["tenants"])
        prepared[kid] = {
            "alg": alg,
            "kty": entry["kty"],
            "secret": secret,
            "modulus": modulus,
            "exponent": exponent,
            "epoch": entry["epoch"],
            "active_from": entry["active_from"],
            "retire_at": entry["retire_at"],
            "tenants": tuple(entry["tenants"]),
        }
    return {
        "issuer": issuer,
        "audience": audience,
        "epoch": bundle["epoch"],
        "keys": prepared,
    }


def _verify_rs256(signing_input, signature, key):
    """Strict RSASSA-PKCS1-v1_5 / SHA-256 verification without leniency."""
    modulus = key["modulus"]
    key_size = (modulus.bit_length() + 7) // 8
    if len(signature) != key_size:
        raise Signature("signature length does not match modulus")
    signature_integer = int.from_bytes(signature, "big")
    if signature_integer >= modulus:
        raise Signature("signature integer is out of range")
    recovered = pow(
        signature_integer, key["exponent"], modulus
    ).to_bytes(key_size, "big")
    if not recovered.startswith(b"\x00\x01"):
        raise Signature("bad PKCS#1 v1.5 padding")
    try:
        separator = recovered.index(b"\x00", 2)
    except ValueError as exc:
        raise Signature("bad PKCS#1 v1.5 padding") from exc
    padding = recovered[2:separator]
    if len(padding) < 8 or any(byte != 0xFF for byte in padding):
        raise Signature("bad PKCS#1 v1.5 padding")
    digest_info = recovered[separator + 1:]
    expected = _SHA256_DIGESTINFO + hashlib.sha256(signing_input).digest()
    if not hmac.compare_digest(digest_info, expected):
        raise Signature("signature digest mismatch")


_V2_DDL = (
    """CREATE TABLE seen_jti(
           issuer TEXT NOT NULL,
           jti TEXT NOT NULL,
           PRIMARY KEY(issuer, jti)
       ) WITHOUT ROWID""",
    """CREATE TABLE seq_state(
           issuer TEXT NOT NULL,
           tenant TEXT NOT NULL,
           sub TEXT NOT NULL,
           method TEXT NOT NULL,
           target TEXT NOT NULL,
           last_seq INTEGER NOT NULL,
           PRIMARY KEY(issuer, tenant, sub, method, target)
       ) WITHOUT ROWID""",
    """CREATE TABLE bundle_epoch(
           issuer TEXT PRIMARY KEY,
           epoch INTEGER NOT NULL
       ) WITHOUT ROWID""",
)


class _Storage:
    def __init__(self, path):
        self.lock = threading.RLock()
        try:
            self.conn = sqlite3.connect(
                path, timeout=30, check_same_thread=False
            )
            self.conn.execute("PRAGMA busy_timeout=30000")
            self.conn.execute("PRAGMA synchronous=FULL")
            try:
                self.conn.execute("PRAGMA journal_mode=WAL")
            except sqlite3.DatabaseError:
                pass
            self._prepare_schema()
        except sqlite3.DatabaseError as exc:
            try:
                self.conn.close()
            except Exception:
                pass
            raise Fenced("database is corrupt or unreadable") from exc

    def _objects(self):
        return self.conn.execute(
            "SELECT type, name FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()

    def _create_v2(self):
        for statement in _V2_DDL:
            self.conn.execute(statement)
        self.conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    def _is_v1_schema(self):
        if [row[0] for row in self._objects()] != ["table"]:
            return False
        name = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchone()[0]
        if name != "seen":
            return False
        columns = self.conn.execute("PRAGMA table_info(seen)").fetchall()
        if [(col[1], col[2]) for col in columns] != [
            ("issuer", "TEXT"),
            ("jti", "TEXT"),
        ]:
            return False
        return sorted(col[1] for col in columns if col[5] > 0) == [
            "issuer",
            "jti",
        ]

    def _prepare_schema(self):
        with self.lock:
            version = self.conn.execute(
                "PRAGMA user_version"
            ).fetchone()[0]
            if version == SCHEMA_VERSION:
                table_names = {
                    row[0]
                    for row in self.conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                if not {
                    "seen_jti",
                    "seq_state",
                    "bundle_epoch",
                }.issubset(table_names):
                    raise Fenced("v2 database is missing required tables")
                return
            if version == 0 and not self._objects():
                self._create_v2()
                self.conn.commit()
                return
            if version == 1 and self._is_v1_schema():
                self._migrate_v1()
                return
            raise Fenced("database schema is unknown, corrupt or from a future version")

    def _migrate_v1(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            for statement in _V2_DDL:
                self.conn.execute(statement)
            self.conn.execute(
                "INSERT OR IGNORE INTO seen_jti(issuer, jti) "
                "SELECT issuer, jti FROM seen"
            )
            self.conn.execute("DROP TABLE seen")
            self.conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

    def close(self):
        self.conn.close()


class Gateway:
    """Validates SecureHook v2 webhook tokens against a local key bundle."""

    def __init__(self, db, keys):
        self.bundle = load_bundle(keys)
        self.storage = _Storage(str(db))
        self._fence_bundle_epoch()

    def _fence_bundle_epoch(self):
        with self.storage.lock:
            self.storage.conn.execute("BEGIN IMMEDIATE")
            try:
                row = self.storage.conn.execute(
                    "SELECT epoch FROM bundle_epoch WHERE issuer=?",
                    (self.bundle["issuer"],),
                ).fetchone()
                if row is not None and self.bundle["epoch"] < row[0]:
                    raise Fenced("key bundle epoch has been rolled back")
                self.storage.conn.execute(
                    "INSERT INTO bundle_epoch(issuer, epoch) VALUES(?, ?) "
                    "ON CONFLICT(issuer) DO UPDATE SET "
                    "epoch=CASE WHEN excluded.epoch > bundle_epoch.epoch "
                    "THEN excluded.epoch ELSE bundle_epoch.epoch END",
                    (self.bundle["issuer"], self.bundle["epoch"]),
                )
                self.storage.conn.commit()
            except BaseException:
                self.storage.conn.rollback()
                raise

    def close(self):
        self.storage.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def verify(self, request, token, now, fault=None):
        return self.verify_many(
            [{"request": request, "token": token}], now, fault
        )[0]

    def verify_many(self, items, now, fault=None):
        if not isinstance(items, list) or not items:
            raise Invalid("items must be a non-empty list")
        if not _is_int(now):
            raise Invalid("now must be an integer")
        normalized = []
        for item in items:
            if not isinstance(item, dict) or set(item) != ITEM_FIELDS:
                raise Invalid("each item must have exactly request and token")
            request = item["request"]
            if isinstance(request, (str, Path)):
                request = loads(Path(request).read_bytes())
            if not isinstance(request, dict):
                raise Invalid("request must be a JSON object")
            token = item["token"]
            if not isinstance(token, str):
                raise Invalid("token must be a string")
            normalized.append((request, token))
        results = []
        batch_jtis = set()
        batch_seq = {}
        with self.storage.lock:
            self.storage.conn.execute("BEGIN IMMEDIATE")
            try:
                for request, token in normalized:
                    result = self._verify_one(
                        request, token, now, batch_jtis, batch_seq
                    )
                    results.append(result)
                if fault is not None:
                    after_replay = getattr(fault, "after_replay", None)
                    if callable(after_replay):
                        after_replay()
                    before_commit = getattr(fault, "before_commit", None)
                    if callable(before_commit):
                        before_commit()
                self.storage.conn.commit()
            except BaseException:
                self.storage.conn.rollback()
                raise
        return results

    def _verify_one(self, request, token, now, batch_jtis, batch_seq):
        parts = token.split(".")
        if len(parts) != 3 or not all(parts):
            raise Invalid("token must have three non-empty segments")
        header_segment, payload_segment, signature_segment = parts
        try:
            header_bytes = b64url_decode(header_segment)
            payload_bytes = b64url_decode(payload_segment)
            signature = b64url_decode(signature_segment)
            header = loads(header_bytes)
            payload = loads(payload_bytes)
        except StrictError as exc:
            raise Invalid("malformed token encoding") from exc
        self._check_header(header)
        self._check_signature(header, payload, signature,
                              header_segment, payload_segment)
        self._check_payload(payload)
        self._check_time(payload, now)
        key = self.bundle["keys"][header["kid"]]
        self._check_key_window(key, payload)
        self._check_request(request, payload)
        issuer = payload["iss"]
        jti = payload["jti"]
        if jti in batch_jtis:
            raise Replay("jti was already used in this batch")
        batch_jtis.add(jti)
        stored_jti = self.storage.conn.execute(
            "SELECT 1 FROM seen_jti WHERE issuer=? AND jti=?",
            (issuer, jti),
        ).fetchone()
        if stored_jti is not None:
            raise Replay("jti has already been used")
        domain = (
            issuer,
            payload["tenant"],
            payload["sub"],
            payload["method"],
            payload["target"],
        )
        seq = payload["seq"]
        prior = batch_seq.get(domain)
        if prior is not None:
            if seq <= prior:
                raise Sequence("seq must strictly increase within the batch")
        else:
            row = self.storage.conn.execute(
                "SELECT last_seq FROM seq_state WHERE "
                "issuer=? AND tenant=? AND sub=? AND method=? AND target=?",
                domain,
            ).fetchone()
            if row is not None:
                prior = row[0]
        if prior is not None and seq <= prior:
            raise Sequence("seq must be strictly greater than the last one")
        batch_seq[domain] = seq
        try:
            self.storage.conn.execute(
                "INSERT INTO seen_jti(issuer, jti) VALUES(?, ?)",
                (issuer, jti),
            )
        except sqlite3.IntegrityError as exc:
            raise Replay("jti has already been used") from exc
        self.storage.conn.execute(
            "INSERT INTO seq_state(issuer, tenant, sub, method, target, last_seq) "
            "VALUES(?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(issuer, tenant, sub, method, target) DO UPDATE SET "
            "last_seq=excluded.last_seq",
            (*domain, seq),
        )
        return {
            "issuer": issuer,
            "subject": payload["sub"],
            "tenant": payload["tenant"],
            "jti": jti,
            "seq": seq,
        }

    def _check_header(self, header):
        if not isinstance(header, dict) or set(header) != HEADER_FIELDS:
            raise Invalid("token header has invalid fields")
        if header["typ"] != "SHK2":
            raise Invalid("token typ must be SHK2")
        if header["alg"] not in ("HS256", "RS256"):
            raise Invalid("unsupported token algorithm")
        if not is_nfc_text(header["kid"]):
            raise Invalid("token kid must be a non-empty NFC string")
        key = self.bundle["keys"].get(header["kid"])
        if key is None:
            raise Signature("unknown key id")
        if key["alg"] != header["alg"] or not (
            (header["alg"] == "HS256" and key["kty"] == "oct")
            or (header["alg"] == "RS256" and key["kty"] == "RSA")
        ):
            raise Signature("algorithm is not bound to this key type")

    def _check_signature(self, header, payload, signature,
                         header_segment, payload_segment):
        key = self.bundle["keys"][header["kid"]]
        signing_input = (
            header_segment + "." + payload_segment
        ).encode("ascii")
        if header["alg"] == "HS256":
            expected = hmac.new(
                key["secret"], signing_input, hashlib.sha256
            ).digest()
            if not hmac.compare_digest(expected, signature):
                raise Signature("bad HMAC signature")
        else:
            _verify_rs256(signing_input, signature, key)

    def _check_payload(self, payload):
        if not isinstance(payload, dict) or set(payload) != PAYLOAD_FIELDS:
            raise Invalid("token payload has invalid fields")
        if payload["iss"] != self.bundle["issuer"]:
            raise Invalid("issuer does not match the key bundle")
        if payload["aud"] != self.bundle["audience"]:
            raise Invalid("audience does not match the key bundle")
        for field in ("sub", "tenant", "jti"):
            if not is_nfc_text(payload[field]):
                raise Invalid(f"{field} must be a non-empty NFC string")
        if not _is_positive_int(payload["seq"]):
            raise Invalid("seq must be a positive integer")
        if not _is_positive_int(payload["key_epoch"]):
            raise Invalid("key_epoch must be a positive integer")
        if payload["method"] not in ALLOWED_METHODS:
            raise Invalid("method is not allowed")
        validate_target(payload["target"])
        digest = payload["body_sha256"]
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(ch not in "0123456789abcdef" for ch in digest)
        ):
            raise Invalid("body_sha256 must be lowercase hexadecimal")

    def _check_time(self, payload, now):
        iat = payload["iat"]
        nbf = payload["nbf"]
        exp = payload["exp"]
        for value in (iat, nbf, exp):
            if not _is_int(value):
                raise Invalid("timestamps must be integers")
        if iat - CLOCK_SKEW > now:
            raise Invalid("token issued in the future")
        if nbf - CLOCK_SKEW > now:
            raise Invalid("token is not yet valid")
        if now >= exp + CLOCK_SKEW:
            raise Invalid("token has expired")
        if nbf < iat - CLOCK_SKEW:
            raise Invalid("nbf is too far before iat")
        if exp <= nbf:
            raise Invalid("exp must be after nbf")
        if exp - iat > MAX_LIFETIME:
            raise Invalid("token lifetime exceeds the maximum")

    def _check_key_window(self, key, payload):
        if payload["key_epoch"] != key["epoch"]:
            raise Invalid("key_epoch does not match the signing key")
        if key["epoch"] > self.bundle["epoch"]:
            raise Invalid("key epoch cannot exceed the bundle epoch")
        if payload["iat"] < key["active_from"]:
            raise Invalid("token predates key activation")
        if key["retire_at"] is not None and payload["iat"] >= key["retire_at"]:
            raise Invalid("token postdates key retirement")
        if payload["tenant"] not in key["tenants"]:
            raise Invalid("tenant is not allowed for this key")

    def _check_request(self, request, payload):
        if set(request) != REQUEST_FIELDS:
            raise Invalid("request has invalid fields")
        if request["method"] != payload["method"]:
            raise Invalid("request method does not match the token")
        if request["target"] != payload["target"]:
            raise Invalid("request target does not match the token")
        if not isinstance(request["body_b64"], str):
            raise Invalid("body_b64 must be a string")
        try:
            body = b64url_decode(request["body_b64"])
        except StrictError as exc:
            raise Invalid("body_b64 is not canonical base64url") from exc
        digest = hashlib.sha256(body).hexdigest()
        if not hmac.compare_digest(digest, payload["body_sha256"]):
            raise Invalid("request body does not match the token")
