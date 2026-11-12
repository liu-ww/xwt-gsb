"""Deterministic, standard-library-only builders for SecureHook v2 tests."""

import base64
import hashlib
import hmac
import json
import random


def b64(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def sha_hex(data):
    return hashlib.sha256(data).hexdigest()


def _is_prime(candidate):
    small_primes = (
        2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47,
        53, 59, 61, 67, 71, 73, 79, 83, 89, 97,
    )
    for divisor in small_primes:
        if candidate % divisor == 0:
            return candidate == divisor
    d = candidate - 1
    s = 0
    while d % 2 == 0:
        s += 1
        d //= 2
    for base in (2, 3, 5, 7, 11, 13, 17):
        value = pow(base, d, candidate)
        if value in (1, candidate - 1):
            continue
        for _ in range(s - 1):
            value = pow(value, 2, candidate)
            if value == candidate - 1:
                break
        else:
            return False
    return True


def make_rsa_prime(bits, rng):
    while True:
        candidate = rng.getrandbits(bits) | (1 << (bits - 1)) | 1
        if _is_prime(candidate):
            return candidate


def new_rsa_keypair(bits=1024):
    rng = random.Random(0x5EC4)
    half = bits // 2
    while True:
        p = make_rsa_prime(half, rng)
        q = make_rsa_prime(bits - half, rng)
        if p == q:
            continue
        modulus = p * q
        if modulus.bit_length() != bits:
            continue
        phi = (p - 1) * (q - 1)
        exponent = 65537
        if phi % exponent != 0:
            break
    private = pow(exponent, -1, phi)
    return modulus, exponent, private


def rsa_sign(signing_input, modulus, private):
    size = (modulus.bit_length() + 7) // 8
    digest_info = bytes.fromhex(
        "3031300d060960864801650304020105000420"
    ) + hashlib.sha256(signing_input).digest()
    padding_length = size - len(digest_info) - 3
    encoded = b"\x00\x01" + (b"\xff" * padding_length) + b"\x00" + digest_info
    signature_integer = pow(
        int.from_bytes(encoded, "big"), private, modulus
    )
    return signature_integer.to_bytes(size, "big")


class Signer:
    def __init__(self, kid, alg, material=None, rsa=None):
        self.kid = kid
        self.alg = alg
        self.material = material
        self.rsa = rsa

    def sign(self, header, payload):
        header_text = b64(
            json.dumps(
                header, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        )
        payload_text = b64(
            json.dumps(
                payload, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        )
        signing_input = (header_text + "." + payload_text).encode("ascii")
        if self.alg == "HS256":
            signature = hmac.new(
                self.material, signing_input, hashlib.sha256
            ).digest()
        else:
            signature = rsa_sign(
                signing_input, self.rsa[0], self.rsa[2]
            )
        return header_text + "." + payload_text + "." + b64(signature)


def hs_key(kid="h1", secret=b"secret-material-please-rotate-32b",
           epoch=1, active_from=0, retire_at=None, tenants=("t",)):
    return {
        "kid": kid,
        "kty": "oct",
        "alg": "HS256",
        "material": b64(secret),
        "epoch": epoch,
        "active_from": active_from,
        "retire_at": retire_at,
        "tenants": list(tenants),
    }


def rs_key(kid, rsa, epoch=1, active_from=0, retire_at=None,
           tenants=("t",)):
    return {
        "kid": kid,
        "kty": "RSA",
        "alg": "RS256",
        "n": b64(rsa[0].to_bytes((rsa[0].bit_length() + 7) // 8, "big")),
        "e": b64(rsa[1].to_bytes((rsa[1].bit_length() + 7) // 8, "big")),
        "epoch": epoch,
        "active_from": active_from,
        "retire_at": retire_at,
        "tenants": list(tenants),
    }


def bundle(keys, epoch=1, issuer="issuer", audience="hooks"):
    return {
        "version": 2,
        "issuer": issuer,
        "audience": audience,
        "epoch": epoch,
        "keys": keys,
    }


def payload(jti="j1", seq=1, method="POST", target="/hook?a=1",
            body=b"{}", key_epoch=1, iat=1000, nbf=1000, exp=1200,
            sub="svc", tenant="t", iss="issuer", aud="hooks"):
    return {
        "iss": iss,
        "sub": sub,
        "aud": aud,
        "tenant": tenant,
        "iat": iat,
        "nbf": nbf,
        "exp": exp,
        "jti": jti,
        "seq": seq,
        "method": method,
        "target": target,
        "body_sha256": sha_hex(body),
        "key_epoch": key_epoch,
    }


def request_obj(body=b"{}", method="POST", target="/hook?a=1"):
    return {
        "method": method,
        "target": target,
        "body_b64": b64(body),
    }


def header(kid="h1", alg="HS256"):
    return {"alg": alg, "kid": kid, "typ": "SHK2"}
