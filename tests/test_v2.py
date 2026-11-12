import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
import uuid
from pathlib import Path

from securehook import Fenced, Gateway, Invalid, Replay, Sequence, Signature
from securehook.codec import StrictError, b64url_decode, loads
from securehook.gateway import load_bundle, validate_target

from tests._helpers import (
    Signer,
    b64,
    bundle,
    header,
    hs_key,
    new_rsa_keypair,
    payload,
    request_obj,
    rs_key,
)


class Workspace:
    def __init__(self):
        self.root = Path(tempfile.gettempdir()) / "securehook-tests" / uuid.uuid4().hex
        self.root.mkdir(parents=True)
        self.db = self.root / "state.sqlite"
        self.keys_path = self.root / "keys.json"

    def write_keys(self, data):
        self.keys_path.write_text(
            json.dumps(data, ensure_ascii=False), encoding="utf-8"
        )
        return self.keys_path

    def gateway(self, data):
        return Gateway(self.db, self.write_keys(data))

    def cleanup(self):
        shutil.rmtree(self.root, ignore_errors=True)


class StrictCodecTests(unittest.TestCase):
    def test_duplicate_keys_are_rejected(self):
        with self.assertRaises(StrictError):
            loads(b'{"a":1,"a":2}')

    def test_non_finite_numbers_are_rejected(self):
        for text in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(text=text):
                with self.assertRaises(StrictError):
                    loads(text)

    def test_non_nfc_and_control_and_surrogate_are_rejected(self):
        nfd = '"e\u0301"'
        with self.assertRaises(StrictError):
            loads(nfd)
        with self.assertRaises(StrictError):
            loads('"line\u0000break"')
        with self.assertRaises(StrictError):
            loads('"lone\ud800surrogate"')

    def test_top_level_scalar_is_rejected(self):
        with self.assertRaises(StrictError):
            loads(b"123")

    def test_base64url_must_be_canonical_and_unpadded(self):
        self.assertEqual(b64url_decode(b64(b"\xfb\xff")), b"\xfb\xff")
        with self.assertRaises(StrictError):
            b64url_decode(b64(b"abc") + "=")
        with self.assertRaises(StrictError):
            b64url_decode("A")
        with self.assertRaises(StrictError):
            b64url_decode("ab c")
        with self.assertRaises(StrictError):
            b64url_decode("ab+c")


class TargetTests(unittest.TestCase):
    def test_valid_targets(self):
        for target in (
            "/hook",
            "/hook/a?a=1&b=2",
            "/x%2Fy?name=%E4%B8%AD",
            "/users/me:profile?x=1&y=&z=a%20b",
        ):
            validate_target(target)

    def test_invalid_targets(self):
        bad = (
            "hook",
            "//hook",
            "/hook//x",
            "/./x",
            "/x/../y",
            "/x/.",
            "/x/..",
            "/a//b?x=1",
            "/x?a",
            "/x?a=1&a=2",
            "/x#frag",
            "/x\\y",
            "/x?a=%61",
            "/x?%61=1",
            "/x?a=%2f",
            "/x?b=2&a=1",
            "/?=1",
            "/x\u0000",
            "/x?a=1&&b=2",
        )
        for target in bad:
            with self.subTest(target=target):
                with self.assertRaises(Invalid):
                    validate_target(target)


class BundleTests(unittest.TestCase):
    def setUp(self):
        self.work = Workspace()

    def tearDown(self):
        self.work.cleanup()

    def test_reject_bad_bundle_shapes(self):
        base = bundle([hs_key()])
        bad_bundles = []
        wrong_version = dict(base, version=1)
        bad_bundles.append(wrong_version)
        bad_bundles.append(dict(base, extra=True))
        bad_bundles.append(dict(base, epoch=0))
        bad_bundles.append(dict(base, epoch=True))
        bad_bundles.append(dict(base, issuer=""))
        bad_bundles.append(dict(base, keys=[]))
        for candidate in bad_bundles:
            with self.subTest(candidate=candidate):
                with self.assertRaises(Invalid):
                    load_bundle(json.dumps(candidate).encode())

    def test_reject_bad_hs_key(self):
        short = hs_key()
        short["material"] = b64(b"x" * 31)
        with self.assertRaises(Invalid):
            load_bundle(json.dumps(bundle([short])).encode())
        padded = hs_key()
        padded["material"] = padded["material"] + "="
        with self.assertRaises(Invalid):
            load_bundle(json.dumps(bundle([padded])).encode())
        wrong_kty = hs_key()
        wrong_kty["kty"] = "RSA"
        with self.assertRaises(Invalid):
            load_bundle(json.dumps(bundle([wrong_kty])).encode())
        bad_window = hs_key(active_from=100, retire_at=50)
        with self.assertRaises(Invalid):
            load_bundle(json.dumps(bundle([bad_window])).encode())
        bad_tenants = hs_key(tenants=("b", "a"))
        with self.assertRaises(Invalid):
            load_bundle(json.dumps(bundle([bad_tenants])).encode())
        dup_tenants = hs_key(tenants=("a", "a"))
        with self.assertRaises(Invalid):
            load_bundle(json.dumps(bundle([dup_tenants])).encode())
        with self.assertRaises(Invalid):
            load_bundle(json.dumps(bundle([hs_key(), hs_key()])).encode())

    def test_reject_bad_rsa_key(self):
        rsa = new_rsa_keypair()
        small_rsa = new_rsa_keypair(512)
        small = rs_key("r1", small_rsa)
        with self.assertRaises(Invalid):
            load_bundle(json.dumps(bundle([small])).encode())
        even_exp = dict(rs_key("r1", rsa))
        even_exp["e"] = b64((65538).to_bytes(3, "big"))
        with self.assertRaises(Invalid):
            load_bundle(json.dumps(bundle([even_exp])).encode())
        leading_zero = dict(rs_key("r1", rsa))
        leading_zero["n"] = b64(b"\x00" + rsa[0].to_bytes(128, "big"))
        with self.assertRaises(Invalid):
            load_bundle(json.dumps(bundle([leading_zero])).encode())
        wrong_kty = dict(rs_key("r1", rsa))
        wrong_kty["kty"] = "oct"
        with self.assertRaises(Invalid):
            load_bundle(json.dumps(bundle([wrong_kty])).encode())


class Hs256GatewayTests(unittest.TestCase):
    def setUp(self):
        self.work = Workspace()
        self.secret = b"0123456789abcdef0123456789abcdef"
        self.signer = Signer("h1", "HS256", material=self.secret)
        self.gateway = self.work.gateway(bundle([hs_key(secret=self.secret)]))

    def tearDown(self):
        self.gateway.close()
        self.work.cleanup()

    def make(self, **overrides):
        data = payload(**overrides)
        token = self.signer.sign(header(), data)
        body = overrides.get("body", b"{}")
        request = request_obj(
            body=body,
            method=data["method"],
            target=data["target"],
        )
        return request, token, data

    def test_happy_path_and_replay(self):
        request, token, data = self.make(jti="j1")
        result = self.gateway.verify(request, token, 1100)
        self.assertEqual(result["jti"], "j1")
        self.assertEqual(result["seq"], 1)
        with self.assertRaises(Replay):
            self.gateway.verify(request, token, 1100)

    def test_replay_persists_after_reopen(self):
        request, token, _ = self.make(jti="persist")
        self.assertEqual(
            self.gateway.verify(request, token, 1100)["jti"], "persist"
        )
        self.gateway.close()
        reopened = Gateway(self.work.db, self.work.keys_path)
        with self.assertRaises(Replay):
            reopened.verify(request, token, 1100)
        reopened.close()

    def test_strict_sequence_per_domain(self):
        request, token_seq1, _ = self.make(jti="s1", seq=1)
        request2, token_seq2, _ = self.make(jti="s2", seq=2)
        request0, token_seq0, _ = self.make(jti="s3", seq=1)
        self.gateway.verify(request, token_seq1, 1100)
        self.gateway.verify(request2, token_seq2, 1100)
        with self.assertRaises(Sequence):
            self.gateway.verify(request0, token_seq0, 1100)
        other_target = request_obj(target="/other")
        other_token = self.signer.sign(
            header(), payload(jti="s4", seq=1, target="/other")
        )
        self.assertEqual(
            self.gateway.verify(other_target, other_token, 1100)["seq"], 1
        )

    def test_time_window_rules(self):
        request, token, _ = self.make(jti="future", iat=2000, nbf=2000, exp=2200)
        with self.assertRaises(Invalid):
            self.gateway.verify(request, token, 1900)
        request, token, _ = self.make(jti="expired", iat=1000, nbf=1000, exp=1001)
        with self.assertRaises(Invalid):
            self.gateway.verify(request, token, 1031)
        request, token, _ = self.make(jti="lived", iat=1000, nbf=1000, exp=1301)
        with self.assertRaises(Invalid):
            self.gateway.verify(request, token, 1100)
        request, token, _ = self.make(jti="nbf", iat=1000, nbf=900, exp=1100)
        with self.assertRaises(Invalid):
            self.gateway.verify(request, token, 1000)
        boundary = self.signer.sign(
            header(), payload(jti="boundary", iat=1000, nbf=1000, exp=1300)
        )
        self.assertEqual(
            self.gateway.verify(request_obj(), boundary, 970)["jti"], "boundary"
        )

    def test_key_active_and_retire_windows(self):
        self.gateway.close()
        keys = bundle([
            hs_key(secret=self.secret, active_from=500, retire_at=1500)
        ])
        self.gateway = self.work.gateway(keys)
        request, early_token, _ = self.make(jti="early", iat=400, nbf=400, exp=600)
        with self.assertRaises(Invalid):
            self.gateway.verify(request, early_token, 500)
        request, retired_token, _ = self.make(jti="old", iat=1500, nbf=1500, exp=1600)
        with self.assertRaises(Invalid):
            self.gateway.verify(request, retired_token, 1520)

    def test_request_binding(self):
        request, token, data = self.make(jti="bind")
        tampered = dict(request, body_b64=b64(b"tampered"))
        with self.assertRaises(Invalid):
            self.gateway.verify(tampered, token, 1100)
        padded = dict(request, body_b64=request["body_b64"] + "=")
        with self.assertRaises(Invalid):
            self.gateway.verify(padded, token, 1100)
        with self.assertRaises(Invalid):
            self.gateway.verify(dict(request, target="/other"), token, 1100)
        with self.assertRaises(Invalid):
            self.gateway.verify(dict(request, extra=True), token, 1100)

    def test_algorithm_confusion_is_rejected(self):
        rsa = new_rsa_keypair()
        rsa_header = header(kid="h1", alg="RS256")
        token = self.signer.sign(rsa_header, payload(jti="confuse"))
        with self.assertRaises(Signature):
            self.gateway.verify(request_obj(), token, 1100)
        other = self.work.gateway(bundle([rs_key("r1", rsa)]))
        hs_token = Signer("r1", "HS256", material=rsa[0].to_bytes(128, "big")).sign(
            header(kid="r1", alg="HS256"), payload(jti="confuse2")
        )
        with self.assertRaises(Signature):
            other.verify(request_obj(), hs_token, 1100)
        other.close()

    def test_token_encoding_and_field_rules(self):
        data = payload(jti="shape")
        token = self.signer.sign(header(), data)
        head, body, sign = token.split(".")
        with self.assertRaises(Invalid):
            self.gateway.verify(request_obj(), head + "." + body, 1100)
        with self.assertRaises(Invalid):
            self.gateway.verify(request_obj(), head + "." + body + ".", 1100)
        with self.assertRaises(Invalid):
            self.gateway.verify(request_obj(), head + "=." + body + "." + sign, 1100)
        bad_header = self.signer.sign(
            dict(header(), typ="JWT"), payload(jti="typ")
        )
        with self.assertRaises(Invalid):
            self.gateway.verify(request_obj(), bad_header, 1100)
        bad_header = self.signer.sign(
            dict(header(), extra=1), payload(jti="extra")
        )
        with self.assertRaises(Invalid):
            self.gateway.verify(request_obj(), bad_header, 1100)
        bad_payload = payload(jti="missing")
        del bad_payload["aud"]
        with self.assertRaises(Invalid):
            self.gateway.verify(
                request_obj(), self.signer.sign(header(), bad_payload), 1100
            )
        bad_payload = payload(jti="bool")
        bad_payload["seq"] = True
        with self.assertRaises(Invalid):
            self.gateway.verify(
                request_obj(), self.signer.sign(header(), bad_payload), 1100
            )
        bad_payload = payload(jti="aud")
        bad_payload["aud"] = "other"
        with self.assertRaises(Invalid):
            self.gateway.verify(
                request_obj(), self.signer.sign(header(), bad_payload), 1100
            )
        bad_payload = payload(jti="epoch")
        bad_payload["key_epoch"] = 9
        with self.assertRaises(Invalid):
            self.gateway.verify(
                request_obj(), self.signer.sign(header(), bad_payload), 1100
            )

    def test_bad_signature_and_unknown_key(self):
        token = self.signer.sign(header(), payload(jti="sig"))
        head, body, sign = token.split(".")
        signature = bytearray(b64url_decode(sign))
        signature[-1] ^= 0x01
        forged = head + "." + body + "." + b64(bytes(signature))
        with self.assertRaises(Signature):
            self.gateway.verify(request_obj(), forged, 1100)
        unknown = self.signer.sign(header(kid="nope"), payload(jti="nope"))
        with self.assertRaises(Signature):
            self.gateway.verify(request_obj(), unknown, 1100)

    def test_tenant_is_bound_to_key(self):
        data = payload(jti="tenant", tenant="other")
        token = self.signer.sign(header(), data)
        with self.assertRaises(Invalid):
            self.gateway.verify(request_obj(), token, 1100)

    def test_float_timestamps_and_non_integer_now_are_rejected(self):
        data = payload(jti="float-time", iat=1000.0, nbf=1000, exp=1200)
        token = self.signer.sign(header(), data)
        with self.assertRaises(Invalid):
            self.gateway.verify(request_obj(), token, 1100)
        good_token = self.signer.sign(header(), payload(jti="bad-now"))
        with self.assertRaises(Invalid):
            self.gateway.verify(request_obj(), good_token, 1100.5)

    def test_duplicate_json_keys_in_token_are_rejected(self):
        head, body, sign = self.signer.sign(header(), payload(jti="dup")).split(".")
        raw = b64url_decode(head)
        duplicated = b'{"alg":"HS256","alg":"HS256","kid":"h1","typ":"SHK2"}'
        bad_head = b64(duplicated)
        with self.assertRaises(Invalid):
            self.gateway.verify(
                request_obj(), ".".join((bad_head, body, sign)), 1100
            )


class Rs256GatewayTests(unittest.TestCase):
    def setUp(self):
        self.work = Workspace()
        self.rsa = new_rsa_keypair(1024)
        self.signer = Signer("r1", "RS256", rsa=self.rsa)
        self.gateway = self.work.gateway(bundle([rs_key("r1", self.rsa)]))

    def tearDown(self):
        self.gateway.close()
        self.work.cleanup()

    def test_rs256_happy_path_and_tamper(self):
        token = self.signer.sign(header("r1", "RS256"), payload(jti="r1"))
        self.assertEqual(
            self.gateway.verify(request_obj(), token, 1100)["jti"], "r1"
        )
        head, body, sign = token.split(".")
        signature = bytearray(b64url_decode(sign))
        signature[-1] ^= 0x01
        with self.assertRaises(Signature):
            self.gateway.verify(
                request_obj(),
                ".".join((head, body, b64(bytes(signature)))),
                1100,
            )

    def test_strict_pkcs1_length_and_digest(self):
        token = self.signer.sign(header("r1", "RS256"), payload(jti="r2"))
        head, body, sign = token.split(".")
        short = b64url_decode(sign)[:-1]
        with self.assertRaises(Signature):
            self.gateway.verify(
                request_obj(), ".".join((head, body, b64(short))), 1100
            )
        size = (self.rsa[0].bit_length() + 7) // 8
        digest_info = bytes.fromhex(
            "3031300d060960864801650304020105000420"
        ) + b"\x00" * 32
        padding_length = size - len(digest_info) - 3
        forged_block = (
            b"\x00\x01" + b"\xff" * padding_length + b"\x00" + digest_info
        )
        forged_signature = pow(
            int.from_bytes(forged_block, "big"), self.rsa[2], self.rsa[0]
        ).to_bytes(size, "big")
        with self.assertRaises(Signature):
            self.gateway.verify(
                request_obj(),
                ".".join((head, body, b64(forged_signature))),
                1100,
            )


class BatchAndFaultTests(unittest.TestCase):
    def setUp(self):
        self.work = Workspace()
        self.secret = b"0123456789abcdef0123456789abcdef"
        self.signer = Signer("h1", "HS256", material=self.secret)
        self.gateway = self.work.gateway(bundle([hs_key(secret=self.secret)]))

    def tearDown(self):
        self.gateway.close()
        self.work.cleanup()

    def item(self, jti, seq=1, **overrides):
        data = payload(jti=jti, seq=seq, **overrides)
        return {
            "request": request_obj(
                method=data["method"], target=data["target"]
            ),
            "token": self.signer.sign(header(), data),
        }

    def test_batch_success_ordering_and_internal_replay(self):
        items = [
            self.item("b1", seq=1),
            self.item("b2", seq=2),
            self.item("b3", seq=1, target="/other"),
        ]
        results = self.gateway.verify_many(items, 1100)
        self.assertEqual([item["jti"] for item in results], ["b1", "b2", "b3"])
        with self.assertRaises(Replay):
            self.gateway.verify_many([self.item("b1")], 1100)

    def test_batch_rejects_duplicate_jti_without_consuming_state(self):
        items = [self.item("dup", seq=1), self.item("dup", seq=2)]
        with self.assertRaises(Replay):
            self.gateway.verify_many(items, 1100)
        single = self.gateway.verify_many([self.item("dup", seq=1)], 1100)
        self.assertEqual(single[0]["jti"], "dup")

    def test_batch_sequence_requires_input_order_increase(self):
        items = [self.item("q1", seq=2), self.item("q2", seq=1)]
        with self.assertRaises(Sequence):
            self.gateway.verify_many(items, 1100)
        self.gateway.verify_many([self.item("q1", seq=1)], 1100)

    def test_failed_batch_is_fully_atomic(self):
        good = self.item("good", seq=1)
        bad_request = dict(good["request"], body_b64=b64(b"different"))
        bad = {"request": bad_request, "token": good["token"]}
        with self.assertRaises(Invalid):
            self.gateway.verify_many([good, bad], 1100)
        self.assertEqual(
            self.gateway.verify_many([self.item("good", seq=1)], 1100)[0]["jti"],
            "good",
        )

    def test_empty_batch_and_bad_item_shape(self):
        with self.assertRaises(Invalid):
            self.gateway.verify_many([], 1100)
        with self.assertRaises(Invalid):
            self.gateway.verify_many([{"token": "x"}], 1100)

    def test_fault_hooks_roll_back_the_whole_batch(self):
        class Fault:
            def after_replay(self):
                self.after_called = True

            def before_commit(self):
                raise RuntimeError("simulated crash before commit")

        fault = Fault()
        with self.assertRaises(RuntimeError):
            self.gateway.verify_many([self.item("crash")], 1100, fault=fault)
        self.assertTrue(fault.after_called)
        self.gateway.close()
        reopened = Gateway(self.work.db, self.work.keys_path)
        self.assertEqual(
            reopened.verify_many([self.item("crash")], 1100)[0]["jti"], "crash"
        )
        reopened.close()


class RotationAndBindingTests(unittest.TestCase):
    def setUp(self):
        self.work = Workspace()
        self.old_secret = b"a" * 32
        self.new_secret = b"b" * 32
        keys = [
            hs_key(
                kid="old", secret=self.old_secret, epoch=1,
                active_from=0, retire_at=1500,
            ),
            hs_key(
                kid="new", secret=self.new_secret, epoch=2,
                active_from=1400, retire_at=None,
            ),
        ]
        self.gateway = self.work.gateway(bundle(keys, epoch=2))

    def tearDown(self):
        self.gateway.close()
        self.work.cleanup()

    def test_both_generations_validate_in_their_windows(self):
        old_signer = Signer("old", "HS256", material=self.old_secret)
        new_signer = Signer("new", "HS256", material=self.new_secret)
        old_token = old_signer.sign(
            header("old"),
            payload(jti="old", key_epoch=1, iat=1450, nbf=1450, exp=1490),
        )
        self.assertEqual(
            self.gateway.verify(request_obj(), old_token, 1460)["jti"], "old"
        )
        new_token = new_signer.sign(
            header("new"),
            payload(jti="new", key_epoch=2, seq=2,
                    iat=1500, nbf=1500, exp=1600),
        )
        self.assertEqual(
            self.gateway.verify(request_obj(), new_token, 1520)["jti"], "new"
        )

    def test_key_epoch_must_match_the_signing_key(self):
        signer = Signer("new", "HS256", material=self.new_secret)
        token = signer.sign(
            header("new"),
            payload(jti="mismatch", key_epoch=1, iat=1500, nbf=1500, exp=1600),
        )
        with self.assertRaises(Invalid):
            self.gateway.verify(request_obj(), token, 1520)

    def test_query_order_is_part_of_the_bound_target(self):
        target = "/hook?a=1&b=2"
        reordered = "/hook?b=2&a=1"
        signer = Signer("new", "HS256", material=self.new_secret)
        token = signer.sign(
            header("new"),
            payload(jti="target", key_epoch=2, target=target,
                    iat=1500, nbf=1500, exp=1600),
        )
        with self.assertRaises(Invalid):
            self.gateway.verify(request_obj(target=reordered), token, 1520)
        self.assertEqual(
            self.gateway.verify(request_obj(target=target), token, 1520)["jti"],
            "target",
        )

    def test_method_must_be_allowed_and_uppercase(self):
        signer = Signer("new", "HS256", material=self.new_secret)
        for method in ("post", "GET", "HEAD", "OPTIONS"):
            data = payload(
                jti="m" + method, key_epoch=2, method=method,
                iat=1500, nbf=1500, exp=1600,
            )
            with self.assertRaises(Invalid):
                self.gateway.verify(
                    request_obj(method=method),
                    signer.sign(header("new"), data), 1520,
                )


class FenceMigrationConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.work = Workspace()
        self.secret = b"0123456789abcdef0123456789abcdef"
        self.signer = Signer("h1", "HS256", material=self.secret)

    def tearDown(self):
        shutil.rmtree(self.work.root, ignore_errors=True)

    def gateway(self, epoch, key_epoch=None):
        data = bundle(
            [hs_key(secret=self.secret, epoch=key_epoch or epoch)],
            epoch=epoch,
        )
        return self.work.gateway(data)

    def test_epoch_rollback_is_fenced_and_state_is_not_lowered(self):
        first = self.gateway(3)
        first.close()
        with self.assertRaises(Fenced):
            self.gateway(2)
        again = self.gateway(3)
        again.close()
        stored = sqlite3.connect(self.work.db)
        self.assertEqual(
            stored.execute("SELECT epoch FROM bundle_epoch").fetchone()[0], 3
        )
        stored.close()

    def test_future_schema_version_is_rejected(self):
        conn = sqlite3.connect(self.work.db)
        conn.execute("PRAGMA user_version=99")
        conn.commit()
        conn.close()
        with self.assertRaises(Fenced):
            Gateway(self.work.db, self.work.write_keys(bundle([hs_key(secret=self.secret)])))

    def test_v1_database_is_migrated_in_place(self):
        conn = sqlite3.connect(self.work.db)
        conn.execute(
            "CREATE TABLE seen(issuer TEXT, jti TEXT, "
            "PRIMARY KEY(issuer, jti))"
        )
        conn.execute("INSERT INTO seen VALUES(?, ?)", ("issuer", "legacy"))
        conn.execute("PRAGMA user_version=1")
        conn.commit()
        conn.close()
        gateway = self.gateway(1)
        stored = sqlite3.connect(self.work.db)
        self.assertEqual(
            stored.execute("PRAGMA user_version").fetchone()[0], 2
        )
        self.assertEqual(
            stored.execute(
                "SELECT jti FROM seen_jti WHERE issuer='issuer'"
            ).fetchone()[0],
            "legacy",
        )
        tables = {
            row[0]
            for row in stored.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        self.assertNotIn("seen", tables)
        self.assertTrue(
            {"seen_jti", "seq_state", "bundle_epoch"}.issubset(tables)
        )
        stored.close()
        data = payload(jti="legacy")
        with self.assertRaises(Replay):
            gateway.verify(
                request_obj(), self.signer.sign(header(), data), 1100
            )
        gateway.close()

    def test_corrupt_legacy_database_is_rejected(self):
        conn = sqlite3.connect(self.work.db)
        conn.execute("CREATE TABLE seen(other TEXT PRIMARY KEY)")
        conn.execute("PRAGMA user_version=1")
        conn.commit()
        conn.close()
        with self.assertRaises(Fenced):
            Gateway(self.work.db, self.work.write_keys(bundle([hs_key(secret=self.secret)])))

    def test_unreadable_database_file_is_rejected(self):
        self.work.db.write_bytes(b"this is definitely not a sqlite file")
        with self.assertRaises(Fenced):
            Gateway(self.work.db, self.work.write_keys(bundle([hs_key(secret=self.secret)])))

    def test_concurrent_verification_only_one_wins(self):
        gateway = self.gateway(1)
        token = self.signer.sign(header(), payload(jti="race"))
        errors = []

        def worker():
            try:
                gateway.verify(request_obj(), token, 1100)
            except Replay as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(errors), 7)
        gateway.close()

    def test_concurrent_gateway_processes_share_database(self):
        first = self.gateway(1)
        token = self.signer.sign(header(), payload(jti="multi"))
        self.assertEqual(first.verify(request_obj(), token, 1100)["jti"], "multi")
        second = Gateway(self.work.db, self.work.keys_path)
        with self.assertRaises(Replay):
            second.verify(request_obj(), token, 1100)
        first.close()
        second.close()


class CliTests(unittest.TestCase):
    def setUp(self):
        self.work = Workspace()
        self.secret = b"0123456789abcdef0123456789abcdef"
        self.signer = Signer("h1", "HS256", material=self.secret)
        self.work.write_keys(bundle([hs_key(secret=self.secret)]))
        self.request_path = self.work.root / "request.json"
        self.token_path = self.work.root / "token.txt"
        self.request_path.write_text(
            json.dumps(request_obj()), encoding="utf-8"
        )

    def tearDown(self):
        self.work.cleanup()

    def run_cli(self, *arguments):
        return subprocess.run(
            [sys.executable, "-m", "securehook",
             "--db", str(self.work.db),
             "--keys", str(self.work.keys_path), *arguments],
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parents[1],
        )

    def test_verify_success_prints_sorted_json(self):
        token = self.signer.sign(header(), payload(jti="cli"))
        self.token_path.write_text(token + "\n", encoding="utf-8")
        result = self.run_cli(
            "verify", str(self.request_path), str(self.token_path),
            "--now", "1100",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        parsed = json.loads(result.stdout.strip())
        self.assertEqual(parsed["jti"], "cli")
        self.assertEqual(
            result.stdout.strip(),
            json.dumps(parsed, sort_keys=True, separators=(",", ":")),
        )

    def test_verify_many_and_replay_exit_codes(self):
        items = [
            {
                "request": request_obj(),
                "token": self.signer.sign(header(), payload(jti="one")),
            },
            {
                "request": request_obj(target="/other"),
                "token": self.signer.sign(
                    header(), payload(jti="two", target="/other")
                ),
            },
        ]
        items_path = self.work.root / "items.json"
        items_path.write_text(json.dumps(items), encoding="utf-8")
        result = self.run_cli(
            "verify-many", str(items_path), "--now", "1100"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(json.loads(result.stdout.strip())), 2)
        replay = self.run_cli(
            "verify-many", str(items_path), "--now", "1100"
        )
        self.assertEqual(replay.returncode, 2)
        error = json.loads(replay.stdout.strip())
        self.assertEqual(error["error"], "Replay")
        self.assertNotIn("Traceback", replay.stdout)

    def test_bad_input_is_exit_two_without_traceback(self):
        result = self.run_cli(
            "verify", str(self.request_path),
            str(self.work.root / "missing-token.txt"), "--now", "1100",
        )
        self.assertEqual(result.returncode, 2)
        payload = json.loads(result.stdout.strip())
        self.assertIn("error", payload)
        self.assertNotIn("Traceback", result.stdout)


if __name__ == "__main__":
    unittest.main()
