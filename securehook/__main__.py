import argparse
import json
import sys
from pathlib import Path

from .codec import loads
from .gateway import Fenced, Gateway, Invalid, Replay, Sequence, Signature


BUSINESS_ERRORS = (Fenced, Invalid, Replay, Sequence, Signature)


def _emit_error(exc):
    payload = {
        "error": type(exc).__name__,
        "message": str(exc),
    }
    print(
        json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
    )


def main(argv=None):
    parser = argparse.ArgumentParser(prog="securehook")
    parser.add_argument("--db", required=True)
    parser.add_argument("--keys", required=True)
    subparsers = parser.add_subparsers(dest="command", required=True)
    verify = subparsers.add_parser("verify")
    verify.add_argument("request")
    verify.add_argument("token")
    verify.add_argument("--now", type=int, required=True)
    verify_many = subparsers.add_parser("verify-many")
    verify_many.add_argument("items")
    verify_many.add_argument("--now", type=int, required=True)
    gateway = None
    try:
        args = parser.parse_args(argv)
        gateway = Gateway(args.db, args.keys)
        if args.command == "verify":
            token = Path(args.token).read_text(encoding="utf-8").strip()
            result = gateway.verify(args.request, token, args.now)
        else:
            items = loads(Path(args.items).read_bytes())
            result = gateway.verify_many(items, args.now)
        print(
            json.dumps(
                result,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0
    except BUSINESS_ERRORS as exc:
        _emit_error(exc)
        return 2
    except (OSError, UnicodeError, ValueError) as exc:
        _emit_error(exc)
        return 2
    except Exception:
        print(
            json.dumps(
                {"error": "Internal", "message": "internal verification failure"},
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1
    finally:
        if gateway is not None:
            gateway.close()


if __name__ == "__main__":
    raise SystemExit(main())
