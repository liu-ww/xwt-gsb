import argparse
import json
from pathlib import Path
from .gateway import Gateway, Invalid, Replay, Sequence, Signature, Fenced


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--keys', required=True)
    sub = parser.add_subparsers(dest='command', required=True)
    one = sub.add_parser('verify'); one.add_argument('request'); one.add_argument('token'); one.add_argument('--now', type=int, required=True)
    many = sub.add_parser('verify-many'); many.add_argument('items'); many.add_argument('--now', type=int, required=True)
    gateway = None
    try:
        args = parser.parse_args(); gateway = Gateway(args.db, args.keys)
        if args.command == 'verify': result = gateway.verify(args.request, Path(args.token).read_text(encoding='utf-8').strip(), args.now)
        else: result = gateway.verify_many(json.loads(Path(args.items).read_text(encoding='utf-8')), args.now)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(',', ':')))
        return 0
    except (Invalid, Replay, Sequence, Signature, Fenced, OSError, ValueError) as exc:
        print(json.dumps({'error': type(exc).__name__, 'message': str(exc)}, ensure_ascii=False, sort_keys=True, separators=(',', ':')))
        return 2
    finally:
        if gateway: gateway.close()


if __name__ == '__main__': raise SystemExit(main())

