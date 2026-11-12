import argparse
import json
import sys

from .pipeline import Warehouse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--warehouse', required=True)
    sub = parser.add_subparsers(dest='command', required=True)
    one = sub.add_parser('apply'); one.add_argument('batch')
    many = sub.add_parser('apply-many'); many.add_argument('batches', nargs='+')
    sub.add_parser('query')
    args = parser.parse_args()
    warehouse = None
    try:
        warehouse = Warehouse(args.warehouse)
        if args.command == 'apply': result = warehouse.apply_batch(args.batch)
        elif args.command == 'apply-many': result = warehouse.apply_batches(args.batches)
        else: result = warehouse.query()
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(',', ':')))
        return 0
    except (ValueError, KeyError, OSError) as exc:
        print(json.dumps({'error': type(exc).__name__, 'message': str(exc)}, ensure_ascii=False, sort_keys=True, separators=(',', ':')))
        return 2
    finally:
        if warehouse is not None: warehouse.close()


if __name__ == '__main__':
    raise SystemExit(main())

