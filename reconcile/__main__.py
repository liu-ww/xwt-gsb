import argparse
import json
from .store import Ledger


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    commands = parser.add_subparsers(dest='command', required=True)
    claim = commands.add_parser('claim')
    claim.add_argument('partition'); claim.add_argument('owner')
    ingest = commands.add_parser('ingest')
    for name in ('partition','owner','epoch','json_file'):
        ingest.add_argument(name)
    commands.add_parser('view')
    export = commands.add_parser('export'); export.add_argument('output')
    ledger = None
    try:
        args = parser.parse_args()
        ledger = Ledger(args.db)
        if args.command == 'claim':
            result = {'epoch':ledger.claim(args.partition,args.owner)}
        elif args.command == 'ingest':
            with open(args.json_file,encoding='utf-8') as source:
                rows = json.load(source)
            result = ledger.ingest(args.partition,args.owner,int(args.epoch),rows)
        elif args.command == 'export':
            result = {'appended':ledger.export(args.output)}
        else:
            result = ledger.view()
        print(json.dumps(result,ensure_ascii=False,sort_keys=True))
        return 0
    except (ValueError,OSError) as exc:
        print(json.dumps({'error':type(exc).__name__,'message':str(exc)},ensure_ascii=False))
        return 2
    finally:
        if ledger:
            ledger.close()


if __name__ == '__main__':
    raise SystemExit(main())
