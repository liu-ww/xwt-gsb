import argparse
import json
import sys
from pathlib import Path

from .extractor import Extractor


def _policy_from_args(args):
    supplied = [value for value in (args.policy, args.policy_option) if value is not None]
    if args.policy_file is not None:
        supplied.append(args.policy_file)
    if len(supplied) > 1:
        raise ValueError("provide exactly one policy source")

    if supplied:
        value = supplied[0]
        if value.lstrip().startswith("{"):
            return json.loads(value)
        return json.loads(Path(value).read_text(encoding="utf-8-sig"))

    values = (
        args.max_files,
        args.max_file_size,
        args.max_total_size,
        args.max_ratio,
        args.max_depth,
        args.allowed_methods,
    )
    if any(value is None for value in values):
        raise ValueError("provide a policy JSON or all policy options")
    methods = args.allowed_methods
    if methods.strip().startswith("["):
        methods = json.loads(methods)
    else:
        methods = [item.strip() for item in methods.split(",") if item.strip()]
    return {
        "max_files": args.max_files,
        "max_file_size": args.max_file_size,
        "max_total_size": args.max_total_size,
        "max_ratio": args.max_ratio,
        "max_depth": args.max_depth,
        "allowed_methods": methods,
    }


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    if argv and argv[0] == "extract":
        argv = argv[1:]

    parser = argparse.ArgumentParser(prog="vaultunpack")
    parser.add_argument("archive", nargs="?")
    parser.add_argument("destination", nargs="?")
    parser.add_argument("policy", nargs="?", help="policy JSON text or policy JSON file")
    parser.add_argument("--archive", dest="archive_option")
    parser.add_argument("--destination", dest="destination_option")
    parser.add_argument("--policy", dest="policy_option", help="policy JSON text or policy JSON file")
    parser.add_argument("--policy-file", "--policy_file")
    parser.add_argument("--max-files", "--max_files", type=int)
    parser.add_argument("--max-file-size", "--max_file_size", type=int)
    parser.add_argument("--max-total-size", "--max_total_size", type=int)
    parser.add_argument("--max-ratio", "--max_ratio", type=float)
    parser.add_argument("--max-depth", "--max_depth", type=int)
    parser.add_argument("--allowed-methods", "--allowed_methods")
    args = parser.parse_args(argv)

    try:
        policy = _policy_from_args(args)
        if args.archive is not None and args.archive_option is not None:
            raise ValueError("provide archive only once")
        if args.destination is not None and args.destination_option is not None:
            raise ValueError("provide destination only once")
        archive = args.archive or args.archive_option
        destination = args.destination or args.destination_option
        if archive is None or destination is None:
            raise ValueError("archive and destination are required")
        result = Extractor(policy).extract(archive, destination)
        sys.stdout.write(
            json.dumps(result, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n"
        )
        return 0
    except Exception as error:
        message = str(error) or error.__class__.__name__
        sys.stderr.write(f"error: {message}\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
