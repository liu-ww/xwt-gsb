import json
import sys

from .compiler import PolicyCompiler, PolicyError


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1 or argv[0] in ('-h', '--help'):
        print('usage: python -m netfence <policy.json>', file=sys.stderr)
        return 2
    try:
        with open(argv[0], 'r', encoding='utf-8') as handle:
            policy = json.load(handle)
        compiled = PolicyCompiler().compile(policy)
    except (OSError, ValueError, PolicyError) as error:
        print('error: ' + str(error), file=sys.stderr)
        return 2
    print(json.dumps(compiled, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    sys.exit(main())
