import json


def append_snapshot(ledger, path, fault=None):
    with open(path, 'a', encoding='utf-8') as stream:
        stream.write(json.dumps(ledger.view(), ensure_ascii=False)+'\n')
        stream.flush()
        if fault:
            fault('after_append')
    return 1
