"""Current projection implementation, predating historical corrections."""
import json


def rebuild(db):
    heads = {}
    for row in db.execute('SELECT entity,entity_key,value_json FROM history ORDER BY rowid'):
        heads[(row[0], row[1])] = json.loads(row[2])
    plans = [value for (kind, _), value in heads.items() if kind == 'plan' and value]
    groups, pending = {}, []
    for (kind, key), order in heads.items():
        if kind != 'order' or not order:
            continue
        matches = [p for p in plans if p['customer'] == order['customer'] and p['start'] <= order['at'] and (p['end'] is None or order['at'] < p['end'])]
        if not matches:
            pending.append(key)
            continue
        plan = matches[-1]
        group_key = (order['at'] // 86400, plan['segment'])
        group = groups.setdefault(group_key, dict(day=group_key[0], segment=group_key[1], count=0, gross_cents=0, net_cents=0))
        group['count'] += 1
        group['gross_cents'] += order['gross_cents']
        group['net_cents'] += round(order['gross_cents'] * (1-plan['discount_bps']/10000))
    return {'groups': [groups[k] for k in sorted(groups)], 'pending': sorted(pending)}
