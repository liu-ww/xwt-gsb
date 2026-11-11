"""Pure projection helpers: canonical JSON, half-even rounding, plan matching."""
import json


def canonical(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def net_cents(gross_cents, discount_bps):
    """Round gross_cents*(10000-discount_bps)/10000 to nearest, ties to even."""
    numerator = gross_cents * (10000 - discount_bps)
    quotient, remainder = divmod(numerator, 10000)
    if 2 * remainder > 10000 or (2 * remainder == 10000 and quotient % 2):
        quotient += 1
    return quotient


def match_plan(db, customer, at):
    """Best plan for (customer, at): max priority, then max start, then min key."""
    return db.execute(
        'SELECT segment,discount_bps FROM plans'
        ' WHERE customer=? AND start<=? AND (finish IS NULL OR finish>?)'
        ' ORDER BY priority DESC, start DESC, plan_key ASC LIMIT 1',
        (customer, at, at)).fetchone()
