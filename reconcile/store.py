"""v2 ledger: immutable history, incremental bitemporal projection, change frames."""
import hashlib
import json
import sqlite3

from .projection import canonical, match_plan, net_cents

SCHEMA = '''
CREATE TABLE IF NOT EXISTS partitions(
    partition TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    checkpoint INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS history(
    partition TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    entity TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    rev INTEGER NOT NULL,
    value_json TEXT NOT NULL,
    PRIMARY KEY(partition, seq)
);
CREATE TABLE IF NOT EXISTS entity_versions(
    entity TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    rev INTEGER NOT NULL,
    value_json TEXT NOT NULL,
    PRIMARY KEY(entity, entity_key, rev)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS event_identities(
    event_id TEXT PRIMARY KEY,
    entity TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    rev INTEGER NOT NULL,
    value_json TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS order_heads(
    order_key TEXT PRIMARY KEY,
    customer TEXT NOT NULL,
    at INTEGER NOT NULL,
    gross_cents INTEGER NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS plan_heads(
    plan_key TEXT PRIMARY KEY,
    customer TEXT NOT NULL,
    start INTEGER NOT NULL,
    finish INTEGER,
    segment TEXT NOT NULL,
    discount_bps INTEGER NOT NULL,
    priority INTEGER NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS group_projection(
    day INTEGER NOT NULL,
    segment TEXT NOT NULL,
    count INTEGER NOT NULL,
    gross_cents INTEGER NOT NULL,
    net_cents INTEGER NOT NULL,
    PRIMARY KEY(day, segment)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS pending_orders(
    order_key TEXT PRIMARY KEY
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS frames(
    generation INTEGER PRIMARY KEY,
    previous TEXT NOT NULL,
    frame_json TEXT NOT NULL,
    digest TEXT NOT NULL
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_order_heads_customer ON order_heads(customer, at, order_key);
CREATE INDEX IF NOT EXISTS idx_plan_heads_customer ON plan_heads(customer, start, finish);
'''


class Conflict(ValueError):
    pass


class Fenced(ValueError):
    pass


class Invalid(ValueError):
    pass


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _non_empty_string(value):
    return isinstance(value, str) and value != ''


def _validate_row(row):
    if not isinstance(row, dict):
        raise Invalid('row must be an object')
    if set(row) != {'seq', 'event_id', 'entity', 'key', 'rev', 'value'}:
        raise Invalid('row has unexpected or missing fields')
    seq, rev, entity, event_id, key, value = (
        row['seq'], row['rev'], row['entity'], row['event_id'], row['key'], row['value'])
    if not _is_int(seq) or seq <= 0:
        raise Invalid('seq must be a positive integer')
    if not _is_int(rev) or rev <= 0:
        raise Invalid('rev must be a positive integer')
    if entity not in ('order', 'plan'):
        raise Invalid('entity must be order or plan')
    for name, text in (('event_id', event_id), ('key', key)):
        if not _non_empty_string(text):
            raise Invalid(f'{name} must be a non-empty string')
    if value is None:
        return entity, key, rev, None
    if not isinstance(value, dict):
        raise Invalid('value must be an object or null')
    if entity == 'order':
        if set(value) != {'customer', 'at', 'gross_cents'}:
            raise Invalid('order value has unexpected or missing fields')
        customer, at, gross = value['customer'], value['at'], value['gross_cents']
        if not _non_empty_string(customer):
            raise Invalid('customer must be a non-empty string')
        if not _is_int(at):
            raise Invalid('at must be an integer')
        if not _is_int(gross):
            raise Invalid('gross_cents must be an integer')
    else:
        if set(value) != {'customer', 'start', 'end', 'segment', 'discount_bps', 'priority'}:
            raise Invalid('plan value has unexpected or missing fields')
        customer = value['customer']
        start, finish = value['start'], value['end']
        segment = value['segment']
        discount, priority = value['discount_bps'], value['priority']
        if not _non_empty_string(customer) or not _non_empty_string(segment):
            raise Invalid('customer and segment must be non-empty strings')
        if not _is_int(start):
            raise Invalid('start must be an integer')
        if not (_is_int(finish) and finish > start) and finish is not None:
            raise Invalid('end must be an integer greater than start or null')
        if not (_is_int(discount) and 0 <= discount <= 10000):
            raise Invalid('discount_bps must be an integer between 0 and 10000')
        if not _is_int(priority):
            raise Invalid('priority must be a signed integer')
    return entity, key, rev, value


class Ledger:
    def __init__(self, path):
        self.path = path
        self.db = sqlite3.connect(path, timeout=30)
        self.db.execute('PRAGMA busy_timeout=30000')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA foreign_keys=ON')
        version = self.db.execute('PRAGMA user_version').fetchone()[0]
        try:
            if version == 0:
                self._create_v2()
            elif version == 1:
                self._migrate_v1()
            elif version != 2:
                raise Invalid(f'unsupported database version {version}')
        except BaseException:
            self.db.close()
            raise

    def _create_v2(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            for statement in SCHEMA.split(';'):
                if statement.strip():
                    self.db.execute(statement)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        self.db.execute('PRAGMA user_version=2')
        self.db.commit()

    def close(self):
        self.db.close()

    # ------------------------------------------------------------------ claim
    def claim(self, partition, owner):
        if not _non_empty_string(partition) or not _non_empty_string(owner):
            raise Invalid('partition and owner must be non-empty strings')
        self.db.execute('BEGIN IMMEDIATE')
        try:
            row = self.db.execute(
                'SELECT epoch FROM partitions WHERE partition=?', (partition,)).fetchone()
            epoch = row[0] + 1 if row else 1
            if row:
                self.db.execute(
                    'UPDATE partitions SET owner=?, epoch=? WHERE partition=?',
                    (owner, epoch, partition))
            else:
                self.db.execute(
                    'INSERT INTO partitions VALUES(?,?,?,0)', (partition, owner, epoch))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return epoch

    # ------------------------------------------------------------------- view
    def view(self):
        self.db.execute('BEGIN DEFERRED')
        try:
            groups = [
                {'day': day, 'segment': segment, 'count': count,
                 'gross_cents': gross, 'net_cents': net}
                for day, segment, count, gross, net in self.db.execute(
                    'SELECT day,segment,count,gross_cents,net_cents'
                    ' FROM group_projection ORDER BY day,segment')]
            pending = [row[0] for row in self.db.execute(
                'SELECT order_key FROM pending_orders ORDER BY order_key')]
            checkpoints = dict(self.db.execute(
                'SELECT partition,checkpoint FROM partitions ORDER BY partition'))
            generation = self.db.execute(
                'SELECT COALESCE(MAX(generation),0) FROM frames').fetchone()[0]
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return {'groups': groups, 'pending': pending,
                'checkpoints': checkpoints, 'generation': generation}

    # ----------------------------------------------------------------- ingest
    def ingest(self, partition, owner, epoch, rows, fault=None):
        if not _non_empty_string(partition) or not _non_empty_string(owner):
            raise Invalid('partition and owner must be non-empty strings')
        if not _is_int(epoch) or epoch <= 0:
            raise Invalid('epoch must be a positive integer')
        if not isinstance(rows, list):
            raise Invalid('rows must be a list')
        parsed = [_validate_row(row) for row in rows]
        seqs = [row['seq'] for row in rows] if rows else []
        if seqs:
            for previous, current in zip(seqs, seqs[1:]):
                if current != previous + 1:
                    raise Invalid('seq values must be consecutive and ascending')

        self.db.execute('BEGIN IMMEDIATE')
        try:
            lease = self.db.execute(
                'SELECT owner,epoch,checkpoint FROM partitions WHERE partition=?',
                (partition,)).fetchone()
            if not lease or lease[0] != owner or lease[1] != epoch:
                raise Fenced('collector no longer owns partition')
            checkpoint = lease[2]
            applied = 0
            touched = {}
            for row, (entity, key, rev, value) in zip(rows, parsed):
                seq = row['seq']
                value_json = canonical(value)
                if seq <= checkpoint:
                    existing = self.db.execute(
                        'SELECT event_id,entity,entity_key,rev,value_json FROM history'
                        ' WHERE partition=? AND seq=?', (partition, seq)).fetchone()
                    expected = (row['event_id'], entity, key, rev, value_json)
                    if not existing or tuple(existing) != expected:
                        raise Conflict('replayed position does not match committed history')
                    continue
                if seq != checkpoint + applied + 1:
                    raise Invalid('new seq values must extend the checkpoint without gaps')
                identity = self.db.execute(
                    'SELECT entity,entity_key,rev,value_json FROM event_identities'
                    ' WHERE event_id=?', (row['event_id'],)).fetchone()
                if identity and tuple(identity) != (entity, key, rev, value_json):
                    raise Conflict('event_id already identifies different content')
                version = self.db.execute(
                    'SELECT value_json FROM entity_versions WHERE entity=? AND entity_key=? AND rev=?',
                    (entity, key, rev)).fetchone()
                if version and version[0] != value_json:
                    raise Conflict(f'{entity} {key} rev {rev} already has different content')
                self.db.execute(
                    'INSERT INTO history VALUES(?,?,?,?,?,?,?)',
                    (partition, seq, row['event_id'], entity, key, rev, value_json))
                if not identity:
                    self.db.execute(
                        'INSERT INTO event_identities VALUES(?,?,?,?,?)',
                        (row['event_id'], entity, key, rev, value_json))
                if not version:
                    self.db.execute(
                        'INSERT INTO entity_versions VALUES(?,?,?,?)',
                        (entity, key, rev, value_json))
                    touched.setdefault((entity, key), set()).add(rev)
                applied += 1

            if applied:
                self.db.execute(
                    'UPDATE partitions SET checkpoint=? WHERE partition=?',
                    (checkpoint + applied, partition))
            if fault and applied:
                fault('after_history')
            generation, recomputed = self._apply_projection(touched, fault, applied)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return {'checkpoint': checkpoint + applied, 'generation': generation,
                'applied': applied, 'recomputed': recomputed}

    # ---------------------------------------------------------- projection
    def _winning(self, entity, key, exclude=()):
        excluded = tuple(exclude)
        if excluded:
            placeholders = ','.join('?' for _ in excluded)
            row = self.db.execute(
                f'SELECT MAX(rev) FROM entity_versions'
                f' WHERE entity=? AND entity_key=? AND rev NOT IN ({placeholders})',
                (entity, key, *excluded)).fetchone()
        else:
            row = self.db.execute(
                'SELECT MAX(rev) FROM entity_versions WHERE entity=? AND entity_key=?',
                (entity, key)).fetchone()
        return row[0]

    def _placement(self, customer, at, gross):
        plan = match_plan(self.db, customer, at)
        if plan is None:
            return None
        segment, discount = plan
        return (at // 86400, segment, gross, net_cents(gross, discount))

    def _capture_state(self, key):
        head = self.db.execute(
            'SELECT customer,at,gross_cents FROM order_heads WHERE order_key=?',
            (key,)).fetchone()
        if head is None:
            return ('absent', None)
        placement = self._placement(head[0], head[1], head[2])
        if placement is None:
            return ('pending', None)
        return ('group', placement)

    def _bump_group(self, day, segment, delta_count, delta_gross, delta_net):
        row = self.db.execute(
            'SELECT count,gross_cents,net_cents FROM group_projection'
            ' WHERE day=? AND segment=?', (day, segment)).fetchone()
        count = (row[0] if row else 0) + delta_count
        gross = (row[1] if row else 0) + delta_gross
        net = (row[2] if row else 0) + delta_net
        if count == 0:
            self.db.execute(
                'DELETE FROM group_projection WHERE day=? AND segment=?', (day, segment))
        elif row:
            self.db.execute(
                'UPDATE group_projection SET count=?,gross_cents=?,net_cents=?'
                ' WHERE day=? AND segment=?', (count, gross, net, day, segment))
        else:
            self.db.execute(
                'INSERT INTO group_projection VALUES(?,?,?,?,?)',
                (day, segment, count, gross, net))

    def _write_frame(self, changes, pending_add, pending_remove):
        previous_row = self.db.execute(
            'SELECT digest FROM frames ORDER BY generation DESC LIMIT 1').fetchone()
        previous = previous_row[0] if previous_row else '0' * 64
        generation = (self.db.execute('SELECT COALESCE(MAX(generation),0) FROM frames').fetchone()[0]) + 1
        body = {'generation': generation, 'previous': previous, 'changes': changes,
                'pending_add': pending_add, 'pending_remove': pending_remove}
        digest = hashlib.sha256(canonical(body).encode('utf-8')).hexdigest()
        frame = dict(body)
        frame['digest'] = digest
        self.db.execute('INSERT INTO frames VALUES(?,?,?,?)',
                        (generation, previous, canonical(frame), digest))
        return generation

    def _apply_projection(self, touched, fault, applied):
        changed_orders = []
        changed_plans = []
        for (entity, key), inserted_revs in touched.items():
            before_rev = self._winning(entity, key, inserted_revs)
            after_rev = self._winning(entity, key)
            if before_rev == after_rev:
                continue
            if entity == 'order':
                changed_orders.append(key)
            else:
                changed_plans.append((key, before_rev, after_rev))

        candidate_keys = set(changed_orders)
        before_state = {key: self._capture_state(key) for key in changed_orders}
        plan_windows = []
        for key, before_rev, after_rev in changed_plans:
            for rev in (before_rev, after_rev):
                if rev is None:
                    continue
                value = json.loads(self.db.execute(
                    'SELECT value_json FROM entity_versions WHERE entity=? AND entity_key=? AND rev=?',
                    ('plan', key, rev)).fetchone()[0])
                if value is not None:
                    plan_windows.append((value['customer'], value['start'], value['end']))
        for customer, start, finish in plan_windows:
            end_bound = finish if finish is not None else 9223372036854775807
            rows = self.db.execute(
                'SELECT order_key FROM order_heads'
                ' WHERE customer=? AND at>=? AND at<?', (customer, start, end_bound))
            for (order_key,) in rows:
                if order_key not in before_state:
                    before_state[order_key] = self._capture_state(order_key)
                candidate_keys.add(order_key)

        for key in changed_orders:
            rev = self._winning('order', key)
            value = json.loads(self.db.execute(
                'SELECT value_json FROM entity_versions WHERE entity=? AND entity_key=? AND rev=?',
                ('order', key, rev)).fetchone()[0])
            if value is None:
                self.db.execute('DELETE FROM order_heads WHERE order_key=?', (key,))
            else:
                self.db.execute(
                    'INSERT INTO order_heads VALUES(?,?,?,?)'
                    ' ON CONFLICT(order_key) DO UPDATE SET'
                    ' customer=excluded.customer,at=excluded.at,gross_cents=excluded.gross_cents',
                    (key, value['customer'], value['at'], value['gross_cents']))
        for key, _before_rev, after_rev in changed_plans:
            value = json.loads(self.db.execute(
                'SELECT value_json FROM entity_versions WHERE entity=? AND entity_key=? AND rev=?',
                ('plan', key, after_rev)).fetchone()[0])
            if value is None:
                self.db.execute('DELETE FROM plan_heads WHERE plan_key=?', (key,))
            else:
                self.db.execute(
                    'INSERT INTO plan_heads VALUES(?,?,?,?,?,?,?)'
                    ' ON CONFLICT(plan_key) DO UPDATE SET customer=excluded.customer,'
                    ' start=excluded.start,finish=excluded.finish,segment=excluded.segment,'
                    ' discount_bps=excluded.discount_bps,priority=excluded.priority',
                    (key, value['customer'], value['start'], value['end'], value['segment'],
                     value['discount_bps'], value['priority']))

        after_state = {key: self._capture_state(key) for key in candidate_keys}

        group_deltas = {}
        pending_add = set()
        pending_remove = set()
        report_changed = False
        for key in candidate_keys:
            before_kind, before_value = before_state[key]
            after_kind, after_value = after_state[key]
            if (before_kind, before_value) == (after_kind, after_value):
                continue
            report_changed = True
            if before_kind == 'group':
                day, segment, gross, net = before_value
                delta = group_deltas.setdefault((day, segment), [0, 0, 0])
                delta[0] -= 1
                delta[1] -= gross
                delta[2] -= net
            elif before_kind == 'pending':
                pending_remove.add(key)
            if after_kind == 'group':
                day, segment, gross, net = after_value
                delta = group_deltas.setdefault((day, segment), [0, 0, 0])
                delta[0] += 1
                delta[1] += gross
                delta[2] += net
            elif after_kind == 'pending':
                pending_add.add(key)
        for key in pending_remove:
            self.db.execute('DELETE FROM pending_orders WHERE order_key=?', (key,))
        for key in pending_add:
            self.db.execute(
                'INSERT OR IGNORE INTO pending_orders VALUES(?)', (key,))
        for (day, segment), (delta_count, delta_gross, delta_net) in group_deltas.items():
            self._bump_group(day, segment, delta_count, delta_gross, delta_net)

        current_generation = self.db.execute(
            'SELECT COALESCE(MAX(generation),0) FROM frames').fetchone()[0]
        if report_changed:
            changes = [
                {'day': day, 'segment': segment, 'count': dc,
                 'gross_cents': dg, 'net_cents': dn}
                for (day, segment), (dc, dg, dn) in sorted(group_deltas.items())
                if dc != 0 or dg != 0 or dn != 0]
            generation = self._write_frame(
                changes, sorted(pending_add), sorted(pending_remove))
        else:
            generation = current_generation
        recomputed = len(candidate_keys)
        if fault and applied:
            fault('after_projection')
            fault('before_commit')
        return generation, recomputed

    # ------------------------------------------------------------- migration
    def _migrate_v1(self):
        names = {row[0] for row in self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        required = {'partitions', 'history'}
        if not required <= names:
            raise Invalid('v1 database is missing required tables')
        for table, columns in (
                ('partitions', ('partition', 'owner', 'epoch', 'checkpoint')),
                ('history', ('partition', 'seq', 'event_id', 'entity', 'entity_key',
                             'rev', 'value_json'))):
            actual = {row[1] for row in self.db.execute(f'PRAGMA table_info({table})')}
            if not set(columns) <= actual:
                raise Invalid(f'v1 table {table} has an unexpected shape')
        v2_tables = ('entity_versions', 'event_identities', 'order_heads', 'plan_heads',
                     'group_projection', 'pending_orders', 'frames')
        self.db.execute('BEGIN IMMEDIATE')
        try:
            for table in v2_tables:
                self.db.execute(f'DROP TABLE IF EXISTS {table}')
            partitions = self.db.execute(
                'SELECT partition,owner,epoch,checkpoint FROM partitions ORDER BY partition').fetchall()
            checkpoints = {partition: checkpoint for partition, _o, _e, checkpoint in partitions}
            seen_positions = {partition: set() for partition, _o, _e, _c in partitions}
            records = self.db.execute(
                'SELECT partition,seq,event_id,entity,entity_key,rev,value_json'
                ' FROM history ORDER BY partition,seq').fetchall()
            replay = []
            for partition, seq, event_id, entity, key, rev, value_json in records:
                if partition not in seen_positions:
                    raise Invalid('v1 history references an unknown partition')
                if seq in seen_positions[partition]:
                    raise Invalid('v1 history contains duplicate positions')
                seen_positions[partition].add(seq)
                try:
                    value = json.loads(value_json)
                except json.JSONDecodeError as exc:
                    raise Invalid('v1 history contains invalid JSON') from exc
                _validate_row({'seq': seq, 'event_id': event_id, 'entity': entity,
                               'key': key, 'rev': rev, 'value': value})
                replay.append((partition, seq, event_id, entity, key, rev, value, value_json))
            for partition, positions in seen_positions.items():
                checkpoint = checkpoints[partition]
                if positions != set(range(1, checkpoint + 1)):
                    raise Invalid('v1 history positions must be contiguous from 1 to checkpoint')
            for statement in SCHEMA.split(';'):
                stripped = statement.strip()
                if stripped and (
                        not stripped.startswith('CREATE TABLE partitions')
                        and not stripped.startswith('CREATE TABLE history')):
                    self.db.execute(stripped)
            for partition, seq, event_id, entity, key, rev, value, value_json in replay:
                self.db.execute(
                    'UPDATE history SET event_id=?,entity=?,entity_key=?,rev=?,value_json=?'
                    ' WHERE partition=? AND seq=?',
                    (event_id, entity, key, rev, canonical(value), partition, seq))
            events = {}
            versions = {}
            for _partition, _seq, event_id, entity, key, rev, value, value_json in replay:
                canonical_json = canonical(value)
                identity = events.get(event_id)
                if identity is not None and identity != (entity, key, rev, canonical_json):
                    raise Conflict('v1 event_id identifies different content')
                events.setdefault(event_id, (entity, key, rev, canonical_json))
                current = versions.get((entity, key, rev))
                if current is not None and current != canonical_json:
                    raise Conflict('v1 version already has different content')
                versions.setdefault((entity, key, rev), canonical_json)
            for event_id, (entity, key, rev, value_json) in events.items():
                self.db.execute('INSERT INTO event_identities VALUES(?,?,?,?,?)',
                                (event_id, entity, key, rev, value_json))
            for (entity, key, rev), value_json in versions.items():
                self.db.execute('INSERT INTO entity_versions VALUES(?,?,?,?)',
                                (entity, key, rev, value_json))
            self._rebuild_heads_and_projection()
            has_report = self.db.execute(
                'SELECT 1 FROM group_projection LIMIT 1').fetchone() is not None
            has_pending = self.db.execute(
                'SELECT 1 FROM pending_orders LIMIT 1').fetchone() is not None
            if has_report or has_pending:
                changes, pending_add = self._snapshot_as_initial_frame()
                self._write_frame(changes, pending_add, [])
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        self.db.execute('PRAGMA user_version=2')
        self.db.commit()

    def _rebuild_heads_and_projection(self):
        heads = self.db.execute(
            'SELECT entity,entity_key,value_json FROM entity_versions'
            ' WHERE rev=(SELECT MAX(rev) FROM entity_versions AS inner_versions'
            ' WHERE inner_versions.entity=entity_versions.entity'
            ' AND inner_versions.entity_key=entity_versions.entity_key)'
        ).fetchall()
        for entity, key, value_json in heads:
            value = json.loads(value_json)
            if value is None:
                continue
            if entity == 'order':
                self.db.execute('INSERT INTO order_heads VALUES(?,?,?,?)',
                                (key, value['customer'], value['at'], value['gross_cents']))
            else:
                self.db.execute('INSERT INTO plan_heads VALUES(?,?,?,?,?,?,?)',
                                (key, value['customer'], value['start'], value['end'],
                                 value['segment'], value['discount_bps'], value['priority']))
        for key, in self.db.execute('SELECT order_key FROM order_heads'):
            customer, at, gross = self.db.execute(
                'SELECT customer,at,gross_cents FROM order_heads WHERE order_key=?',
                (key,)).fetchone()
            placement = self._placement(customer, at, gross)
            if placement is None:
                self.db.execute('INSERT INTO pending_orders VALUES(?)', (key,))
            else:
                day, segment, _gross, net = placement
                self._bump_group(day, segment, 1, gross, net)

    def _snapshot_as_initial_frame(self):
        changes = [
            {'day': day, 'segment': segment, 'count': count,
             'gross_cents': gross, 'net_cents': net}
            for day, segment, count, gross, net in self.db.execute(
                'SELECT day,segment,count,gross_cents,net_cents'
                ' FROM group_projection ORDER BY day,segment')]
        pending = [row[0] for row in self.db.execute(
            'SELECT order_key FROM pending_orders ORDER BY order_key')]
        return changes, pending

    # ---------------------------------------------------------------- export
    def export(self, path, fault=None):
        from .exporter import export_stream
        return export_stream(self, path, fault)

    def committed_frames(self):
        return [row[0].encode('utf-8') + b'\n' for row in self.db.execute(
            'SELECT frame_json FROM frames ORDER BY generation')]
