import hashlib
import ipaddress
import json
import unicodedata


class PolicyError(ValueError):
    pass


_ALLOWED_ACTIONS = ('allow', 'deny')
_PROTOCOLS = ('tcp', 'udp', 'any')
_PACKET_PROTOCOLS = ('tcp', 'udp')
_POLICY_FIELDS = ('version', 'default', 'rules')
_RULE_FIELDS = ('id', 'priority', 'action', 'protocol', 'src', 'dst',
                 'ports', 'principals')
_PACKET_FIELDS = ('principal', 'src_ip', 'dst_ip', 'protocol', 'port')
_MAX_PORT = 65535


def _dumps(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False)


def _exact_fields(obj, fields, label):
    if not isinstance(obj, dict):
        raise PolicyError(label + ' must be an object')
    if set(obj.keys()) != set(fields):
        raise PolicyError(label + ' fields must be exactly ' + ','.join(fields))


def _nfc(text):
    return unicodedata.normalize('NFC', text)


def _parse_cidrs(value, label):
    if not isinstance(value, list) or not value:
        raise PolicyError(label + ' must be a non-empty array')
    networks = []
    for item in value:
        if not isinstance(item, str):
            raise PolicyError(label + ' entries must be CIDR strings')
        try:
            network = ipaddress.ip_network(item, strict=True)
        except ValueError:
            raise PolicyError(label + ' contains invalid or non-canonical CIDR: '
                              + item)
        if str(network) != item:
            raise PolicyError(label + ' CIDR is not canonical: ' + item)
        networks.append(network)
    for index, network in enumerate(networks):
        for other in networks[index + 1:]:
            if network.version != other.version:
                continue
            if network.subnet_of(other) or other.subnet_of(network):
                raise PolicyError(label + ' contains contained or duplicate networks')
            if network.prefixlen == other.prefixlen and network.prefixlen > 0 \
                    and network.supernet() == other.supernet():
                raise PolicyError(label + ' contains mergeable adjacent networks')
    canonical = [str(network) for network in networks]
    if canonical != sorted(canonical) or len(set(canonical)) != len(canonical):
        raise PolicyError(label + ' must be sorted and free of duplicates')
    return networks


def _parse_ports(value, protocol):
    if not isinstance(value, list) or not value:
        raise PolicyError('ports must be a non-empty array')
    intervals = []
    for item in value:
        if (not isinstance(item, list) or len(item) != 2
                or not all(isinstance(number, int) and not isinstance(number, bool)
                           for number in item)):
            raise PolicyError('ports entries must be [start,end] integer pairs')
        start, end = item
        if not 1 <= start <= end <= _MAX_PORT:
            raise PolicyError('ports must be within 1..65535')
        intervals.append((start, end))
    if intervals != sorted(intervals):
        raise PolicyError('ports must be sorted')
    for index in range(1, len(intervals)):
        if intervals[index][0] <= intervals[index - 1][1] + 1:
            raise PolicyError('ports must not overlap or be adjacent')
    if protocol == 'any' and intervals != [(1, _MAX_PORT)]:
        raise PolicyError('protocol any requires ports [[1,65535]]')
    return [list(interval) for interval in intervals]


def _parse_principals(value):
    if not isinstance(value, list) or not value:
        raise PolicyError('principals must be a non-empty array')
    for item in value:
        if not isinstance(item, str) or not item:
            raise PolicyError('principals must be non-empty strings')
        if _nfc(item) != item:
            raise PolicyError('principals must be NFC normalized')
    if len(set(value)) != len(value):
        raise PolicyError('principals must be unique')
    if value != sorted(value):
        raise PolicyError('principals must be sorted')
    if '*' in value and len(value) != 1:
        raise PolicyError('principal wildcard * must appear alone')
    return list(value)


def _cidrs_overlap(left, right):
    return any(network.version == other.version and network.overlaps(other)
               for network in left for other in right)


def _cidrs_contain(container, contained):
    for network in contained:
        if not any(network.version == other.version
                   and network.subnet_of(other)
                   for other in container):
            return False
    return True


def _ports_overlap(left, right):
    return any(start <= other_end and other_start <= end
               for start, end in left
               for other_start, other_end in right)


def _ports_contain(container, contained):
    for start, end in contained:
        if not any(c_start <= start and end <= c_end
                   for c_start, c_end in container):
            return False
    return True


def _principals_overlap(left, right):
    return '*' in left or '*' in right or bool(set(left) & set(right))


def _principals_contain(container, contained):
    if '*' in container:
        return True
    return set(contained).issubset(set(container))


def _protocols_overlap(left, right):
    return left == 'any' or right == 'any' or left == right


def _protocol_contain(container, contained):
    return container == 'any' or container == contained


class PolicyCompiler:
    def compile(self, policy):
        _exact_fields(policy, _POLICY_FIELDS, 'policy')
        if policy['version'] != 1:
            raise PolicyError('version must be 1')
        if policy['default'] not in _ALLOWED_ACTIONS:
            raise PolicyError('default must be allow or deny')
        rules_input = policy['rules']
        if not isinstance(rules_input, list):
            raise PolicyError('rules must be an array')

        rules = []
        seen_ids = set()
        for raw in rules_input:
            _exact_fields(raw, _RULE_FIELDS, 'rule')
            rule_id = raw['id']
            if not isinstance(rule_id, str) or not rule_id:
                raise PolicyError('rule id must be a non-empty string')
            if rule_id in seen_ids:
                raise PolicyError('duplicate rule id: ' + rule_id)
            seen_ids.add(rule_id)
            priority = raw['priority']
            if not isinstance(priority, int) or isinstance(priority, bool):
                raise PolicyError('priority must be an integer')
            action = raw['action']
            if action not in _ALLOWED_ACTIONS:
                raise PolicyError('action must be allow or deny')
            protocol = raw['protocol']
            if protocol not in _PROTOCOLS:
                raise PolicyError('protocol must be tcp, udp or any')
            src = _parse_cidrs(raw['src'], 'src')
            dst = _parse_cidrs(raw['dst'], 'dst')
            ports = _parse_ports(raw['ports'], protocol)
            principals = _parse_principals(raw['principals'])
            rules.append({
                'id': rule_id,
                'priority': priority,
                'action': action,
                'protocol': protocol,
                'src': src,
                'dst': dst,
                'ports': ports,
                'principals': principals,
            })

        self._check_ambiguity(rules)
        self._check_unreachable(rules)

        ordered = sorted(rules, key=lambda rule: (-rule['priority'], rule['id']))
        serialized = [{
            'id': rule['id'],
            'priority': rule['priority'],
            'action': rule['action'],
            'protocol': rule['protocol'],
            'src': [str(network) for network in rule['src']],
            'dst': [str(network) for network in rule['dst']],
            'ports': rule['ports'],
            'principals': rule['principals'],
        } for rule in ordered]
        spec = {'version': 1, 'default': policy['default'], 'rules': serialized}
        spec_hash = hashlib.sha256(_dumps(spec).encode('utf-8')).hexdigest()
        return {
            'spec': spec,
            'spec_sha256': spec_hash,
            'checksum': hashlib.sha256(
                ('netfence-compiled-v1|' + spec_hash).encode('utf-8')
            ).hexdigest(),
        }

    @staticmethod
    def _check_ambiguity(rules):
        for index in range(len(rules)):
            current = rules[index]
            for other in rules[index + 1:]:
                if current['priority'] != other['priority']:
                    continue
                if current['action'] == other['action']:
                    continue
                if (_protocols_overlap(current['protocol'], other['protocol'])
                        and _principals_overlap(current['principals'],
                                                other['principals'])
                        and _cidrs_overlap(current['src'], other['src'])
                        and _cidrs_overlap(current['dst'], other['dst'])
                        and _ports_overlap(current['ports'], other['ports'])):
                    raise PolicyError(
                        'ambiguous same-priority rules with different actions: '
                        + current['id'] + ' vs ' + other['id'])

    @staticmethod
    def _check_unreachable(rules):
        for rule in rules:
            for higher in rules:
                if higher['priority'] <= rule['priority']:
                    continue
                if higher['action'] != rule['action']:
                    continue
                if (_protocol_contain(higher['protocol'], rule['protocol'])
                        and _principals_contain(higher['principals'],
                                                rule['principals'])
                        and _cidrs_contain(higher['src'], rule['src'])
                        and _cidrs_contain(higher['dst'], rule['dst'])
                        and _ports_contain(higher['ports'], rule['ports'])):
                    raise PolicyError(
                        'unreachable rule fully covered by higher priority: '
                        + rule['id'])

    def decide(self, compiled, packet):
        self._verify(compiled)
        _exact_fields(packet, _PACKET_FIELDS, 'packet')
        principal = packet['principal']
        if not isinstance(principal, str) or not principal:
            raise PolicyError('packet principal must be a non-empty string')
        if _nfc(principal) != principal:
            raise PolicyError('packet principal must be NFC normalized')
        protocol = packet['protocol']
        if protocol not in _PACKET_PROTOCOLS:
            raise PolicyError('packet protocol must be tcp or udp')
        port = packet['port']
        if not isinstance(port, int) or isinstance(port, bool) \
                or not 1 <= port <= _MAX_PORT:
            raise PolicyError('packet port must be an integer 1..65535')
        src_ip = self._parse_packet_ip(packet['src_ip'], 'src_ip')
        dst_ip = self._parse_packet_ip(packet['dst_ip'], 'dst_ip')
        if src_ip.version != dst_ip.version:
            raise PolicyError('src_ip and dst_ip must be the same IP family')

        matches = []
        for rule in compiled['spec']['rules']:
            if not _protocols_overlap(rule['protocol'], protocol):
                continue
            if not _principals_overlap(rule['principals'], [principal]):
                continue
            if not self._ip_in_cidrs(src_ip, rule['src']):
                continue
            if not self._ip_in_cidrs(dst_ip, rule['dst']):
                continue
            if not any(start <= port <= end for start, end in rule['ports']):
                continue
            matches.append(rule)

        if not matches:
            return {'decision': compiled['spec']['default'],
                    'rule_id': None, 'priority': None}
        top_priority = max(rule['priority'] for rule in matches)
        top = [rule for rule in matches if rule['priority'] == top_priority]
        chosen = min(top, key=lambda rule: rule['id'])
        return {'decision': chosen['action'], 'rule_id': chosen['id'],
                'priority': chosen['priority']}

    @staticmethod
    def _ip_in_cidrs(ip_value, cidrs):
        for cidr in cidrs:
            network = ipaddress.ip_network(cidr, strict=True)
            if ip_value.version == network.version and ip_value in network:
                return True
        return False

    @staticmethod
    def _parse_packet_ip(value, label):
        if not isinstance(value, str) or not value:
            raise PolicyError(label + ' must be a non-empty IP string')
        if '%' in value:
            raise PolicyError(label + ' must not contain a zone id')
        try:
            parsed = ipaddress.ip_address(value)
        except ValueError:
            raise PolicyError(label + ' is not a valid canonical IP')
        if str(parsed) != value:
            raise PolicyError(label + ' is not a canonical IP literal')
        if parsed.version == 6 and parsed.ipv4_mapped is not None:
            raise PolicyError(label + ' must not be an IPv4-mapped IPv6 address')
        return parsed

    @staticmethod
    def _verify(compiled):
        if not isinstance(compiled, dict):
            raise PolicyError('invalid compiled policy')
        spec = compiled.get('spec')
        spec_hash = compiled.get('spec_sha256')
        checksum = compiled.get('checksum')
        if not isinstance(spec, dict) or not isinstance(spec_hash, str) \
                or not isinstance(checksum, str):
            raise PolicyError('compiled policy is missing integrity fields')
        actual_hash = hashlib.sha256(
            _dumps(spec).encode('utf-8')).hexdigest()
        if actual_hash != spec_hash:
            raise PolicyError('compiled policy spec failed integrity check')
        expected_checksum = hashlib.sha256(
            ('netfence-compiled-v1|' + spec_hash).encode('utf-8')).hexdigest()
        if expected_checksum != checksum:
            raise PolicyError('compiled policy checksum failed')
