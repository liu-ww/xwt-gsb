import ipaddress
class PolicyCompiler:
 def compile(self,policy):return {'version':1,'default':policy['default'],'rules':policy['rules']}
 def decide(self,compiled,packet):
  for r in compiled['rules']:
   if r['protocol'] not in ('any',packet['protocol']):continue
   if not any(ipaddress.ip_address(packet['src_ip']) in ipaddress.ip_network(x) for x in r['src']):continue
   if not any(ipaddress.ip_address(packet['dst_ip']) in ipaddress.ip_network(x) for x in r['dst']):continue
   if not any(a<=packet['port']<=b for a,b in r['ports']):continue
   if '*' not in r['principals'] and packet['principal'] not in r['principals']:continue
   return {'decision':r['action'],'rule_id':r['id'],'priority':r['priority']}
  return {'decision':compiled['default'],'rule_id':None,'priority':None}

