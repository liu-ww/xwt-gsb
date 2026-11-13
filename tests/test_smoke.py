import unittest
from netfence import PolicyCompiler
class Smoke(unittest.TestCase):
 def test_allow(s):
  r={'id':'web','priority':1,'action':'allow','protocol':'tcp','src':['10.0.0.0/24'],'dst':['192.0.2.0/24'],'ports':[[443,443]],'principals':['svc']};p={'version':1,'default':'deny','rules':[r]};c=PolicyCompiler();s.assertEqual(c.decide(c.compile(p),{'principal':'svc','src_ip':'10.0.0.1','dst_ip':'192.0.2.1','protocol':'tcp','port':443})['decision'],'allow')

