#! /usr/bin/env python

from hash_ring import MemcacheRing
import pylibmc
import unittest2

hosts = [
    "127.0.0.1:11222",
    "127.0.0.1:11223",
    "127.0.0.1:11224",
]

class MCTestCase(unittest2.TestCase):

    def setUp(self):
        self.pmc = pylibmc.Client(hosts)
        self.pmc.behaviors = {'ketama': True}
        self.mcr = MemcacheRing(hosts)

class TestMCSetGet(MCTestCase):

    def test_pmc_set_mcr_get(self):

        for i in range(0, 5000):
            key = 'key:%d' % (i,)
            self.pmc.set(key, 'test')
            self.assertEquals(self.pmc.get(key), self.mcr.get(key))

    def test_mcr_set_pmc_get(self):
        for i in range(0, 5000):
            key = 'otherkey:%d' % (i,)
            self.mcr.set(key, 'test')
            self.assertEquals(self.mcr.get(key), self.pmc.get(key))

class SomeType(object):
    def __init__(self, one, two):
        self.one = one
        self.two = two

    def __eq__(self, other):
        return self.one == other.one and self.two == other.two

class TestPickling(MCTestCase):

    def test_pmc_pickle_mcr_unpickle(self):
        obj = SomeType('hi', 'ok')
        self.pmc.set('testkey', obj)
        self.assertEquals(self.mcr.get('testkey'), obj)

    def test_mcr_pickle_pmc_unpickle(self):
        obj = SomeType('hi', 'ok')
        self.mcr.set('testkey', obj)
        self.assertEquals(self.pmc.get('testkey'), obj)

class TestAppend(MCTestCase):

    def test_pmc_set_mcr_append(self):
        self.pmc.set('testkey', '+55 ')
        self.mcr.append('testkey', '-44 ')
        self.assertEquals(self.mcr.get('testkey'), '+55 -44 ')

    def test_mcr_set_pmc_append(self):
        self.mcr.set('testkey', '+55 ')
        self.pmc.append('testkey', '-44 ')
        self.assertEquals(self.pmc.get('testkey'), '+55 -44 ')

if __name__ == '__main__':
    unittest2.main()