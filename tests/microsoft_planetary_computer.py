import unittest

from base import _TestBase

import terragon


class TestPC(_TestBase, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.tg = terragon.init('pc')
        self.arguments['collection'] = 'sentinel-2-l2a'
        self.arguments['bands'] = ['B02', 'B03', 'B04']

if __name__ == '__main__':
    unittest.main()