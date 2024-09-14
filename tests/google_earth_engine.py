import unittest
import os
import ee
import terragon
from base import _TestBase
from utils import load_env_variables

class Test01GEE(unittest.TestCase): # 01 is important since it should run first
    def test_not_initialized(self):
        self.assertRaises(RuntimeError, terragon.init, 'gee')

class Test02GEE(_TestBase, unittest.TestCase):
    def setUp(self):
        super().setUp()
        load_env_variables() # load the .env vars if running locally
        ee.Initialize(project=os.getenv('GEE_PROJECT_NAME'))
        
        self.tg = terragon.init('gee')
        self.arguments['collection'] = 'COPERNICUS/S2_SR_HARMONIZED'
        self.arguments['bands'] = ['B2', 'B3', 'B4']

    def test_collections(self):
        """overwrite test collections since they are not implemented in GEE"""
        self.assertRaises(NotImplementedError, self.tg.retrieve_collections)

    def test_search(self):
        """overwrite test search since item size needs to be validated differently"""
        items = self.tg.search(**self.arguments)
        col_size = items.size().getInfo()

        self.assertTrue(col_size > 0)

if __name__ == '__main__':
    unittest.main()
