import unittest
import os
import ee
import dotenv
import terragon
from base import _TestBase

class Test01GEE(unittest.TestCase): # 01 is important since it should run first
    def test_not_initialized(self):
        self.assertRaises(RuntimeError, terragon.init, 'gee')

class Test02GEE(_TestBase, unittest.TestCase):
    def setUp(self):
        super().setUp()

        dotenv.load_dotenv()
        ee.Initialize(project=os.getenv('gee_project_name'))
        
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
    suite = unittest.TestSuite()

    # make sure init test is run before, otherwise gee will be already initialized
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(Test01GEE))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(Test02GEE))

    runner = unittest.TextTestRunner()
    runner.run(suite)
