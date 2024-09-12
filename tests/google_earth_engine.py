# %%
import unittest
import os
import ee
import dotenv
import terragon
import geopandas as gpd
from pathlib import Path
from base import _TestBase

class TestGEEInit(unittest.TestCase):
    def test_not_initialized(self):
        self.assertRaises(RuntimeError, terragon.init, 'gee')

class TestGEE(_TestBase, unittest.TestCase):
    def setUp(self):
        super().setUp()

        dotenv.load_dotenv()
        ee.Initialize(project=os.getenv('gee_project_name'))
        
        self.tg = terragon.init('gee')
        self.gdf = gpd.read_file(Path("demo_files/data/TUM_OTN.geojson"))
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
