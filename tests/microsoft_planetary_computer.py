import unittest
import terragon
import geopandas as gpd
import os
from pathlib import Path

class TestPC(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.tg = terragon.init('pc')
        self.gdf = gpd.read_file(Path("demo_files/data/TUM_OTN.shp.zip"))
        self.arguments = dict(shp=self.gdf, collection='sentinel-2-l2a', start_date='2021-01-01', end_date='2021-01-05', bands=['B02', 'B03', 'B04'], resolution=20, download_folder='tests/download/')

    def test_collections(self):
        col = self.tg.retrieve_collections('sentinel')
        self.assertTrue(len(col) > 0)

    def test_search(self):
        items = self.tg.search(**self.arguments)
        self.assertTrue(len(items) > 0)

    def test_download(self):
        items = self.tg.search(**self.arguments)
        data = self.tg.download(items)
        self.assertTrue(data is not None)

    def test_download_tifs(self):
        items = self.tg.search(**self.arguments)
        fns = self.tg.download(items, create_minicube=False)
        self.assertTrue(len(fns) > 0)
        for fn in fns:
            self.assertTrue(fn.exists())
            fn.unlink()

    def test_create(self):
        self.tg.create(**self.arguments)
        
if __name__ == '__main__':
    unittest.main()