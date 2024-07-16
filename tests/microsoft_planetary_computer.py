import unittest
import EOVoxelCraft as eovc
import geopandas as gpd
import os
from pathlib import Path

class TestPC(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.crafter = eovc.init('pc')
        self.gdf = gpd.read_file(Path("demo_files/data/TUM_OTN.shp.zip"))
        self.arguments = dict(shp=self.gdf, collection='sentinel-2-l2a', start_date='2021-01-01', end_date='2021-01-05', bands=['B02', 'B03', 'B04'], resolution=20, download_folder='tests/download/')

    def test_collections(self):
        col = self.crafter.retrieve_collections('sentinel')
        self.assertTrue(len(col) > 0)

    def test_search(self):
        items = self.crafter.search(**self.arguments)
        self.assertTrue(len(items) > 0)

    def test_download(self):
        items = self.crafter.search(**self.arguments)
        data = self.crafter.download(items)
        self.assertTrue(data is not None)

    def test_download_tifs(self):
        items = self.crafter.search(**self.arguments)
        fns = self.crafter.download(items, create_minicube=False)
        self.assertTrue(len(fns) > 0)
        for fn in fns:
            self.assertTrue(fn.exists())
            fn.unlink()

    def test_create(self):
        self.crafter.create(**self.arguments)
        
if __name__ == '__main__':
    unittest.main()