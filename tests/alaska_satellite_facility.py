import unittest
import terragon
import geopandas as gpd
import shutil
import os
from pathlib import Path
from base import _TestBase

class TestASF(_TestBase, unittest.TestCase):
    def setUp(self):
        self.tg = terragon.init('asf')
        self.tg.set_credentials(credentials_path=r"credentials\asf_credentials.json")
        self.gdf = gpd.read_file(Path("demo_files/data/TUM_OTN.geojson"))
        self.gdf['geometry'] = self.gdf['geometry']
        self.arguments = dict(shp=self.gdf, collection='sentinel-1', start_date='2021-01-01', end_date='2021-06-30', download_folder='downloads', processing_level='GRD_HD')

    def test_credentials(self):
        self.assertTrue(self.tg.credentials is not None)

    def test_collections(self):
        cols = self.tg.retrieve_collections(filter_by_name='sentinel-1')
        self.assertTrue(len(cols) > 0)

    def test_search(self):
        items = self.tg.search(**self.arguments)
        self.assertTrue(len(items) > 0)
        
    def test_download(self):
        items = self.tg.search(**self.arguments)
        data = self.tg.download(items[:1])
        self.assertTrue(data is not None)

    def test_download_zips(self):
        items = self.tg.search(**self.arguments)
        fns = self.tg.download(items, create_minicube=False)
        self.assertTrue(len(fns) > 0)
        for fn in fns:
            self.assertTrue(Path(fn).exists())
        shutil.rmtree(self.arguments["download_folder"])

if __name__ == '__main__':
    unittest.main()