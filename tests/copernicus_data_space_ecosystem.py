import unittest
import terragon
import geopandas as gpd
import shutil
from pathlib import Path
import json

class TestCDSE(unittest.TestCase):
    def setUp(self):
        with open(file="/localhome/hoeh_pa/Organization/Technical/credentials/credentials.json") as file:
            self.credentials = json.load(file)
        self.tg = terragon.init('cdse', self.credentials)
        self.gdf = gpd.read_file(Path("demo_files/data/TUM_OTN_4326.shp.zip"))
        self.arguments = dict(shp=self.gdf, collection='SENTINEL-2', start_date='2021-01-01', end_date='2021-01-05', bands=['B02', 'B03', 'B04'], resolution=20, download_folder='tests/download/')

    def test_collections(self):
        col = self.tg.retrieve_collections('sentinel')
        self.assertTrue(len(col) > 0)
    
    def test_search(self):
        items = self.tg.search(**self.arguments)
        self.assertTrue(len(items) > 0)

    def test_cube(self):
        items = self.tg.search(**self.arguments)
        cube = self.tg.download(items[:4], create_minicube=True)
        self.assertTrue(cube is not None)

    def test_download_tifs(self):
        items = self.tg.search(**self.arguments)
        fns = self.tg.download(items[:4], create_minicube=False)
        self.assertTrue(len(fns) > 0)
        for fn in fns:
            self.assertTrue(Path(fn).exists())
        shutil.rmtree(self.arguments["download_folder"])

    def test_create(self):
        self.tg.create(**self.arguments)

if __name__ == '__main__':
    unittest.main()