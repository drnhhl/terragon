# %%
import unittest
import os
import ee
import dotenv
import terragon
import geopandas as gpd
from pathlib import Path

class TestGEEInit(unittest.TestCase):
    def test_not_initialized(self):
        self.assertRaises(RuntimeError, terragon.init, 'gee')

class TestGEE(unittest.TestCase):
    def setUp(self):
        dotenv.load_dotenv()
        ee.Initialize(project=os.getenv('gee_project_name'))
        self.tg = terragon.init('gee')
        self.gdf = gpd.read_file(Path("demo_files/data/TUM_OTN.shp.zip"))
        self.arguments = dict(shp=self.gdf, collection='COPERNICUS/S2_SR_HARMONIZED', start_date='2021-01-01', end_date='2021-01-05', bands=['B2', 'B3', 'B4'], resolution=20, download_folder='tests/download/')

    def test_collections(self):
        self.assertRaises(NotImplementedError, self.tg.retrieve_collections)

    def test_search(self):
        items = self.tg.search(**self.arguments)
        col_size = items.size().getInfo()

        self.assertTrue(col_size > 0)

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

    def test_create_xee(self):
        # TODO: gets a lot of time steps for whatever reason
        # TODO get different amount of pixels
        self.tg = terragon.init('gee', engine='xee')
        # ds = self.tg.create(**self.arguments)
        # print(ds)
        # try utm crs: EPSG:32632
        self.arguments['shp'] = self.arguments['shp'].to_crs('EPSG:32632')
        ds = self.tg.create(**self.arguments)
        print(ds.time)

if __name__ == '__main__':
    unittest.main()
