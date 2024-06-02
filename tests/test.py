import unittest
import EOVoxelCraft as eovc
import geopandas as gpd
from shapely.geometry import Polygon
import os
import logging

class GeneralTest(unittest.TestCase):
    def setUp(self):
        self.crafter = eovc.init('pc')
        self.gdf = gpd.GeoDataFrame(geometry=[Polygon(
            [(446993, 3383569),
            (446993, 3371569),
            (434993, 3371569),
            (434993, 3383569),
            (446993, 3383569)]),
            Polygon(
            [(446993, 3383569),
            (446993, 3371569),
            (434993, 3371569),
            (434993, 3383569),
            (446993, 3383569)])],
            crs='EPSG:32616')
        self.arguments = dict(shp=self.gdf, collection='sentinel-2-l2a', start_date='2021-01-01', end_date='2021-01-05', bands=['B02', 'B03', 'B04'], resolution=20, download_folder='tests/download/')

    def test_shps(self,):
        self.crafter.search(**self.arguments)
        self.assertTrue(len(self.crafter._parameters['shp'].index) == 1)

class TestPC(unittest.TestCase):
    def setUp(self):
        self.crafter = eovc.init('pc')
        self.gdf = gpd.GeoDataFrame(geometry=[Polygon(
            [(446993, 3383569),
            (446993, 3371569),
            (434993, 3371569),
            (434993, 3383569),
            (446993, 3383569)])],
            crs='EPSG:32616')
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
            self.assertTrue(os.path.exists(fn))
            os.remove(fn)

    def test_create(self):
        self.crafter.create(**self.arguments)
        
class TestGEE(unittest.TestCase):
    def setUp(self):
        self.crafter = eovc.init('gee')
        self.gdf = gpd.GeoDataFrame(geometry=[Polygon(
            [(446993, 3383569),
            (446993, 3371569),
            (434993, 3371569),
            (434993, 3383569),
            (446993, 3383569)])],
            crs='EPSG:32616')
        self.arguments = dict(shp=self.gdf, collection='COPERNICUS/S2_SR_HARMONIZED', start_date='2021-01-01', end_date='2021-01-05', bands=['B2', 'B3', 'B4'], resolution=20, download_folder='tests/download/')

    def test_collections(self):
        self.assertRaises(NotImplementedError, self.crafter.retrieve_collections)

    def test_search(self):
        items = self.crafter.search(**self.arguments)
        col_size = items.size().getInfo()

        self.assertTrue(col_size > 0)

    def test_download(self):
        items = self.crafter.search(**self.arguments)
        data = self.crafter.download(items)
        self.assertTrue(data is not None)

    def test_download_tifs(self):
        items = self.crafter.search(**self.arguments)
        fns = self.crafter.download(items, create_minicube=False)
        self.assertTrue(len(fns) > 0)
        for fn in fns:
            self.assertTrue(os.path.exists(fn))
            os.remove(fn)

    def test_create(self):
        self.crafter.create(**self.arguments)

class TestASF(unittest.TestCase):
    def setUp(self):
        self.crafter = eovc.init('asf')
        self.gdf = gpd.GeoDataFrame(geometry=[Polygon(
            [(446993, 3383569),
            (446993, 3371569),
            (434993, 3371569),
            (434993, 3383569),
            (446993, 3383569)])],
            crs='EPSG:32616')
        self.arguments = dict(shp=self.gdf, collection='sentinel-1', start_date='2021-01-01', end_date='2021-01-31', processing_level='GRD')

    def test_collections(self):
        col = self.crafter.retrieve_collections(filter_by_name='sentinel-1')
        print(f"Number of collections retrieved: {len(col)}")
        self.assertTrue(len(col) > 0)

    def test_search(self):
        items = self.crafter.search(**self.arguments)
        # [print(item.properties["fileName"]) for item in items]
        self.assertTrue(len(items) > 0)

    def test_download(self):
        pass
        
if __name__ == '__main__':
    unittest.main()
