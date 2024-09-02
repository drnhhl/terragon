import unittest
import terragon
import geopandas as gpd
from terragon.utils import fix_winding_order
import os

# class GeneralTest(unittest.TestCase):
#     def setUp(self):
#         self.tg = terragon.init('pc')
#         self.gdf = gpd.read_file(r"demo_files\TUM_OTN.shp")
#         self.gdf["geometry"] = self.gdf["geometry"].apply(fix_winding_order)
#         self.arguments = dict(shp=self.gdf, collection='sentinel-2-l2a', start_date='2021-01-01', end_date='2021-01-05', bands=['B02', 'B03', 'B04'], resolution=20, download_folder='tests/download/')

#     def test_shps(self,):
#         self.tg.search(**self.arguments)
#         self.assertTrue(len(self.tg._parameters['shp'].index) == 1)

# class TestPC(unittest.TestCase):
#     def setUp(self):
#         self.tg = terragon.init('pc')
#         self.gdf = gpd.read_file(r"demo_files\TUM_OTN.shp")
#         self.gdf["geometry"] = self.gdf["geometry"].apply(fix_winding_order)
#         self.arguments = dict(shp=self.gdf, collection='sentinel-2-l2a', start_date='2021-01-01', end_date='2021-01-05', bands=['B02', 'B03', 'B04'], resolution=20, download_folder='tests/download/')

#     def test_collections(self):
#         col = self.tg.retrieve_collections('sentinel')
#         self.assertTrue(len(col) > 0)

#     def test_search(self):
#         items = self.tg.search(**self.arguments)
#         self.assertTrue(len(items) > 0)

#     def test_download(self):
#         items = self.tg.search(**self.arguments)
#         data = self.tg.download(items)
#         self.assertTrue(data is not None)

#     def test_download_tifs(self):
#         items = self.tg.search(**self.arguments)
#         fns = self.tg.download(items, create_minicube=False)
#         self.assertTrue(len(fns) > 0)
#         for fn in fns:
#             self.assertTrue(os.path.exists(fn))
#             os.remove(fn)

#     def test_create(self):
#         self.tg.create(**self.arguments)
        
# class TestGEE(unittest.TestCase):
#     def setUp(self):
#         self.tg = terragon.init('gee')
#         self.gdf = gpd.read_file(r"demo_files\TUM_OTN.shp")
#         self.gdf['geometry'] = self.gdf['geometry'].apply(fix_winding_order)
#         self.arguments = dict(shp=self.gdf, collection='COPERNICUS/S2_SR_HARMONIZED', start_date='2021-01-01', end_date='2021-01-05', bands=['B2', 'B3', 'B4'], resolution=20, download_folder='tests/download/')

#     def test_collections(self):
#         self.assertRaises(NotImplementedError, self.tg.retrieve_collections)

#     def test_search(self):
#         items = self.tg.search(**self.arguments)
#         col_size = items.size().getInfo()

#         self.assertTrue(col_size > 0)

#     def test_download(self):
#         items = self.tg.search(**self.arguments)
#         data = self.tg.download(items)
#         self.assertTrue(data is not None)

#     def test_download_tifs(self):
#         items = self.tg.search(**self.arguments)
#         fns = self.tg.download(items, create_minicube=False)
#         self.assertTrue(len(fns) > 0)
#         for fn in fns:
#             self.assertTrue(os.path.exists(fn))
#             os.remove(fn)

#     def test_create(self):
#         self.tg.create(**self.arguments)

class TestASF(unittest.TestCase):
    def setUp(self):
        self.tg = terragon.init('asf')
        self.tg.set_credentials(credentials_path=r"credentials\asf_credentials.json")
        self.gdf = gpd.read_file(r"demo_files\data\TUM_OTN.shp")
        self.gdf['geometry'] = self.gdf['geometry'].apply(fix_winding_order)
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
        folders = self.tg.download(items, create_minicube=False)
        self.assertTrue(len(folders) > 0)
        for folder in folders:
            self.assertTrue(os.path.exists(folder))
            os.remove(folder)

if __name__ == '__main__':
    unittest.main()
