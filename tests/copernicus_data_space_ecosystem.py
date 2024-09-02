import unittest
import terragon
import geopandas as gpd
import os
from pathlib import Path

# class TestCDSE(unittest.TestCase):
#     def setUp(self):
#         self.tg = terragon.init('cdse')
#         self.gdf = gpd.read_file(Path("demo_files/data/TUM_OTN.shp.zip"))
#         # self.arguments = dict(shp=self.gdf, collection='COPERNICUS/S2_SR_HARMONIZED', start_date='2021-01-01', end_date='2021-01-05', bands=['B2', 'B3', 'B4'], resolution=20, download_folder='tests/download/')

#     def test_collections(self):
#         pass
    
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

if __name__ == '__main__':
    unittest.main()