import unittest
import EOVoxelCraft as eovc
import geopandas as gpd
from EOVoxelCraft.utils import fix_winding_order
import os
from pathlib import Path

class TestASF(unittest.TestCase):
    def setUp(self):
        self.crafter = eovc.init('asf')
        self.crafter.set_credentials(credentials_path=r"credentials\asf_credentials.json")
        self.gdf = gpd.read_file(Path("demo_files/data/TUM_OTN.shp.zip"))
        self.gdf['geometry'] = self.gdf['geometry']
        self.arguments = dict(shp=self.gdf, collection='sentinel-1', start_date='2021-01-01', end_date='2021-06-30', download_folder='downloads', processing_level='GRD_HD')

    def test_credentials(self):
        self.assertTrue(self.crafter.credentials is not None)

    def test_collections(self):
        cols = self.crafter.retrieve_collections(filter_by_name='sentinel-1')
        self.assertTrue(len(cols) > 0)

    def test_search(self):
        items = self.crafter.search(**self.arguments)
        self.assertTrue(len(items) > 0)
        
    def test_download(self):
        items = self.crafter.search(**self.arguments)
        data = self.crafter.download(items[:1])
        self.assertTrue(data is not None)

    def test_download_zips(self):
        items = self.crafter.search(**self.arguments)
        folders = self.crafter.download(items, create_minicube=False)
        self.assertTrue(len(folders) > 0)
        for folder in folders:
            self.assertTrue(os.path.exists(folder))
            os.remove(folder)

if __name__ == '__main__':
    unittest.main()