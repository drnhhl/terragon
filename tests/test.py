import unittest
import EOVoxelCraft as eovc
import geopandas as gpd
from shapely.geometry import Polygon

class TestPC(unittest.TestCase):
    def setUp(self):
        t = eovc.VoxelCrafter()
        t.download()
        self.crafter = eovc.init('pc')
        self.gdf = gpd.GeoDataFrame(geometry=[Polygon(
            [(446993.4666833645, 3383569.8682138897),
            (446993.4666833645, 3371569.8682138897),
            (434993.4666833645, 3371569.8682138897),
            (434993.4666833645, 3383569.8682138897),
            (446993.4666833645, 3383569.8682138897)])],
            crs='EPSG:32616')
        self.arguments = dict(shp=self.gdf, collection='sentinel-2-l2a', start_date='2021-01-01', end_date='2021-01-31')

    def collections(self):
        col = self.crafter.retrieve_collections('sentinel')
        self.assertTrue(len(col) > 0)

        
if __name__ == '__main__':
    unittest.main()
