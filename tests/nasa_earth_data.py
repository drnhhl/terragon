import os
import unittest

import earthaccess
import geopandas as gpd
from base import _TestBase
from shapely.geometry import box
from utils import load_env_variables

import terragon

# TODO remove
# bounding_box = (minx, miny, maxx, maxy)
bounding_box = (-10, 20, 10, 50)
geom = box(*bounding_box)
gdf = gpd.GeoDataFrame({"geometry": [geom]}, crs="EPSG:4326")
# earthaccess.search_data(short_name="Daymet_Daily_V4R1_2129",bounding_box=(-10, 20, 10, 50),temporal=("1999-02", "2019-03"))
#


class Test01ED(unittest.TestCase):  # 01 is important since it should run first
    def test_not_initialized(self):
        self.assertRaises(RuntimeError, terragon.init, "ed")


class Test02ED(_TestBase, unittest.TestCase):
    @classmethod
    def setUpClass(self):
        super().setUpClass()
        load_env_variables()  # load the .env vars if running locally
        earthaccess.login(strategy="environment")

        self.tg = terragon.init("ed")
        self.arguments["collection"] = "C2021957657-LPCLOUD"
        self.arguments["shp"] = gdf
        self.arguments["bands"] = [""]
        self.arguments["start_date"] = "2019-05-01"
        self.arguments["end_date"] = "2019-05-01"

    def test_collections(self):
        col = self.tg.retrieve_collections("landsat")
        self.assertTrue(len(col) > 0)


if __name__ == "__main__":
    unittest.main()
