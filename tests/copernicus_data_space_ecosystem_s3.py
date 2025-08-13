import os
import unittest

import geopandas as gpd
from base import _TestBase
from shapely.geometry import Polygon
from utils import load_env_variables

import terragon


class TestCDSES3(_TestBase, unittest.TestCase):
    @classmethod
    def setUpClass(self):
        super().setUpClass()

        load_env_variables()  # load the .env vars if running locally
        credentials = {
            "aws_access_key_id": os.environ.get("S3_ACCESS_KEY"),
            "aws_secret_access_key": os.environ.get("S3_SECRET_KEY"),
        }
        self.tg = terragon.init("cdse", credentials)
        self.arguments["collection"] = "SENTINEL-2"
        self.arguments["bands"] = ["B02", "B03", "B04"]
        self.arguments["filter"] = {"processingLevel": {"eq": "S2MSI2A"}}

    def test_error_on_no_band(self):
        """test error if no band is given"""
        args = self.arguments.copy()
        args["bands"] = []
        self.assertRaises(ValueError, self.tg.create, **args)

    def test_not_using_rasterio_virt_env(self):
        """test when the rasterio environment is not used"""
        args = self.arguments.copy()
        args["bands"] = ["B02"]
        args["use_virtual_rasterio_file"] = False
        ds = self.tg.create(**args)

        self.assertTrue(
            len(ds.time) == self.nr_time_steps
            and self.width - 1 <= len(ds.x) <= self.width + 1
            and self.height - 1 <= len(ds.y) <= self.height + 1
        )

    def test_s2_mosaic(self):
        """test Sentinel-2 data"""
        args = self.arguments.copy()
        args["collection"] = "GLOBAL-MOSAICS"
        args["bands"] = ["B02"]
        args["start_date"] = "2022-01-01"
        args["end_date"] = "2022-07-31"
        args["resolution"] = 10
        args["filter"] = {"platformShortName": {"eq": "SENTINEL-2"}}

        ds = self.tg.create(**args)

        self.assertTrue(
            len(ds.time) == 3
            and self.width - 1 <= len(ds.x) <= self.width + 1
            and self.height - 1 <= len(ds.y) <= self.height + 1
        )

    def test_s1(self):
        """test Sentinel-1 data"""
        args = self.arguments.copy()
        args["collection"] = "SENTINEL-1"
        args["bands"] = ["VV", "VH"]
        args["start_date"] = "2022-01-01"
        args["end_date"] = "2022-01-04"
        args["resolution"] = 10
        args["filter"] = {"productType": {"eq": "IW_GRDH_1S-COG"}}
        args["save_metadata"] = ["id"]

        ds = self.tg.create(**args)

        # check meta data
        self.assertTrue("id" in ds.coords)
        times = ds.time.dt.strftime("%Y%m%d")
        ids_dates = [id.split("_")[5].split("T")[0] for id in ds.id.values]
        self.assertTrue(all(times == ids_dates))

        # test if the dimensions are correct, with +/- 1 pixel distance
        self.assertTrue(
            len(ds.time) == self.nr_time_steps
            and self.width - 1 <= len(ds.x) <= self.width + 1
            and self.height - 1 <= len(ds.y) <= self.height + 1
        )

        # not supported file format for raw data
        args["filter"] = {"productType": {"eq": "IW_RAW__0S"}}  # only dat files
        self.assertRaises(RuntimeError, self.tg.create, **args)

    def test_multiple_files_s1(self):
        """test Sentinel-1 data with SLC, which have multiple files."""
        args = self.arguments.copy()
        args["collection"] = "SENTINEL-1"
        args["bands"] = ["VV"]
        args["start_date"] = "2022-01-01"
        args["end_date"] = "2022-01-03"
        args["resolution"] = 10
        args["filter"] = {"productType": {"eq": "IW_SLC__1S"}}
        # use other region to include two files -> test only for 2 vars and time
        args["shp"] = gpd.GeoDataFrame(
            geometry=[
                Polygon(
                    [
                        (9.295, 47.425),
                        (9.296, 47.425),
                        (9.296, 47.426),
                        (9.295, 47.426),
                    ]
                )
            ],
            crs="EPSG:4326",
        )

        ds = self.tg.create(**args)

        self.assertTrue(len(ds.data_vars) == 2 and len(ds.time) == 1)

    def test_s1_rtc(self):
        """test Sentinel-1-RTC data"""
        args = self.arguments.copy()
        coords = [
            [
                [39.618673, -14.899018],
                [39.618673, -14.891632],
                [39.631033, -14.891632],
                [39.631033, -14.899018],
                [39.618673, -14.899018],
            ]
        ]
        polygon = Polygon(coords[0])
        args["shp"] = gpd.GeoDataFrame(index=[0], crs="EPSG:4326", geometry=[polygon])
        args["collection"] = "SENTINEL-1-RTC"
        args["bands"] = ["VV"]
        args["start_date"] = "2018-02-25"
        args["end_date"] = "2018-02-25"
        args["resolution"] = 20
        args["filter"] = {}

        ds = self.tg.create(**args)

        nr_time_steps = 2
        # do not test width and height because the shape was taken arbitrarily from the first example
        self.assertTrue(len(ds.time) == nr_time_steps)

    def test_s2_landcover(self):
        """test Sentinel-2 landcover data with RGB file."""
        args = self.arguments.copy()
        args["collection"] = "S2GLC"
        args["bands"] = ["S2GLC"]
        args["start_date"] = "2019-01-01"
        args["end_date"] = "2019-12-31"
        args["resolution"] = 10
        args["filter"] = {}
        # args["filter_asset_path"] = {"S2GLC": "^(?!.*RGB).*"}
        args["filter_asset_path"] = {"S2GLC": ".*RGB.tif"}

        ds = self.tg.create(**args)

        self.assertTrue(
            len(ds.data_vars) == 3
            and len(ds.time) == 1
            and self.width - 1 <= len(ds.x) <= self.width + 1
            and self.height - 1 <= len(ds.y) <= self.height + 1
        )

    def test_merge_bands_multiband_and_band_s2_landcover(self):
        """test Sentinel-2 landcover data with both RGB and normal file
        this uses two files, one with 3 bands and one with 1 band"""
        args = self.arguments.copy()
        args["collection"] = "S2GLC"
        args["bands"] = ["S2GLC"]
        args["start_date"] = "2019-01-01"
        args["end_date"] = "2019-12-31"
        args["resolution"] = 10
        args["filter"] = {}

        ds = self.tg.create(**args)

        self.assertTrue(len(ds.data_vars) == 4)

        self.assertTrue(
            len(ds.time) == 1
            and self.width - 1 <= len(ds.x) <= self.width + 1
            and self.height - 1 <= len(ds.y) <= self.height + 1
        )

    def test_dem(self):
        args = self.arguments.copy()
        args["collection"] = "COP-DEM"
        args["bands"] = ["DEM"]
        args["start_date"] = "2010-01-01"
        args["end_date"] = "2010-12-31"
        args["resolution"] = 90
        args["filter"] = {
            "spatialResolution": {"eq": 90},
            "productType": {"eq": "DGE_90"},
        }

        ds = self.tg.create(**args)

        self.assertTrue(
            len(ds.time) == 1
            and self.width // 9 - 1 <= len(ds.x) <= self.width // 9 + 1
            and self.height // 9 - 1 <= len(ds.y) <= self.height // 9 + 1
        )

        # does contain two product types: DGE_30 and DTE_30
        args["resolution"] = 30
        args["filter"] = {"spatialResolution": {"eq": 30}}
        ds = self.tg.create(**args)

        self.assertTrue(
            len(ds.time) == 2
            and self.width // 3 - 1 <= len(ds.x) <= self.width // 3 + 1
            and self.height // 3 - 1 <= len(ds.y) <= self.height // 3 + 1
        )

    def test_modis(self):
        args = self.arguments.copy()
        args["collection"] = "TERRAAQUA"
        args["bands"] = [""]
        args["start_date"] = "2021-01-01"
        args["end_date"] = "2021-01-16"
        args["resolution"] = 500
        args["filter"] = {}

        # it seems that no item has assets or a download url (tested on 15.11.24)
        self.assertRaises(RuntimeError, self.tg.create, **args)

    def test_l5(self):
        args = self.arguments.copy()
        coords = [
            [
                [25.74646, 53.944763],
                [25.808258, 53.944763],
                [25.808258, 53.918878],
                [25.74646, 53.918878],
                [25.74646, 53.944763],
            ]
        ]
        polygon = Polygon(coords[0])
        args["shp"] = gpd.GeoDataFrame(index=[0], crs="EPSG:4326", geometry=[polygon])
        args["collection"] = "LANDSAT-5"
        args["bands"] = ["B1"]
        args["start_date"] = "1984-12-08"
        args["end_date"] = "1984-12-08"
        args["resolution"] = 120
        args["filter"] = {}

        nr_time_steps = 2

        ds = self.tg.create(**args)

        # only test time steps since the shape was taken arbitrarily from the first sample
        self.assertTrue(len(ds.time) == nr_time_steps)

    def test_l7(self):
        args = self.arguments.copy()
        coords = [[[32.9, 62.5], [33.0, 62.5], [33.0, 62.49], [32.9, 62.49], [32.9, 62.5]]]
        polygon = Polygon(coords[0])
        args["shp"] = gpd.GeoDataFrame(index=[0], crs="EPSG:4326", geometry=[polygon])
        args["collection"] = "LANDSAT-7"
        args["bands"] = ["B1"]
        args["start_date"] = "1999-11-01"
        args["end_date"] = "1999-11-01"
        args["resolution"] = 30
        args["filter"] = {}

        nr_time_steps = 2

        ds = self.tg.create(**args)

        # only test time steps since the shape was taken arbitrarily from the first sample
        self.assertTrue(len(ds.time) == nr_time_steps)


if __name__ == "__main__":
    unittest.main()
