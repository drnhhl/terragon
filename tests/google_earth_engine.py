import os
import unittest

import ee
from base import _TestBase
from utils import load_env_variables

import terragon


class Test01GEE(unittest.TestCase):  # 01 is important since it should run first
    def test_not_initialized(self):
        self.assertRaises(RuntimeError, terragon.init, "gee")


class Test02GEE(_TestBase, unittest.TestCase):
    @classmethod
    def setUpClass(self):
        super().setUpClass()
        load_env_variables()  # load the .env vars if running locally
        ee.Initialize(project=os.getenv("GEE_PROJECT_NAME"))

        self.tg = terragon.init("gee")
        self.arguments["collection"] = "COPERNICUS/S2_SR_HARMONIZED"
        self.arguments["bands"] = ["B2", "B3", "B4"]

    def test_collections(self):
        """overwrite test collections since they are not implemented in GEE"""
        self.assertRaises(NotImplementedError, self.tg.retrieve_collections)

    def test_search(self):
        """overwrite test search since item size needs to be validated differently"""
        items = self.tg.search(**self.arguments)
        col_size = items.size().getInfo()

        self.assertTrue(col_size > 0)

    def test_create_meta(self):
        args = self.arguments.copy()
        args["save_metadata"] = ["system:id", "MEAN_INCIDENCE_AZIMUTH_ANGLE_B3"]

        ds = self.tg.create(**args)
        # check meta data
        self.assertTrue(all(m in ds.coords for m in args["save_metadata"]))
        times = ds.time.dt.strftime("%Y%m%d")
        ids_dates = [id.split("/")[-1].split("T")[0] for id in ds[args["save_metadata"][0]].values]
        self.assertTrue(all(times == ids_dates))

        self.assertTrue(
            len(ds.time) == self.nr_time_steps
            and self.width - 1 <= len(ds.x) <= self.width + 1
            and self.height - 1 <= len(ds.y) <= self.height + 1
        )


if __name__ == "__main__":
    unittest.main()
