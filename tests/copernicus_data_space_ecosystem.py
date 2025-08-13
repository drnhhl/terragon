import os
import unittest

from base import _TestBase
from utils import load_env_variables

import terragon


class TestCDSE(_TestBase, unittest.TestCase):
    @classmethod
    def setUpClass(self):
        super().setUpClass()

        load_env_variables()  # load the .env vars if running locally
        credentials = {
            "aws_access_key_id": os.environ.get("S3_ACCESS_KEY"),
            "aws_secret_access_key": os.environ.get("S3_SECRET_KEY"),
        }
        self.tg = terragon.init("cdse", credentials)
        self.arguments["collection"] = "sentinel-2-global-mosaics"
        self.arguments["bands"] = ["B02", "B03", "B04"]
        self.arguments["start_date"] = "2024-09-03"
        self.arguments["end_date"] = "2024-09-15"

    def test_download_tifs(self):
        items = self.tg.search(**self.arguments, create_minicube=False)
        self.assertRaises(NotImplementedError, self.tg.download, items)

    def test_s1(self):
        """test Sentinel-1 data."""
        args = self.arguments.copy()
        args["collection"] = "sentinel-1-grd"
        args["bands"] = ["vv", "vh"]
        args["start_date"] = "2021-09-03"
        args["end_date"] = "2021-09-04"
        args["resolution"] = 10
        args["save_metadata"] = ["id", "sat:absolute_orbit"]

        ds = self.tg.create(**args)

        # check meta data
        self.assertTrue(all(m in ds.coords for m in args["save_metadata"]))
        times = ds.time.dt.strftime("%Y%m%d")
        ids_dates = [id.split("_")[4].split("T")[0] for id in ds.id.values]
        self.assertTrue(all(times == ids_dates))

        # test if the dimensions are correct, with +/- 1 pixel distance
        self.assertTrue(
            self.width - 1 <= len(ds.x) <= self.width + 1
            and self.height - 1 <= len(ds.y) <= self.height + 1
        )


if __name__ == "__main__":
    unittest.main()
