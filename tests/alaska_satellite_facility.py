import os
import unittest

from base import _TestBase
from utils import load_env_variables

import terragon


class TestASF(_TestBase, unittest.TestCase):
    def setUp(self):
        super().setUp()
        load_env_variables()
        credentials = {
            "asf_username": os.getenv("ASF_USERNAME"),
            "asf_password": os.getenv("ASF_PASSWORD"),
        }
        self.tg = terragon.init("asf", credentials=credentials)
        self.arguments["start_date"] = "2021-01-01"
        self.arguments["end_date"] = "2021-01-02"
        self.arguments["collection"] = "SENTINEL-1"
        self.arguments["filter"] = {"processingLevel": "GRD_HD"}
        self.arguments["bands"] = ["VH", "VV"]
        self.arguments["num_workers"] = 4
        self.arguments["rm_tmp_files"] = False

    def test_alos_palsar(self):
        args = self.arguments.copy()
        args["collection"] = "ALOS PALSAR"
        args["start_date"] = "2009-01-01"
        args["end_date"] = "2009-12-31"
        args["bands"] = ["HH", "HV"]
        args["resolution"] = 10
        args["filter"] = {"processingLevel": "L2.2"}

        ds = self.tg.create(**args)

        self.assertTrue(
            len(ds.time) == 3  # 3 items in 2009
            and self.width - 1 <= len(ds.x) <= self.width + 1
            and self.height - 1 <= len(ds.y) <= self.height + 1
        )

    def test_alos_avnir2(self):
        args = self.arguments.copy()
        args["collection"] = "ALOS AVNIR-2"
        args["start_date"] = "2009-01-01"
        args["end_date"] = "2009-12-31"
        args["bands"] = ["IMG-01", "IMG-02", "IMG-03", "IMG-04"]
        args["resolution"] = 10
        args["filter"] = {}

        ds = self.tg.create(**args)

        self.assertTrue(
            len(ds.time) == self.nr_time_steps
            and self.width - 1 <= len(ds.x) <= self.width + 1
            and self.height - 1 <= len(ds.y) <= self.height + 1
        )


if __name__ == "__main__":
    unittest.main()
