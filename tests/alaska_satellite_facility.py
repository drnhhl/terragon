import os
import shutil
import unittest

import pandas as pd
from base import _TestBase
from utils import load_env_variables

import terragon


class TestASF(_TestBase, unittest.TestCase):
    DOWNLOAD_FOLDER = "tests/download/"

    @classmethod
    def tearDownClass(self):
        if os.path.exists(self.DOWNLOAD_FOLDER):
            print(f"[TEARDOWN] Removing folder: {self.DOWNLOAD_FOLDER}")
            shutil.rmtree(self.DOWNLOAD_FOLDER)
        else:
            print(f"[TEARDOWN] Folder does not exist: {self.DOWNLOAD_FOLDER}")

    @classmethod
    def setUpClass(self):
        super().setUpClass()
        load_env_variables()
        credentials = {
            "asf_username": os.getenv("ASF_USERNAME"),
            "asf_password": os.getenv("ASF_PASSWORD"),
            "asf_edl_token": os.getenv("ASF_EDL_TOKEN"),
        }
        self.tg = terragon.init("asf", credentials=credentials)
        self.arguments["start_date"] = "2021-01-01"
        self.arguments["end_date"] = "2021-01-02"
        self.arguments["collection"] = "SENTINEL-1"
        self.arguments["filter"] = {"processingLevel": "GRD_HD"}
        self.arguments["bands"] = ["VH"]
        self.arguments["num_workers"] = 4
        self.arguments["rm_tmp_files"] = False
        self.arguments["download_folder"] = self.DOWNLOAD_FOLDER

    # skip test to save time
    @unittest.skip("Skip base class resolution test")
    def test_resolution(self):
        pass

    @unittest.skip("Skip base class download test")
    def test_download(self):
        pass

    @unittest.skip("Skip base class donwload tif test")
    def test_download_tifs(self):
        pass

    def test_alos_palsar(self):
        args = self.arguments.copy()
        args["collection"] = "ALOS PALSAR"
        args["start_date"] = "2009-10-22"
        args["end_date"] = "2009-10-23"
        args["bands"] = ["HH", "HV"]  # test also 2 bands
        args["resolution"] = 10
        args["filter"] = {"processingLevel": "L2.2"}

        ds = self.tg.create(**args)

        self.assertTrue(
            len(ds.time) == 1
            and self.width - 1 <= len(ds.x) <= self.width + 1
            and self.height - 1 <= len(ds.y) <= self.height + 1
        )

    def test_alos_avnir2(self):
        args = self.arguments.copy()
        args["collection"] = "ALOS AVNIR-2"
        args["start_date"] = "2009-12-19"
        args["end_date"] = "2009-12-21"
        args["bands"] = ["IMG-01"]  # IMG_02, IMG_03, IMG_04
        args["resolution"] = 10
        args["filter"] = {}

        ds = self.tg.create(**args)

        self.assertTrue(
            len(ds.time) == 1
            and self.width - 1 <= len(ds.x) <= self.width + 1
            and self.height - 1 <= len(ds.y) <= self.height + 1
        )

    def test_create_meta(self):
        """test create with metadata"""
        args = self.arguments.copy()
        args["save_metadata"] = ["fileID"]

        ds = self.tg.create(**args)

        self.assertTrue("fileID" in ds.coords)
        times = (
            pd.to_datetime(ds["time"].values, utc=True).tz_convert(None).strftime("%Y%m%d").tolist()
        )
        ids_dates = [id.split("_")[4].split("T")[0] for id in ds.fileID.values]

        self.assertTrue(times == ids_dates)

        self.assertTrue(
            len(ds.time) == self.nr_time_steps
            and self.width - 1 <= len(ds.x) <= self.width + 1
            and self.height - 1 <= len(ds.y) <= self.height + 1
        )


if __name__ == "__main__":
    unittest.main()
