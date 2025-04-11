import os
import unittest
import shutil
import atexit

from base import _TestBase
from utils import load_env_variables
import terragon

# Define a global download folder that both tests and cleanup use.
DOWNLOAD_FOLDER = "tests/download/"

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
        self.arguments["download_folder"] = DOWNLOAD_FOLDER

    @unittest.skip("Skip base class crs test")
    def test_crs(self):
        pass

    @unittest.skip("Skip base class resolution test")
    def test_resolution(self):
        pass

    def test_alos_palsar(self):
        args = self.arguments.copy()
        args["collection"] = "ALOS PALSAR"
        args["start_date"] = "2009-10-22"
        args["end_date"] = "2009-10-23"
        args["bands"] = ["HH"]
        args["resolution"] = 10
        args["filter"] = {"processingLevel": "L2.2"}

        ds = self.tg.create(**args)

        self.assertTrue(
            len(ds.time) == 1 and 
            self.width - 1 <= len(ds.x) <= self.width + 1 and
            self.height - 1 <= len(ds.y) <= self.height + 1
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
            len(ds.time) == 1 and 
            self.width - 1 <= len(ds.x) <= self.width + 1 and
            self.height - 1 <= len(ds.y) <= self.height + 1
        )

# Final cleanup function
def final_cleanup():
    if os.path.exists(DOWNLOAD_FOLDER):
        print(f"[FINAL CLEANUP] Removing folder: {DOWNLOAD_FOLDER}")
        shutil.rmtree(DOWNLOAD_FOLDER)

# Register the final cleanup function to be executed on exit.
atexit.register(final_cleanup)

if __name__ == "__main__":
    unittest.main()
