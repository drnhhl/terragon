import unittest

from base import _TestBase

import terragon


class TestPC(_TestBase, unittest.TestCase):
    @classmethod
    def setUpClass(self):
        super().setUpClass()
        self.tg = terragon.init("pc")
        self.arguments["collection"] = "sentinel-2-l2a"
        self.arguments["bands"] = ["B02", "B03", "B04"]

    def test_create_meta(self):
        """test create with metadata"""
        args = self.arguments.copy()
        args["save_metadata"] = ["id"]

        ds = self.tg.create(**args)

        # check meta data
        self.assertTrue("id" in ds.coords)
        times = ds.time.dt.strftime("%Y%m%d")
        ids_dates = [id.split("_")[2].split("T")[0] for id in ds.id.values]
        self.assertTrue(all(times == ids_dates))

        self.assertTrue(
            len(ds.time) == self.nr_time_steps
            and self.width - 1 <= len(ds.x) <= self.width + 1
            and self.height - 1 <= len(ds.y) <= self.height + 1
        )


if __name__ == "__main__":
    unittest.main()
