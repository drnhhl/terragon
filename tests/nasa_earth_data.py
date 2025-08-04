import unittest

from base import _TestBase
from utils import load_env_variables

import terragon


class Test01ED(unittest.TestCase):
    def test_not_initialized(self):  # 01 is important since it should run first
        self.assertRaises(ValueError, terragon.init, "ed")


class Test02ED(_TestBase, unittest.TestCase):
    @classmethod
    def setUpClass(self):
        super().setUpClass()
        load_env_variables()

        self.tg = terragon.init("ed")
        self.arguments["collection"] = "C1996881146-POCLOUD" # works with open_virtual_mfdataset
        self.arguments["start_date"] = "2022-04-01"
        self.arguments["end_date"] = "2022-05-01"
        self.arguments["filter"] = {"version": "4.1", "provider": "POCLOUD", "count": 5}
        self.arguments["ed_kwargs"] = {"combine_attrs": "drop_conflicts"}

    def test_collections(self):
        col = self.tg.retrieve_collections("sentinel")
        self.assertTrue(len(col) > 0)

    def test_search(self):
        items = self.tg.search(**self.arguments)
        self.assertTrue(len(items) > 0)

    def test_download_tifs(self):
        args = self.arguments.copy()
        args["collection"] = "C2021957295-LPCLOUD"  # HLSS30
        args["filter"] = {"version": "2.0", "count": 1}
        items = self.tg.search(**args, create_minicube=False)
        fns = self.tg.download(items)
        self.assertTrue(len(fns) > 0)
        for fn in fns:
            self.assertTrue(fn.exists())
            fn.unlink()
            

    def test_download(self):
        items = self.tg.search(**self.arguments)
        ds = self.tg.download(items)
        self.assertTrue(len(ds.time) == 5)  # clipping etc not implemented yet

    def test_create(self):
        ds = self.tg.create(**self.arguments)
        self.assertTrue(len(ds.time) == 5)  # clipping etc not implemented yet

    def test_crs(self):
        """test it with utm crs (in meter)"""
        pass  # Not implemented yet

    def test_resolution(self):
        """test it with a different resolution and crs"""
        pass  # Not implemented yet

    def test_fail_on_missing_params(self):
        for arg in ["shp", "collection"]:
            args = self.arguments.copy()
            args.pop(arg)
            self.assertRaises(TypeError, self.tg.create, **args)


if __name__ == "__main__":
    unittest.main()
