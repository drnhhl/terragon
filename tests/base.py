from pathlib import Path

import geopandas as gpd


class _TestBase:
    """Base class in order define the basic test functionality."""

    @classmethod
    def setUpClass(self):
        """set up the basic parameters for the tests.
        arguments collection, bands, etc. need to be defined in the child class."""
        super().setUpClass()
        self.gdf = gpd.read_file(Path("docs/demo_files/data/TUM_OTN.geojson"))
        self.arguments = dict(
            shp=self.gdf,
            start_date="2021-01-01",
            end_date="2021-01-05",
            resolution=10,
            download_folder="tests/download/",
        )
        # determing pixel size:
        # 10m in epsg:4326 is 0.00013405 in x and 0.00008987 in y:
        # bounds = gdf.bounds
        # (bounds.minx - bounds.maxx) / 0.00013405 = -27.109288
        # (bounds.miny - bounds.maxy) / 0.00008987 = -16.557249
        # pixel size in utm is 10m:
        # bounds = gdf_utm.to_crs("EPSG:32632").bounds
        # (bounds.minx - bounds.maxx) / 10 = -27.226094
        # (bounds.miny - bounds.maxy) / 10 = -17.268867

        # test with +/- 1 pixel error because of variations in data source
        self.width, self.height, self.nr_time_steps = 27, 17, 2

    def test_collections(self):
        col = self.tg.retrieve_collections("sentinel")
        self.assertTrue(len(col) > 0)

    def test_search(self):
        items = self.tg.search(**self.arguments)
        self.assertTrue(len(items) > 0)

    def test_download(self):
        items = self.tg.search(**self.arguments)
        ds = self.tg.download(items)

        self.assertTrue(
            len(ds.time) == self.nr_time_steps
            and self.width - 1 <= len(ds.x) <= self.width + 1
            and self.height - 1 <= len(ds.y) <= self.height + 1
        )

    def test_download_tifs(self):
        items = self.tg.search(**self.arguments, create_minicube=False)
        fns = self.tg.download(items)
        self.assertTrue(len(fns) > 0)
        for fn in fns:
            self.assertTrue(fn.exists())
            fn.unlink()

    def test_create(self):
        ds = self.tg.create(**self.arguments)

        self.assertTrue(
            len(ds.time) == self.nr_time_steps
            and self.width - 1 <= len(ds.x) <= self.width + 1
            and self.height - 1 <= len(ds.y) <= self.height + 1
        )

    def test_crs(self):
        """test it with utm crs (in meter)"""
        args = self.arguments.copy()
        args["shp"] = args["shp"].to_crs("EPSG:32632")
        args["resolution"] = 10  # 10m resolution
        ds = self.tg.create(**args)

        self.assertTrue(
            len(ds.time) == self.nr_time_steps
            and self.width - 1 <= len(ds.x) <= self.width + 1
            and self.height - 1 <= len(ds.y) <= self.height + 1
        )

    def test_resolution(self):
        """test it with a different resolution and crs"""
        args = self.arguments.copy()
        args["shp"] = args["shp"].to_crs("EPSG:32632")
        args["resolution"] = 20
        ds = self.tg.create(**args)
        width, height = 14, 9

        self.assertTrue(
            len(ds.time) == self.nr_time_steps
            and width - 1 <= len(ds.x) <= width + 1
            and height - 1 <= len(ds.y) <= height + 1
        )

    def test_fail_on_missing_params(self):
        for arg in ["shp", "collection"]:
            args = self.arguments.copy()
            args.pop(arg)
            self.assertRaises(TypeError, self.tg.create, **args)
