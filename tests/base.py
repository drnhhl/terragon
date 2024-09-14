from pathlib import Path

import geopandas as gpd


class _TestBase():
    """Base class in order define the basic test functionality."""
    def setUp(self):
        """set up the basic parameters for the tests.
        arguments collection, bands, etc. need to be defined in the child class."""
        super().setUp()
        self.gdf = gpd.read_file(Path("demo_files/data/TUM_OTN.geojson"))
        self.arguments = dict(shp=self.gdf, start_date='2021-01-01', end_date='2021-01-05',
                              resolution=10, download_folder='tests/download/')
        self.width, self.height, self.nr_time_steps = 40, 16, 2

    def test_collections(self):
        col = self.tg.retrieve_collections('sentinel')
        self.assertTrue(len(col) > 0)

    def test_search(self):
        items = self.tg.search(**self.arguments)
        self.assertTrue(len(items) > 0)

    def test_download(self):
        items = self.tg.search(**self.arguments)
        ds = self.tg.download(items)
        self.assertTrue(ds is not None)
        self.assertTrue(len(ds.time) == self.nr_time_steps and len(ds.x) == self.width and len(ds.y) == self.height)

    def test_download_tifs(self):
        items = self.tg.search(**self.arguments)
        fns = self.tg.download(items, create_minicube=False)
        self.assertTrue(len(fns) > 0)
        for fn in fns:
            self.assertTrue(fn.exists())
            fn.unlink()

    def test_create(self):
        ds = self.tg.create(**self.arguments)
        self.assertTrue(len(ds.time) == self.nr_time_steps and len(ds.x) == self.width and len(ds.y) == self.height)
        
    def test_crs(self):
        """test it with utm crs (in meter)"""
        args = self.arguments.copy()
        args['shp'] = args['shp'].to_crs('EPSG:32632')
        args['resolution'] = 10 # 10m resolution
        ds = self.tg.create(**args)
        width, height = 27, 18
        self.assertTrue(len(ds.time) == self.nr_time_steps and len(ds.x) == width and len(ds.y) == height)

    def test_resolution(self):
        """test it with a different resolution and crs"""
        args = self.arguments.copy()
        args['shp'] = args['shp'].to_crs('EPSG:32632')
        args['resolution'] = 20
        ds = self.tg.create(**args)
        width, height = 13, 8
        self.assertTrue(len(ds.time) == self.nr_time_steps and len(ds.x) == width and len(ds.y) == height)

    def test_fail_on_missing_params(self):
        for arg in ['shp', 'collection']:
            args = self.arguments.copy()
            args.pop(arg)
            self.assertRaises(TypeError, self.tg.create, **args)
