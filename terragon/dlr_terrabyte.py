from pystac_client import Client 
import odc.stac
from datetime import datetime 

from .utils import bbox_to_geojson_polygon, resolve_resolution
from joblib import Parallel, delayed
from .base import Base

class TB(Base):
    def __init__(self, credentials:dict=None, base_url:str="https://stac.terrabyte.lrz.de/public/api"):
        super().__init__()
        self.base_url = base_url

    def retrieve_collections(self, filter_by_name: str=None):
        try:
            catalog = Client.open(self.base_url)
        except Exception as e:
            raise RuntimeError(f"Failed to open catalog: {e}")

        collections = catalog.get_all_collections()
        if collections:
            collections = [col.id for col in collections]
            if filter_by_name:
                collections = [collection for collection in collections if filter_by_name in collection.lower()]
            return collections
        else:
            raise RuntimeError("Failed to retrieve collections")

    def search(self, **kwargs):
        super().search(**kwargs)

        catalog = Client.open(self.base_url)

        start_date = datetime.strptime(self.get_param('start_date'), "%Y-%m-%d") # '2016-01-01T00:00:00Z'
        end_date = datetime.strptime(self.get_param('end_date'), "%Y-%m-%d")
        bounds_4326 = self._reproject_shp(self.get_param('shp', raise_error=True)).total_bounds
        bounds_4326 = bbox_to_geojson_polygon(bounds_4326)

        search = catalog.search(
            collections=[self.get_param('collection', raise_error=True)],
            datetime=[start_date, end_date],
            intersects = bounds_4326,
            # query=self.get_param('filter'), # {'eo:cloud_cover': {"gte": 0, "lte": 60},'grid:code': {'eq': 'MGRS-32UPU'}}
        )

        items = search.item_collection()
        if len(items) == 0:
            raise ValueError(f"No items found")
        return items

    def download(self, items, create_minicube=True):
        assert len(items) > 0, "No images to download."
        
        shp = self.get_param('shp', raise_error=True)
        bounds = list(shp.bounds.values[0])
        crs = shp.crs
        res = resolve_resolution(shp, self.get_param('resolution', raise_error=True))

        if create_minicube:
            data = odc.stac.load(items,
                bands=self.get_param('bands'),
                resolution=res,
                crs=crs,
                x=(bounds[0], bounds[2]),
                y=(bounds[1], bounds[3])
            )
            return data
        else:
            bands = self.get_param('bands')
            if bands is None:
                bands = items[0].assets.keys()
            self.get_param('download_folder', raise_error=True).mkdir(parents=True, exist_ok=True)
            fns = [self.get_param('download_folder', raise_error=True).joinpath(f"{self.get_param('collection', raise_error=True)}_{band}_{item.id}.tif") for item in items for band in bands]
            urls = [item.assets[band].href for item in items for band in bands]
            Parallel(n_jobs=self.get_param('num_workers', 1))(delayed(self.download_file)(url, fn) for url, fn in zip(urls, fns))
            return fns