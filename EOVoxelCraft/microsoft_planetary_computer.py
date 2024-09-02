import odc.stac
import pystac_client
import requests
import planetary_computer as pc

from urllib.parse import urljoin
from joblib import Parallel, delayed
from .crafter import VoxelCrafter
from.utils import resolve_resolution

class PC(VoxelCrafter):
    def __init__(self, credentials:str=None, base_url:str="https://planetarycomputer.microsoft.com/api/stac/v1/"):
        super().__init__()
        self.base_url = base_url
        if credentials:
            pc.set_subscription_key(self.api_key)

    def retrieve_collections(self, filter_by_name: str=None):
        collections_url = urljoin(self.base_url, "collections")
        response = requests.get(collections_url)

        if response.status_code == 200:
            data = response.json()
            collections = [collection['id'] for collection in data['collections']]
            if filter_by_name:
                collections = [collection for collection in collections if filter_by_name in collection.lower()]
            return collections
        else:
            raise RuntimeError("Failed to retrieve collections")

    def search(self, **kwargs):
        super().search(**kwargs)
        bounds_4326 = self._reproject_shp(self.get_param('shp', raise_error=True)).total_bounds

        catalog = pystac_client.Client.open(
            self.base_url,
            modifier=pc.sign_inplace,
        )

        start_date = self.get_param('start_date')
        end_date = self.get_param('end_date')
        datetime=f"{start_date}/{end_date}" if start_date and end_date else None
        search = catalog.search(
            collections=self.get_param('collection', raise_error=True),
            bbox=bounds_4326,
            datetime=datetime,
            query=self.get_param('filter'),
        )

        items = search.item_collection()
        if len(items) == 0:
            raise ValueError(f"No items found")
        return items

    def download(self, items=None, create_minicube=True):
        assert len(items) > 0, "No images to download."
        
        shp = self.get_param('shp', raise_error=True)
        bounds = list(shp.bounds.values[0])
        crs = shp.crs
        res = resolve_resolution(shp, self.get_param('resolution', raise_error=True))

        if create_minicube:
            data = odc.stac.load(items,
                bands=self.get_param('bands'),
                crs=crs,
                resolution=res,
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

