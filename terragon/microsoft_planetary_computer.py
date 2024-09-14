from urllib.parse import urljoin

import odc.stac
import planetary_computer as pc
import pystac_client
import requests
from joblib import Parallel, delayed

from .base import Base
from .utils import meters_to_crs_unit


class PC(Base):
    def __init__(
        self,
        credentials: dict = None,
        base_url: str = "https://planetarycomputer.microsoft.com/api/stac/v1/",
    ):
        super().__init__()
        self.base_url = base_url
        if credentials:
            pc.set_subscription_key(credentials["api_key"])

    def retrieve_collections(self, filter_by_name: str = None):
        collections_url = urljoin(self.base_url, "collections")
        response = requests.get(collections_url)

        if response.status_code == 200:
            data = response.json()
            collections = [collection["id"] for collection in data["collections"]]
            if filter_by_name:
                collections = [
                    collection
                    for collection in collections
                    if filter_by_name in collection.lower()
                ]
            return collections
        else:
            raise RuntimeError("Failed to retrieve collections")

    def search(self, **kwargs):
        super().search(**kwargs)
        bounds_4326 = self._reproject_shp(self.param("shp")).total_bounds

        catalog = pystac_client.Client.open(
            self.base_url,
            modifier=pc.sign_inplace,
        )

        start_date = self.param("start_date")
        end_date = self.param("end_date")
        datetime = f"{start_date}/{end_date}" if start_date and end_date else None
        search = catalog.search(
            collections=self.param("collection"),
            bbox=bounds_4326,
            datetime=datetime,
            query=self.param("filter"),
        )

        items = search.item_collection()
        if len(items) == 0:
            raise ValueError("No items found")
        return items

    def download(self, items=None, create_minicube=True):
        assert len(items) > 0, "No images to download."

        shp = self.param("shp")
        bounds = list(shp.bounds.values[0])
        res = meters_to_crs_unit(self.param("resolution"), shp)

        if create_minicube:
            ds = odc.stac.load(
                items,
                bands=self.param("bands"),
                crs=shp.crs,
                resolution=res,
                x=(bounds[0], bounds[2]),
                y=(bounds[1], bounds[3]),
            )
            ds = self.prepare_cube(ds)
            return ds
        else:
            bands = self.param("bands")
            if bands is None:
                bands = items[0].assets.keys()
            self.param("download_folder").mkdir(parents=True, exist_ok=True)
            fns = [
                self.param("download_folder").joinpath(
                    f"{self.param('collection')}_{band}_{item.id}.tif"
                )
                for item in items
                for band in bands
            ]
            urls = [item.assets[band].href for item in items for band in bands]
            Parallel(n_jobs=self.param("num_workers"))(
                delayed(self.download_file)(url, fn) for url, fn in zip(urls, fns)
            )
            return fns
