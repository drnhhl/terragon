import os
from urllib.parse import urljoin

import odc.stac
import pystac_client
import requests

from .base import Base
from .utils import gather_assign_meta, meters_to_crs_unit


class CDSE(Base):
    """Class to interact with the Copernicus Data Space Ecosystem. The images are downloaded via the STAC API.
    Check the official CDSE website for the supported collections: https://documentation.dataspace.copernicus.eu/APIs/newSTACcatalogue.html

    :param credentials: credentials to authenticate, expected format: {'aws_access_key_id': <id>, 'aws_secret_access_key': <key>}.
    :param base_url: the URL for the STAC catalog, defaults to "https://stac.dataspace.copernicus.eu/v1/"
    """

    def __init__(
        self,
        credentials: dict,
        base_url: str = "https://stac.dataspace.copernicus.eu/v1/",
    ):
        """Initialize class and save the credentials.

        :param credentials: credentials to authenticate, expected format: {'aws_access_key_id': <id>, 'aws_secret_access_key': <key>}.
        :param base_url: the URL for the STAC catalog, defaults to "https://stac.dataspace.copernicus.eu/v1/"
        :raises ValueError: when the credentials are in the wrong format
        """
        super().__init__()
        self.base_url = base_url
        if credentials:
            if "aws_access_key_id" not in credentials or "aws_secret_access_key" not in credentials:
                raise ValueError(
                    "aws_access_key_id or aws_secret_access_key not in credentials, could not initialize."
                )
        else:
            # fallback to credentials saved in the environment/files from aws
            pass

        # only needed for direct-stac # TODO
        os.environ["GDAL_HTTP_TCP_KEEPALIVE"] = "YES"
        os.environ["AWS_S3_ENDPOINT"] = "eodata.dataspace.copernicus.eu"
        os.environ["AWS_HTTPS"] = "YES"
        os.environ["AWS_VIRTUAL_HOSTING"] = "FALSE"
        os.environ["GDAL_HTTP_UNSAFESSL"] = "YES"
        os.environ["AWS_ACCESS_KEY_ID"] = credentials.get("aws_access_key_id", None)
        os.environ["AWS_SECRET_ACCESS_KEY"] = credentials.get("aws_secret_access_key", None)
        self.credentials = credentials

    def retrieve_collections(self, filter_by_name: str = None):
        """Search the collections provided by Copernicus Data Space Ecosystem.

        :param filter_by_name: name to filter the collections for, defaults to None
        :raises RuntimeError: if the request to the collections endpoint fails
        :return: a list of collection names
        """
        collections_url = urljoin(self.base_url, "collections")
        response = requests.get(collections_url, timeout=60)
        response.raise_for_status()
        try:
            data = response.json()
            collections = [collection["id"] for collection in data["collections"]]
        except json.JSONDecodeError:
            raise RuntimeError("Failed to decode JSON response from collections endpoint.")
        
        if filter_by_name:
            filter_by_name = filter_by_name.lower()
            collections = [
                collection for collection in collections if filter_by_name in collection.lower()
            ]

        return collections

    def search(
        self,
        *args,
        **kwargs,
    ):
        """Search for items in the Copernicus Data Space Ecosystem collections via stac. For a description of the args/kwargs parameters see the Base class function.

        :raises ValueError: when no items are found or parameters are in the wrong format
        :raises RuntimeError: when the corresponding files for the items are not found

        :return: a list of items
        """
        super().search(*args, **kwargs)

        # use pystac_client to search for items
        catalog = pystac_client.Client.open(self.base_url)
        catalog.add_conforms_to("ITEM_SEARCH")
        bounds_4326 = self._reproject_shp(self._param("shp")).total_bounds
        start_date = self._param("start_date")
        end_date = self._param("end_date")
        datetime = f"{start_date}/{end_date}" if start_date and end_date else None
        search = catalog.search(
            collections=self._param("collection"),
            bbox=bounds_4326,
            datetime=datetime,
            query=self._param("filter"),
        )

        items = list(search.items())
        return items

    def download(self, items):
        """Download the items from Copernicus Data Space Ecosystem as xr.Dataset or download the files.

        :param items: items to download
        :return: xarray.Dataset or list of filenames
        """
        if len(items) < 1:
            raise ValueError("No items to download.")

        if self._param("create_minicube"):
            shp = self._param("shp")
            bounds = list(shp.bounds.values[0])
            res = meters_to_crs_unit(self._param("resolution"), shp)

            items = sorted(
                items, key=lambda x: x.id
            )  # preserve_original_order does not work -> order items

            ds = odc.stac.load(
                items,
                groupby="id",
                bands=self._param("bands"),
                crs=shp.crs,
                resolution=odc.geo.resxy_(res[0], -res[1]),
                x=(bounds[0], bounds[2]),
                y=(bounds[1], bounds[3]),
                **self._param("odc_stac_kwargs", default={}),
                stac_cfg={
                    "access": {
                        "env": {
                            "GDAL_NUM_THREADS": "ALL_CPUS",
                            "GDAL_HTTP_TCP_KEEPALIVE": "YES",
                            "AWS_VIRTUAL_HOSTING": "FALSE",
                            "AWS_HTTPS": "YES",
                        },
                    }
                },
            )
            ds = gather_assign_meta(self, items, ds)
            ds = self._prepare_cube(ds)
            return ds
        else:
            raise NotImplementedError(
                "Downloading files is not implemented for Copernicus Data Space Ecosystem. Please, use create_minicube=True or use the S3 API - cdse_s3."
            )
