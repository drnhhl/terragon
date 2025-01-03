from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

import geopandas as gpd
import rioxarray as rxr  # noqa: F401 # rioxarray needed for .rio accessor


class Base(ABC):
    """Abstract base class for all EO data providers.

    :param ABC: abstract base class
    :raises ValueError: when there are missing parameters
    """

    _base_url = None
    _parameters = {}

    @abstractmethod
    def __init__(self):
        pass

    def create(self, **kwargs):
        """Execute search and download within one command.
        For explanation of parameters see search function."""
        items = self.search(**kwargs)
        return self.download(items)

    @abstractmethod
    def retrieve_collections(
        self,
    ):
        pass

    def search(
        self,
        shp: gpd.GeoDataFrame,
        collection: str,
        bands: List[str] = None,
        start_date: str = None,
        end_date: str = None,
        resolution: float = None,
        filter: dict = None,
        clip_to_shp: bool = True,
        download_folder: str = "./eo_download/",
        num_workers: int = 1,
        create_minicube: bool = True,
    ):
        """Search for items in the backend (This abstract function only takes all arguments and stores them).

        :param shp: the shape of the area of interest, the output will be reprojected to the shape crs
        :param collection: collection name
        :param bands: band names to download, defaults to None
        :param start_date: the first date of the time series of interest, defaults to None
        :param end_date: the last date of the time series of interest (if there is no time in the date it defaults to include the end_date), defaults to None
        :param resolution: resolution of the output in meters, defaults to None
        :param filter: filters applied to the meta data in format: {'<meta-var-name>': {<filter-matching>:<value>}, ...}, defaults to None
        :param clip_to_shp: if True set pixels outside of the shape to nan (using rio.clip), else clip to bounding box, defaults to True
        :param download_folder: the folder to download files (also temporary files), defaults to "./eo_download/"
        :param num_workers: the number of workers in parallel to use for downloading, defaults to 1
        :param create_minicube: if True return a xarray.Dataset, otherwise return the downloaded filenames, defaults to True
        """
        # create a union of a dataframe of more than one shape in shp
        if len(shp.index) > 1:
            shp = gpd.GeoDataFrame(geometry=[shp.unary_union], crs=shp.crs)
        if isinstance(download_folder, str):
            download_folder = Path(download_folder)
        self._parameters.update(
            {
                "shp": shp,
                "collection": collection,
                "bands": bands,
                "start_date": start_date,
                "end_date": end_date,
                "resolution": resolution,
                "filter": filter,
                "clip_to_shp": clip_to_shp,
                "download_folder": Path(download_folder),
                "num_workers": num_workers,
                "create_minicube": create_minicube,
            }
        )

    @abstractmethod
    def download(self, items: List[object]):
        """Download the given items.

        :param items: list of items to download, the item type is specific to the backend
        :return: xarray.Dataset or list of filenames
        """
        pass

    def _param(self, name: str, **kwargs):
        """Return a standard parameter from the class with predefined settings."""
        defaults = {
            "shp": (None, True),
            "collection": (None, True),
            "bands": ([], False),
            "start_date": (None, False),
            "end_date": (None, False),
            "resolution": (None, False),
            "clip_to_shp": (True, False),
            "download_folder": (Path("./eo_download/"), False),
            "num_workers": (1, False),
            "create_minicube": (True, False),
        }

        if not kwargs and name in defaults:
            default_value, raise_error = defaults.get(name)
            return self._get_param(name, default_value, raise_error)
        else:
            return self._get_param(name, **kwargs)

    def _get_param(self, name: str, default=None, raise_error=False):
        """Simplify returning a parameter from the class, possible to raise an error when it is not set or None"""
        if raise_error and (
            name not in self._parameters.keys()
            or self._parameters[name] is None
            or (
                isinstance(self._parameters[name], (list, tuple, set, dict))
                and not self._parameters[name]
            )
        ):
            raise ValueError(f"Parameter '{name}' was not set, but is required for this operation.")
        return self._parameters.get(name, default)

    def _reproject_shp(self, shp, epsg="EPSG:4326"):
        """reproject shp to EPSG:4326."""
        if shp.crs != epsg:
            shp = shp.to_crs(epsg)
        return shp

    def _prepare_cube(self, ds):
        """rename, reorder, and remove/add attributes to the dataset."""
        # clip extend to the exact shape
        if self._param("clip_to_shp"):
            ds = ds.rio.clip(self._param("shp").geometry, all_touched=True)

        # delete the attrs
        ds.attrs = {}
        for var in ds:
            ds[var].attrs = {}

        # rename dimensions and reorder
        if "latitude" in ds.dims:
            ds = ds.rename({"latitude": "y", "longitude": "x"})
        if "lat" in ds.dims:
            ds = ds.rename({"lat": "y", "lon": "x"})
        if "X" in ds.dims:
            ds = ds.rename({"X": "x", "Y": "y"})

        if "time" in ds.dims:
            ds = ds.transpose("time", "y", "x")
        else:
            ds = ds.transpose("y", "x")

        # add attributes
        ds.attrs = {
            "crs": ds.rio.crs.to_string(),
            "data_source": self.__class__.__name__,
            "collection": self._param("collection"),
        }

        return ds
