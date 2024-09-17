import shutil
from abc import ABC, abstractmethod
from pathlib import Path

import geopandas as gpd
import requests
import rioxarray as rxr


class Base(ABC):
    base_url = None
    _parameters = {}

    @abstractmethod
    def __init__(
        self,
    ):
        pass

    def create(self, **kwargs):
        """Execute search and download within one command."""
        items = self.search(**kwargs)
        return self.download(items)

    def param(self, name, **kwargs):
        """Return a standard parameter from the class with predefined settings."""
        dic = {
            "shp": (
                self.get_param("shp", raise_error=True)
                if not kwargs
                else self.get_param("shp", **kwargs)
            ),
            "collection": (
                self.get_param("collection", raise_error=True)
                if not kwargs
                else self.get_param("collection", **kwargs)
            ),
            "bands": (
                self.get_param("bands", [])
                if not kwargs
                else self.get_param("bands", **kwargs)
            ),
            "start_date": (
                self.get_param("start_date", None)
                if not kwargs
                else self.get_param("start_date", **kwargs)
            ),
            "end_date": (
                self.get_param("end_date", None)
                if not kwargs
                else self.get_param("end_date", **kwargs)
            ),
            "resolution": (
                self.get_param("resolution", None)
                if not kwargs
                else self.get_param("resolution", **kwargs)
            ),
            "clip_to_shp": (
                self.get_param("clip_to_shp", True)
                if not kwargs
                else self.get_param("clip_to_shp", **kwargs)
            ),
            "download_folder": (
                self.get_param("download_folder", Path("./eo_download/"))
                if not kwargs
                else self.get_param("download_folder", **kwargs)
            ),
            "num_workers": (
                self.get_param("num_workers", 1)
                if not kwargs
                else self.get_param("num_workers", **kwargs)
            ),
        }

        if name in dic:
            return dic[name]
        else:
            return self.get_param(name)

    def get_param(self, name, default=None, raise_error=False):
        """Simplify returning a parameter from the class, possible to raise an error when it is not set or None"""
        if raise_error and (
            name not in self._parameters.keys() or self._parameters[name] is None
        ):
            raise ValueError(
                f"Parameter {name} was not set, but is required for this operation."
            )
        return self._parameters.get(name, default)

    @abstractmethod
    def retrieve_collections(
        self,
    ):
        pass

    def search(
        self,
        shp: gpd.GeoDataFrame,
        collection: str,
        bands: list = None,
        start_date: str = None,
        end_date: str = None,
        resolution=None,
        filter: dict = None,
        clip_to_shp: bool = True,
        download_folder: str = None,
        num_workers: int = 1,
    ):
        """Take all arguments and store them."""
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
                "download_folder": download_folder,
                "num_workers": num_workers,
            }
        )

    @abstractmethod
    def download(self, items, create_minicube=True):
        pass

    def _reproject_shp(self, shp, epsg="EPSG:4326"):
        """reproject shp to EPSG:4326."""
        if shp.crs != epsg:
            shp = shp.to_crs(epsg)
        return shp

    def prepare_cube(self, ds):
        """rename, reorder, and remove/add attributes to the dataset."""
        # clip extend to the exact shape
        if self.param(
            "clip_to_shp"
        ):  # TODO gives warning for gee: /mnt/SSD1/adrian/miniconda3/envs/v1/lib/python3.10/site-packages/xarray/core/duck_array_ops.py:215: RuntimeWarning: invalid value encountered in cast return data.astype(dtype, **kwargs)
            ds = ds.rio.clip(self.param("shp").geometry)

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
            "collection": self.param("collection"),
        }

        return ds

    def download_file(self, url, fn):
        """download a file from a url into fn."""
        if fn.exists():
            return
        response = requests.get(url, stream=True)
        if response.status_code != 200:
            raise RuntimeError(f"Url {url} response code: {response.status_code}.")
        try:  # download the file
            with open(fn, "wb") as f:
                shutil.copyfileobj(response.raw, f)
        except Exception as e:
            if fn.exists():
                fn.unlink()
            raise RuntimeError(f"Failed to download {url} with error {e}")
        finally:
            response.close()
