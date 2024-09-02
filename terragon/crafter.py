import requests
import os
import shutil
import xarray as xr
import rioxarray as rxr
import pandas as pd
import geopandas as gpd

from abc import ABC, abstractmethod
from joblib import Parallel, delayed
from pathlib import Path
from datetime import datetime, timedelta

class VoxelCrafter(ABC):
    base_url = None
    _parameters = {}
    
    @abstractmethod
    def __init__(self, ):
        pass

    def create(self, **kwargs):
        """Execute search and download within one command."""
        items = self.search(**kwargs)
        self.download(items)

    def get_param(self, name, default=None, raise_error=False):
        """Simplify returning a parameter from the class, possible to raise an error when it is not set or None"""
        if raise_error and (not name in self._parameters.keys() or self._parameters[name] is None):
            raise ValueError(f'Parameter {name} was not set, but is required for this operation.')
        return self._parameters.get(name, default)

    @abstractmethod
    def retrieve_collections(self, ):
        pass
    
    # to much varibales in search, how to make it more flexible?
    def search(self, shp:gpd.GeoDataFrame, collection:str, bands:list=None, start_date:str=None, end_date:str=None, resolution=None, filter:dict=None, download_folder:str=None, processing_level:str=None, num_workers:int=1):
        """Take all arguments and store them."""
        # create a union of a dataframe of more than one shape in shp
        if len(shp.index) > 1:
            shp = gpd.GeoDataFrame(geometry=[shp.unary_union], crs=shp.crs)
        if isinstance(download_folder, str):
            download_folder = Path(download_folder)
        self._parameters.update({'shp': shp, 'collection': collection, 'bands': bands, 'start_date': start_date,
                                'end_date': end_date, 'resolution': resolution, 'filter': filter, 'download_folder': download_folder, "processing_level": processing_level, 'num_workers': num_workers})
        
    @abstractmethod
    def download(self, items, create_minicube=True):
        pass

    def _reproject_shp(self, shp, epsg='EPSG:4326'):
        """reproject shp to EPSG:4326."""
        if shp.crs != epsg:
            shp = shp.to_crs(epsg)
        return shp

    def download_file(self, url, fn):
        """download a file from a url into fn."""
        if fn.exists():
            return
        response = requests.get(url, stream=True)
        if response.status_code != 200:
            raise RuntimeError(f"Url {url} response code: {response.status_code}.")
        try: # download the file
            with open(fn, 'wb') as f:
                shutil.copyfileobj(response.raw, f)
        except Exception as e:
            if fn.exists():
                fn.unlink()
            raise RuntimeError(f"Failed to download {url} with error {e}")
        finally:
            response.close()
