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

class Base(ABC):
    base_url = None
    _parameters = {}
    
    @abstractmethod
    def __init__(self, ):
        pass

    def create(self, **kwargs):
        """Execute search and download within one command."""
        items = self.search(**kwargs)
        return self.download(items)

    def param(self, name, **kwargs):
        """Return a standard parameter from the class with predefined settings."""
        dic = {
            'shp': self.get_param('shp', raise_error=True) if not kwargs else self.get_param('shp', **kwargs),
            'collection': self.get_param('collection', raise_error=True) if not kwargs else self.get_param('collection', **kwargs),
            'bands': self.get_param('bands', []) if not kwargs else self.get_param('bands', **kwargs),
            'start_date': self.get_param('start_date', None) if not kwargs else self.get_param('start_date', **kwargs),
            'end_date': self.get_param('end_date', None) if not kwargs else self.get_param('end_date', **kwargs),
            'resolution': self.get_param('resolution', None) if not kwargs else self.get_param('resolution', **kwargs),
            'download_folder': self.get_param('download_folder', Path('./eo_download/')) if not kwargs else self.get_param('download_folder', **kwargs),
            'num_workers': self.get_param('num_workers', 1) if not kwargs else self.get_param('num_workers', **kwargs),
        }

        if name in dic:
            return dic[name]
        else:
            return self.get_param(name)

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
