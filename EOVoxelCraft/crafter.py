import geopandas as gpd
import planetary_computer as pc
import ee
import geedim
import requests
import pystac_client
from urllib.parse import urljoin
import odc.stac
import json
from abc import ABC, abstractmethod
import os
import hashlib
import geedim
from joblib import Parallel, delayed
import xarray as xr
import rioxarray as rxr
import re
import pandas as pd
import shutil

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

    def search(self, shp:gpd.GeoDataFrame, collection:str, bands:list=None, start_date:str=None, end_date:str=None, resolution=None, filter:dict=None, download_folder:str=None):
        """Take all arguments and store them."""
        # create a union of a dataframe of more than one shape in shp
        if len(shp.index) > 1:
            shp = gpd.GeoDataFrame(geometry=[shp.unary_union], crs=shp.crs)
        self._parameters.update({'shp': shp, 'collection': collection, 'bands': bands, 'start_date': start_date,
                                'end_date': end_date, 'resolution': resolution, 'filter': filter, 'download_folder': download_folder})
        
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
        if os.path.exists(fn):
            return
        response = requests.get(url, stream=True)
        if response.status_code != 200:
            raise RuntimeError(f"Url {url} response code: {response.status_code}.")
        try: # download the file
            with open(fn, 'wb') as f:
                shutil.copyfileobj(response.raw, f)
        except Exception as e:
            if os.path.exists(fn):
                os.remove(fn)
            raise RuntimeError(f"Failed to download {url} with error {e}")
        finally:
            response.close()

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
                collections = [collection for collection in collections if filter_by_name in collection]
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
        
        bounds = list(self.get_param('shp', raise_error=True).bounds.values[0])
        crs = self.get_param('shp', raise_error=True).crs

        if create_minicube:
            data = odc.stac.load(items,
                bands=self.get_param('bands'),
                crs=crs,
                x=(bounds[0], bounds[2]),
                y=(bounds[1], bounds[3]),
                resolution=self.get_param('resolution'),
                dtype=self.get_param('dtype'),
            )
            return data
        else:
            bands = self.get_param('bands')
            if bands is None:
                bands = items[0].assets.keys()
            os.makedirs(self.get_param('download_folder', raise_error=True), exist_ok=True)
            fns = [os.path.join(self.get_param('download_folder', raise_error=True), f"{self.get_param('collection', raise_error=True)}_{band}_{item.id}.tif") for item in items for band in bands]
            urls = [item.assets[band].href for item in items for band in bands]
            Parallel(n_jobs=self.get_param('num_workers', 1))(delayed(self.download_file)(url, fn) for url, fn in zip(urls, fns))
            return fns

class GEE(VoxelCrafter):
    def __init__(self, credentials=None):
        # TODO check if ee.Authenticate was executed before otherwise execute it
        super().__init__()
        try:
            ee.Initialize()
        except Exception as e:
            raise RuntimeError(f"Failed to initialize GEE with {e}. Did you run 'ee.Authenticate()'?")

    def retrieve_collections(self, filter_by_name: str=None):
        raise NotImplementedError("GEE does not have a collection endpoint. Please, visit https://developers.google.com/earth-engine/datasets/catalog")

    def search(self, **kwargs):
        super().search(**kwargs)

        img_col = ee.ImageCollection(self.get_param('collection', raise_error=True))
        start_date = self.get_param('start_date')
        end_date = self.get_param('end_date')
        if start_date and end_date:
            img_col = img_col.filterDate(start_date, end_date)
        bands = self.get_param('bands')
        if bands:
            img_col = img_col.select(bands)
        
        # reproject images
        self.crs_epsg = f"EPSG:{self.get_param('shp', raise_error=True).crs.to_epsg()}"
        img_col = img_col.map(lambda img: img.reproject(crs=self.crs_epsg, crsTransform=None, scale=self.get_param('resolution')))
            
        shp_4326 = self._reproject_shp(self.get_param('shp', raise_error=True))
        self._region = ee.FeatureCollection(json.loads(shp_4326['geometry'].to_json()))
        img_col = img_col.filterBounds(self._region)
        img_col = img_col.map(lambda img: img.clip(self._region))
        
        return img_col

    def download(self, img_col, create_minicube=True, remove_tmp=True):
        col_size = img_col.size().getInfo()
        assert col_size > 0, "No images to download."
        img_col = img_col.toList(col_size)
        tmp_dir = self.get_param('download_folder', '/tmp/eo_download/', raise_error=~create_minicube)
        os.makedirs(tmp_dir, exist_ok=True)
        # iterate and download tifs
        fns = []
        for i in range(col_size):
            img = ee.Image(img_col.get(i))
            id_prop = next((prop for prop in img.propertyNames().getInfo() if 'PRODUCT_ID' in prop), None)
            img_id = img.get(id_prop).getInfo()

            fileName = os.path.join(tmp_dir, f'{img_id}_{hashlib.sha256(self.get_param("shp", raise_error=True).geometry.iloc[0].wkt.encode("utf-8")).hexdigest()}.tif')
            if not os.path.exists(fileName):
                img = geedim.MaskedImage(img)
                img.download(fileName, crs=self.crs_epsg, scale=self.get_param('resolution'), region=self._region.geometry())
            fns.append(fileName)
        
        if create_minicube:
            return self.merge_gee_tifs(fns, remove_tmp)
        else:
            return fns

    def merge_gee_tifs(self, fns, remove_tmp=True):
        """merge the tifs and crop the to the shp"""
        if len(fns) < 1:
            raise ValueError("No files provided to merge.")
        date_pattern = r'\d{8}'
        shp = self.get_param('shp', raise_error=True)
        def load_tif(fn):
            da = rxr.open_rasterio(fn)
            if da.rio.crs != shp.crs:
                da = da.rio.reproject(shp.crs, resolution=self.get_param('resolution', raise_error=True))
            da = da.rio.clip(shp.geometry)
            time_str = re.findall(date_pattern, fn)[0]
            da = da.assign_coords(time=pd.to_datetime(time_str, format='%Y%m%d'))
            return da

        out = Parallel(n_jobs=self.get_param('num_workers', 1))(delayed(load_tif)(fn) for fn in fns)

        ds = xr.concat(out, dim='time').compute()
        ds = ds.sortby('time')
        ds = ds.to_dataset(dim='band')
        ds = ds.rename_vars({dim: name for dim, name in zip(ds.data_vars.keys(), ds.attrs['long_name'])})
        if 'FILL_MASK' in ds.data_vars:
            ds = ds.drop_vars('FILL_MASK')

        # remove the files
        if remove_tmp:
            self.rm_temp_files(fns)
        return ds

    def rm_temp_files(self, fns):
        for fn in fns:
            try:
                os.remove(fn)
            except Exception as e:
                print(f"Failed to remove file in download folder {fn}: {e}")

class CDSE(VoxelCrafter):
# https://documentation.dataspace.copernicus.eu/APIs/STAC.html
    def __init__(self, credentials=None):
        super().__init__()
        pass

class ASF(VoxelCrafter):
    def __init__(self, credentials=None):
        super().__init__()
        pass
