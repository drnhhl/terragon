import ee
import geedim
import json
import os
import hashlib
import re
import xarray as xr
import rioxarray as rxr
import pandas as pd

from pathlib import Path
from joblib import Parallel, delayed
from .base import Base
from .utils import rm_files

class GEE(Base):
    def __init__(self, credentials=None):
        super().__init__()
        if not ee.data._credentials:
            raise RuntimeError("GEE not initialized. Did you run 'ee.Authenticate()' and ee.Initialize(project='my-project')?")

    def retrieve_collections(self, filter_by_name: str=None):
        raise NotImplementedError("GEE does not have a collection endpoint. Please, visit https://developers.google.com/earth-engine/datasets/catalog")

    def search(self, **kwargs):
        super().search(**kwargs)

        img_col = ee.ImageCollection(self.param('collection'))
        start_date = self.param('start_date')
        end_date = self.param('end_date')
        if start_date and end_date:
            img_col = img_col.filterDate(start_date, end_date)
        elif start_date:
            img_col = img_col.filterDate(start_date)
        elif end_date:
            raise ValueError("In GEE end_date must be used with start_date.")
        bands = self.param('bands')
        if bands:
            img_col = img_col.select(bands)
                
        return img_col

    def download(self, img_col, create_minicube=True, remove_tmp=True):
        shp_4326 = self._reproject_shp(self.param('shp'))

        # reproject images
        self.crs_epsg = f"EPSG:{self.param('shp').crs.to_epsg()}"
        img_col = img_col.map(lambda img: img.reproject(crs=self.crs_epsg, crsTransform=None, scale=self.param('resolution')))
        
        # clip images
        self._region = ee.FeatureCollection(json.loads(shp_4326['geometry'].to_json()))
        img_col = img_col.filterBounds(self._region)
        img_col = img_col.map(lambda img: img.clip(self._region))

        col_size = img_col.size().getInfo()
        assert col_size > 0, "No images to download."
        img_col = img_col.toList(col_size)
        tmp_dir = self.param('download_folder', raise_error=~create_minicube)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        # iterate and download tifs
        fns = []
        for i in range(col_size): # TODO: parallelize, see https://developers.google.com/earth-engine/guides/usage
            img = ee.Image(img_col.get(i))
            id_prop = next((prop for prop in img.propertyNames().getInfo() if 'PRODUCT_ID' in prop), None)
            img_id = img.get(id_prop).getInfo()

            fileName = tmp_dir.joinpath(f'{img_id}_{hashlib.sha256(self.param("shp").geometry.iloc[0].wkt.encode("utf-8")).hexdigest()}.tif')
            if not fileName.exists():
                img = geedim.MaskedImage(img)
                img.download(fileName, crs=self.crs_epsg, scale=self.param('resolution'), region=self._region.geometry())
            fns.append(fileName)
    
        if not create_minicube:
            return fns
        ds = self.merge_gee_tifs(fns)
        # remove the temp files
        if remove_tmp:
            rm_files(fns)

        ds = self.prepare_cube(ds)
        return ds

    def merge_gee_tifs(self, fns):
        """merge the tifs and crop the to the shp"""
        if len(fns) < 1:
            raise ValueError("No files provided to merge.")
        date_pattern = r'\d{8}'
        shp = self.param('shp')
        def load_tif(fn):
            da = rxr.open_rasterio(fn)
            if da.rio.crs != shp.crs:
                da = da.rio.reproject(shp.crs, resolution=self.param('resolution'))
            da = da.rio.clip(shp.geometry)
            time_str = re.findall(date_pattern, str(fn))[0]
            da = da.assign_coords(time=pd.to_datetime(time_str, format='%Y%m%d'))
            return da

        out = Parallel(n_jobs=self.get_param('num_workers'))(delayed(load_tif)(fn) for fn in fns)

        ds = xr.concat(out, dim='time').compute()
        ds = ds.sortby('time')
        ds = ds.to_dataset(dim='band')
        ds = ds.rename_vars({dim: name for dim, name in zip(ds.data_vars.keys(), ds.attrs['long_name'])})
        if 'FILL_MASK' in ds.data_vars:
            ds = ds.drop_vars('FILL_MASK')
        return ds
