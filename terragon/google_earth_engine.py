import hashlib
import json
import re
import warnings

import ee
import geedim
import pandas as pd
import rioxarray as rxr
import xarray as xr
from joblib import Parallel, delayed

from .base import Base
from .utils import meters_to_crs_unit, rm_files


class GEE(Base):
    def __init__(self, credentials: dict = None):
        super().__init__()
        if not ee.data._credentials:
            raise RuntimeError(
                "GEE not initialized. Did you run 'ee.Authenticate()' and ee.Initialize(project='my-project')?"
            )

    def retrieve_collections(self, filter_by_name: str = None):
        raise NotImplementedError(
            "GEE does not have a collection endpoint. Please, visit https://developers.google.com/earth-engine/datasets/catalog"
        )

    def search(self, **kwargs):
        super().search(**kwargs)

        img_col = ee.ImageCollection(self.param("collection"))
        start_date = self.param("start_date")
        end_date = self.param("end_date")
        if start_date and end_date:
            img_col = img_col.filterDate(start_date, end_date)
        elif start_date:
            img_col = img_col.filterDate(start_date)
        elif end_date:
            raise ValueError("In GEE end_date must be used with start_date.")
        bands = self.param("bands")
        if bands:
            img_col = img_col.select(bands)

        return img_col

    def download(self, img_col, create_minicube=True, remove_tmp=True):
        shp_4326 = self._reproject_shp(self.param("shp"))

        # reproject images
        img_col = img_col.map(
            lambda img: img.reproject(
                crs=f"EPSG:{self.param('shp').crs.to_epsg()}",
                crsTransform=None,
                scale=self.param("resolution"),
            )
        )

        # clip images
        self._region = ee.FeatureCollection(json.loads(shp_4326["geometry"].to_json()))
        img_col = img_col.filterBounds(self._region)
        img_col = img_col.map(lambda img: img.clip(self._region))

        col_size = img_col.size().getInfo()
        assert col_size > 0, "No images to download."
        img_col = img_col.toList(col_size)
        tmp_dir = self.param("download_folder", raise_error=not create_minicube)
        tmp_dir.mkdir(parents=True, exist_ok=True)

        # iterate and download tifs
        num_workers = self.param("num_workers")
        if num_workers > 40:
            warnings.warn(
                f"{num_workers} workers is most likely too high. \
                Setting it to 40 for downloading, see https://developers.google.com/earth-engine/guides/usage."
            )
            num_workers = 40
        fns = Parallel(n_jobs=num_workers, backend="threading")(
            delayed(self.download_img)(
                img_col, i, tmp_dir, self.param("shp"), self.param("resolution")
            )
            for i in range(col_size)
        )

        if not create_minicube:
            return fns
        ds = self.merge_gee_tifs(fns)
        # remove the temp files
        if remove_tmp:
            rm_files(fns)

        ds = self.prepare_cube(ds)
        return ds

    def download_img(self, img_col, i, tmp_dir, shp, resolution):
        img = ee.Image(img_col.get(i))
        # get the system id
        id_prop = next(
            (prop for prop in img.propertyNames().getInfo() if "system:id" in prop),
            None,
        )
        if not id_prop:
            warnings.warn(
                f"Could not find system:id property in image {i}. \
                Using consecutive numbers of images, but this can lead to problems wiht overwriting files."
            )
            img_id = i
        else:
            img_id = img.get(id_prop).getInfo()
            # replace the / with _ to avoid problems with file paths
            img_id = img_id.replace("/", "_")

        # create a unique filename through geometry since we are downloading clipped images
        geom_hash = hashlib.sha256(shp.geometry.iloc[0].wkt.encode("utf-8")).hexdigest()
        fileName = tmp_dir.joinpath(f"{img_id}_{geom_hash}.tif")
        if not fileName.exists():
            img = geedim.MaskedImage(img)
            img.download(
                fileName,
                crs=f"EPSG:{shp.crs.to_epsg()}",
                scale=resolution,
                region=self._region.geometry(),
            )
        return fileName

    def merge_gee_tifs(self, fns):
        """merge the tifs and crop the to the shp"""
        if len(fns) < 1:
            raise ValueError("No files provided to merge.")
        date_pattern = r"\d{8}"
        shp = self.param("shp")
        resolution = self.param("resolution")

        def load_tif(fn):
            da = rxr.open_rasterio(fn)
            if da.rio.crs != shp.crs:
                res = meters_to_crs_unit(resolution, shp)
                da = da.rio.reproject(shp.crs, resolution=res)
            time_str = re.findall(date_pattern, str(fn))[0]
            da = da.assign_coords(time=pd.to_datetime(time_str, format="%Y%m%d"))
            return da

        out = Parallel(n_jobs=self.get_param("num_workers"))(
            delayed(load_tif)(fn) for fn in fns
        )

        ds = xr.concat(out, dim="time").compute()
        ds = ds.sortby("time")
        ds = ds.to_dataset(dim="band")
        ds = ds.rename_vars(
            {dim: name for dim, name in zip(ds.data_vars.keys(), ds.attrs["long_name"])}
        )
        if "FILL_MASK" in ds.data_vars:
            ds = ds.drop_vars("FILL_MASK")
        return ds
