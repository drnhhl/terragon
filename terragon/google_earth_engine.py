import hashlib
import json
import math
import re
import warnings
from typing import List, Union

import ee
import geedim
import pandas as pd
import rioxarray as rxr
import xarray as xr
from joblib import Parallel, delayed
from rasterio.transform import from_origin

from .base import Base
from .utils import meters_to_crs_unit, rm_files


class GEE(Base):
    """The class for Google Earth Engine downloads. The package geedim will be used to download the images
    and they are stored intermedialtly in .tif format.

    :param Base: Base class defining the interface and some common methods
    :param credentials: unused, kept for compatibility, defaults to None
    """

    def __init__(self, credentials: dict = None) -> None:
        """Initialize class and GEE.

        :param credentials: unused, kept for compatibility, defaults to None
        :raises RuntimeError: when GEE is not initialized with ee.Authenticate() and ee.Initialize(project='my-project')
        """
        super().__init__()
        if not ee.data._credentials:
            raise RuntimeError(
                "GEE not initialized. Did you run 'ee.Authenticate()' and ee.Initialize(project='my-project')?"
            )

    def retrieve_collections(self, filter_by_name: str = None) -> None:
        """Not implemented, because GEE does not have a collection endpoint.

        :param filter_by_name: unused, kept for compatibility, defaults to None
        :raises NotImplementedError: GEE does not have a collection endpoint
        """
        raise NotImplementedError(
            "GEE does not have a collection endpoint. Please, visit https://developers.google.com/earth-engine/datasets/catalog"
        )

    def search(self, rm_tmp_files=True, **kwargs) -> ee.ImageCollection:
        """Search for items in the GEE collections. For a description of the kwargs parameters see the Base class function.

        :param rm_tmp_files: remove temporarily downloaded files after creating the minicube, defaults to True
        :raises ValueError: when parameters are missing or in the wrong format
        :return: ee.ImageCollection
        """
        super().search(**kwargs)
        self._parameters.update({"rm_tmp_files": rm_tmp_files})

        img_col = ee.ImageCollection(self._param("collection"))
        start_date = self._param("start_date")
        end_date = self._param("end_date")
        # end date is exclusive in GEE, make end_date inclusive
        end_date = f"{end_date}T23:59:59.999" if "T" not in end_date else end_date
        if start_date and end_date:
            img_col = img_col.filterDate(start_date, end_date)
        elif start_date:
            img_col = img_col.filterDate(start_date)
        elif end_date:
            raise ValueError("In GEE end_date must be used with start_date.")
        bands = self._param("bands")
        if bands:
            img_col = img_col.select(bands)

        return img_col

    def download(self, img_col: ee.ImageCollection) -> Union[xr.Dataset, List]:
        """Download the clipped images from the GEE ImageCollection, store them as temporary .tif files and create a minicube.

        :param img_col: ee.ImageCollection to download
        :return: xarray.Dataset or list of filenames
        """
        shp_4326 = self._reproject_shp(self._param("shp"))

        # compute the outline and transform
        shp = self._param("shp")
        res = meters_to_crs_unit(self._param("resolution"), shp)
        transform = from_origin(shp.total_bounds[0], shp.total_bounds[3], res[0], res[1])
        # geedim needs: (height, width)
        outline = (
            math.ceil(abs(shp.total_bounds[3] - shp.total_bounds[1]) / res[1]),
            math.ceil(abs(shp.total_bounds[2] - shp.total_bounds[0]) / res[0]),
        )

        # clip images
        region = ee.FeatureCollection(json.loads(shp_4326["geometry"].to_json()))
        img_col = img_col.filterBounds(region)

        col_size = img_col.size().getInfo()
        assert col_size > 0, "No images to download."
        img_col = img_col.toList(col_size)
        tmp_dir = self._param("download_folder", raise_error=not self._param("create_minicube"))
        tmp_dir.mkdir(parents=True, exist_ok=True)

        # iterate and download tifs
        num_workers = self._param("num_workers")
        if num_workers > 40:
            warnings.warn(
                f"{num_workers} workers is most likely too high, see https://developers.google.com/earth-engine/guides/usage."
            )
        fns = Parallel(n_jobs=num_workers, backend="threading")(
            delayed(self._download_img)(
                img_col, i, tmp_dir, self._param("shp"), region, transform, outline
            )
            for i in range(col_size)
        )

        if not self._param("create_minicube"):
            return fns
        ds = self._merge_gee_tifs(fns)
        # remove the temp files
        if self._param("rm_tmp_files"):
            rm_files(fns)

        ds = self._prepare_cube(ds)
        return ds

    def _download_img(self, img_col, i, tmp_dir, shp, region, transform, shape):
        """Download a single image from the GEE ImageCollection."""
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
                crs_transform=transform,
                region=region.geometry(),
                shape=shape,
            )
        return fileName

    def _merge_gee_tifs(self, fns) -> xr.Dataset:
        """merge the tifs and crop them to the shp"""
        if len(fns) < 1:
            raise ValueError("No files provided to merge.")
        date_pattern = r"\d{8}"

        def load_tif(fn):
            da = rxr.open_rasterio(fn)
            time_str = re.findall(date_pattern, str(fn))[0]
            da = da.assign_coords(time=pd.to_datetime(time_str, format="%Y%m%d"))
            return da

        out = Parallel(n_jobs=self._param("num_workers"), backend="threading")(
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
