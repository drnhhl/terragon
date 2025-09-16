import hashlib
import json
import math
import warnings
from typing import List, Union

import ee
import geedim
import pandas as pd
import rioxarray as rxr
import xarray as xr
from joblib import Parallel, delayed
from rasterio.enums import Resampling
from rasterio.transform import from_origin

from .base import Base
from .utils import align_coords, meters_to_crs_unit, rm_files


class GEE(Base):
    """The class for Google Earth Engine downloads. The package geedim will be used to download the images
    and they are stored intermedialtly in .tif format.

    :param Base: Base class defining the interface and some common methods
    :param credentials: unused, kept for compatibility, defaults to None
    """

    _GEE_ID_PROP_NAME = "system:id"
    _GEE_DATE_PROP_NAME = "system:time_start"
    _GEE_ADD_BAND = "FILL_MASK"

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

    def retrieve_collections(self, query: dict = {}, fields: list[str] = []) -> None:
        """Not implemented, because GEE does not have a collection endpoint.

        :param query: query to filter the collections for in style '{<key>:<regex>}', defaults to {}
        :param fields: list of fields to include in the response, defaults to []
        :raises NotImplementedError: GEE does not have a collection endpoint
        """
        raise NotImplementedError(
            "GEE does not have a collection endpoint. Please, visit https://developers.google.com/earth-engine/datasets/catalog"
        )

    def search(self, *args, rm_tmp_files=True, **kwargs) -> ee.ImageCollection:
        """Search for items in the GEE collections, return the items and their meta data, and store the parameters in the class in order to access them later in the download function.
        For a description of the args/kwargs parameters see the Base class function.

        :param rm_tmp_files: remove temporarily downloaded files after creating the minicube, defaults to True
        :param args/kwargs: Parameters which are handled by the parent class, these parameters are the same for all data providers. See the 'Base' class for more information.
        :raises ValueError: when parameters are missing or in the wrong format
        :return: ee.ImageCollection
        """
        super().search(*args, **kwargs)
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
        """Download the prepared images from the GEE ImageCollection, store them as temporary .tif files and return them as a xarray.Dataset.
        If `create_minicube` is set to True, a xarray.Dataset will be returned, otherwise a list of filenames will be returned.
        if `rm_tmp_files` is set to True, the temporary files will be removed after creating the xarray.Dataset.

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
        if col_size < 1:
            raise ValueError("No images to download.")
        img_col = img_col.toList(col_size)
        tmp_dir = self._param("download_folder", raise_error=not self._param("create_minicube"))
        tmp_dir.mkdir(parents=True, exist_ok=True)

        # iterate and download tifs
        num_workers = self._param("num_workers")
        if num_workers > 40:
            warnings.warn(
                f"{num_workers} workers is most likely too high, see https://developers.google.com/earth-engine/guides/usage."
            )
        result = Parallel(n_jobs=num_workers, backend="threading")(
            delayed(self._download_img)(
                img_col, i, tmp_dir, self._param("shp"), region, transform, outline
            )
            for i in range(col_size)
        )
        fns, meta = zip(*result)

        if not self._param("create_minicube"):
            return fns
        ds = self._merge_gee_tifs(fns)
        # remove the temp files
        if self._param("rm_tmp_files"):
            rm_files(fns)

        if len(self._param("save_metadata")) > 0:
            # add metadata to the dataset
            meta = {k: [d[k] for d in meta] for k in self._param("save_metadata")}
            ds = ds.assign_coords({key: ("time", values) for key, values in meta.items()})

        ds = self._prepare_cube(ds)
        return ds

    def _download_img(self, img_col, i, tmp_dir, shp, region, transform, shape):
        """Download a single image from the GEE ImageCollection."""
        img = ee.Image(img_col.get(i))
        id_prop = self._get_img_property(img, self._GEE_ID_PROP_NAME)
        date_prop = self._get_img_property(img, self._GEE_DATE_PROP_NAME)
        if id_prop is None:
            warnings.warn(
                f"Could not find system:id property in image {i}. \
                Using consecutive numbers of images, but this can lead to problems with overwriting files."
            )
            id_prop = i
        else:
            # replace the / with _ to avoid problems with file paths
            id_prop = id_prop.replace("/", "_")
        if date_prop is None:
            warnings.warn(
                f"Could not find system:time_start property in image {i}. \
                Using the current date, but this can lead to problems."
            )
            # current date in ms
            date_prop = int(pd.Timestamp.now().timestamp() * 1000)
        # meta data
        props = {k: self._get_img_property(img, k) for k in self._param("save_metadata")}

        # create a unique filename through geometry since we are downloading clipped images
        geom_hash = hashlib.sha256(shp.geometry.iloc[0].wkt.encode("utf-8")).hexdigest()
        fileName = tmp_dir.joinpath(f"{date_prop}_{id_prop}_{geom_hash}.tif")
        if not fileName.exists():
            img = geedim.MaskedImage(img)
            img.download(
                fileName,
                crs=f"EPSG:{shp.crs.to_epsg()}",
                crs_transform=transform,
                region=region.geometry(),
                shape=shape,
            )
        return fileName, props

    def _get_img_property(self, img, prop_name):
        """Get a property from an image."""
        prop = img.get(prop_name).getInfo()
        if prop is None:
            # fallback to in
            id_prop = next(
                (prop for prop in img.propertyNames().getInfo() if prop_name in prop),
                None,
            )
            if id_prop is not None:
                prop = img.get(id_prop).getInfo()
        return prop

    def _merge_gee_tifs(self, fns) -> xr.Dataset:
        """merge the tifs and crop them to the shp"""
        if len(fns) < 1:
            raise ValueError("No files provided to merge.")

        def load_tif(fn):
            da = rxr.open_rasterio(fn)
            # first string is date, see _download_img
            time_str = fn.name.split("_")[0]
            da = da.assign_coords(time=pd.to_datetime(int(time_str), unit="ms"))
            return da

        out = Parallel(n_jobs=self._param("num_workers"), backend="threading")(
            delayed(load_tif)(fn) for fn in fns
        )

        out = align_coords(out, self._param("shp"), Resampling.nearest)

        ds = xr.concat(out, dim="time")
        ds = ds.sortby("time")
        ds = ds.to_dataset(dim="band")
        ds = ds.rename_vars(
            {dim: name for dim, name in zip(ds.data_vars.keys(), ds.attrs["long_name"])}
        )
        if self._GEE_ADD_BAND in ds.data_vars:
            ds = ds.drop_vars(self._GEE_ADD_BAND)
        return ds
