import itertools
import re
import warnings
from pathlib import Path
from urllib.parse import urljoin, urlparse

import boto3
import geopandas as gpd
import pandas as pd
import rasterio
import requests
import rioxarray as rxr
import xarray as xr
from joblib import Parallel, delayed
from rasterio.vrt import WarpedVRT
from shapely.geometry import box

from .base import Base
from .utils import align_coords, align_resolutions


class CDSE(Base):
    """Class to interact with the Copernicus Data Space Ecosystem. The images are downloaded from the AWS bucket.
    The packages rasterio/rioxarray/boto will be used to download the images.
    Currently only these collections are supported: COP-DEM, GLOBAL-MOSAICS, LANDSAT-5, LANDSAT-7, LANDSAT-8-ESA,
    TERRAAQUA, S2GLC, SENTINEL-1, SENTINEL-1-RTC, SENTINEL-2.

    :param credentials: credentials to authenticate, expected format: {'aws_access_key_id': <id>, 'aws_secret_access_key': <key>}. If None, it will fallback to the credential handling from boto3/rasterio.
    :param base_url: the URL for the STAC catalog, defaults to "https://catalogue.dataspace.copernicus.eu/stac/"
    :param end_point_url: the URL for the data endpoint, defaults to "https://eodata.dataspace.copernicus.eu"
    """

    _file_extensions = [
        ".jp2",
        ".tif",
        ".tiff",
        ".nc",
        ".dt2",
        ".dt1",
        ".img",
        ".JP2",
        ".TIF",
        ".TIFF",
        ".NC",
        ".DT2",
        ".DT1",
        ".IMG",
    ]

    _supported_collections = [
        "COP-DEM",
        "GLOBAL-MOSAICS",
        "LANDSAT-5",
        "LANDSAT-7",
        "LANDSAT-8-ESA",
        "TERRAAQUA",
        "S2GLC",
        "SENTINEL-1",
        "SENTINEL-1-RTC",
        "SENTINEL-2",
    ]

    def __init__(
        self,
        credentials: dict,
        base_url: str = "https://catalogue.dataspace.copernicus.eu/stac/",
        end_point_url: str = "https://eodata.dataspace.copernicus.eu",
    ):
        """Initialize class and save the credentials.

        :param credentials: credentials to authenticate, expected format: {'aws_access_key_id': <id>, 'aws_secret_access_key': <key>}. If None, it will fallback to the credential handling from boto3/rasterio.
        :param base_url: the URL for the STAC catalog, defaults to "https://catalogue.dataspace.copernicus.eu/stac/"
        :param end_point_url: the URL for the data endpoint, defaults to "https://eodata.dataspace.copernicus.eu"
        :raises ValueError: when the credentials are in the wrong format
        """
        super().__init__()
        self.base_url = base_url
        self.end_point_url = end_point_url
        if credentials:
            if "aws_access_key_id" not in credentials or "aws_secret_access_key" not in credentials:
                raise ValueError(
                    "aws_access_key_id or aws_secret_access_key not in credentials, could not initialize."
                )
        else:
            # fallback to credentials saved in the environment/files from aws
            credentials = {"aws_access_key_id": None, "aws_secret_access_key": None}
        self.credentials = credentials

    def retrieve_collections(self, filter_by_name: str = None):
        """Search the collections provided by Copernicus Data Space Ecosystem.

        :param filter_by_name: name to filter the collections for, defaults to None
        :raises RuntimeError: if the request to the collections endpoint fails
        :return: a list of collection names
        """
        collections_url = urljoin(self.base_url, "collections")
        response = requests.get(collections_url)

        if response.status_code == 200:
            data = response.json()
            collections = [collection["id"] for collection in data["collections"]]
            if filter_by_name:
                collections = [
                    collection for collection in collections if filter_by_name in collection.lower()
                ]
            warnings.warn(
                f"Currently we only support the following collections: {self._supported_collections}"
            )
            return collections
        else:
            raise RuntimeError("Failed to retrieve collections")

    def search(
        self,
        *args,
        resampling=rasterio.enums.Resampling.nearest,
        use_virtual_rasterio_file=True,
        rm_tmp_files=True,
        filter_asset_path={"COP-DEM": ".*/DEM/.*", "SENTINEL-2": ".*/IMG_DATA/.*"},
        **kwargs,
    ):
        """Search for items in the Copernicus Data Space Ecosystem collections via stac. For a description of the args/kwargs parameters see the Base class function.

        :param resampling: rasterio Resampling method is used to reproject the cubes, defaults to rasterio.enums.Resampling.nearest
        :param use_virtual_rasterio_file: use rasterio virtual file function when True, when False whole file is downloaded, defaults to True
        :param rm_tmp_files: only used with 'use_virtual_rasterio_file=False' to remove the files after the minicube is created, defaults to True
        :param filter_asset_path: manual filtering of the filepath in the AWS bucket, used for collections with ambiguous file names, defaults to {"COP-DEM": ".*/DEM/.*", "SENTINEL-2": ".*/IMG_DATA/.*"}
        :raises ValueError: when no items are found or parameters are in the wrong format
        :raises RuntimeError: when the corresponding files for the items are not found

        :return: a list of items
        """
        super().search(*args, **kwargs)
        self._parameters.update(
            {
                "rm_tmp_files": rm_tmp_files,
                "use_virtual_rasterio_file": use_virtual_rasterio_file,
                "resampling": resampling,
                "filter_asset_path": filter_asset_path,
            }
        )

        if self._param("collection") not in self._supported_collections:
            warnings.warn(f"Currently we only support collections: {self._supported_collections}")
        if self._param("num_workers") > 4:
            warnings.warn(
                "More than 4 workers are not recommended, because only 4 concurrent connections are allowed: https://documentation.dataspace.copernicus.eu/Quotas.html."
            )

        shp_4326 = self._reproject_shp(self._param("shp"))
        bbox = shp_4326.total_bounds
        start_date = self._param("start_date")
        end_date = self._param("end_date")
        # make end_date inclusive
        start_date = (
            f"{start_date}T00:00:00.000" if start_date and "T" not in start_date else start_date
        )
        end_date = f"{end_date}T23:59:59.999" if end_date and "T" not in end_date else end_date
        datetime = f"{start_date}/{end_date}" if start_date and end_date else None

        data = {
            "bbox": bbox.tolist(),
            "datetime": datetime,
            "collections": [self._param("collection")],
            "limit": 1000,
        }
        items = self._get_pages(data)

        if len(items) == 0:
            raise ValueError(f"No items found for {self._param('collection')} between {datetime}.")

        # apply filters
        return self._filter_items(items, self._param("filter"))

    def _filter_items(self, items, filter):
        if filter is not None and len(filter) > 0:
            for option in filter:
                for k, v in filter[option].items():
                    if k == "eq":
                        items = [item for item in items if item["properties"][option] == v]
                    elif k == "ueq":
                        items = [item for item in items if item["properties"][option] != v]
                    elif k == "in":
                        # value should be list
                        if not isinstance(v, list):
                            raise ValueError(f"Filter option {k} needs a list.")
                        items = [item for item in items if item["properties"][option] in v]
                    elif k == "lt":
                        items = [item for item in items if float(item["properties"][option]) < v]
                    elif k == "gt":
                        items = [item for item in items if float(item["properties"][option]) > v]
                    else:
                        raise ValueError(f"Filter option {k} not supported.")

        if len(items) == 0:
            raise ValueError(f"No items found for {self._param('collection')} after filtering.")

        return items

    def _get_pages(self, data):
        """loop through the api pages and return all items."""
        items = []
        for i in range(1, 100):
            _data = data.copy()
            _data["page"] = i
            response = requests.post(urljoin(self.base_url, "search"), json=_data)
            response.raise_for_status()
            page = response.json()

            if "features" not in page:
                raise ValueError(f"There was an error with the request: {page}")
            if len(page["features"]) == 0:
                break
            else:
                items.extend(page["features"])

            if i == 99:
                raise ValueError(
                    "Max number of pages reached. Consider using a smaller time frame."
                )

        return items

    def download(self, items):
        """Download the items from Copernicus Data Space Ecosystem as xr.Dataset or download the files.

        :param items: items to download
        :return: xarray.Dataset or list of filenames
        """
        if self._param("create_minicube"):
            ds = self._download_to_minicube(
                items,
                self._param("shp"),
                self._param("collection"),
                self._param("bands", raise_error=True),
                self._param("resolution"),
                self._param("resampling"),
                self._param("filter_asset_path"),
                self._param("use_virtual_rasterio_file"),
            )
            ds = self._prepare_cube(ds)
            return ds
        else:
            return self._download_to_files(
                items,
                self._param("shp"),
                self._param("collection"),
                self._param("bands", raise_error=True),
                self._param("resolution"),
                self._param("resampling"),
                self._param("filter_asset_path"),
                self._param("use_virtual_rasterio_file"),
            )

    def _download_to_files(
        self,
        items,
        shp,
        collection,
        bands,
        resolution,
        resampling,
        filter_asset_path,
        use_virtual_rasterio_file,
    ):
        """Download all the items and return the file paths."""
        fns = Parallel(n_jobs=self._param("num_workers"))(
            delayed(self._download_to_file)(
                item,
                shp,
                collection,
                band,
                resolution,
                resampling,
                filter_asset_path,
                use_virtual_rasterio_file,
            )
            for item, band in itertools.product(items, bands)
        )
        # extract list of lists
        fns = [fn for sublist in fns for fn in sublist]
        return fns

    def _download_to_file(
        self,
        item,
        shp,
        collection,
        band,
        resolution,
        resampling,
        filter_asset_path,
        use_virtual_rasterio_file,
    ):
        """Download item to file."""
        f_paths = self._get_asset_path(item, collection, band, resolution, filter_asset_path)
        # replace extension to tif and collapse folders to name (there can be multiple files with the same name)
        fns = []
        for f_path in f_paths:
            fn = self._param("download_folder") / Path("_".join(f_path.with_suffix(".tif").parts))
            if not fn.exists():
                clipped = self._download_file(f_path, shp, resampling, use_virtual_rasterio_file)
                fn.parent.mkdir(parents=True, exist_ok=True)
                clipped.rio.to_raster(fn)
            fns.append(fn)
        return fns

    def _download_to_minicube(
        self,
        items,
        shp,
        collection,
        bands,
        resolution,
        resampling,
        filter_asset_path,
        use_virtual_rasterio_file,
    ):
        """Download all items and merge them into a dataset."""
        # do not rely on threading here, because it will mess up the xarrays
        datasets = Parallel(n_jobs=self._param("num_workers"))(
            delayed(self._download_item)(
                item,
                shp,
                collection,
                band,
                resolution,
                resampling,
                filter_asset_path,
                use_virtual_rasterio_file,
            )
            for item, band in itertools.product(items, bands)
        )
        # extract list of lists [[item1band1, item1band2, ...], [item2band1, item2band2, ...], ...]
        datasets = [datasets[i : i + len(bands)] for i in range(0, len(datasets), len(bands))]
        # combine them to dataset with time data [ds1, ds2, ...]
        time_data = Parallel(n_jobs=self._param("num_workers"))(
            delayed(self._combine_bands)(datasets[i], shp, bands, resolution, resampling)
            for i in range(len(datasets))
        )

        # combine the time data
        if all([ds is None for ds in time_data]):
            raise RuntimeError("No items were found.")

        # extract time information for each dataset
        if len(time_data) != len(items):
            raise RuntimeError("Lengths of downloaded items and requested items do not match.")
        # skip items which were not found
        time_data, times = map(
            list,
            zip(
                *[
                    (ds, item["properties"]["datetime"])
                    for ds, item in zip(time_data, items)
                    if ds is not None
                ]
            ),
        )

        time_data = align_coords(time_data, shp, resampling)

        # add time coords (would have been removed by reproject_match in align_coords)
        time_data = [
            ds.assign_coords(time=("time", pd.to_datetime([time]).tz_convert(None)))
            for ds, time in zip(time_data, times)
        ]

        # merge time
        data = xr.concat(time_data, dim="time", join="exact").sortby("time")
        return data

    def _download_item(
        self,
        item,
        shp,
        collection,
        band,
        resolution,
        resampling,
        filter_asset_path,
        use_virtual_rasterio_file,
    ):
        """Download all bands of an item and merge them into a dataset."""
        # go through the bands
        f_paths = self._get_asset_path(item, collection, band, resolution, filter_asset_path)
        if len(f_paths) > 1:
            # if there are multiple assets, merge them
            datasets = []
            for f_path in f_paths:
                ds = self._download_file(f_path, shp, resampling, use_virtual_rasterio_file)
                if ds is None:
                    continue
                if "band" not in ds.coords:
                    # add band dim
                    ds = ds.expand_dims("band")
                # rename band each band coord with filename to make sure they are unique + identifyable later
                ds = ds.assign_coords(
                    band=[f"{f_path.stem}_{old}" for old in ds.coords["band"].values]
                )
                datasets.append(ds)
            if len(datasets) > 1:
                warnings.warn(
                    f"Multiple files found for band {band}: {f_paths}.\nAdding them as new bands."
                )
                if not all([ds.rio.crs == datasets[0].rio.crs for ds in datasets]):
                    datasets = align_coords(datasets, shp, resampling)
                if not all(
                    [ds.rio.resolution()[0] == datasets[0].rio.resolution()[0] for ds in datasets]
                ):
                    datasets = align_resolutions(datasets, shp, resolution, resampling)
                # add them as new bands
                clipped = xr.concat(datasets, dim="band")
            elif len(datasets) == 0:
                clipped = None
            else:
                clipped = datasets[0]
        else:
            # single asset
            clipped = self._download_file(f_paths[0], shp, resampling, use_virtual_rasterio_file)

        return clipped

    def _combine_bands(self, band_data, shp, bands, resolution, resampling):
        # check if all bands were found
        if len(band_data) != len(bands):
            raise RuntimeError("Length of band_data and bands do not match.")
        if all([ds is None for ds in band_data]):
            warnings.warn("Not all bands were downloaded.")
            return None
        if not all([ds is not None for ds in band_data]):
            raise RuntimeError("Bands were not downloaded.")

        # resample if needed
        band_data = align_resolutions(band_data, shp, resolution, resampling)

        # pad everything to make sure it has shape of shp
        band_data = [
            ds.rio.pad_box(*list(shp.total_bounds)).rio.clip_box(*list(shp.total_bounds))
            for ds in band_data
        ]

        # merge bands
        if not len(band_data) > 0:
            return
        # remove band dimension
        for i, ds in enumerate(band_data):
            if "band" in ds.dims:
                if len(ds.band) > 1:
                    band_data[i] = band_data[i].to_dataset(dim="band")
                    band_data[i] = band_data[i].rename(
                        {old: f"{bands[i]}_{old}" for old in band_data[i].data_vars}
                    )
                else:
                    band_data[i] = band_data[i].drop_vars("band").squeeze("band")
                    band_data[i] = band_data[i].to_dataset(name=bands[i])
        ds = xr.combine_by_coords(band_data)

        return ds

    def _download_file(self, f_path, shp, resampling, use_virtual_rasterio_file):
        """wrapper to decide whether to use rasterio or patch download."""
        if use_virtual_rasterio_file:
            return self._download_file_rasterio(f_path, shp, resampling)
        else:
            return self._download_file_tile(f_path, shp, resampling)

    def _download_file_rasterio(self, f_path, shp, resampling):
        """Download a band of an item and clip it to the shapefile."""
        session = rasterio.session.AWSSession(
            aws_unsigned=False,
            endpoint_url=(
                urlparse(self.end_point_url).netloc
                if "://" in self.end_point_url
                else self.end_point_url
            ),
            aws_access_key_id=self.credentials["aws_access_key_id"],
            aws_secret_access_key=self.credentials["aws_secret_access_key"],
        )
        with rasterio.env.Env(session=session, AWS_VIRTUAL_HOSTING=False):
            clipped = self._clip_to_region("s3://eodata/" + f_path.as_posix(), shp, resampling)
            return clipped

    def _download_file_tile(self, f_path, shp, resampling):
        """Download a band of an item and clip it to the shapefile."""
        # download the file locally
        download_path = self._param("download_folder") / f_path
        download_path.parent.mkdir(parents=True, exist_ok=True)
        if not download_path.exists():
            _s3 = boto3.Session(
                aws_access_key_id=self.credentials["aws_access_key_id"],
                aws_secret_access_key=self.credentials["aws_secret_access_key"],
                region_name="default",
            ).resource("s3", endpoint_url=self.end_point_url)

            _s3.Bucket("eodata").download_file(f_path.as_posix(), download_path)

        # clip to shp
        clipped = self._clip_to_region(download_path, shp, resampling)

        # optionally remove the downloaded file
        if self._param("rm_tmp_files"):
            download_path.unlink(missing_ok=True)

        return clipped

    def _get_asset_path(self, item, collection, band, resolution, filter_asset_path):
        # set up session to read file structure
        _s3 = boto3.Session(
            aws_access_key_id=self.credentials["aws_access_key_id"],
            aws_secret_access_key=self.credentials["aws_secret_access_key"],
            region_name="default",
        ).resource("s3", endpoint_url=self.end_point_url)

        # extract item path
        try:
            s3_path = item["assets"]["PRODUCT"]["alternate"]["s3"]["href"]
        except KeyError:
            raise RuntimeError(
                "It seems that no s3 path exists for this item. Returned item: ", item
            )
        folder_name = "/".join(s3_path.split("/")[2:])
        response = _s3.Bucket("eodata").objects.filter(Prefix=folder_name)

        # filter for extension
        paths = [
            obj for obj in response if any([obj.key.endswith(x) for x in self._file_extensions])
        ]
        if len(paths) == 0:
            raise RuntimeError("No file with valid extension found.")

        # transform to Path object
        paths = [Path(obj.key) for obj in paths]

        # filter for band
        if band is not None and len(band) > 0:
            paths_new = [path for path in paths if band in path.name]
            if len(paths_new) > 0:
                paths = paths_new
            else:
                # try again with lower and upper case
                paths = [
                    path for path in paths if band.lower() in path.name or band.upper() in path.name
                ]
            if len(paths) == 0:
                raise RuntimeError(f"Band {band} not found, is it written correctly?")

        # apply regex path filters
        if filter_asset_path and collection in filter_asset_path:
            pattern = re.compile(filter_asset_path[collection])
            paths = [path for path in paths if re.search(pattern, path.as_posix())]
            if len(paths) == 0:
                raise RuntimeError(
                    "There are no files matching the filter_asset_path: ",
                    filter_asset_path,
                )

        paths_new = [
            path for path in paths if f"{resolution}m" in str(path)
        ]  # filter for resolution (currently only applies for S2)
        if len(paths_new) > 0:  # if it was found take it, otherwise will be resampled later
            paths = paths_new

        if len(paths) == 0:
            raise RuntimeError("No file found.")

        return paths

    def _clip_to_region(self, file, shp, resampling):
        try:
            ds = rxr.open_rasterio(file)
            gcps = ds.rio.get_gcps()
            if gcps and not any(c in ds.coords for c in ["x", "y"]):
                with rasterio.open(file) as src:
                    gcps, crs = src.get_gcps()
                    # use WarpedVRT to get the correct transform/coordinates
                    with WarpedVRT(src, src_crs=crs, resampling=resampling) as vrt:
                        ds = rxr.open_rasterio(vrt)
            if ds.rio.crs is None:
                warnings.warn("No crs found, continuing with EPSG:4326.")
                ds = ds.rio.write_crs("EPSG:4326")
            src_crs = ds.rio.crs
            if src_crs != shp.crs:
                # reproject shp and clip with margin
                shp_crs = shp.to_crs(src_crs)
                # create bbox with 10% margin
                margin = (
                    (shp_crs.bounds.maxx.item() - shp_crs.bounds.minx.item()) * 0.1,
                    (shp_crs.bounds.maxy.item() - shp_crs.bounds.miny.item()) * 0.1,
                )
                shapely_box = box(
                    shp_crs.bounds.minx.item() - margin[0],
                    shp_crs.bounds.miny.item() - margin[1],
                    shp_crs.bounds.maxx.item() + margin[0],
                    shp_crs.bounds.maxy.item() + margin[1],
                )
                gdf = gpd.GeoDataFrame(geometry=[shapely_box], crs=src_crs)
                clipped = ds.rio.clip_box(*list(gdf.total_bounds))
                # then reproject to shp crs
                clipped = clipped.rio.reproject(shp.crs, resampling=resampling)
                clipped = clipped.rio.clip_box(*list(shp.total_bounds))
            else:
                clipped = ds.rio.clip_box(*list(shp.total_bounds))
            clipped.load()
            ds.close()
            return clipped
        except rxr.exceptions.NoDataInBounds:
            warnings.warn("No data found in bounds.")
            return None
