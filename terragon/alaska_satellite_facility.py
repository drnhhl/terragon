import logging
import shutil
import warnings
from pathlib import Path

import asf_search as asf
import pandas as pd
import rasterio
import rioxarray as rxr
import xarray as xr
from joblib import Parallel, delayed
from rasterio.vrt import WarpedVRT
from shapely.geometry import box

from .base import Base
from .utils import align_coords, align_resolutions


class ASF(Base):
    """Class to interact with the Alaska Satellite Facility (ASF). This class provides functionality to search for, download, and process satellite imagery from ASF.
    It downloads complete image tiles via HTTP into a temporary folder and subsequently crops, reprojects, and aligns the imagery to match a given shapefile.
    The processing pipeline leverages several libraries including asf_search, rasterio, rioxarray, xarray, pandas, and joblib to facilitate efficient parallel processing.

    Currently, the following satellite data collections are supported: SENTINEL-1, ALOS PALSAR, and ALOS AVNIR-2.

    :param credentials: A dictionary for ASF authentication. Expected format: {'asf_username': <username>, 'asf_password': <pwd>}.
    """

    _chunk_size = 131072  # chunks size for downloading files

    _supported_collections = ["SENTINEL-1", "ALOS PALSAR", "ALOS AVNIR-2"]

    def __init__(self, credentials: dict = {}):
        """Initialize the ASF instance with the provided credentials.

        :param credentials: Credentials to authenticate with ASF.
                            Expected format: {'asf_username': <username>, 'asf_password': <pw>}.
        """
        super().__init__()
        self.credentials = credentials
        self.session = None

    def _init_asf_session(self):
        """Authenticate and initialize an ASF session.

        :return: An authenticated ASF session object.
        :raises ValueError: If credentials are not provided.
        """
        if not self.credentials:
            raise ValueError(
                "Credentials are required to initialize an ASF session for downloading."
            )
        return asf.ASFSession().auth_with_creds(
            username=self.credentials.get("asf_username"),
            password=self.credentials.get("asf_password"),
        )

    def _get_session(self):
        """Lazily initialize and return the ASF session.

        :return: The active ASF session object.
        """
        if not self.session:
            self.session = self._init_asf_session()
        return self.session

    def retrieve_collections(self, filter_by_name: str = None):
        """Search the collections provided by the Alaska Satellite Facility.

        :param filter_by_name: Name to filter the collections for, defaults to None.
        :raises RuntimeError: If the request to the collections endpoint fails.
        :return: A list of collection names.
        """
        # Get all collections, ignoring private or hidden ones
        collections = [
            getattr(asf.PLATFORM, attr) for attr in dir(asf.PLATFORM) if not attr.startswith("_")
        ]

        # Apply optional filtering
        if filter_by_name:
            filter_by_name = filter_by_name.lower()
            collections = [
                collection for collection in collections if filter_by_name in collection.lower()
            ]

        if not collections:
            raise RuntimeError("Failed to retrieve collections")

        warnings.warn(
            f"Currently we only support the following collections: {self._supported_collections}"
        )
        return collections

    def search(self, rm_tmp_files=True, resampling=rasterio.enums.Resampling.nearest, **kwargs):
        """Search for items in the Alaska Satellite Facility collections. For a description of the args/kwargs parameters see the Base class function.

        :param rm_tmp_files: Remove downloaded temporary files after creating the data cube, defaults to True.
        :param resampling: Resampling method to use when reprojecting images, defaults to rasterio.enums.Resampling.nearest.
        :raises ValueError: If no items are found for the given search parameters.
        :return: A list of ASF products (items).
        """
        super().search(**kwargs)
        self._parameters.update(
            {
                "resampling": resampling,
                "rm_tmp_files": rm_tmp_files,
            }
        )

        if self._param("collection") not in self._supported_collections:
            warnings.warn(f"Currently we only support collections: {self._supported_collections}")

        # Reproject shapefile bounds to EPSG:4326 (required by ASF)
        bounds_4326 = self._reproject_shp(self._param("shp")).total_bounds
        bounds_wkt = box(*bounds_4326).wkt

        # Define time range for the search using pandas datetime
        start_date = self._param("start_date")
        end_date = self._param("end_date")
        if "T" not in start_date:
            start_date += "T00:00:00.000"
        start_date = pd.to_datetime(start_date, format="%Y-%m-%dT%H:%M:%S.%f")
        # change the end date to the end of the day
        if "T" not in end_date:
            end_date += "T23:59:59.999"
        end_date = pd.to_datetime(end_date, format="%Y-%m-%dT%H:%M:%S.%f")

        # Retrieve items from the specified collection
        collection = self._param("collection")
        filter_param = self._param("filter") or {}
        items = asf.geo_search(
            dataset=collection,
            start=start_date,
            end=end_date,
            intersectsWith=bounds_wkt,
            **filter_param,
        )

        if len(items) == 0:
            raise ValueError("No items found.")

        return items

    def _download_item(self, item, session, output_dir, bands=None):
        """Download a complete ASF item via HTTP and extract its relevant TIFF files.

        If the item has already been downloaded and the expected files exist, the download is skipped.
        Otherwise, the method downloads the zip file, extracts the specified TIFF files, and updates the item.

        :param item: ASF item containing metadata and the file URL.
        :param session: HTTP session to use for the download.
        :param output_dir: Directory where the item's files will be stored.
        :param bands: Optional list of band identifiers to filter for during extraction, defaults to None.
        :return: The updated ASF item with its 'band_files' property containing the paths to the extracted TIFF files.
        """
        # Determine a unique identifier for the item.
        item_id = item.properties.get("fileID")
        if not item_id:
            start_time_str = item.properties.get("startTime")
            start_date = pd.to_datetime(start_time_str, format="%Y-%m-%dT%H:%M:%SZ")
            item_id = start_date.strftime("%Y%m%d")
            item.properties["fileID"] = item_id

        item_dir = output_dir / item_id
        item.properties["tmp_folder"] = str(item_dir)
        item_dir.mkdir(parents=True, exist_ok=True)

        band_files = {}

        with item.remotezip(session=session) as z:

            # Filter TIFF files
            file_paths = [
                file.filename
                for file in z.filelist
                if file.filename.endswith(".tiff") or file.filename.endswith(".tif")
            ]

            # Optionally filter by bands
            if bands:
                file_paths = [
                    file for file in file_paths if any(band in file.lower() for band in bands)
                ]

            for idx, file_path in enumerate(file_paths):
                # Compute the desired local file path using only the base name
                file_name = Path(file_path).name
                local_path = item_dir / file_name

                with z.open(file_path) as src, open(local_path, "wb") as dst:
                    for chunk in iter(lambda: src.read(self._chunk_size), b""):
                        dst.write(chunk)

                # Determine the band name
                if bands:
                    band_name = next(
                        (band for band in bands if band in file_path.lower()), "unknown"
                    )
                else:
                    band_name = f"band_{idx + 1}"
                band_files[band_name] = str(local_path)

        for band_name, file_path in band_files.items():
            item.properties.setdefault("band_files", {})[band_name] = str(file_path)

        return item

    def download(self, items):
        """Download the items from Alaska Satellite Facility as xr.Dataset or download the files.

        :param items: List of ASF items to download.
        :return: An xarray.Dataset if the 'create_minicube' parameter is True; otherwise, a list of file paths to the TIFF files.
        :raises ValueError: If no items are provided for download.
        """
        if len(items) == 0:
            raise ValueError("No items to download.")

        session = self._get_session()  # Assumes this returns a requests.Session
        output_dir = Path(self._param("download_folder", raise_error=True))
        output_dir.mkdir(parents=True, exist_ok=True)

        bands = self._param("bands")
        if bands:
            bands = [band.lower() for band in bands]
        num_workers = self._param("num_workers")
        logging.info(f"Downloading {len(items)} items using {num_workers} worker(s).")

        # Parallelize the download per item.
        items = Parallel(n_jobs=num_workers, backend="threading", verbose=0)(
            delayed(self._download_item)(item, session, output_dir, bands) for item in items
        )

        if self._param("create_minicube"):
            ds = self._create_minicube(items)
            ds = self._prepare_cube(ds)

            if self._param("rm_tmp_files"):
                self._cleanup_tmp(items)
            return ds
        else:
            # Process and save each item in parallel
            def process_and_save(item, shp, resolution, resampling, output_dir):
                ds = self._load_band_data(item, shp, resolution, resampling)
                if ds is not None:
                    fp = output_dir / f"{item.properties.get('fileID')}.tiff"
                    ds.rio.to_raster(fp)
                    return fp
                return None

            fps = Parallel(n_jobs=self._param("num_workers"), backend="threading")(
                delayed(process_and_save)(
                    item,
                    self._param("shp"),
                    self._param("resolution"),
                    self._param("resampling"),
                    output_dir,
                )
                for item in items
            )

            if self._param("rm_tmp_files"):
                self._cleanup_tmp(items)

            return [fp for fp in fps if fp is not None]

    def _cleanup_tmp(self, items):
        for item in items:
            tmp = item.properties.get("tmp_folder")
            if tmp and Path(tmp).exists():
                try:
                    shutil.rmtree(tmp)
                    logging.info(f"Removed temporary folder: {tmp}")
                except Exception as e:
                    logging.error(f"Error removing temporary folder {tmp}: {e}")

    def _load_band_data(self, item, shp, resolution, resampling):
        """Load and preprocess band data for a single ASF item.

        The method opens the band's TIFF files, reprojects and clips the image data based on the provided shapefile,
        and ensures that all bands are aligned to a consistent resolution and spatial extent.
        Finally, it combines the processed bands into a single xarray.Dataset.

        :param item: ASF item containing band file paths and associated metadata.
        :param shp: Shapefile geometry used for clipping and reprojection.
        :param resolution: Target resolution for reprojection.
        :param resampling: Resampling method to use when reprojecting images.
        :return: An xarray.Dataset containing the processed band data, or None if processing fails.
        """
        band_data = []
        bands = []

        # Iterate over all band files in the item
        for band, fp in item.properties.get("band_files", {}).items():
            try:
                with rasterio.Env(GTIFF_SRS_SOURCE="EPSG", OSR_USE_NON_DEPRECATED="NO"):
                    with rasterio.open(fp) as src:
                        gcps, crs = src.get_gcps()
                        with WarpedVRT(src, src_crs=crs, resampling=resampling) as vrt:
                            with rxr.open_rasterio(vrt) as da:
                                # Reproject and clip the data as needed
                                da = da.rio.reproject(shp.crs)
                                da = da.rio.clip_box(*shp.total_bounds)
                                da.load()
                                bands.append(band)
                                band_data.append(da)
            except Exception as e:
                logging.error(f"Failed to process {band} from file {fp}: {e}")

        if not band_data:
            return None

        # Align all bands to the same resolution and spatial extent
        band_data = align_resolutions(band_data, shp, resolution, resampling)

        # Pad bands to ensure consistent spatial dimensions
        band_data = [
            ds.rio.pad_box(*list(shp.total_bounds)).rio.clip(shp.geometry, all_touched=True)
            for ds in band_data
        ]

        # Handle multi-band datasets and ensure proper naming
        for i, ds in enumerate(band_data):
            if "band" in ds.dims:
                if len(ds.band) > 1:
                    # If there are multiple bands, convert to a dataset and rename variables
                    band_data[i] = ds.to_dataset(dim="band")
                    band_data[i] = band_data[i].rename(
                        {var: f"{bands[i]}_{var}" for var in band_data[i].data_vars}
                    )
                else:
                    # Drop the "band" dimension for single-band datasets
                    band_data[i] = ds.drop_vars("band").squeeze("band")
                    band_data[i] = band_data[i].to_dataset(name=bands[i])

        # Combine all processed bands into a single dataset
        ds = xr.combine_by_coords(band_data)
        return ds

    def _create_minicube(self, items):
        """Merge multiple ASF items into a single multi-temporal data cube.

        The method processes individual ASF items, aligns their spatial coordinates,
        and concatenates them along the time dimension to produce a unified dataset.

        :param items: List of ASF items to merge.
        :return: A concatenated xarray.Dataset sorted by time.
        :raises RuntimeError: If the number of processed items does not match the input count.
        """
        shp = self._param("shp")
        resolution = self._param("resolution")
        resampling = self._param("resampling")

        time_data = Parallel(n_jobs=self._param("num_workers"), backend="threading")(
            delayed(self._load_band_data)(item, shp, resolution, resampling) for item in items
        )

        if len(time_data) != len(items):
            raise RuntimeError("Lengths of downloaded items and requested items do not match.")

        times = [item.properties["startTime"] for item in items]
        time_data, times = zip(
            *[(ds, time) for ds, time in zip(time_data, times) if ds is not None]
        )
        time_data = list(time_data)
        times = list(times)

        # Align datasets and concatenate along time
        time_data = align_coords(time_data, shp, resampling)
        time_data = [
            ds.assign_coords(
                time=pd.to_datetime(time, unit="ns" if isinstance(time, int) else None)
            )
            for ds, time in zip(time_data, times)
        ]

        data = xr.concat(time_data, dim="time", join="exact").sortby("time")
        return data
