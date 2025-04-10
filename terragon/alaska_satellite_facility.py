import logging
import shutil
import warnings
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

import asf_search as asf
import pandas as pd
import rasterio
import rioxarray as rxr
import xarray as xr
from joblib import Parallel, delayed
from rasterio.vrt import WarpedVRT
from shapely.geometry import box

from .base import Base
from .utils import indices_are_identical, meters_to_crs_unit

supported_collections = ["SENTINEL-1", "ALOS PALSAR", "ALOS AVNIR-2"]
warnings.simplefilter("ignore")


class ASF(Base):
    def __init__(self, credentials: dict = None):
        """
        Initialize the ASF class for searching, downloading, and processing ASF datasets.

        Args:
            credentials (dict): Optional ASF credentials (username and password).
        """
        super().__init__()
        self.credentials = credentials or {}
        self.session = None

        # Suppress irrelevant warnings and errors for cleaner logs
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        logging.getLogger("asf_search").setLevel(logging.ERROR)

    def _init_asf_session(self):
        """Authenticate and initialize an ASF session."""
        if not self.credentials:
            raise ValueError(
                "Credentials are required to initialize an ASF session for downloading."
            )
        return asf.ASFSession().auth_with_creds(
            username=self.credentials.get("asf_username"),
            password=self.credentials.get("asf_password"),
        )

    def get_session(self):
        """Lazy initialization of the ASF session."""
        if not self.session:
            self.session = self._init_asf_session()
        return self.session

    def retrieve_collections(self, filter_by_name: str = None):
        """
        Retrieve ASF collections and filter them if a name filter is provided.

        Args:
            filter_by_name (str): A substring to filter collections by name.

        Returns:
            list: Filtered list of collection names.
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
            f"Currently we only support the following collections: {supported_collections}"
        )
        return collections

    def search(self, rm_tmp_files=True, resampling=rasterio.enums.Resampling.nearest, **kwargs):
        """
        Search for ASF products matching the specified parameters.

        Returns:
            list: Search results as ASF items.
        """
        super().search(**kwargs)
        self._parameters.update(
            {
                "resampling": resampling,
                "rm_tmp_files": rm_tmp_files,
            }
        )

        if self._param("collection") not in supported_collections:
            warnings.warn(f"Currently we only support collections: {supported_collections}")

        # Reproject shapefile bounds to EPSG:4326 (required by ASF)
        bounds_4326 = self._reproject_shp(self._param("shp")).total_bounds
        bounds_wkt = box(*bounds_4326).wkt

        # Define time range for the search
        start_date = self._param("start_date")
        end_date = self._param("end_date")
        if "T" not in start_date:
            start_date += "T00:00:00.000"
        start_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S.%f")
        if "T" not in end_date:
            end_date += "T23:59:59.999"
        end_date = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S.%f")

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
            raise ValueError("No items found")

        return items

    def _download_via_http(self, url, session, output_dir, chunk_size=131072):
        """
        Download a file from an HTTP URL using requests and save it locally.

        Args:
            url (str): HTTP URL to download.
            session: A requests.Session (or similar) for the download.
            output_dir (Path): Local directory where the file will be saved.
            chunk_size (int): Size of each chunk in bytes.

        Returns:
            Path: Path to the downloaded local file.
        """
        local_file_path = output_dir / Path(url).name
        with session.get(url, stream=True, timeout=30) as response:
            response.raise_for_status()
            with open(local_file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)

        return local_file_path

    def _extract_files(self, zip_path, output_dir, bands=None):
        """
        Extract TIFF files from a zip archive using multiple threads,
        flattening the directory structure so that all files are extracted directly
        into output_dir (i.e. without recreating any subfolders).

        Args:
            zip_path (Path): Path to the zip archive.
            output_dir (Path): Directory to extract files.
            bands (list, optional): List of band identifiers to filter.

        Returns:
            dict: Dictionary mapping band names to file paths.
        """
        # Open the zip file once to get the list of relevant file names.
        with ZipFile(zip_path, "r") as z:
            file_names = [name for name in z.namelist() if name.lower().endswith((".tiff", ".tif"))]

        if bands:
            bands_lower = [band.lower() for band in bands]
            file_names = [
                name for name in file_names if any(b in name.lower() for b in bands_lower)
            ]
        else:
            bands_lower = []

        def extract_file(file_name):
            # Each worker re-opens the zip file independently.
            with ZipFile(zip_path, "r") as z:
                # Read the file contents from the zip.
                data = z.read(file_name)
            # Write the file directly to output_dir using only its basename.
            local_file = output_dir / Path(file_name).name
            with open(local_file, "wb") as f:
                f.write(data)
            band_name = next((band for band in bands_lower if band in file_name.lower()), "unknown")
            return band_name, str(local_file)

        # Use joblib to run extraction in parallel and collect results.
        results = Parallel(n_jobs=-1)(delayed(extract_file)(file_name) for file_name in file_names)

        # Aggregate the results into a dictionary.
        band_files = {band: file_path for band, file_path in results}
        return band_files

    def _download_item(
        self, item, session, output_dir, s3_creds=None, bands=None, chunk_size=131072
    ):
        """
        Download the entire zip file for an ASF item—using S3 if an "S3Url" property is available,
        falling back to HTTP if not. Once downloaded, extract only the desired TIFF files directly into output_dir
        (flattening any subfolder structure).

        Args:
            item: ASF item to download.
            session: Session for HTTP downloads.
            output_dir (Path): Directory where files are saved.
            bands (list, optional): List of band strings to filter for.
            chunk_size (int): Chunk size in bytes for downloads.

        Returns:
            The updated item with its 'band_files' property updated.
        """
        # Determine a unique identifier for the item.
        item_id = item.properties.get("fileID")
        if not item_id:
            start_time_str = item.properties.get("startTime")
            start_date = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M:%SZ")
            item_id = start_date.strftime("%Y%m%d")
            item.properties["fileID"] = item_id

        item_dir = output_dir / item_id
        item.properties["tmp_folder"] = str(item_dir)

        band_files = {}
        if item_dir.exists():
            for band in bands or []:
                matching_files = list(item_dir.rglob(f"*{band.lower()}*.tif*"))
                if matching_files:
                    band_files[band] = str(matching_files[0])
        if band_files:
            item.properties.setdefault("band_files", {}).update(band_files)
            logging.info("Found existing files; skipping download for this item.")
            return item
        else:
            logging.info("Item folder exists but expected files were not found. Redownloading...")

        # Create the directory for this item.
        item_dir.mkdir(parents=True, exist_ok=True)

        # Always use HTTP download - S3 is not running so far - but maybe later on
        url = item.properties.get("url")
        if url is None:
            raise ValueError("No URL found in item properties for downloading.")
        local_zip_path = self._download_via_http(url, session, item_dir, chunk_size)

        # Extract the desired TIFF files from the downloaded zip file.
        band_files = self._extract_files(local_zip_path, item_dir, bands)
        for band_name, file_path in band_files.items():
            item.properties.setdefault("band_files", {})[band_name] = str(file_path)

        # Remove the downloaded zip file.
        local_zip_path.unlink()

        return item

    def download(self, items):
        """
        Download ASF items and optionally merge them into a data cube.

        Args:
            items: List of ASF items to download.

        Returns:
            Either an xarray dataset (if create_minicube is True) or a list of file paths.
        """
        assert len(items) > 0, "No images to download."
        session = self.get_session()  # Assumes this returns a requests.Session
        output_dir = Path(self._get_param("download_folder", raise_error=True))
        output_dir.mkdir(parents=True, exist_ok=True)

        bands = self._param("bands")
        if bands:
            bands = [band.lower() for band in bands]
        num_workers = self._get_param("num_workers", default=1)
        logging.info(f"Downloading {len(items)} items using {num_workers} worker(s).")

        s3_creds = self._get_s3_credentials()

        # Parallelize the download per item.
        items = Parallel(n_jobs=num_workers, backend="threading", verbose=0)(
            delayed(self._download_item)(item, session, output_dir, s3_creds, bands)
            for item in items
        )

        if self._param("create_minicube"):
            ds = self._create_minicube(items)
            ds = self._prepare_cube(ds)

            if self._param("rm_tmp_files"):
                # Force evaluation/computation so the dataset no longer depends on the temporary files.
                ds = ds.compute()
                for item in items:
                    tmp_folder = item.properties.get("tmp_folder")
                    if tmp_folder and Path(tmp_folder).exists():
                        try:
                            shutil.rmtree(tmp_folder)
                            logging.info(f"Removed temporary folder: {tmp_folder}")
                        except Exception as e:
                            logging.error(f"Error removing temporary folder {tmp_folder}: {e}")
            return ds
        else:
            fps = []
            for item in items:
                ds = self._load_band_data(
                    item, self._param("shp"), self._param("resolution"), self._param("resampling")
                )
                fp = output_dir / f"{item.properties.get("fileID")}.tiff"
                ds.rio.to_raster(fp)
                fps.append(fp)
            return fps

    def _load_band_data(self, item, shp, resolution, resampling):
        """
        Load and preprocess band data for a single item.

        Args:
            item: The ASF item containing band files and metadata.
            shp: Shapefile geometry for clipping and reprojection.
            resolution: Target resolution for reprojection.

        Returns:
            xarray.Dataset: Combined dataset with all bands as variables.
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
                                da = (
                                    da.load()
                                )  # Force full loading of the data into memory, which releases file handles
                                bands.append(band)
                                band_data.append(da)
            except Exception as e:
                logging.error(f"Failed to process {band} from file {fp}: {e}")

        if not band_data:
            return None

        # Align all bands to the same resolution and spatial extent
        band_data = self._align_resolutions(band_data, shp, resolution, resampling)

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
        """
        Merge multiple ASF files into a single dataset.
        :param fps: List of file paths to merge.
        """
        shp = self._param("shp")
        resolution = self._param("resolution")
        resampling = self._param("resampling")

        time_data = Parallel(n_jobs=self._get_param("num_workers", default=1), backend="threading")(
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
        time_data = self._align_coords(time_data, shp, resampling)
        time_data = [
            ds.assign_coords(
                time=pd.to_datetime(time, unit="ns" if isinstance(time, int) else None)
            )
            for ds, time in zip(time_data, times)
        ]

        data = xr.concat(time_data, dim="time", join="exact").sortby("time")
        return data

    def _align_resolutions(self, datasets, shp, resolution, resampling):
        """unify the resolutions of the list of the xr.Datasets."""
        ress = [ds.rio.resolution() for ds in datasets]
        resolution = meters_to_crs_unit(resolution, shp)
        # round degrees to 8 decimal places for cm resolution
        ress = [(round(res[0], 8), round(res[0], 8)) for res in ress]
        resolution = [round(res, 8) for res in resolution]
        if len(set(ress)) > 1 or (len(ress) > 0 and ress[0] != resolution):
            # get closest resolution
            idx = sorted(
                enumerate(ress),
                key=lambda item: abs(resolution[0] - abs(item[1][0]))
                + abs(resolution[1] - abs(item[1][1])),
            )[0][0]
        if ress[idx] != resolution:
            datasets[idx] = datasets[idx].rio.reproject(
                shp.crs, resolution=resolution, resampling=resampling
            )
        # reproject rest to master
        datasets = [
            (ds.rio.reproject_match(datasets[idx], resampling=resampling) if i != idx else ds)
            for i, ds in enumerate(datasets)
        ]
        return datasets

    def _align_coords(self, datasets, shp, resampling):
        """unify the crs and indices of the datasets."""
        # make sure the coordinates are the same and match (e.g. if there are other crs)
        if not all(
            [ds.rio.crs == datasets[0].rio.crs for ds in datasets]
        ) or not indices_are_identical(datasets):
            crs_list = [ds.rio.crs for ds in datasets]
            # match all to master (first shp crs)
            idx = [i for i, crs in enumerate(crs_list) if crs == shp.crs]
            if len(idx) == 0:
                warnings.warn(
                    f"No matching crs found and all crs are different. Using first crs {crs_list[0]} as master."
                )
                idx = 0
            else:
                idx = idx[0]
            datasets = [
                (ds.rio.reproject_match(datasets[idx], resampling=resampling) if i != idx else ds)
                for i, ds in enumerate(datasets)
            ]
        return datasets
