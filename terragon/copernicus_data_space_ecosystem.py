import json
import requests
from urllib.parse import urljoin
from datetime import datetime, timedelta
from pathlib import Path
from joblib import Parallel, delayed
import xarray as xr
import rioxarray as rxr
import rasterio
from fs_s3fs import S3FS
import boto3
import re
import geopandas as gpd
import pandas as pd
from .utils import meters_to_crs_unit, indices_are_identical
from .base import Base
from shapely.geometry import Polygon, box
import warnings
from rioxarray.rioxarray import _make_coords
import itertools

supported_collections = ['COP-DEM','GLOBAL-MOSAICS','LANDSAT-5','LANDSAT-7','LANDSAT-8-ESA','TERRAAQUA','S2GLC','SENTINEL-1','SENTINEL-1-RTC','SENTINEL-2']

class CDSE(Base):
    """Class to interact with the Copernicus Data Space Ecosystem."""
    f"""currently only {supported_collections} are supported."""
    s3 = None # object storing the s3 session

    def __init__(self, credentials:dict=None, base_url:str="https://catalogue.dataspace.copernicus.eu/stac/", end_point_url:str="https://eodata.dataspace.copernicus.eu"):
        super().__init__()
        self.base_url = base_url
        self.end_point_url = end_point_url
        if credentials:
            if not "aws_access_key_id" in credentials or not "aws_secret_access_key" in credentials:
                raise ValueError("aws_access_key_id or aws_secret_access_key not in credentials, could not initialize.")
        self.credentials = credentials

    def retrieve_collections(self, filter_by_name: str=None):
        collections_url = urljoin(self.base_url, "collections")
        response = requests.get(collections_url)

        if response.status_code == 200:
            data = response.json()
            collections = [collection['id'] for collection in data['collections']]
            if filter_by_name:
                collections = [collection for collection in collections if filter_by_name in collection.lower()]
            warnings.warn(f"Currently we only support the following collections: {supported_collections}")
            return collections
        else:
            raise RuntimeError("Failed to retrieve collections")

    def search(self, rm_tmp_files=True, use_virtual_rasterio_file=True, resampling=rasterio.enums.Resampling.nearest, filter_asset_path={'COP-DEM': ".*/DEM/.*", 'SENTINEL-2': '.*/IMG_DATA/.*'}, **kwargs):
        super().search(**kwargs)
        self._parameters.update({'rm_tmp_files': rm_tmp_files, 'use_virtual_rasterio_file': use_virtual_rasterio_file, 'resampling':resampling, 'filter_asset_path':filter_asset_path})

        if self.param("collection") not in supported_collections:
            warnings.warn(f"Currently we only support collections: {supported_collections}")
        if self.param("num_workers") > 4:
            warnings.warn("More than 4 workers are not recommended, because only 4 concurrent connections are allowed: https://documentation.dataspace.copernicus.eu/Quotas.html.")
        
        shp_4326 = self._reproject_shp(self.param("shp"))
        bbox = shp_4326.total_bounds
        start_date = self.param("start_date")
        end_date = self.param("end_date")
        # make end_date inclusive
        start_date = f"{start_date}T00:00:00.000" if start_date and not "T" in start_date else start_date
        end_date = f"{end_date}T23:59:59.999" if end_date and not "T" in end_date else end_date
        datetime=f"{start_date}/{end_date}" if start_date and end_date else None

        data = {
            "bbox": bbox.tolist(),
            "datetime": datetime,
            "collections": [self.param("collection")],
            "limit": 1000,
        }
        items = self._get_pages(data)

        if len(items) == 0:
            raise ValueError(f"No items found for {self.param('collection')} between {datetime}.")

        # apply filters
        filter = self.param("filter")
        if filter is not None and len(filter) > 0:
            for option in filter:
                for k,v in filter[option].items():
                    if k == 'eq':
                        items = [item for item in items if item['properties'][option] == v]
                    elif k == 'ueq':
                        items = [item for item in items if item['properties'][option] != v]
                    elif k == 'in':
                        # value should be list
                        if not isinstance(v, list):
                            raise ValueError(f"Filter option {k} needs a list.")
                        items = [item for item in items if item['properties'][option] in v]
                    elif k == 'lt':
                        items = [item for item in items if float(item['properties'][option]) < v]
                    elif k == 'gt':
                        items = [item for item in items if float(item['properties'][option]) > v]
                    else:
                        raise ValueError(f"Filter option {k} not supported.")

        if len(items) == 0:
            raise ValueError(f"No items found for {self.param('collection')} between {datetime} after filtering.")

        return items

    def _get_pages(self, data):
        """loop through the api pages and return all items."""
        items = []
        for i in range(1,100):
            _data = data.copy()
            _data["page"] = i
            page = requests.post(urljoin(self.base_url, "search"), json=_data).json()

            if not "features" in page:
                raise ValueError(f"There was an error with the request: {page}")
            if len(page["features"]) == 0:
                break            
            else:
                items.extend(page["features"])

            if i == 100:
                raise ValueError("Max number of pages reached. Consider using a smaller time frame.")

        return items

    def download(self, items, create_minicube=True):
        if create_minicube:
            ds = self._download_to_minicube(items, self.param("resolution"), self.param("bands", raise_error=True), self.param("shp"))
            ds = self.prepare_cube(ds)
            return ds
        else:
            return self._download_to_files(items, self.param("resolution"), self.param("bands", raise_error=True), self.param("shp"))

    def _download_to_files(self, items, resolution, bands, shp):
        """Download all the items and return the file paths."""
        fns = Parallel(n_jobs=self.param('num_workers'))(delayed(self._download_to_file)(item, resolution, band, shp) for item, band in itertools.product(items, bands))
        # extract list of lists
        fns = [fn for sublist in fns for fn in sublist]
        return fns

    def _download_to_file(self, item, resolution, band, shp):
        """Download item to file."""
        f_paths = self._get_asset_path(item, band, resolution)
        # replace extension to tif and collapse folders to name (there can be multiple files with the same name)
        fns = []
        for f_path in f_paths:
            fn = self.param('download_folder') / Path('_'.join(f_path.with_suffix(".tif").parts))
            if not fn.exists():
                clipped = self._download_file(f_path, self.param("shp"))
                clipped.rio.to_raster(fn)
            fns.append(fn)
        return fns

    def _download_to_minicube(self, items, resolution, bands, shp):
        """Download all items and merge them into a dataset."""
        datasets = Parallel(n_jobs=self.param('num_workers'))(delayed(self._download_item)(item, resolution, band, shp) for item, band in itertools.product(items, bands))
        # extract list of lists [[item1band1, item1band2, ...], [item2band1, item2band2, ...], ...]
        datasets = [datasets[i:i + len(bands)] for i in range(0, len(datasets), len(bands))]
        # combine them to dataset with time data [ds1, ds2, ...]
        time_data = Parallel(n_jobs=self.param('num_workers'))(delayed(self._combine_bands)(datasets[i], resolution, shp, bands) for i in range(len(datasets)))

        # combine the time data
        if all([ds is None for ds in time_data]):
            raise RuntimeError("No items were found.")
        
        # extract time information for each dataset
        if len(time_data) != len(items):
            raise RuntimeError("Lengths of downloaded items and requested items do not match.")
        # skip items which were not found
        time_data, times = map(list, zip(*[(ds, item["properties"]["datetime"]) for ds, item in zip(time_data, items) if ds is not None]))

        time_data = self._align_coords(time_data, shp)

        # add time coords (would have been removed by reproject_match in _align_coords)
        time_data = [ds.assign_coords(time=("time", pd.to_datetime([time]).tz_convert(None))) for ds, time in zip(time_data, times)]

        # merge time
        data = xr.concat(time_data, dim='time', join="exact").sortby('time')
        return data

    def _download_item(self, item, resolution, band, shp):
        """Download all bands of an item and merge them into a dataset."""
        # go through the bands
        f_paths = self._get_asset_path(item, band, resolution)
        if len(f_paths) > 1:
            # if there are multiple assets, merge them
            datasets = []
            for f_path in f_paths:
                ds = self._download_file(f_path, shp)
                if ds is None:
                    continue
                if not 'band' in ds.coords:
                    # add band dim
                    ds = ds.expand_dims('band')
                # rename band each band coord with filename to make sure they are unique + identifyable later
                ds = ds.assign_coords(band=[f"{f_path.stem}_{old}" for old in ds.coords['band'].values])
                datasets.append(ds)
            if len(datasets) > 1:
                warnings.warn(f"Multiple files found for band {band}: {f_paths}.\nWill continue to add them as new bands.")
                if not all([ds.rio.crs == datasets[0].rio.crs for ds in datasets]):
                    datasets = self._align_coords(datasets, shp)
                if not all([ds.rio.resolution()[0] == datasets[0].rio.resolution()[0] for ds in datasets]):
                    datasets = self._align_resolutions(datasets, resolution, shp)
                # add them as new bands
                clipped = xr.concat(datasets, dim='band')
            elif len(datasets) == 0:
                clipped = None
            else:
                clipped = datasets[0]
        else:
            # single asset
            clipped = self._download_file(f_paths[0], shp)

        return clipped       

    def _combine_bands(self, band_data, resolution, shp, bands):
        # check if all bands were found
        if len(band_data) != len(bands):
            raise RuntimeError("Length of band_data and bands do not match.")
        if all([ds is None for ds in band_data]):
            warnings.warn(f'Not all bands were downloaded.')
            return None
        if not all([ds is not None for ds in band_data]):
            raise RuntimeError("Bands were not downloaded.")

        # resample if needed
        band_data = self._align_resolutions(band_data, resolution, shp)

        # pad everything to make sure it has shape of shp
        band_data = [ds.rio.pad_box(*list(shp.total_bounds)).rio.clip(shp.geometry) for ds in band_data]

        # merge bands
        if not len(band_data) > 0:
            return
        # remove band dimension
        for i, ds in enumerate(band_data):
            if 'band' in ds.dims:
                if len(ds.band) > 1:
                    band_data[i] = band_data[i].to_dataset(dim="band")
                    band_data[i] = band_data[i].rename({old: f"{bands[i]}_{old}" for old in band_data[i].data_vars})
                else:
                    band_data[i] = band_data[i].drop_vars('band').squeeze('band')
                    band_data[i] = band_data[i].to_dataset(name=bands[i])
        ds = xr.combine_by_coords(band_data)

        return ds

    def _align_coords(self, datasets, shp):
        """unify the crs and indices of the datasets."""
        # make sure the coordinates are the same and match (e.g. if there are other crs)
        if not all([ds.rio.crs == datasets[0].rio.crs for ds in datasets]) or not indices_are_identical(datasets):
            crs_list = [ds.rio.crs for ds in datasets]
            # match all to master (first shp crs)
            idx = [i for i, crs in enumerate(crs_list) if crs == shp.crs]
            if len(idx) == 0:
                warnings.warn(f"No matching crs found and all crs are different. Using first crs {crs_list[0]} as master.")
                idx = 0
            else:
                idx = idx[0]
            datasets = [ds.rio.reproject_match(datasets[idx], resampling=self.param("resampling")) if i != idx else ds for i, ds in enumerate(datasets)]
        return datasets

    def _align_resolutions(self, datasets, resolution, shp):
        """unify the resolutions of the list of the xr.Datasets."""
        ress = [ds.rio.resolution()[0] for ds in datasets]
        resolution = meters_to_crs_unit(resolution, shp)
        # round the resolution to x decimal places
        resolution = round(resolution, 5)
        ress = [round(res, 5) for res in ress]
        if len(set(ress)) > 1 or (len(ress) > 0 and ress[0] != resolution): # TODO here it needs to be rounded to some extent, otherwise it will reproject even with the same resolution
            idx = [i for i, res in enumerate(ress) if res == resolution]
            if len(idx) == 0:
                warnings.warn(f"No matching resolution found in the bands. Using first band as master with resolution {ress[0]}.")
                idx = 0
            else:
                idx = idx[0]
            if ress[idx] != resolution:
                datasets[idx] = datasets[idx].rio.reproject(shp.crs, resolution=resolution, resampling=self.param("resampling"))
            # reproject rest to master
            datasets = [ds.rio.reproject_match(datasets[idx], resampling=self.param("resampling")) if i != idx else ds for i, ds in enumerate(datasets)]
        return datasets

    def _download_file(self, f_path, shp):
        """wrapper to decide whether to use rasterio or patch download."""
        if self.param("use_virtual_rasterio_file"):
            return self._download_file_rasterio(f_path, shp)
        else:
            return self._download_file_tile(f_path, shp)

    def _download_file_rasterio(self, f_path, shp):
        """Download a band of an item and clip it to the shapefile."""

        fs = S3FS(
            bucket_name="eodata",
            dir_path=str(f_path.parent),
            aws_access_key_id=self.credentials['aws_access_key_id'],
            aws_secret_access_key=self.credentials['aws_secret_access_key'],
            endpoint_url=self.end_point_url
        )

        with fs.open(str(f_path.name), 'rb') as remote_file:
            # clip without downloading whole file
            clipped = self._clip_to_region(remote_file, shp)
            return clipped

    def _download_file_tile(self, f_path, shp):
        """Download a band of an item and clip it to the shapefile."""
        # download the file locally
        download_path = self.param("download_folder") / f_path
        download_path.parent.mkdir(parents=True, exist_ok=True)
        if not download_path.exists():
            self.s3.Bucket("eodata").download_file(str(f_path), download_path)

        # clip to shp
        clipped = self._clip_to_region(download_path, shp)

        # optionally remove the downloaded file
        if self.param("rm_tmp_files"):
            download_path.unlink(missing_ok=True)

        return clipped

    def _get_asset_path(self, item, band, resolution):
        # set up session to read file structure
        if not self.s3:
            self.s3 = boto3.Session(
                aws_access_key_id=self.credentials['aws_access_key_id'],
                aws_secret_access_key=self.credentials['aws_secret_access_key'],
                region_name='default'
            ).resource(
                's3',
                endpoint_url=self.end_point_url
            )

        # extract item path
        try:
            s3_path = item["assets"]["PRODUCT"]["alternate"]["s3"]["href"]
        except KeyError:
            raise RuntimeError("It seems that no s3 path exists for this item. Returned item: ", item)
        folder_name = '/'.join(s3_path.split('/')[2:]) 
        response = self.s3.Bucket("eodata").objects.filter(Prefix=folder_name)

        # filter for extension
        file_extensions = ['.jp2', '.tif', '.tiff', '.nc', '.dt2', '.dt1', '.img']
        file_extensions.extend([x.upper() for x in file_extensions])
        paths = [obj for obj in response if any([obj.key.endswith(x) for x in file_extensions])]
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
                paths = [path for path in paths if band.lower() in path.name or band.upper() in path.name]
            if len(paths) == 0:
                raise RuntimeError(f"Band {band} not found, is it written correctly?")
               
        # apply regex path filters
        col = self.param('collection')
        filter = self.param("filter_asset_path")
        if col in filter:
            pattern = re.compile(filter[col])
            paths = [path for path in paths if re.search(pattern, str(path))]
            if len(paths) == 0:
                raise RuntimeError("There are no files matching the filter_asset_path: ", filter)
            
        paths_new = [path for path in paths if f"{resolution}m" in str(path)] # filter for resolution (currently only applies for S2)
        if len(paths_new) > 0: # if it was found take it, otherwise will be resampled later
            paths = paths_new

        if len(paths) == 0:
            raise RuntimeError("No file found.")
        
        return paths

    def _clip_to_region(self, file, shp):
        try:
            ds = rxr.open_rasterio(file,masked=True)
            # try to get it with rasterio
            with rasterio.open(file) as src:
                # if rasterio does not find the crs in meta data, assume it is gcp
                # hence transform/coords need to be set explicitly
                if src.crs is None:
                    gcps, src_crs = src.gcps
                    if gcps:
                        # transform from the gcps
                        transform = rasterio.transform.from_gcps(gcps)
                        ds = ds.rio.write_crs(src_crs)
                        ds = ds.rio.write_transform(transform)
                        ds = ds.rio.reproject(ds.rio.crs) # TODO how to do that differently?
                        # coords = _make_coords(src_data_array=ds, dst_affine=transform, dst_width=ds.sizes['x'], dst_height=ds.sizes['y'], force_generate=True)
                        # ds = ds.assign_coords(coords)
                        # if 'xc' in ds.coords: # TODO renaming does not work whysoever
                        #     ds = ds.rename({'xc': 'x', 'yc': 'y'})
            if ds.rio.crs is None:
                warnings.warn("No crs found, continuing with EPSG:4326.")
                ds = ds.rio.write_crs("EPSG:4326")
            res = ds.rio.resolution()[0]
            src_crs = ds.rio.crs
            if src_crs != shp.crs:
                # reproject shp and clip with margin
                shp_crs = shp.to_crs(src_crs)
                # create bbox with 10% margin
                margin = ((shp_crs.bounds.maxx.item()-shp_crs.bounds.minx.item())*.1, (shp_crs.bounds.maxy.item()-shp_crs.bounds.miny.item())*.1)
                shapely_box = box(shp_crs.bounds.minx.item()-margin[0], shp_crs.bounds.miny.item()-margin[1],
                                shp_crs.bounds.maxx.item()+margin[0], shp_crs.bounds.maxy.item()+margin[1])
                gdf = gpd.GeoDataFrame(geometry=[shapely_box], crs=src_crs)
                # ds = rxr.open_rasterio(file,masked=True)
                ds = ds.rio.clip(gdf.geometry, from_disk=True)
                # then reproject to shp crs
                crs_res = meters_to_crs_unit(res, shp)
                ds = ds.rio.reproject(shp.crs, resolution=crs_res, resampling=self.param("resampling"))
                ds = ds.rio.clip(shp.geometry)
            else:
                ds = ds.rio.clip(shp.geometry, from_disk=True)
        except rxr.exceptions.NoDataInBounds as e:
            warnings.warn("No data found in bounds.")
            ds = None

        return ds
