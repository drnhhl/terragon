import json
import requests
from urllib.parse import urljoin
from datetime import datetime, timedelta
from pathlib import Path
from joblib import Parallel, delayed
import xarray as xr
import rioxarray as rxr
import rasterio
import boto3
import re
import geopandas as gpd
import pandas as pd
from .utils import meters_to_crs_unit, indices_are_identical
from .base import Base
from shapely.geometry import Polygon, box
import warnings
import itertools
from urllib.parse import urlparse
from rasterio.vrt import WarpedVRT

supported_collections = ['COP-DEM','GLOBAL-MOSAICS','LANDSAT-5','LANDSAT-7','LANDSAT-8-ESA','TERRAAQUA','S2GLC','SENTINEL-1','SENTINEL-1-RTC','SENTINEL-2']

class CDSE(Base):
    """Class to interact with the Copernicus Data Space Ecosystem."""
    f"""currently only {supported_collections} are supported."""
    file_extensions = ['.jp2', '.tif', '.tiff', '.nc', '.dt2', '.dt1', '.img', '.JP2', '.TIF', '.TIFF', '.NC', '.DT2', '.DT1', '.IMG']
    
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
            ds = self._download_to_minicube(items, self.param("shp"), self.param("collection"), self.param("bands", raise_error=True), self.param("resolution"), self.param("resampling"), self.param("filter_asset_path"), self.param("use_virtual_rasterio_file"))
            ds = self.prepare_cube(ds)
            return ds
        else:
            return self._download_to_files(items, self.param("shp"), self.param("collection"), self.param("bands", raise_error=True), self.param("resolution"), self.param("resampling"), self.param("filter_asset_path"), self.param("use_virtual_rasterio_file"))

    def _download_to_files(self, items, shp, collection, bands, resolution, resampling, filter_asset_path, use_virtual_rasterio_file):
        """Download all the items and return the file paths."""
        fns = Parallel(n_jobs=self.param('num_workers'))(delayed(self._download_to_file)(item, shp, collection, band, resolution, resampling, filter_asset_path, use_virtual_rasterio_file) for item, band in itertools.product(items, bands))
        # extract list of lists
        fns = [fn for sublist in fns for fn in sublist]
        return fns

    def _download_to_file(self, item, shp, collection, band, resolution, resampling, filter_asset_path, use_virtual_rasterio_file):
        """Download item to file."""
        f_paths = self._get_asset_path(item, collection, band, resolution, filter_asset_path)
        # replace extension to tif and collapse folders to name (there can be multiple files with the same name)
        fns = []
        for f_path in f_paths:
            fn = self.param('download_folder') / Path('_'.join(f_path.with_suffix(".tif").parts))
            if not fn.exists():
                clipped = self._download_file(f_path, shp, resampling, use_virtual_rasterio_file)
                clipped.rio.to_raster(fn)
            fns.append(fn)
        return fns

    def _download_to_minicube(self, items, shp, collection, bands, resolution, resampling, filter_asset_path, use_virtual_rasterio_file):
        """Download all items and merge them into a dataset."""
        # do not rely on threading here, because it will mess up the xarrays
        datasets = Parallel(n_jobs=self.param('num_workers'))(delayed(self._download_item)(item, shp, collection, band, resolution, resampling, filter_asset_path, use_virtual_rasterio_file) for item, band in itertools.product(items, bands))
        # extract list of lists [[item1band1, item1band2, ...], [item2band1, item2band2, ...], ...]
        datasets = [datasets[i:i + len(bands)] for i in range(0, len(datasets), len(bands))]
        # combine them to dataset with time data [ds1, ds2, ...]
        time_data = Parallel(n_jobs=self.param('num_workers'))(delayed(self._combine_bands)(datasets[i], shp, bands, resolution, resampling) for i in range(len(datasets)))

        # combine the time data
        if all([ds is None for ds in time_data]):
            raise RuntimeError("No items were found.")
        
        # extract time information for each dataset
        if len(time_data) != len(items):
            raise RuntimeError("Lengths of downloaded items and requested items do not match.")
        # skip items which were not found
        time_data, times = map(list, zip(*[(ds, item["properties"]["datetime"]) for ds, item in zip(time_data, items) if ds is not None]))

        time_data = self._align_coords(time_data, shp, resampling)

        # add time coords (would have been removed by reproject_match in _align_coords)
        time_data = [ds.assign_coords(time=("time", pd.to_datetime([time]).tz_convert(None))) for ds, time in zip(time_data, times)]

        # merge time
        data = xr.concat(time_data, dim='time', join="exact").sortby('time')
        return data

    def _download_item(self, item, shp, collection, band, resolution, resampling, filter_asset_path, use_virtual_rasterio_file):
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
                if not 'band' in ds.coords:
                    # add band dim
                    ds = ds.expand_dims('band')
                # rename band each band coord with filename to make sure they are unique + identifyable later
                ds = ds.assign_coords(band=[f"{f_path.stem}_{old}" for old in ds.coords['band'].values])
                datasets.append(ds)
            if len(datasets) > 1:
                warnings.warn(f"Multiple files found for band {band}: {f_paths}.\nAdding them as new bands.")
                if not all([ds.rio.crs == datasets[0].rio.crs for ds in datasets]):
                    datasets = self._align_coords(datasets, shp, resampling)
                if not all([ds.rio.resolution()[0] == datasets[0].rio.resolution()[0] for ds in datasets]):
                    datasets = self._align_resolutions(datasets, shp, resolution, resampling)
                # add them as new bands
                clipped = xr.concat(datasets, dim='band')
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
            warnings.warn(f'Not all bands were downloaded.')
            return None
        if not all([ds is not None for ds in band_data]):
            raise RuntimeError("Bands were not downloaded.")

        # resample if needed
        band_data = self._align_resolutions(band_data, shp, resolution, resampling)

        # pad everything to make sure it has shape of shp
        band_data = [ds.rio.pad_box(*list(shp.total_bounds)).rio.clip_box(*list(shp.total_bounds)) for ds in band_data]

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

    def _align_coords(self, datasets, shp, resampling):
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
            datasets = [ds.rio.reproject_match(datasets[idx], resampling=resampling) if i != idx else ds for i, ds in enumerate(datasets)]
        return datasets

    def _align_resolutions(self, datasets, shp, resolution, resampling):
        """unify the resolutions of the list of the xr.Datasets."""
        ress = [ds.rio.resolution() for ds in datasets]
        resolution = meters_to_crs_unit(resolution, shp)
        # round degrees to 8 decimal places for cm resolution
        ress = [(round(res[0], 8),round(res[0], 8)) for res in ress]
        resolution = [round(res, 8) for res in resolution]
        if len(set(ress)) > 1 or (len(ress) > 0 and ress[0] != resolution):
            # get closest resolution
            idx = sorted(enumerate(ress), key=lambda item: abs(resolution[0] - abs(item[1][0])) + abs(resolution[1] - abs(item[1][1])))[0][0]
        if ress[idx] != resolution:
            datasets[idx] = datasets[idx].rio.reproject(shp.crs, resolution=resolution, resampling=resampling)
        # reproject rest to master
        datasets = [ds.rio.reproject_match(datasets[idx], resampling=resampling) if i != idx else ds for i, ds in enumerate(datasets)]
        return datasets

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
            endpoint_url=urlparse(self.end_point_url).netloc if '://' in self.end_point_url else self.end_point_url,
            aws_access_key_id=self.credentials['aws_access_key_id'],
            aws_secret_access_key=self.credentials['aws_secret_access_key'],
        )
        with rasterio.env.Env(session=session, AWS_VIRTUAL_HOSTING=False):
            clipped = self._clip_to_region("s3://eodata/" + str(f_path), shp, resampling)
            return clipped

    def _download_file_tile(self, f_path, shp, resampling):
        """Download a band of an item and clip it to the shapefile."""
        # download the file locally
        download_path = self.param("download_folder") / f_path
        download_path.parent.mkdir(parents=True, exist_ok=True)
        if not download_path.exists():
            _s3 = boto3.Session(
                aws_access_key_id=self.credentials['aws_access_key_id'],
                aws_secret_access_key=self.credentials['aws_secret_access_key'],
                region_name='default'
            ).resource(
                's3',
                endpoint_url=self.end_point_url
            )

            _s3.Bucket("eodata").download_file(str(f_path), download_path)

        # clip to shp
        clipped = self._clip_to_region(download_path, shp, resampling)

        # optionally remove the downloaded file
        if self.param("rm_tmp_files"):
            download_path.unlink(missing_ok=True)

        return clipped

    def _get_asset_path(self, item, collection, band, resolution, filter_asset_path):
        # set up session to read file structure
        _s3 = boto3.Session(
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
        response = _s3.Bucket("eodata").objects.filter(Prefix=folder_name)

        # filter for extension
        paths = [obj for obj in response if any([obj.key.endswith(x) for x in self.file_extensions])]
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
        if filter_asset_path and collection in filter_asset_path:
            pattern = re.compile(filter_asset_path[collection])
            paths = [path for path in paths if re.search(pattern, str(path))]
            if len(paths) == 0:
                raise RuntimeError("There are no files matching the filter_asset_path: ", filter_asset_path)
            
        paths_new = [path for path in paths if f"{resolution}m" in str(path)] # filter for resolution (currently only applies for S2)
        if len(paths_new) > 0: # if it was found take it, otherwise will be resampled later
            paths = paths_new

        if len(paths) == 0:
            raise RuntimeError("No file found.")
        
        return paths

    def _clip_to_region(self, file, shp, resampling):
        try:
            ds = rxr.open_rasterio(file)
            gcps = ds.rio.get_gcps()
            if gcps and not any(c in ds.coords for c in ['x', 'y']):
                with rasterio.open(file) as src:
                    gcps, crs = src.get_gcps()
                    # use WarpedVRT to get the correct transform/coordinates
                    with WarpedVRT(src, src_crs=crs, resampling=resampling) as vrt:
                        ds = rxr.open_rasterio(vrt)
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
                ds = ds.rio.clip_box(*list(gdf.total_bounds))
                # then reproject to shp crs
                ds = ds.rio.reproject(shp.crs, resampling=resampling)
                ds = ds.rio.clip_box(*list(shp.total_bounds))
            else:
                ds = ds.rio.clip_box(*list(shp.total_bounds))
            ds.load()
            ds.close()
        except rxr.exceptions.NoDataInBounds:
            warnings.warn("No data found in bounds.")
            ds = None

        return ds
