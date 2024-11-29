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
    def __init__(self, credentials:dict=None, base_url:str="https://catalogue.dataspace.copernicus.eu/stac/"):
        super().__init__()
        self.base_url = base_url
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
        self._parameters.update({'rm_tmp_files': rm_tmp_files, 'use_virtual_rasterio_file': use_virtual_rasterio_file, 'resampling':resampling, 'filter_asset_path':filter_asset_path})# TODO move this to the input params

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
        items = self.get_pages(data)

        if len(items) == 0:
            raise ValueError(f"No items found for {self.param('collection')} between {datetime}.")

        # apply filters
        filter = self.param("filter")
        if filter is not None and len(filter) > 0:
            for option in filter:
                for k,v in filter[option].items():
                    if k == 'eq':
        if self.access_token:
            headers = {"Authorization": f"Bearer {self.access_token}"}
            catalog = Client.open(self.base_url, headers=headers)
        else: 
            catalog = Client.open(self.base_url)
        
        bounds_4326 = list(self.get_param('shp', raise_error=True).bounds.values[0])
        start_date = datetime.strptime(self.get_param('start_date'), "%Y-%m-%d") if self.get_param('start_date') else datetime.now()
        end_date = datetime.strptime(self.get_param('end_date'), "%Y-%m-%d") if self.get_param('end_date') else datetime.now() + timedelta(days=10)
        time_interval =f"{start_date.isoformat()}Z/{end_date.isoformat()}Z" if start_date and end_date else None
        
        search = catalog.search(
            collections=[self.get_param('collection', raise_error=True)],
            bbox=bounds_4326,
            datetime=time_interval
        )

        items = search.item_collection()

        # TODO how to filter items? Search is not really good at filtering out duplicates for example -> set more filter parameters in search dict?
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

    def get_pages(self, data):
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
            ds = self._download_to_minicube(items, self.param("resolution"), self.param("bands"), self.param("shp"))
            ds = self.prepare_cube(ds)
            return ds
        else:
            return self._download_to_files(items, self.param("resolution"), self.param("bands"), self.param("shp"))

    def _download_to_files(self, items, resolution, bands, shp):
        """Download all the items and return the file paths."""
        fns = Parallel(n_jobs=self.param('num_workers'))(delayed(self._download_to_file)(item, resolution, band, shp) for item, band in itertools.product(items, bands))
                print(f"Failed to download file. Status code: {response.status_code}")
                print(response.text)
                return None
    
    def download_data_from_stac(self, items):

        shp = self.get_param('shp', raise_error=True)
        bounds = list(shp.bounds.values[0])
        crs = shp.crs
        res = resolve_resolution(shp, self.get_param('resolution', raise_error=True))
        
        # Load data using odc-stac with the authenticated session
        data = odc.stac.load(
            items,
            crs=crs,
            resolution=res,
            x=(bounds[0], bounds[2]),
            y=(bounds[1], bounds[3])
        )
        
        return fns

    def _download_to_file(self, item, resolution, band, shp):
        """Download item to file."""
        f_path, s3 = self._get_asset_path(item, band, resolution)
        # replace extension to tif and collapse folders to name (there can be multiple files with the same name)
        fn = self.param('download_folder') / Path('_'.join(f_path.with_suffix(".tif").parts))
        if not fn.exists():
            clipped = self._download_file(f_path, s3, self.param("shp"))[0]
            clipped.rio.to_raster(fn)
        return fn

    def _download_to_minicube(self, items, resolution, bands, shp):
        """Download all items and merge them into a dataset."""
        # TODO how to parallelize?
        # go through the items
        time_data, crs_list = [], []
        for item in items:
            ds, src_crs = self._download_item(item, resolution, bands, shp)
            crs_list.append(src_crs) # keep track of crs for each item
            time_data.append(ds)

        # skip items which were not found
        if all([ds is None for ds in time_data]):
            raise RuntimeError("No items were found.")
        
        if len(time_data) != len(items):
            raise RuntimeError("Lengths of downloaded items and requested items do not match.")
        times = [item["properties"]["datetime"] for item in items]
        time_data, crs_list, times = zip(*[(ds, crs, time) for ds, crs, time in zip(time_data, crs_list, times) if ds is not None])
        time_data = list(time_data)
        crs_list = list(crs_list)
        times = list(times)
        # make sure the coordinates are the same and match (e.g. if there are other crs)
        if len(set(crs_list)) > 1 or not indices_are_identical(time_data):
            # match all to master (first shp crs)
            idx = [i for i, crs in enumerate(crs_list) if crs == shp.crs]
            if len(idx) == 0:
                warnings.warn(f"No matching crs found and all crs are different. Using first crs {crs_list[0]} as master.")
                idx = 0
            else:
                idx = idx[0]
            time_data = [ds.rio.reproject_match(time_data[idx], resampling=self.param("resampling")) if i != idx else ds for i, ds in enumerate(time_data)]

        # add coords (would have been removed by reproject_match)
        time_data = [ds.assign_coords(time=("time", pd.to_datetime([time]).tz_convert(None))) for ds, time in zip(time_data, times)]
        # time_data = [ds.assign_coords(id=("time", [item['id']])) for ds, item in zip(time_data, items)]

        # merge time
        data = xr.concat(time_data, dim='time', join="exact").sortby('time')
        return data

    def _download_item(self, item, resolution, bands, shp):
        """Download all bands of an item and merge them into a dataset."""
        # go through the bands
        band_data = []
        ress = []

        for band in bands:
            f_path, s3 = self._get_asset_path(item, band, resolution)
            if self.param("use_virtual_rasterio_file"):
                clipped, res, src_crs = self._download_file_rasterio(f_path, shp)
            else:
                clipped, res, src_crs = self._download_file(f_path, s3, shp)
            band_data.append(clipped)
            ress.append(res)
        # check if all bands were found
        if all([ds is None for ds in band_data]):
            warnings.warn(f'Item {item["assets"]["PRODUCT"]["alternate"]["s3"]["href"]} was not found.')
            return None, None
        if not all([ds is not None for ds in band_data]):
            raise RuntimeError("Some bands were not found.")

        # resample if needed
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
                band_data[idx] = band_data[idx].rio.reproject(shp.crs, resolution=resolution, resampling=self.param("resampling"))
            # reproject rest to master
            band_data = [ds.rio.reproject_match(band_data[idx], resampling=self.param("resampling")) if i != idx else ds for i, ds in enumerate(band_data)]
        # pad everything to make sure it has shape of shp
        band_data = [ds.rio.pad_box(*list(shp.total_bounds)).rio.clip(shp.geometry) for ds in band_data]

        # merge bands
        if not len(band_data) > 0:
            return
        da = xr.concat(band_data, dim='band')
        ds = da.to_dataset(dim="band")
        if bands is not None and len(bands) > 0:
            ds = ds.rename({old: new for old, new in zip(range(len(ds.data_vars)), bands)})
        return ds, src_crs

    def _download_file_rasterio(self, f_path, shp):
        """Download a band of an item and clip it to the shapefile."""

        fs = S3FS(
            bucket_name="eodata",
            dir_path=str(f_path.parent),
            aws_access_key_id=self.credentials['aws_access_key_id'],
            aws_secret_access_key=self.credentials['aws_secret_access_key'],
            endpoint_url='https://eodata.dataspace.copernicus.eu'
        )

        with fs.open(str(f_path.name), 'rb') as remote_file:
            # clip without downloading whole file
            clipped, res, src_crs = self._clip_to_region(remote_file, shp)
            return clipped, res, src_crs

    def _download_file(self, f_path, s3, shp):
        """Download a band of an item and clip it to the shapefile."""
        # download the file locally
        download_path = self.param("download_folder") / f_path
        download_path.parent.mkdir(parents=True, exist_ok=True)
        if not download_path.exists():
            s3.Bucket("eodata").download_file(str(f_path), download_path)

        # clip to shp
        clipped, res, src_crs = self._clip_to_region(download_path, shp)

        # optionally remove the downloaded file
        if self.param("rm_tmp_files"):
            download_path.unlink(missing_ok=True)

        return clipped, res, src_crs

    def _get_asset_path(self, item, band, resolution):
        # set up session to read file structure
        s3 = boto3.Session(
            aws_access_key_id=self.credentials['aws_access_key_id'],
            aws_secret_access_key=self.credentials['aws_secret_access_key'],
            region_name='default'
        ).resource(
            's3',
            endpoint_url='https://eodata.dataspace.copernicus.eu'
        )

        # extract item path
        try:
            s3_path = item["assets"]["PRODUCT"]["alternate"]["s3"]["href"]
        except KeyError:
            raise RuntimeError("It seems that no s3 path exists for this item. Returned item: ", item)
        folder_name = '/'.join(s3_path.split('/')[2:]) 
        response = s3.Bucket("eodata").objects.filter(Prefix=folder_name)

        # filter for extension
        file_extensions = ['.jp2', '.tif', '.tiff', '.nc', '.dt2', '.dt1', '.img']
        file_extensions.extend([x.upper() for x in file_extensions])
        paths = [obj for obj in response if any([obj.key.endswith(x) for x in file_extensions])]
        if len(paths) == 0:
            raise RuntimeError("No file with valid extension found.")

        # transform to Path object
        paths = [Path(obj.key) for obj in paths]
    def download(self, items, create_minicube=True, delete_zip=True):

        output_dir = Path(self.get_param('download_folder', raise_error=True))
        output_dir.mkdir(parents=True, exist_ok=True)
                
        tasks = preprocess_download_task(items, output_dir) 
        max_imgs_parallel = 4
        num_workers = self.get_param('num_workers', 1)

        zip_files = []
           
        for i in range(0, len(tasks), max_imgs_parallel):
            batch = tasks[i:i+4] 
            results = Parallel(n_jobs=num_workers)(delayed(self._download_file)(*task) for task in batch)
            for result in results:
                try:
                    if result:
                        zip_files.append(result)
                        print(f"Downloaded item: {result}")
                except Exception as e:
                    print(f"Failed to download file with error: {e}")
        
        output_dir = unzip_files(zip_files, output_dir, delete_zip=delete_zip)

        bands = self.get_param('bands')
        res = self.get_param('resolution')
        shp = self.get_param('shp')
        
        if create_minicube:
            return build_minicube(output_dir, bands, shp, res, num_workers=num_workers)
        else:
            if bands:
                return [img_path for img_path in output_dir.glob(f"**/IMG_DATA/**/*.jp2") if any(band in img_path.stem for band in bands)]

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
        elif len(paths) > 1:
            raise RuntimeError("Multiple files found in folder, only one is expected: ", [path for path in paths])
        
        # take first one
        f_path = paths[0]

        return f_path, s3

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
                        coords = _make_coords(src_data_array=ds, dst_affine=transform, dst_width=ds.sizes['x'], dst_height=ds.sizes['y'], force_generate=True)
                        ds = ds.assign_coords(coords)
                        # ds = ds.rename({'xc': 'x', 'yc': 'y'})
            if ds.rio.crs is None:
                warnings.warn("No crs found, continuing with EPSG:4326.")
                ds = ds.rio.write_crs("EPSG:4326")
            res = ds.rio.resolution()[0]
            src_crs = ds.rio.crs
            if ds.rio.crs != shp.crs:
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
            # remove band dimension
            ds = ds.drop_vars('band').squeeze('band')
        except rxr.exceptions.NoDataInBounds as e:
            warnings.warn("No data found in bounds.")
            ds = res = src_crs = None

        return ds, res, src_crs
