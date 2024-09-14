import math
import re
import shutil
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import geopandas as gpd
import pandas as pd
import pyproj
import rioxarray as rxr
import xarray as xr
from joblib import Parallel, delayed
from pystac import ItemCollection
from shapely.geometry import MultiPolygon, Point, Polygon
from shapely.geometry.polygon import orient


def extract_band_name(file_path):
    """ Extracts the band name from a Sentinel file path using a regex. """
    pattern = r'(B\d{2}|TCI|WFP|SCL|AOT)'
    match = re.search(pattern, str(file_path.name))
    return match.group(0) if match else None

def remove_duplicate_bands(file_paths):
    band_files = {}
    for file_path in file_paths:
        band_name = extract_band_name(file_path)
        if band_name:
            # Prioritize by choosing the first encountered or implement other logic
            if band_name not in band_files:
                band_files[band_name] = file_path
    
    return list(band_files.values())

def read_and_process_bands(img_folder, bands, gdf, target_resolution):

    if bands:
        band_files = [img_path for img_path in img_folder.glob(f"**/IMG_DATA/**/*.jp2") if any(band in img_path.stem for band in bands)]
    else:
        band_files = list(img_folder.glob(f"**/IMG_DATA/**/*.jp2"))

    band_files = remove_duplicate_bands(band_files)

    band_arrays = []
    for band_file in band_files:
        
        da = rxr.open_rasterio(band_file)
        
        if da.rio.crs != gdf.crs or da.rio.resolution()[0] != target_resolution:
            da = da.rio.reproject(gdf.crs, resolution=target_resolution)

        da = da.rio.clip(gdf.geometry, gdf.crs)
        
        if 'band' in da.dims:
            da = da.squeeze('band').drop_vars('band', errors='ignore')
        
        # Add time coordinate
        time = pd.to_datetime(re.search(r'\d{8}', str(band_file)).group(), format='%Y%m%d')
        da.name = extract_band_name(band_file)
        da = da.expand_dims('time').assign_coords(time=('time', [time]))
        band_arrays.append(da)
    
    band_arrays = sorted(band_arrays, key=lambda da: da.name)  # Example: sort by band name
    return xr.merge(band_arrays)

def build_minicube(output_dir, bands, gdf, resolution, num_workers=4):

    resolution = resolve_resolution(gdf, resolution)

    imgs_datasets = Parallel(n_jobs=num_workers)(delayed(read_and_process_bands)(img_dir, bands, gdf, resolution)for img_dir in output_dir.iterdir())
    # Merge all datasets into a single xarray Dataset
    if imgs_datasets:
        ds = xr.concat(imgs_datasets, dim='time')
        ds = ds.sortby('time')  
    else:
        ds = xr.Dataset()
    
    return ds

def meters_to_degrees(meters, latitude):
    # Radius of the Earth at the equator in kilometers
    earth_radius_km = 6378.137
    # Convert latitude from degrees to radians
    rad = math.radians(latitude)
    # Calculate the number of kilometers per degree at this latitude
    km_per_degree = math.cos(rad) * math.pi * earth_radius_km / 180
    # Convert meters to kilometers and then to degrees
    return meters / 1000 / km_per_degree

def degrees_to_meters(degrees, latitude):
    # Radius of the Earth at the equator in kilometers
    earth_radius_km = 6378.137
    # Convert latitude from degrees to radians
    rad = math.radians(latitude)
    # Calculate the number of kilometers per degree at this latitude
    km_per_degree = math.cos(rad) * math.pi * earth_radius_km / 180
    # Convert degrees to kilometers and then to meters
    return degrees * km_per_degree * 1000

def resolve_resolution(shp, resolution):
    crs = shp.crs
    bounds = shp.total_bounds
    central_latitude = (bounds[1] + bounds[3]) / 2

    if crs.is_geographic:
        print(f"Assuming the provided resolution of '{resolution}' is in meters since the CRS is geographic. Automatically converting this resolution to degrees.")
        return meters_to_degrees(resolution, central_latitude)

def filter_unique_items(items, tile_id, product_type, max_cloud_cover=50):
    def extract(item, key, default=None):
        # Extracts values from properties or defaults if not found
        return item.properties.get(key, default)

    seen = {}
    for item in items:
        if extract(item, 's2:mgrs_tile') != tile_id:
            continue  # Skip items that don't match the specified tileId

        date = datetime.strptime(item.id.split('_')[2][:8], '%Y%m%d').date()
        level, cloud_cover = extract(item, 's2:product_type'), extract(item, 'eo:cloud_cover', 100)
        key = (date, tile_id)

        if key not in seen or \
           (level == product_type and extract(seen[key], 's2:product_type') != product_type) or \
           (level == extract(seen[key], 's2:product_type') and cloud_cover < extract(seen[key], 'eo:cloud_cover', 100)):

            if cloud_cover <= max_cloud_cover:
                seen[key] = item

    return ItemCollection(seen.values())

def preprocess_download_task(items, output_dir):
    zip_url = "https://zipper.dataspace.copernicus.eu/odata/v1/"
    tasks = []
    for item in items:
        id = item.id.split(".")[0]
        output_file = output_dir / f"{id}.zip"
        url_parts = item.assets["PRODUCT"].href.split("/")
        product_url = f"{'/'.join(url_parts[-2:])}"
        url = urljoin(zip_url, product_url)
        tasks.append((url, output_file))
    return tasks

def stack_cdse_bands(img_folder: Path, shp: gpd.GeoDataFrame, target_res: int) -> xr.Dataset:
    """Stacks bands from an image folder, reprojects if necessary, and clips to the provided shapefile."""
    data_arrays = []

    for file in img_folder.glob('*.jp2'):
        # Extract date and band name
        time = re.findall(r'\d{8}', str(file))[0]
        band_name = file.stem.split('_')[-2]

        # Open the raster and reproject if resolution is different from target
        da = rxr.open_rasterio(file)
        if da.rio.resolution()[0] != target_res:
            da = da.rio.reproject(da.rio.crs, resolution=target_res)

        # Align shp to raster CRS and clip raster to the shp geometry
        shp = shp.to_crs(da.rio.crs) if shp.crs != da.rio.crs else shp
        da = da.rio.clip(shp.geometry)

        # Remove 'band' dimension if it's unnecessary
        if 'band' in da.dims and da.sizes['band'] == 1:
            da = da.squeeze('band', drop=True)
            da = xr.DataArray(da, dims=['y', 'x'], name=band_name)
            da = da.assign_coords(time=pd.to_datetime(time, format='%Y%m%d'))
            da = da.expand_dims('time')
            data_arrays.append(da)

    # Sort bands by name or other criteria before merging
    data_arrays = sorted(data_arrays, key=lambda da: da.name)  # Example: sort by band name
    return xr.merge(data_arrays)

def stack_asf_bands(img_folder:Path, shp:gpd.GeoDataFrame, resolution:int) -> xr.Dataset:
    data_arrays = []
    for file in img_folder.glob('*.tiff'):
        time = re.findall(r'\d{8}', str(file))[0]
        band_name = file.stem.split('-')[3]
        
        da = rxr.open_rasterio(file)
        da = da.rio.reproject(shp.crs, resolution=resolution)
        da = da.rio.clip(shp.geometry)

        if 'band' in da.dims and da.sizes['band'] == 1:
            da = da.squeeze('band', drop=True)

        da = xr.DataArray(da, dims=['y', 'x'], name=band_name)
        da = da.assign_coords(time=pd.to_datetime(time, format='%Y%m%d'))
        da = da.expand_dims('time')
        data_arrays.append(da)
        
    return xr.merge(data_arrays)

def fix_winding_order(geometry):
    if isinstance(geometry, Polygon) or isinstance(geometry, MultiPolygon):
        return orient(geometry, sign=1.0)
    return geometry

def unzip_files(zip_files, output_dir:Path, delete_zip=True):
    # Create a temporary directory for extraction
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        print(f"Unzipping to temporary directory: {temp_path}")
        # Unzip all zipped folders in the temp directory and keep the same name
        for zip_file in zip_files:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(temp_path)
            # Check each item in the temp directory
            for item in temp_path.iterdir():
                output_file = str(output_dir / item.name)
                shutil.move(str(item), output_file)
            print(f"Extraction to output directory complete: {output_file}")
            if delete_zip:
                zip_file.unlink()
    return output_dir

def bbox_to_geojson_polygon(bbox):
    min_lon, min_lat, max_lon, max_lat = map(float, bbox)
    return {
        "type": "Polygon",
        "coordinates": [[
            [min_lon, min_lat],
            [min_lon, max_lat],
            [max_lon, max_lat],
            [max_lon, min_lat],
            [min_lon, min_lat]
        ]]
    }

def rm_files(fns):
    for fn in fns:
        if fn.exists():
            try:
                fn.unlink()
            except Exception as e:
                print(f"Failed to remove file in download folder {fn}: {e}")

def shp_to_utm_crs(shp):
    """convert the shape from WGS84 to UTM crs."""
    if shp.crs.to_epsg() != 4326:
      shp = shp.to_crs(epsg=4326)
    utm_crs_list = pyproj.database.query_utm_crs_info(
        datum_name="WGS 84",
        area_of_interest=pyproj.aoi.AreaOfInterest(*shp.geometry.bounds.values[0]),
    )

    # Save the CRS
    epsg = utm_crs_list[0].code
    utm_crs = pyproj.CRS.from_epsg(epsg)
    shp = shp.to_crs(utm_crs)
    return shp

def meters_to_crs_unit(meters, shp):
    """Convert meters to the shape's CRS units."""
    # Convert the shape to UTM CRS where distances are in meters
    shp_utm = shp_to_utm_crs(shp)
    # reference point    
    point = shp_utm.geometry[0].centroid
    # offset point
    offset_point = Point(point.x, point.y + meters)
    
    # Convert the points to the CRS of the shape
    transformer = pyproj.Transformer.from_crs(shp_utm.crs, shp.crs, always_xy=True)
    orig_point = transformer.transform(point.x, point.y)
    offset_point_in_orig_crs = transformer.transform(offset_point.x, offset_point.y)
    
    # distance in the shape's CRS units
    distance_units = Point(orig_point).distance(Point(offset_point_in_orig_crs))

    return distance_units