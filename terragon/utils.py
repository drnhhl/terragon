import xarray as xr
import pandas as pd
import rioxarray as rxr
import re
from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.polygon import orient
from pathlib import Path
import geopandas as gpd
import tempfile 
import zipfile
import shutil
from urllib.parse import urljoin
from datetime import datetime
from pystac import ItemCollection
import math 

def meters_to_degrees(meters, latitude):
    """
    Convert meters to geographic degrees at a given latitude based on the Earth's radius.
    """
    # Approximate kilometers per degree of latitude at the equator
    km_per_degree_latitude = 111.32  # More precise value for kilometers per degree at the equator
    km = meters / 1000  # Convert meters to kilometers
    
    # Adjust kilometers per degree based on the cosine of the latitude
    radians_latitude = math.radians(latitude)
    adjusted_km_per_degree = km_per_degree_latitude * math.cos(radians_latitude)
    
    # Calculate the degree change for the given number of kilometers
    degrees = km / adjusted_km_per_degree
    
    return degrees

def degrees_to_meters(degrees, latitude):
    """
    Convert geographic degrees to meters at a given latitude based on the Earth's radius.
    """
    # Approximate kilometers per degree of latitude at the equator
    km_per_degree_latitude = 111.32
    radians_latitude = math.radians(latitude)
    adjusted_km_per_degree = km_per_degree_latitude * math.cos(radians_latitude)

    # Calculate the kilometers for the given number of degrees
    km = degrees * adjusted_km_per_degree
    meters = km * 1000  # Convert kilometers to meters

    return meters

def resolve_resolution(shp, resolution):
    """
    Adjust resolution based on CRS and input unit (degrees or meters).
    """
    crs = shp.crs
    bounds = shp.total_bounds
    central_latitude = (bounds[1] + bounds[3]) / 2

    if crs.is_geographic:
        if isinstance(resolution, (int, float)) and resolution >= 1:  # Assuming resolution is in meters
            return meters_to_degrees(resolution, central_latitude)
        else:
            return resolution  # Already in degrees
    elif crs.is_projected:
        if isinstance(resolution, (int, float)) and resolution < 1:  # Assuming resolution is in degrees
            return degrees_to_meters(resolution, central_latitude)
        else:
            return resolution  # Already in meters

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