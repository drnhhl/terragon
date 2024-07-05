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

def stack_cdse_bands(img_folder:Path, shp:gpd.GeoDataFrame, resolution:int) -> xr.Dataset:
    data_arrays = []
    for file in img_folder.glob('*.jp2'):
        time = re.findall(r'\d{8}', str(file))[0]
        band_name = file.stem.split('_')[-2]

        da = rxr.open_rasterio(file)        
        shp = shp.to_crs(da.rio.crs) if shp.crs != da.rio.crs else shp
        da = da.rio.reproject(shp.crs, resolution=resolution)
        da = da.rio.clip(shp.geometry)

        if 'band' in da.dims and da.sizes['band'] == 1:
            da = da.squeeze('band', drop=True)
            da = xr.DataArray(da, dims=['y', 'x'], name=band_name)
            da = da.assign_coords(time=pd.to_datetime(time, format='%Y%m%d'))
            da = da.expand_dims('time')
            data_arrays.append(da)
        
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