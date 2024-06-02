import xarray as xr
import pandas as pd
import rioxarray as rxr
import re

def load_tif(filename, shp, resolution) -> xr.DataArray:
    date_pattern = r'\d{8}'
    da = rxr.open_rasterio(filename)
    if da.rio.crs != shp.crs:
        da = da.rio.reproject(shp.crs, resolution=resolution)
    da = da.rio.clip(shp.geometry)
    time_str = re.findall(date_pattern, filename)[0]
    da = da.assign_coords(time=pd.to_datetime(time_str, format='%Y%m%d'))
    return da
