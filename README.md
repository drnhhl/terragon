<!-- define style to remove borders in tables -->
<style>
td, th {
   border: none!important;
}
</style>

<div align="center">
<table>
  <tr>
    <td><a href="https://github.com/drnhhl/terragon"><img src="docs/_static/logo.png" style="width: 200px; margin-right: 10px;" ></a></td>
    <td style="vertical-align: top; padding-top:15px; padding-left: 10px;"><a style="margin-left: 20px;" href="https://github.com/drnhhl/terragon">
        <span style="font-size:8em;">Terragon</span>
    </a></td>
  </tr>
</table>
</div>

<p align="center">
    <em>Terragon - Earth(Poly)gon. Create EO Minicubes from Polygons and simplify EO Data downloading.</em>
</p>
<p align="center">
    <a href="https://opensource.org/licenses/MIT" target="_blank">
        <img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License">
    </a>
</p>

You want to download Earth Observation data but don't want to spend hours just for accessing all different APIs? Then this is for you!

We currently support four different backend data sources:
- [Planetary Computer (pc)](https://planetarycomputer.microsoft.com/catalog)
- [Google Earth Engine (gee)](https://developers.google.com/earth-engine/datasets)
- [Alaska Satellite Facility (asf)](https://asf.alaska.edu/how-to/data-basics/datasets-available-from-asf-sar-daac/)
- [Copernicus Data Space Ecosystem (cdse)](https://dataspace.copernicus.eu/explore-data/data-collections)

## Usage
### Installation
Install the package via PyPi:
´´´python
pip install terragon
´´´
### Downloading EO data
```python
import terragon
import geopandas as gpd
from shapely.geometry import Polygon

# example polygon
gdf = gpd.GeoDataFrame(geometry=[Polygon(
            [(446993, 3383569),
            (446993, 3371569),
            (434993, 3371569),
            (434993, 3383569),
            (446993, 3383569)])],
            crs='EPSG:32616' # the dataset will receive the crs from the dataframe, otherwise it will get EPSG:4326
            )

# initialize backend/data source (here planetary computer)
tg = terragon.init('pc')

# download data
da = tg.create(shp=gdf, # polygon in geopandas format (minicube will receive the same CRS)
                    collection="sentinel-2-l2a", # name of the collection
                    start_date="2021-01-01", # start date of tiles
                    end_date="2021-01-05", # end date of tiles
                    bands=["B02", "B03", "B04"], # bands to retrieve
                    resolution=20, # pixel size in m
                    )
```
Other data backends work with the same principle, check out the [Demos](demo_files).

## Contribute
You found a bug or a data source is missing? Please raise an issue or provide a PR.

## License
This work is licensed under the MIT license.

## Citation
If you use this work, please consider citing the following paper:

## Acknowledgement
This work is inspired by [cubo](https://github.com/ESDS-Leipzig/cubo)