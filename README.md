# Terragon
<p align="center">
    <a href="https://github.com/drnhhl/terragon"><img src="https://github.com/drnhhl/terragon/raw/main/docs/_static/logo.png" style="width: 200px" ></a>
</p>
<p align="center">
    <em>Terragon - Earth(Poly)gon. Create EO Minicubes from Polygons and simplify EO Data downloading.</em>
</p>
<p align="center">
    <a href='https://terragon.readthedocs.io/en/latest/?badge=latest'>
        <img src='https://img.shields.io/badge/Readthedocs-%23000000.svg?style=for-the-badge&logo=readthedocs&logoColor=white' alt='Documentation' />
    </a>
    <a href="https://github.com/drnhhl/terragon" target="_blank">
        <img src="https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white" alt="GitHub">
    </a>
</p>
<p align="center">
    <a href='https://pypi.python.org/pypi/terragon-downloader'>
        <img src='https://img.shields.io/pypi/v/terragon-downloader.svg' alt='PyPI' />
    </a>
    <a href="https://opensource.org/licenses/MIT" target="_blank">
        <img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License">
    </a>
</p>

You want to download Earth Observation data but don't want to spend hours just for accessing all different APIs? Then this is for you!

We currently support these data providers:
- [Planetary Computer (pc)](https://planetarycomputer.microsoft.com/catalog)
- [Google Earth Engine (gee)](https://developers.google.com/earth-engine/datasets)

Coming soon:
- [Alaska Satellite Facility (asf)](https://asf.alaska.edu/how-to/data-basics/datasets-available-from-asf-sar-daac/)
- [Copernicus Data Space Ecosystem (cdse)](https://dataspace.copernicus.eu/explore-data/data-collections)

## Usage
### Installation
Install the package via PyPi:
```python
pip install terragon-downloader
```
Optional with the dependency you want to use:
```python
pip install terragon-downloader[pc]
```
### Downloading EO data

## Contribute
A data source is missing? Or you found a bug? Please raise an issue or provide a PR.
## License
This work is licensed under the MIT license.
## Citation

## Acknowledgement
This work is inspired by cubo: https://github.com/ESDS-Leipzig/cubo