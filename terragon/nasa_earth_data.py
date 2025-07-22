import hashlib
import json
import math
import warnings
from pathlib import Path
from typing import List, Union

import earthaccess
import xarray as xr
from earthaccess import DataGranule

from .base import Base


class ED(Base):
    def __init__(self, credentials: dict = None) -> None:
        super().__init__()
        # TODO test if it is initialized

    def retrieve_collections(self, filter_by_name: str = None) -> None:
        out = earthaccess.search_datasets(keyword=filter_by_name)
        out = [
            c["meta"]["concept-id"] for c in out
        ]  # TODO this only returns some id which does not really tell much -> add description?
        return out

    def search(self, *args, **kwargs) -> List[DataGranule]:
        super().search(*args, **kwargs)
        self._parameters.update({})  # TODO add parameters to search

        bounds_4326 = self._reproject_shp(self._param("shp")).total_bounds
        start_date = self._param("start_date")
        end_date = self._param("end_date")
        items = earthaccess.search_data(
            concept_id=self._param(
                "collection"
            ),  # find the collection id here: https://www.earthdata.nasa.gov/data/catalog
            temporal=(start_date, end_date),
            bounding_box=tuple(bounds_4326),
            # query=self._param("filter"),# TODO how to add filters? (doi, provider, version, cloud coverage, ...): https://search.earthdata.nasa.gov/
        )

        return items

    def download(self, items: list) -> Union[xr.Dataset, List]:
        if len(items) < 1:
            raise ValueError("No items to download")

        if not self._param("create_minicube"):
            self._param("download_folder").mkdir(parents=True, exist_ok=True)
            files = earthaccess.download(
                items,
                local_path=self._param("download_folder"),
                threads=self._param("num_workers"),
            )
            files = [Path(file) for file in files]
            return files

        # TODO download items
        # files = earthaccess.open(items)
        # ds = xr.open_mfdataset(items)
