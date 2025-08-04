import os
from pathlib import Path
from typing import List, Union

import earthaccess
import xarray as xr
from earthaccess import DataGranule

from .base import Base


class ED(Base):
    def __init__(self, credentials: dict = None) -> None:
        super().__init__()
        username = os.getenv("EARTHDATA_USERNAME")
        password = os.getenv("EARTHDATA_PASSWORD")
        if not username or not password:
            raise ValueError(
                "EARTHDATA_USERNAME and EARTHDATA_PASSWORD must be set in environment variables."
            )
        auth = earthaccess.login(strategy="environment", persist=False)
        if auth.authenticated:
            print(f"Successfully logged in to Earthdata as '{auth.username}'.")
        else:
            print("Earthdata login failed. Please check your credentials.")

    def retrieve_collections(self, filter_by_name: str = None) -> None:
        collections = earthaccess.search_datasets(keyword=filter_by_name)
        results = []
        for coll in collections:
            try:
                results.append(coll.summary())
            except KeyError:
                # Skip collections that don't have the required structure
                continue
        return results

    def search(self, ed_kwargs={}, *args, **kwargs) -> List[DataGranule]:
        super().search(*args, **kwargs)
        self._parameters.update({"ed_kwargs": ed_kwargs})

        bounds_4326 = self._reproject_shp(self._param("shp")).total_bounds
        start_date = self._param("start_date")
        end_date = self._param("end_date")
        filter_args = self._param("filter") or {}

        # Remove potential conflicts from filter_args
        for key in ["concept_id", "temporal", "bounding_box"]:
            filter_args.pop(key, None)

        items = earthaccess.search_data(
            concept_id=self._param("collection"),
            temporal=(start_date, end_date),
            bounding_box=tuple(bounds_4326),
            **filter_args,
        )

        return items

    def download(self, items: list) -> Union[xr.Dataset, List]:
        if len(items) < 1:
            raise ValueError("No items to download")

        if self._param("create_minicube"):
            ds = earthaccess.open_virtual_mfdataset(
            granules=items,
            access="indirect",
            load=True,
            concat_dim="time",
            coords="minimal",
            compat="override",
            **self._param("ed_kwargs", default={}),
            )
            # ds = self._prepare_cube(ds)
            return ds
        else:
            bands = self._param("bands")
            if bands:
                items = [item for item in items if any(band in item.data_links() for band in bands)]

            download_folder = self._param("download_folder")
            download_folder.mkdir(parents=True, exist_ok=True)

            fns = earthaccess.download(
                granules=items,
                local_path=download_folder,
                provider=self._param("provider"),
                threads=self._param("num_workers"),
            )
            # Convert string paths to Path objects
            return [Path(fn) for fn in fns]
