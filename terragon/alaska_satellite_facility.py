import json
import asf_search as asf
import hyp3_sdk as sdk
from datetime import datetime 
from pathlib import Path 
import xarray as xr 
from joblib import Parallel, delayed

from .base import Base
from .utils import stack_asf_bands, unzip_files, fix_winding_order

class ASF(Base):
    def __init__(self, credentials:dict=None):
        super().__init__()
        self.credentials = {}
        if credentials:
            self.set_credentials(credentials)

    def set_credentials(self, credentials:dict):
        self.credentials = credentials
    
    def retrieve_collections(self, filter_by_name:str=None):
        # Retrieve all collections from asf.PLATFORM that do not start with "_"
        collections = [getattr(asf.PLATFORM, attr) for attr in dir(asf.PLATFORM) if not attr.startswith("_")]

        if collections:
            if filter_by_name:
                filter_by_name_upper = filter_by_name.upper()
                # Filter collections case-insensitively, maintaining original names
                selected_collections = [collection for collection in collections if filter_by_name_upper in collection.upper()]
                return selected_collections
            else:
                # Return all collections if no filter is specified
                return collections
        else:
            # Raise an error if no collections could be retrieved
            raise RuntimeError("Failed to retrieve collections")

    def search(self, **kwargs):
        super().search(**kwargs)

        # asf expects a certain order of the shp coordinates -> make sure it is right
        self._parameters['shp'] = self.get_param('shp', raise_error=True).apply(fix_winding_order)

        start_date = self.get_param("start_date")
        end_date = self.get_param("end_date")
        collection = self.get_param("collection")	
        shp_4326 = self._reproject_shp(self.get_param('shp', raise_error=True))
        shp_4326_wkt = shp_4326.iloc[0]['geometry'].wkt
        processing_level = self.get_param("processing_level")

        items = asf.geo_search(
            platform=collection,
            start=datetime.strptime(start_date, "%Y-%m-%d"),
            end=datetime.strptime(end_date, "%Y-%m-%d"),
            intersectsWith=shp_4326_wkt,
            processingLevel=processing_level)

        if len(items) == 0:
            raise ValueError(f"No items found")
        return items   

    def start_rtc_jobs(self, items, rtc_specifications=None, job_name="rtc_jobs"):
        granule_ids = [item.properties["sceneName"] for item in items]
        # Prepare default parameters and update with rtc_specifications if provided
        default_params = {"name": job_name}
        if rtc_specifications:
            default_params.update(rtc_specifications)

        # Create a batch job for all granule IDs
        rtc_jobs = sdk.Batch()
        for granule_id in granule_ids:
            rtc_jobs += self.hyp3_session.submit_rtc_job(granule_id, **default_params)

        return rtc_jobs

    def start_insar_jobs(self, insar_specifications=None, job_name="insar_jobs"):
        # Prepare default parameters and update with rtc_specifications if provided
        default_params = {"name": job_name}
        if insar_specifications:
            default_params.update(insar_specifications)

        # Create a batch job for all granule IDs
        insar_jobs = sdk.Batch()
        for result in self.search_results:
            granule_id = result.properties["sceneName"]
            neighbors = self.get_nearest_neighbors(granule_id, max_neighbors=2)
            for secondary in neighbors:
                insar_jobs += self.hyp3_session.submit_insar_job(
                    granule_id, secondary.properties["sceneName"], **default_params
                )
        return insar_jobs

    def build_minicube(self, img_folders):
        if len(img_folders) < 1:
            raise ValueError("No folders with data provided.")

        shp = self.get_param('shp')
        num_workers = self.get_param('num_workers', 1)
        resolution = self.get_param('resolution', 10)
        
        # Create a list of delayed tasks
        tasks = [delayed(stack_asf_bands)(img_folder, shp, resolution) for img_folder in img_folders]
        datasets = Parallel(n_jobs=num_workers)(tasks)

        # Concatenate datasets along the 'time' dimension and process the combined dataset
        ds = xr.concat(datasets, dim='time').compute()
        ds = ds.sortby('time')
        return ds

    def download(self, items, create_minicube=True):
        assert len(items) > 0, "No images to download in items."

        if "username" not in self.credentials or "password" not in self.credentials:
            raise ValueError("No credentials provided for ASF download.")
        
        output_dir = Path(self.get_param('download_folder', raise_error=True))
        output_dir.mkdir(parents=True, exist_ok=True)
        
        asf_session = asf.ASFSession().auth_with_creds(self.credentials["username"], self.credentials["password"])
        item_urls = [item.properties["url"] for item in items]      
        
        print(f"Downloading {len(item_urls)} items to directory: {output_dir}")
        asf.download_urls(urls=item_urls, path=str(output_dir), session=asf_session, processes=self.get_param('num_workers', 1))

        zip_files = list(output_dir.glob("*.zip"))
        output_dir = unzip_files(zip_files)
        image_folders = list(output_dir.glob("**/measurement"))

        if create_minicube:
            return self.build_minicube(image_folders)
        else:
            return image_folders