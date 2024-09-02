import json
import requests
from pystac_client import Client
from pystac import ItemCollection
from urllib.parse import urljoin
from datetime import datetime, timedelta
from pathlib import Path
from joblib import Parallel, delayed
import xarray as xr
import odc.stac

from .utils import stack_cdse_bands, preprocess_download_task, unzip_files
from .crafter import VoxelCrafter

class CDSE(VoxelCrafter):
    def __init__(self, credentials:dict=None, base_url:str="https://catalogue.dataspace.copernicus.eu/stac/"):
        super().__init__()
        self.base_url = base_url
        self.credentials = credentials
        if credentials:
            self.access_token = self.get_access_token()

    def set_access_token(self, credentials):
        self.credentials = credentials
        self.access_token = self.get_access_token()

    def retrieve_collections(self, filter_by_name: str=None):
        collections_url = urljoin(self.base_url, "collections")
        response = requests.get(collections_url)

        if response.status_code == 200:
            data = response.json()
            collections = [collection['id'] for collection in data['collections']]
            if filter_by_name:
                collections = [collection for collection in collections if filter_by_name in collection.lower()]
            return collections
        else:
            raise RuntimeError("Failed to retrieve collections")

    def search(self, **kwargs):
        super().search(**kwargs)
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

        if len(items) == 0:
            raise ValueError(f"No items found")

        return items

    def get_access_token(self):
        data = {
            "client_id": "cdse-public",
            "username": self.credentials['username'],
            "password": self.credentials['password'],
            "grant_type": "password",
            }
        try:
            r = requests.post("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",data=data)
            r.raise_for_status()
        except Exception as e:
            raise Exception(f"Access token creation failed. Reponse from the server was: {r.json()}")
        return r.json()["access_token"]

    def build_minicube(self, output_dir:Path):
        
        output_dir = Path(output_dir) if isinstance(output_dir, str) else output_dir

        if not any(output_dir.iterdir()):
            raise ValueError("No folders with data provided.")

        gdf = self.get_param('shp')
        res = self.get_param('resolution', 10)

        img_folders = list(output_dir.glob(f"**/IMG_DATA/R{res}m"))
        
        tasks = [delayed(stack_cdse_bands)(img_folder, gdf, res) for img_folder in img_folders]
        datasets = Parallel(n_jobs=self.get_param('num_workers', 1))(tasks)

        # Concatenate datasets along the 'time' dimension and process the combined dataset
        ds = xr.concat(datasets, dim='time').compute()
        ds = ds.sortby('time')

        gdf = gdf.to_crs(ds.rio.crs) if gdf.crs != ds.rio.crs else gdf
        ds = ds.rio.pad_box(*gdf.total_bounds)

        return ds
    
    def download_file(self, url, file_path, block_size=32768, max_retries=3):
        assert self.access_token, "No access token for download; please call set_access_token()."

        headers = {"Authorization": f"Bearer {self.access_token}"}
        retry_count = 0
        
        while retry_count < max_retries:
            with requests.Session() as session:
                session.headers.update(headers)
                response = session.get(url, stream=True)

                # Handle token expiration by checking for a 401 response
                if response.status_code == 401 and "Expired signature" in response.text:
                    print("Access token expired. Refreshing token and retrying...")
                    self.access_token = self.get_access_token()  # Refresh token
                    headers["Authorization"] = f"Bearer {self.access_token}"
                    retry_count += 1
                    continue  # Retry the request with new token

                # Handle successful response
                if response.status_code == 200:
                    with open(file_path, "wb") as file:
                        for chunk in response.iter_content(chunk_size=block_size):
                            if chunk:
                                file.write(chunk)
                    return file_path  # Return path if successful

                # Handle other HTTP errors
                else:
                    print(f"Failed to download file. Status code: {response.status_code}")
                    print(response.text)
                    return None
        
        # If maximum retries are exceeded
        print(f"Failed to download after {max_retries} attempts due to repeated authorization errors.")
        return None
    
    def download_data_from_stac(self, items):

        bounds = list(self.get_param('shp', raise_error=True).bounds.values[0])
        crs = self.get_param('shp', raise_error=True).crs
        resolution = self.get_param('resolution', raise_error=True)

        # Load data using odc-stac with the authenticated session
        data = odc.stac.load(
            items,
            crs=crs,
            resolution=resolution,
            x=(bounds[0], bounds[2]),
            y=(bounds[1], bounds[3])
        )
        
        return data

    def download(self, items, create_minicube=True, delete_zip=True):

        output_dir = Path(self.get_param('download_folder', raise_error=True))
        output_dir.mkdir(parents=True, exist_ok=True)
                
        tasks = preprocess_download_task(items, output_dir) 
        max_imgs_parallel = 4
        num_workers = self.get_param('num_workers', 1)

        zip_files = []
           
        for i in range(0, len(tasks), max_imgs_parallel):
            batch = tasks[i:i+4] 
            results = Parallel(n_jobs=num_workers)(delayed(self.download_file)(*task) for task in batch)
            for result in results:
                try:
                    if result:
                        zip_files.append(result)
                        print(f"Downloaded item: {result}")
                except Exception as e:
                    print(f"Failed to download file with error: {e}")
        
        output_dir = unzip_files(zip_files, output_dir, delete_zip=delete_zip)

        if create_minicube:
            return self.build_minicube(output_dir)
        else:
            return [folder for folder in output_dir.iterdir() if folder.is_dir()]