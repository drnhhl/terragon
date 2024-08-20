import json
import requests
from pystac_client import Client
from urllib.parse import urljoin
from datetime import datetime, timedelta
from pathlib import Path
from joblib import Parallel, delayed
import xarray as xr

from .utils import stack_cdse_bands, preprocess_download_task, unzip_files
from .crafter import VoxelCrafter

class CDSE(VoxelCrafter):
    def __init__(self, credentials_path:str=None, base_url:str="https://catalogue.dataspace.copernicus.eu/stac/"):
        super().__init__()
        self.base_url = base_url
        self.credentials = {}
        if credentials_path:
            self.set_credentials(credentials_path)

    def set_credentials(self, credentials_path:str):
        with open(credentials_path, "r") as file:
            self.credentials = json.load(file) # will store username and password

    def retrieve_collections(self, filter_by_name: str=None):
        collections_url = urljoin(self.base_url, "collections")
        response = requests.get(collections_url)

        if response.status_code == 200:
            data = response.json()
            collections = [collection['id'] for collection in data['collections']]
            if filter_by_name:
                collections = [collection for collection in collections if filter_by_name in collection]
            return collections
        else:
            raise RuntimeError("Failed to retrieve collections")

    def search(self, **kwargs):
        super().search(**kwargs)
        bounds_4326 = list(self.get_param('shp', raise_error=True).bounds.values[0])

        catalog = Client.open(self.base_url)

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

    def get_cdse_access_token(self):
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

    def build_minicube(self, output_dir):
        if isinstance(output_dir, str):
            output_dir = Path(output_dir)

        if len([file for file in Path(output_dir).iterdir()]) < 1:
            raise ValueError("No folders with data provided.")

        gdf = self.get_param('shp')
        num_workers = self.get_param('num_workers', 1)
        res = self.get_param('resolution', 10)

        img_folders = list(output_dir.glob(f"**/IMG_DATA/R{res}m"))
        
        # Create a list of delayed tasks
        for img_folder in img_folders:
            ds = stack_cdse_bands(img_folder, gdf, res)
        
        tasks = [delayed(stack_cdse_bands)(img_folder, gdf, res) for img_folder in img_folders]
        datasets = Parallel(n_jobs=num_workers)(tasks)

        # Concatenate datasets along the 'time' dimension and process the combined dataset
        ds = xr.concat(datasets, dim='time').compute()
        ds = ds.sortby('time')

        gdf = gdf.to_crs(ds.rio.crs) if gdf.crs != ds.rio.crs else gdf
        ds = ds.rio.pad_box(*gdf.total_bounds)

        return ds
    
    def download_cdse_file(self, url, file_path):
        access_token = self.get_cdse_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}

        with requests.Session() as session:
            session.headers.update(headers)
            response = session.get(url, stream=True)
            block_size = 8192  # 8 Kilobytes

            if response.status_code == 200:
                with open(file_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=block_size):
                        if chunk:
                            file.write(chunk)
            else:
                print(f"Failed to download file. Status code: {response.status_code}")
                print(response.text)
        return file_path

    def download(self, items, create_minicube=True, delete_zip=True):

        output_dir = Path(self.get_param('download_folder', raise_error=True))
        output_dir.mkdir(parents=True, exist_ok=True)
                
        tasks = preprocess_download_task(items, output_dir) 

        zip_files = []
           
        for i in range(0, len(tasks), 4):
            batch = tasks[i:i+4] 
            futures = Parallel(n_jobs=self.get_param('num_workers', 1))(delayed(self.download_cdse_file)(*task) for task in batch)
            for future in futures:
                try:
                    result = future.result()
                    if result:
                        zip_files.append(result)
                except Exception as e:
                    print(f"Failed to download file with error: {e}")
        
        output_dir = unzip_files(zip_files, output_dir, delete_zip=delete_zip)

        if create_minicube:
            return self.build_minicube(output_dir)
        else:
            return [folder for folder in output_dir.iterdir() if folder.is_dir()]