import geopandas as gpd
import planetary_computer as pc
import ee
import geedim
import requests
import pystac_client
from urllib.parse import urljoin
import odc.stac
from abc import ABC, abstractmethod

class VoxelCrafter(ABC):
    @abstractmethod
    def __init__(self, ):
        pass

    @abstractmethod
    def create(self, ):
        items = self.search()
        self.download(items)

    @abstractmethod
    def retrieve_collections(self, ):
        pass

    @abstractmethod
    def search(self, ):
        return items

    @abstractmethod
    def download(self, items):
        pass


class PC(VoxelCrafter):
    def __init__(self, credentials:str=None, base_url:str="https://planetarycomputer.microsoft.com/api/stac/v1/"):
        super().__init__()
        self.base_url = base_url
        if credentials:
            pc.set_subscription_key(self.api_key)

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
class GEE(VoxelCrafter):
    def __init__(self, credentials=None):
        super().__init__()
        pass

class CDSE(VoxelCrafter):
# https://documentation.dataspace.copernicus.eu/APIs/STAC.html
    def __init__(self, credentials=None):
        super().__init__()
        pass

class ASF(VoxelCrafter):
    def __init__(self, credentials=None):
        super().__init__()
        pass
