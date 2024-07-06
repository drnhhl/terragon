import EOVoxelCraft as eovc
import geopandas as gpd
from pathlib import Path

crafter = eovc.init('cdse')
credentials_path = Path(r"C:\Users\hoeh_pa\Desktop\EOVoxelCraft\credentials\cdse_credentials.json")
crafter.set_credentials(credentials_path)

arguments = dict(
    shp=gpd.read_file(r"C:\Users\hoeh_pa\Desktop\EOVoxelCraft\demo_files\data\TUM_OTN.shp"), 
    resolution=20,
    collection='SENTINEL-2', 
    start_date='2021-06-01', 
    end_date='2021-07-15', 
    download_folder=r'C:\Users\hoeh_pa\Desktop\EOVoxelCraft\demo_files\data\downloads', 
    num_workers=8
    )

items = crafter.search(**arguments)
item_sel = [item for item in items if item.properties["tileId"] == "32UQU" and item.properties['processingLevel'] == "S2MSI2A" and item.properties['cloudCover'] < 10.0]
cube = crafter.download(item_sel, create_minicube=True, delete_zip=True)
import pdb; pdb.set_trace()