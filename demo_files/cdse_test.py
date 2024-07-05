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
    end_date='2021-08-10', 
    download_folder=r'C:\Users\hoeh_pa\Desktop\EOVoxelCraft\demo_files\data\downloads\S2', 
    num_workers=8
    )

items = crafter.search(**arguments)
item_sel = [item for item in items if item.properties['processingLevel'] == "S2MSIL2A" and item.properties['cloudCover'] < 10.0]
import pdb; pdb.set_trace()
cube = crafter.download(item_sel, create_minicubes=True, delete_zip=True)
import pdb; pdb.set_trace()