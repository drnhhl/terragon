import EOVoxelCraft as eovc
import geopandas as gpd
from EOVoxelCraft.utils import fix_winding_order
from pathlib import Path

crafter = eovc.init('asf')
credentials_path = Path(r"C:\Users\hoeh_pa\Desktop\EOVoxelCraft\credentials\asf_credentials.json")
crafter.set_credentials(credentials_path)

shapefile = Path(r"C:\Users\hoeh_pa\Desktop\EOVoxelCraft\demo_files\data\TUM_OTN.shp")
gdf = gpd.read_file(shapefile)
gdf['geometry'] = gdf['geometry'].apply(fix_winding_order)
arguments = dict(shp=gdf, collection='sentinel-1', start_date='2021-01-01', end_date='2021-06-30', download_folder=r'C:\Users\hoeh_pa\Desktop\EOVoxelCraft\demo_files\data\downloads', processing_level="GRD_HD")

# Search for Sentinel-1 data
items = crafter.search(**arguments)

# items_sel = items[:3]
# crafter.download(items_sel, create_minicube=True)

output_dir = Path(r"C:\Users\hoeh_pa\Desktop\EOVoxelCraft\demo_files\data\downloads")
image_folders = list(output_dir.glob("**/measurement"))

ds = crafter.build_minicube(image_folders)
import pdb; pdb.set_trace()