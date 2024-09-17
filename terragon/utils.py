import pyproj
from shapely.geometry import Point


def rm_files(fns):
    for fn in fns:
        if fn.exists():
            try:
                fn.unlink()
            except Exception as e:
                print(f"Failed to remove file in download folder {fn}: {e}")


def shp_to_utm_crs(shp):
    """convert the shape from WGS84 to UTM crs."""
    if shp.crs.to_epsg() != 4326:
        shp = shp.to_crs(epsg=4326)
    utm_crs_list = pyproj.database.query_utm_crs_info(
        datum_name="WGS 84",
        area_of_interest=pyproj.aoi.AreaOfInterest(*shp.geometry.bounds.values[0]),
    )

    # Save the CRS
    epsg = utm_crs_list[0].code
    utm_crs = pyproj.CRS.from_epsg(epsg)
    shp = shp.to_crs(utm_crs)
    return shp


def meters_to_crs_unit(meters, shp):
    """Convert meters to the shape's CRS units."""
    # Convert the shape to UTM CRS where distances are in meters
    shp_utm = shp_to_utm_crs(shp)
    # reference point
    point = shp_utm.geometry[0].centroid
    # offset point
    offset_point = Point(point.x, point.y + meters)

    # Convert the points to the CRS of the shape
    transformer = pyproj.Transformer.from_crs(shp_utm.crs, shp.crs, always_xy=True)
    orig_point = transformer.transform(point.x, point.y)
    offset_point_in_orig_crs = transformer.transform(offset_point.x, offset_point.y)

    # distance in the shape's CRS units
    distance_units = Point(orig_point).distance(Point(offset_point_in_orig_crs))

    return distance_units
