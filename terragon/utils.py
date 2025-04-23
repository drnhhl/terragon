import warnings

import pyproj
from shapely.geometry import Point


def indices_are_identical(datasets: list) -> bool:
    """Check if the indices from the xarray datasets are equal."""
    if len(datasets) == 1:
        return True

    # get all indices
    coords = list(set(key for ds in datasets for key in list(ds.indexes.keys())))

    # check if all indices exist in all datasets
    if not all(all(coord in ds.indexes for ds in datasets) for coord in coords):
        return False

    # check if all indices are equal
    if not all(
        all(datasets[0].indexes[coord].equals(ds.indexes[coord]) for ds in datasets)
        for coord in coords
    ):
        return False

    return True


def align_coords(datasets, shp, resampling):
    """unify the crs and indices of the datasets. Often the coordinates differ by a very small amount
    due to rounding errors, xarray will merge these into one dataframe, here the unify them to prevent
    empty pixels in the dataset."""
    # make sure the coordinates are the same and match (e.g. if there are other crs)
    if not all([ds.rio.crs == datasets[0].rio.crs for ds in datasets]) or not indices_are_identical(
        datasets
    ):
        crs_list = [ds.rio.crs for ds in datasets]
        # match all to master (first shp crs)
        idx = [i for i, crs in enumerate(crs_list) if crs == shp.crs]
        if len(idx) == 0:
            warnings.warn(
                f"No matching crs found and all crs are different. Using first crs {crs_list[0]} as master."
            )
            idx = 0
        else:
            idx = idx[0]
        datasets = [
            (ds.rio.reproject_match(datasets[idx], resampling=resampling) if i != idx else ds)
            for i, ds in enumerate(datasets)
        ]
    return datasets


def align_resolutions(datasets, shp, resolution, resampling):
    """unify the resolutions of the list of the xr.Datasets."""
    ress = [ds.rio.resolution() for ds in datasets]
    resolution = meters_to_crs_unit(resolution, shp)
    # round degrees to 8 decimal places for cm resolution
    ress = [(round(res[0], 8), round(res[0], 8)) for res in ress]
    resolution = [round(res, 8) for res in resolution]
    if len(set(ress)) > 1 or (len(ress) > 0 and ress[0] != resolution):
        # get closest resolution
        idx = sorted(
            enumerate(ress),
            key=lambda item: abs(resolution[0] - abs(item[1][0]))
            + abs(resolution[1] - abs(item[1][1])),
        )[0][0]
    if ress[idx] != resolution:
        datasets[idx] = datasets[idx].rio.reproject(
            shp.crs, resolution=resolution, resampling=resampling
        )
    # reproject rest to master
    datasets = [
        (ds.rio.reproject_match(datasets[idx], resampling=resampling) if i != idx else ds)
        for i, ds in enumerate(datasets)
    ]
    return datasets


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
    point = shp_utm.geometry.iloc[0].centroid
    # offset point
    offset_point_x = Point(point.x + meters, point.y)
    offset_point_y = Point(point.x, point.y + meters)

    # Convert the points to the CRS of the shape
    transformer = pyproj.Transformer.from_crs(shp_utm.crs, shp.crs, always_xy=True)
    orig_point = transformer.transform(point.x, point.y)
    offset_point_in_orig_crs_x = transformer.transform(offset_point_x.x, offset_point_x.y)
    offset_point_in_orig_crs_y = transformer.transform(offset_point_y.x, offset_point_y.y)

    # distance in the shape's CRS units
    distance_units_x = Point(orig_point).distance(Point(offset_point_in_orig_crs_x))
    distance_units_y = Point(orig_point).distance(Point(offset_point_in_orig_crs_y))

    return distance_units_x, distance_units_y
