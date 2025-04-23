def init(api: str, credentials: dict = None, **kwargs) -> object:
    """Instantiate a new data source object.

    :param api: name of the API to use
    :param credentials: dict of the credentials, see requirements of the class, defaults to None
    :param **kwargs: other keyword arguments for the specific class.__init__
    :raises ValueError: when 'api' is not supported
    :return: object of data source instance
    """
    if api == "pc" or api == "planetary_computer":
        from .microsoft_planetary_computer import PC

        return PC(credentials, **kwargs)
    elif api == "gee" or api == "earthengine":
        from .google_earth_engine import GEE

        return GEE(credentials, **kwargs)
    elif api == "cdse" or api == "copernicus_data_space_ecosystem":
        from .copernicus_data_space_ecosystem import CDSE

        return CDSE(credentials, **kwargs)
    elif api == "asf" or api == "alaska_satellite_facility":
        from .alaska_satellite_facility import ASF

        return ASF(credentials, **kwargs)
    else:
        raise ValueError(f'API {api} not supported. Please use "pc", "gee", "cdse", or "asf".')
