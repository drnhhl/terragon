def init(api, credentials=None, **kwargs):
    if api == "pc" or api == "planetary_computer":
        from .microsoft_planetary_computer import PC

        return PC(credentials, **kwargs)
    elif api == "gee" or api == "earthengine":
        from .google_earth_engine import GEE

        return GEE(credentials, **kwargs)
    elif api == "cdse" or api == "copernicus_data_space_ecosystem":
        from .copernicus_data_space_ecosystem import CDSE
        
        return CDSE(credentials, **kwargs)
    else:
        raise ValueError(f'API {api} not supported. Please use "pc" or "gee".')
