
def init(api, credentials=None, **kwargs):
    if api == 'pc' or api == 'planetary_computer':
        from .microsoft_planetary_computer import PC
        return PC(credentials, **kwargs)
    elif api == 'gee' or api == 'earthengine':
        from .google_earth_engine import GEE
        return GEE(credentials, **kwargs)
    elif api == 'cdse' or api == 'copernicus':
        from .copernicus import CDSE
        return CDSE(credentials, **kwargs)
    elif api == 'asf':
        from .alaska_satellite_facility import ASF
        return ASF(credentials, **kwargs)
    else:
        raise ValueError(f'API {api} not supported. Please use "pc", "gee", "cdse" or "asf".')
