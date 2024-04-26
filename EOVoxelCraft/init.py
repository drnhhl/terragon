from .crafter import PC, GEE, CDSE, ASF

def init(api, credentials=None, **kwargs):
    if api == 'pc' or api == 'planetary_computer':
        return PC(credentials, **kwargs)
    elif api == 'gee' or api == 'earthengine':
        return GEE(credentials, **kwargs)
    elif api == 'cdse' or api == 'copernicus':
        return CDSE(credentials, **kwargs)
    elif api == 'asf':
        return ASF(credentials, **kwargs)
    else:
        raise ValueError(f'API {api} not supported. Please use "pc", "gee", "cdse" or "asf".')