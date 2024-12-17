def init(api: str, credentials: dict = None, **kwargs) -> object:
    """instantiate a new data source object.

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
    else:
        raise ValueError(f'API {api} not supported. Please use "pc" or "gee".')
