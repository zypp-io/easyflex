class ServiceNotKnownError(Exception):
    """Error when the chosen service is not in the list of available services"""

    pass


class NoDataToImport(Exception):
    """Error when there is no data to be imported"""

    pass
