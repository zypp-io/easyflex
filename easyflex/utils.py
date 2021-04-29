import logging
import os


def create_dir(destination) -> str:
    """

    Parameters
    ----------
    destination: str
        Het pad dat aangemaakt dient te worden.

    Returns
    -------
    destination: str
        Het pad dat is aangemaakt (zelfde als input)
    """

    try:
        if not os.path.exists(destination):
            os.makedirs(destination)
    except OSError:
        logging.warning("Error Creating directory. " + destination)
    return destination
