import logging
import os
from datetime import datetime


def set_logging(run_params):

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    fileloc = set_logging_file(run_params)

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # add the handler to the root logger
    logging.getLogger("").addHandler(console)

    return fileloc


def set_logging_file(run_params):

    logfilename = "runlog_" + datetime.now().strftime(fmt="%Y%m%d_%H%M") + ".log"
    full_path = os.path.join(run_params.logdir, logfilename)
    logging.basicConfig(
        filename=full_path,
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
    )

    return full_path


def create_dir(destination):

    try:
        if not os.path.exists(destination):
            os.makedirs(destination)
    except OSError:
        logging.warning("Error Creating directory. " + destination)
    return destination
