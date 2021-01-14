import pandas as pd
import logging
import os
from datetime import datetime
import fnmatch


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

    logfilename = "runlog_" + datetime.now().strftime(format="%Y%m%d_%H%M") + ".log"
    full_path = os.path.join(run_params.logdir, logfilename)
    logging.basicConfig(
        filename=full_path,
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
    )

    return full_path


def cat_modules(run_params, module):

    all_pickles = list()

    for file in os.listdir(run_params.pickledir):

        if file.find(module) == -1:
            continue

        filepath = os.path.join(run_params.pickledir, file)
        df = pd.read_pickle(filepath)
        all_pickles.append(df)

    data = pd.concat(all_pickles, axis=0, sort=False)

    return data


def cleanup(directory, ext):  # verwijder alle bestanden uit map

    files = fnmatch.filter(os.listdir(directory), "*")
    files = [x for x in files if x.endswith(ext)]
    for file in files:
        os.remove(os.path.join(directory, file))
        logging.info("removed: {}".format(file))
    logging.info("{} files removed in directory".format(len(files)))


def create_dir(destination):

    try:
        if not os.path.exists(destination):
            os.makedirs(destination)
    except OSError:
        logging.warning("Error Creating directory. " + destination)
    return destination


def export_module(data, module_name, run_params):

    data["werkmaatschappij"] = run_params.adm_code

    filename = "{}_{}.pkl".format(run_params.adm_code, module_name)
    data.to_pickle(os.path.join(run_params.pickledir, filename))
    logging.info("{} geexporteerd! ({} records)".format(filename, len(data)))
