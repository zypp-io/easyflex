import logging
import os

import pandas as pd

from easyflex.dataservices.soap_request import OperatieParameters
from easyflex.dataservices.soap_request import get_data
from .fields import pull_fields
from .format import format_data
from tqdm import tqdm


def pull_module(run_params, module_name):

    data = pd.DataFrame()
    velden = pull_fields()

    onderwerpen = [
        "Locatie verwijderd",
        "Locatie toegevoegd",
        "Locatie gewijzigd",
    ]

    for onderwerp in tqdm(onderwerpen, desc="uitvragen van onderwerpen..."):
        runcount = 0
        run = True
        while run:
            parameters = {
                "bronnummer": "6000",
                "onderwerp": onderwerp,
            }

            operatie = OperatieParameters(
                naam="ds_wm_notificaties",
                output_prefix="",
                parameters=parameters,
                fields=velden,
                limit=5000,
                runcount=runcount,
                incremental=run_params.incremental,
                day_offset=run_params.day_offset,
            )

            df = get_data(run_params, operatie)
            data = pd.concat([data, df], sort=False, ignore_index=True)

            if not len(df) == operatie.limit or run_params.debug:
                run = False
                continue

            runcount += 1

    if len(data):
        export_module(data, module_name, run_params)


def export_module(data, module_name, run_params):
    data = format_data(data)
    data["werkmaatschappij"] = run_params.adm_code
    data["notificatie_id"] = data["werkmaatschappij"] + " - " + data["wm_notificatie_notitienummer"]

    filename = "{}_{}.pkl".format(run_params.adm_code, module_name)
    data.to_pickle(os.path.join(run_params.pickledir, filename))
    logging.info("{} geexporteerd! ({} records)".format(filename, len(data)))
