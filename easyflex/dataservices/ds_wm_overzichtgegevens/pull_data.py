import pandas as pd
import os
import logging

from easyflex.dataservices.soap_request import OperatieParameters
from easyflex.dataservices.soap_request import get_data

from .fields import pull_fields
from .format import format_data


def pull_module(run_params, module_name):
    runcount = 0
    data = pd.DataFrame()
    run = True
    velden = pull_fields()

    while run:
        parameters = {"berichtbron": 10052, "medewerker": 16612}
        operatie = OperatieParameters(
            naam="ds_wm_overzichtgegevens",
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

        runcount += 1

    if len(data):
        export_module(data, module_name, run_params)


def export_module(data, module_name, run_params):

    data = format_data(data)
    data["werkmaatschappij"] = run_params.adm_code
    data["ovz_id"] = data["werkmaatschappij"] + " - " + data["wm_overzicht_identificatie"]
    filename = "{}_{}.pkl".format(run_params.adm_code, module_name)
    data.to_pickle(os.path.join(run_params.pickledir, filename))
    logging.info("{} geexporteerd! ({} records)".format(filename, len(data)))
