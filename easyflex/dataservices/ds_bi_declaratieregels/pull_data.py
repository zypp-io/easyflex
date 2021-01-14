import pandas as pd
import os
import logging

from easyflex.dataservices.soap_request import OperatieParameters
from easyflex.dataservices.soap_request import get_data

from .fields import pull_fields
from .format import format_data


def pull_module(run_params, module_name):

    jaren = run_params.years
    maanden = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

    for jaar in jaren:
        pull_jaar(jaar, maanden, run_params, module_name)


def pull_jaar(jaar, maanden, run_params, module_name):

    for maand in maanden:

        parameters = {"jaar": jaar, "maand": maand}
        velden = pull_fields(module_name)

        data = pull_maand(run_params, parameters, velden)
        if len(data) != 0:
            export_module(data, jaar, maand, module_name, run_params)


def limit(run_params):

    if run_params.test:
        limit = 100
    else:
        limit = 5000

    return limit


def pull_maand(run_params, parameters, velden):

    data = pd.DataFrame()
    run = True
    runcount = 0

    while run:

        operatie = OperatieParameters(
            naam="ds_bi_declaratieregels",
            output_prefix="",
            parameters=parameters,
            fields=velden,
            limit=limit(run_params),
            runcount=runcount,
            incremental=run_params.incremental,
            day_offset=run_params.day_offset,
        )

        df = get_data(run_params, operatie)
        data = pd.concat([data, df], sort=False, ignore_index=True)

        if not len(df) == operatie.limit or run_params.test:
            run = False

        runcount += 1

    return data


def export_module(data, jaar, maand, module_name, run_params):

    data = format_data(data)
    data["werkmaatschappij"] = run_params.adm_code
    data["bi_id"] = data["werkmaatschappij"] + " - " + data["bi_declid"]

    filename = "{}_{}_{}_{}.pkl".format(run_params.adm_code, module_name, jaar, maand)
    data.to_pickle(os.path.join(run_params.pickledir, filename))
    logging.info("{} geexporteerd! ({} records)".format(filename, len(data)))
