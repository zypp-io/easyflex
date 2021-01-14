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

        operatie = OperatieParameters(
            naam="ds_fw_reserveringen",
            output_prefix="",
            parameters={},
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

    if not len(data):
        return logging.info("geen data geexporteerd.")
    data = format_data(data)
    data["werkmaatschappij"] = run_params.adm_code
    data["res_id"] = (
        data["werkmaatschappij"]
        + " - "
        + data["fw_res_registratienummer"]
        + " - "
        + data["fw_res_lc_idnr"]
        + data["fw_res_speccode"]
    )

    filename = "{}_{}.pkl".format(run_params.adm_code, module_name)
    data.to_pickle(os.path.join(run_params.pickledir, filename))
    logging.info("{} geexporteerd! ({} records)".format(filename, len(data)))
