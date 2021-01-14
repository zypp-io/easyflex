import pandas as pd
import logging

from easyflex.dataservices.soap_request import OperatieParameters
from easyflex.dataservices.soap_request import get_data
from easyflex.functions import cat_modules, export_module

from .fields import pull_fields
from .format import format_data


def pull_module(run_params, module_name):
    data = pd.DataFrame()
    velden = pull_fields()

    regnrs = cat_modules(run_params, "{}_ds_fw_fases.pkl".format(run_params.adm_code))
    regnrs = regnrs.fa_registratienummer.drop_duplicates().to_list()

    logging.info("fase details opvragen voor {} flexwerkers".format(len(regnrs)))
    for regnr in regnrs:
        runcount = 0
        run = True

        while run:
            parameters = {"registratienummer": regnr}
            operatie = OperatieParameters(
                naam="ds_fw_fase_details",
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

            if not len(df) == operatie.limit or run_params.test:
                run = False

            runcount += 1

    if len(data):
        data = format_data(data)
        export_module(data, module_name, run_params)
