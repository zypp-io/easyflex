import os
import logging
import xml.etree.ElementTree as ET
import pandas as pd
from easyflex.dataservices.soap_request import OperatieParameters
from easyflex.dataservices.soap_request import send_soap_request
from easyflex.dataservices.soap_request import namespaces

from .fields import pull_fields
from .format import format_data


def pull_module(run_params, module_name):
    runcount = 0
    data = pd.DataFrame()
    run = True
    velden = pull_fields()

    while run:
        parameters = {"memorubrieken": "true"}
        operatie = OperatieParameters(
            naam="ds_fw_persoonsgegevens_all",
            output_prefix="fw_",
            parameters=parameters,
            fields=velden,
            limit=5000,
            runcount=runcount,
            incremental=run_params.incremental,
            day_offset=run_params.day_offset,
        )

        resultcount, df = custom_request(run_params, operatie)
        data = pd.concat([data, df], sort=False, ignore_index=True)
        if not resultcount == operatie.limit or run_params.debug:
            run = False

        runcount += 1

    if len(data) != 0:
        export_module(data, module_name, run_params)
    else:
        logging.info(f"geen data in module {module_name} voor {run_params.adm_code}")


def export_module(data, module_name, run_params):

    data = format_data(data)
    data["werkmaatschappij"] = run_params.adm_code
    data = data[
        (
            [
                "werkmaatschappij",
                "fw_registratienummer",
                "fw_memonummer",
                "fw_memonaam",
                "fw_memowaarde",
            ]
        )
    ]
    filename = "{}_{}.pkl".format(run_params.adm_code, module_name)
    data.to_pickle(os.path.join(run_params.pickledir, filename))
    logging.info("{} geexporteerd! ({} records)".format(filename, len(data)))


def custom_request(run_params, operatie):
    ns, ns_txt = namespaces()
    response = send_soap_request(run_params, operatie)
    content = ET.fromstring(response.content.decode("utf-8")).find("ds:Body", ns)
    result = content.findall("urn:{}_result/urn:fields/urn:item".format(operatie.naam), ns)

    resultcount = len(result)
    data = list()  # maak een lege lijst aan waar records aan toe kunnen worden gevoegd

    for records in result:

        d = dict()

        for content in records:
            dtype = content.attrib.get(ns_txt["schema"] + "type", "string").replace("xsd:", "")
            if dtype.find("Array") == -1:
                col = content.tag.replace(ns_txt["urn"], "")
                val = content.text
                d[col] = val
            else:

                for sub_content in content:

                    for sub_sub in sub_content:
                        col = sub_sub.tag.replace(ns_txt["urn"], "")
                        val = sub_sub.text
                        d[col] = val
                    data.append(d.copy())  # deze functie parsed de data

    df = pd.DataFrame(data=data, index=range(len(data)))
    df.columns = [col.replace(operatie.naam[3:] + "_", "") for col in df.columns]
    df.columns = [operatie.output_prefix + col for col in df.columns]  # add prefix

    return resultcount, df
