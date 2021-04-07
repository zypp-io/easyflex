import pandas as pd
import os
import logging
import xml.etree.ElementTree as ET
from easyflex.dataservices.soap_request import OperatieParameters
from easyflex.dataservices.soap_request import namespaces
from easyflex.dataservices.soap_request import send_soap_request
from easyflex.dataservices.soap_request import check_for_errors

from .fields import pull_fields
from .format import format_data


def pull_module(run_params, module_name):
    runcount = 0
    data = list()
    run = True
    velden = pull_fields()
    while run:
        parameters = {"communicatie": "true"}
        operatie = OperatieParameters(
            naam="ds_fw_persoonsgegevens_all",
            output_prefix="fwcomm_",
            parameters=parameters,
            fields=velden,
            limit=5000,
            runcount=runcount,
            incremental=run_params.incremental,
            day_offset=run_params.day_offset,
        )

        datasubset = get_data(run_params, operatie)
        data = data + datasubset

        if not len(datasubset) == operatie.limit or run_params.debug:
            run = False

        runcount += 1

    df = create_dataframe(data, operatie)

    if len(df):
        export_module(df, module_name, run_params)


def get_data(run_params, operatie):

    ns, ns_txt = namespaces()

    response = send_soap_request(run_params, operatie)
    content = ET.fromstring(response.content.decode("utf-8")).find("ds:Body", ns)

    check_for_errors(content)

    data = parse_all_data(content, operatie)

    logging.info(
        "{} - {} - {} records pulled (runcount: {})".format(
            run_params.adm_code, operatie.naam, len(data), operatie.runcount
        )
    )
    return data


def parse_all_data(content, operatie):
    ns, ns_txt = namespaces()

    result = content.findall("urn:{}_result/urn:fields/urn:item".format(operatie.naam), ns)

    if not result:
        result = content.findall("urn:{}_result/urn:fields".format(operatie.naam), ns)

    data = list()  # maak een lege lijst aan waar records aan toe kunnen worden gevoegd

    for records in result:
        data.append(parse_records(records))  # deze functie parsed de data

    return data


def create_dataframe(data, operatie):

    df = pd.DataFrame(data=data, index=range(len(data)))
    df_exploded = dict_to_columns(df, column="fw_persoonsgegevens_all_communicatie")
    df_exploded.columns = [col.replace(operatie.naam[3:] + "_", "") for col in df_exploded.columns]

    df_exploded.columns = [
        operatie.output_prefix + col for col in df_exploded.columns
    ]  # add prefix

    return df_exploded


def parse_records(record):
    ns, ns_txt = namespaces()
    data = dict()

    for column in record:
        if not len(column):
            data[column.tag.replace(ns_txt["urn"], "")] = column.text
        else:
            sub_layer = parse_sublayer(column)
            data = {**data, **sub_layer}

    return data


def parse_sublayer(sublayer):
    ns, ns_txt = namespaces()

    sublayer_name = sublayer.tag.replace(ns_txt["urn"], "")
    sublayers = sublayer.findall("urn:item", ns)

    data_list = list()
    data = dict()
    for record in sublayers:
        sublayer_data = parse_records(record)
        data_list.append(sublayer_data)
    data[sublayer_name] = data_list

    return data


def dict_to_columns(df, column):

    df = df[~df[column].isna()]
    df = df.explode(column, ignore_index=True)
    df_dict = pd.DataFrame(df[column].tolist())
    df = pd.concat([df.drop(columns=column), df_dict], axis=1)

    return df


def export_module(data, module_name, run_params):

    data["werkmaatschappij"] = run_params.adm_code
    data["fw_id"] = data["werkmaatschappij"] + " - " + data["fwcomm_registratienummer"]

    data = format_data(data)

    filename = "{}_{}.pkl".format(run_params.adm_code, module_name)
    data.to_pickle(os.path.join(run_params.pickledir, filename))
    logging.info("{} geexporteerd! ({} records)".format(filename, len(data)))
