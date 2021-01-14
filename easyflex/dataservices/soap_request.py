import logging
import os
import xml.etree.ElementTree as ET

import pandas as pd
import requests
from datetime import datetime, timedelta


class OperatieParameters:
    def __init__(
        self, naam, output_prefix, parameters, fields, limit, runcount, incremental, day_offset
    ):

        self.naam = naam
        self.runcount = runcount
        self.limit = limit
        self.incremental = incremental
        self.day_offset = day_offset

        # voeg vanaf, totenmet en datumtijdgewijzigd toe aan parameters
        parameters = self.vanaf_tm(parameters)
        parameters = self.datumtijdgewijzigd(parameters)

        self.parameters_xml = self.parameters_xml(parameters)
        self.fields_xml = self.fields_xml(fields)
        self.output_prefix = output_prefix

    def vanaf_tm(self, parameters):

        parameters["vanaf"] = 1 + (self.limit * self.runcount)
        parameters["totenmet"] = self.limit + (self.limit * self.runcount)

        return parameters

    def datumtijdgewijzigd(self, parameters):

        if not self.incremental:
            return parameters
        else:
            from_date = datetime.now() - timedelta(days=self.day_offset)
            parameters["datumtijdgewijzigd"] = from_date.strftime("%Y-%m-%dT%H:%M:%S")
            logging.info(f'incremental update data from {from_date.strftime("%Y-%m-%d")}')

        return parameters

    def parameters_xml(self, parameters):

        xml_lines = list()

        for parameter_name, parameter_value in parameters.items():
            xml_lines.append(
                "<urn:{}>{}</urn:{}>".format(parameter_name, parameter_value, parameter_name)
            )

        xml_lines = "\n".join(xml_lines)
        xml_format = """<urn:parameters>\n{}\n</urn:parameters>""".format(xml_lines)

        return xml_format

    def fields_xml(self, fields):

        if fields is None:
            fields = list()
        xml_lines = ["""<urn:{}></urn:{}>""".format(x, x) for x in fields]
        xml_lines = "\n".join(xml_lines)

        xml_format = """<urn:fields>\n{}\n</urn:fields>""".format(xml_lines)

        return xml_format


def send_soap_request(run_params, operatie):
    headers = {"content-type": "text/xml"}
    url = "https://www.easyflex.net/dataservice/"

    body = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:EfDataService">
        <soapenv:Header/>
            <soapenv:Body>
                <urn:{}>
                    <urn:license>{}</urn:license>
                    {}
                    {}
                </urn:{}>
        </soapenv:Body>
    </soapenv:Envelope>""".format(
        operatie.naam,
        run_params.apikey,
        operatie.parameters_xml,
        operatie.fields_xml,
        operatie.naam,
    )

    # logging.info(body)

    response = requests.post(url, data=body, headers=headers)

    return response


def namespaces():
    ns = {
        "ds": "http://schemas.xmlsoap.org/soap/envelope/",
        "urn": "urn:EfDataService",
        "schema": "http://www.w3.org/2001/XMLSchema-instance",
    }

    ns_txt = {k: "{" + v + "}" for k, v in ns.items()}

    return ns, ns_txt


def parse_records_v2(rec):
    ns, ns_txt = namespaces()
    kolomnaam, inhoud, datatype = list(), list(), list()

    for content in rec:
        kolomnaam.append(content.tag.replace(ns_txt["urn"], ""))  # kolomnaam
        inhoud.append(content.text)  # waarde van het veld
        datatype.append(
            content.attrib.get(ns_txt["schema"] + "type", "string").replace("xsd:", "")
        )  # xml schema geeft datatype aan

    data = dict(zip(kolomnaam, inhoud))

    return data


def parse_all_data(content, operatie):
    ns, ns_txt = namespaces()

    result = content.findall("urn:{}_result/urn:fields/urn:item".format(operatie.naam), ns)

    if not result:
        result = content.findall("urn:{}_result/urn:fields".format(operatie.naam), ns)

    data = list()  # maak een lege lijst aan waar records aan toe kunnen worden gevoegd

    for records in result:
        data.append(parse_records_v2(records))  # deze functie parsed de data

    if data[0] == dict():
        # print('yes')
        return pd.DataFrame()

    df = pd.DataFrame(data=data, index=range(len(data)))
    df.columns = [col.replace(operatie.naam[3:] + "_", "") for col in df.columns]

    df.columns = [operatie.output_prefix + col for col in df.columns]  # add prefix

    return df


def check_for_errors(content):
    ns, ns_txt = namespaces()

    if content[0].tag.replace(ns_txt["ds"], "") == "Fault":
        test = ET.tostring(content, encoding="utf8", method="xml")
        logging.error("foutcode van easyflex server: {}".format(test))
        exit()


def get_data(run_params, operatie):

    ns, ns_txt = namespaces()

    response = send_soap_request(run_params, operatie)
    content = ET.fromstring(response.content.decode("utf-8")).find("ds:Body", ns)

    check_for_errors(content)

    df = parse_all_data(content, operatie)

    logging.debug(
        "{} - {} - {} records pulled (runcount: {})".format(
            run_params.adm_code, operatie.naam, len(df), operatie.runcount
        )
    )
    return df
