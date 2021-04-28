import logging
import os
from xml.etree import ElementTree as Et
import requests
import pandas as pd
from easyflex.exceptions import ServiceNotKnownError


class OperatieParameters:
    def __init__(self, api_key, adm_code, naam, parameters, fields, limit, runcount, service):
        self.api_key = api_key
        self.adm_code = adm_code
        self.naam = naam
        self.runcount = runcount
        self.limit = limit

        parameters = self.vanaf_tm(parameters)

        self.parameters_xml = self.parameters_xml(parameters)
        self.fields_xml = self.fields_xml(fields)

        self.headers = {"content-type": "text/xml"}

        if service == "webservice":
            self.endpoint = "https://www.easyflex.net/webservice/"
            self.urn = "EfWebservice"
        elif service == "dataservice":
            self.endpoint = "https://www.easyflex.net/dataservice/"
            self.urn = "EfDataService"
        else:
            raise ServiceNotKnownError("service niet bekend. Kies uit dataservice of webservice")

        self.ns, self.ns_txt = self.namespaces()

    def vanaf_tm(self, parameters):

        parameters["vanaf"] = 1 + (self.limit * self.runcount)
        parameters["totenmet"] = self.limit + (self.limit * self.runcount)

        return parameters

    @staticmethod
    def parameters_xml(parameters):

        xml_lines = list()

        for parameter_name, parameter_value in parameters.items():
            xml_lines.append(
                "<urn:{}>{}</urn:{}>".format(parameter_name, parameter_value, parameter_name)
            )

        xml_lines = "\n".join(xml_lines)
        xml_format = """<urn:parameters>\n{}\n</urn:parameters>""".format(xml_lines)

        return xml_format

    @staticmethod
    def fields_xml(fields):

        if fields is None:
            fields = list()
        xml_lines = ["""<urn:{}></urn:{}>""".format(x, x) for x in fields]
        xml_lines = "\n".join(xml_lines)

        xml_format = """<urn:fields>\n{}\n</urn:fields>""".format(xml_lines)

        return xml_format

    def export_module(self, data, module_name, run_params):

        data["werkmaatschappij"] = run_params.adm_code

        filename = "{}_{}.pkl".format(run_params.adm_code, module_name)
        data.to_pickle(os.path.join(run_params.pickledir, filename))
        logging.info("{} geexporteerd! ({} records)".format(filename, len(data)))

    def check_for_errors(self, content):

        fault = content.find("ds:Fault/detail/detail", self.ns)
        if fault is not None:
            logging.warning("foutcode van easyflex server: {}".format(fault.text))

    def create_body(self):
        body = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:{self.urn}">
                    <soapenv:Header/>
                        <soapenv:Body>
                            <urn:{self.naam}>
                                <urn:license>{self.api_key}</urn:license>
                                {self.parameters_xml}
                                {self.fields_xml}
                            </urn:{self.naam}>
                    </soapenv:Body>
                </soapenv:Envelope>"""

        return body

    def namespaces(self):
        ns = {
            "ds": "http://schemas.xmlsoap.org/soap/envelope/",
            "urn": f"urn:{self.urn}",
            "schema": "http://www.w3.org/2001/XMLSchema-instance",
        }

        ns_txt = {k: "{" + v + "}" for k, v in ns.items()}

        return ns, ns_txt

    def parse_records(self, rec):

        kolomnaam, inhoud, datatype = list(), list(), list()

        for content in rec:
            kolomnaam.append(content.tag.replace(self.ns_txt["urn"], ""))  # kolomnaam
            inhoud.append(content.text)  # waarde van het veld
            datatype.append(
                content.attrib.get(self.ns_txt["schema"] + "type", "string").replace("xsd:", "")
            )  # xml schema geeft datatype aan

        data = dict(zip(kolomnaam, inhoud))

        return data

    def parse_all_data(self, content):

        result = content.findall("urn:{}_result/urn:fields/urn:item".format(self.naam), self.ns)

        if not result:
            result = content.findall("urn:{}_result/urn:fields".format(self.naam), self.ns)

        data = list()  # maak een lege lijst aan waar records aan toe kunnen worden gevoegd

        for records in result:
            data.append(self.parse_records(records))  # deze functie parsed de data

        if not len(data):
            return pd.DataFrame()
        if data[0] == dict():
            return pd.DataFrame()

        df = pd.DataFrame(data=data, index=range(len(data)))

        return df

    def post_request(self):

        body = self.create_body()
        response = requests.post(url=self.endpoint, data=body, headers=self.headers)
        content = Et.fromstring(response.content.decode("utf-8")).find("ds:Body", self.ns)
        self.check_for_errors(content)
        df = self.parse_all_data(content)

        return df
