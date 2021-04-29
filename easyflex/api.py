from xml.etree import ElementTree as Et
from easyflex.exceptions import ServiceNotKnownError
import requests
import logging
import pandas as pd


class OperatieParameters:
    def __init__(self, api_key, adm_code, naam, parameters, fields, limit, runcount, service):
        self.api_key = api_key
        self.adm_code = adm_code
        self.naam = naam
        self.runcount = runcount
        self.limit = limit
        self.parameters = self.create_parameters(parameters)
        self.fields = fields

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

    def parameters_vanaf_tm(self):

        additional_params = dict()
        additional_params["vanaf"] = 1 + (self.limit * self.runcount)
        additional_params["totenmet"] = self.limit + (self.limit * self.runcount)

        return additional_params

    def create_parameters(self, parameters):
        if parameters is None:
            parameters = {}

        additional_parameters = self.parameters_vanaf_tm()
        parameters = {**parameters, **additional_parameters}

        return parameters

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

        result = content.findall(f"urn:{self.naam}_result/urn:fields/urn:item", self.ns)

        if not result:
            result = content.findall(f"urn:{self.naam}_result/urn:fields", self.ns)

        data = list()  # maak een lege lijst aan waar records aan toe kunnen worden gevoegd

        for records in result:
            data.append(self.parse_records(records))  # deze functie parsed de data

        if not len(data):
            return pd.DataFrame()
        if data[0] == dict():
            return pd.DataFrame()

        df = pd.DataFrame(data=data, index=range(len(data)))

        return df

    def check_for_errors(self, content):

        fault = content.find("ds:Fault/detail/detail", self.ns)
        if fault is not None:
            logging.warning("foutcode van easyflex server: {}".format(fault.text))

    def add_fields(self, topelement):
        fields_xml = Et.SubElement(topelement, "fields")

        if self.fields is not None:
            for field in self.fields:
                Et.SubElement(fields_xml, f"urn:{field}")

    def add_parameters(self, topelement):
        parameters_xml = Et.SubElement(topelement, "parameters")

        if self.parameters is not None:
            for name, value in self.parameters.items():
                parameter = Et.SubElement(parameters_xml, name)
                parameter.text = str(value)

    def add_body(self, topelement):
        body = Et.SubElement(topelement, "soapenv:Body")
        module = Et.SubElement(body, f"urn:{self.naam}")
        license_xml = Et.SubElement(module, "urn:license")
        license_xml.text = self.api_key

        return module

    def create_envelope(self):

        xml_request = Et.Element("soapenv:Envelope")
        xml_request.set("xmlns:soapenv", "http://schemas.xmlsoap.org/soap/envelope/")
        xml_request.set("xmlns:urn", f"urn:{self.urn}")
        Et.SubElement(xml_request, "soapenv:Header")

        return xml_request

    def build_soap_request(self):
        xml_request = self.create_envelope()
        body = self.add_body(xml_request)
        self.add_parameters(body)
        self.add_fields(body)

        xml_request_str = Et.tostring(xml_request)

        return xml_request_str

    def post_request(self):

        xml_request = self.build_soap_request()
        response = requests.post(url=self.endpoint, data=xml_request, headers=self.headers)
        content = Et.fromstring(response.content.decode("utf-8")).find("ds:Body", self.ns)
        self.check_for_errors(content)
        df = self.parse_all_data(content)

        return df
