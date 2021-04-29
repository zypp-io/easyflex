from xml.etree import ElementTree as Et
from easyflex.exceptions import ServiceNotKnownError
import requests
import logging
import pandas as pd


class OperatieParameters:
    def __init__(
        self,
        api_key: str,
        adm_code: str,
        naam: str,
        parameters: dict,
        fields: list,
        limit: int,
        runcount: int,
        service: str,
    ):
        """

        Parameters
        ----------
        api_key: str
            api_key van de administratie
        adm_code: str
            administratiecode van de administratie
        naam: str
            naam van de module
        parameters: dict
            parameters die horen bij de module
        fields: list
            veldnamen die uitgevraagd moeten worden
        limit: int
            het pagina limiet van de module. Deze staan genoemd in de Easyflex documentatie. Deze
            is in de meeste gevallen 5000
        runcount: int
            De teller die het aantal verzoeken bijhoud. De eerste keer is deze 0
        service: str
            De dataservices die is gekozen
        """

        self.api_key = api_key
        self.adm_code = adm_code
        self.naam = naam
        self.runcount = runcount
        self.limit = limit
        self.parameters = self.create_parameters(parameters)  # vanaf en totenmet parameters
        self.fields = fields

        self.headers = {"content-type": "text/xml"}

        # afhankelijk van de service worden de endpoints en namespaces bepaald.
        if service == "webservice":
            self.endpoint = "https://www.easyflex.net/webservice/"
            self.urn = "EfWebservice"
        elif service == "dataservice":
            self.endpoint = "https://www.easyflex.net/dataservice/"
            self.urn = "EfDataService"
        else:
            raise ServiceNotKnownError("service niet bekend. Kies uit dataservice of webservice")

        self.ns, self.ns_txt = self.namespaces()

    def parameters_vanaf_tm(self) -> dict:
        """

        Returns
        -------
        additional_params: dict
            de extra parameters vanaf en totenmet. Deze worden bepaald op basis van de runcount.
            Wanneer de runcount 0 is: vanaf 1 tot en met 5000. Wanneer de runcount 1 is wordt dit
            vanaf 5001 tot en met 10000.
        """

        additional_params = dict()
        additional_params["vanaf"] = 1 + (self.limit * self.runcount)
        additional_params["totenmet"] = self.limit + (self.limit * self.runcount)

        return additional_params

    def create_parameters(self, parameters: dict) -> dict:
        """

        Parameters
        ----------
        parameters: dict
            De parameters die door de gebruiker zijn opgegeven.

        Returns
        -------
        parameters: dict
            De oorspronkelijke parameters, aangevuld met vanaf en totenmet parameters.

        """
        if parameters is None:
            parameters = {}

        additional_parameters = self.parameters_vanaf_tm()
        parameters = {**parameters, **additional_parameters}

        return parameters

    def namespaces(self) -> tuple:
        """

        Returns
        -------
        ns, ns_txt: tuple
            De namespaces van de XML berichten. Deze verschillen voor de web- en dataservices.
        """

        ns = {
            "ds": "http://schemas.xmlsoap.org/soap/envelope/",
            "urn": f"urn:{self.urn}",
            "schema": "http://www.w3.org/2001/XMLSchema-instance",
        }

        ns_txt = {k: "{" + v + "}" for k, v in ns.items()}

        return ns, ns_txt

    def parse_records(self, rec: Et.Element) -> dict:
        """

        Parameters
        ----------
        rec: Et.Element
            Het XML element met de data (op recordniveau) die geparsed dient te worden.

        Returns
        -------
        data: dict
            dictionary met kolomnaam en kolominhoud van de record.
        """

        kolomnaam, inhoud = list(), list()

        for content in rec:
            kolomnaam.append(content.tag.replace(self.ns_txt["urn"], ""))  # kolomnaam
            inhoud.append(content.text)  # waarde van het veld

        data = dict(zip(kolomnaam, inhoud))

        return data

    def parse_all_data(self, content: Et.Element) -> pd.DataFrame:
        """

        Parameters
        ----------
        content: Et.Element
            De XML root van de response die van de Easyflex Server is gegeven.

        Returns
        -------
        df: pd.DataFrame
            Het resultaat van de parsing van het XML bericht in een pandas dataframe.
        """

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

    def check_for_errors(self, content: Et.Element) -> None:
        """

        Parameters
        ----------
        content: Et.Element
            De root van het XML bericht. Deze wordt gechecked op eventuele fouten

        Returns
        -------
        None
        Deze functie geeft een warning wanneer er een foutbericht van Easyflex is gekomen.
        """

        fault = content.find("ds:Fault/detail/detail", self.ns)
        if fault is not None:
            logging.warning("foutcode van easyflex server: {}".format(fault.text))

    def add_fields(self, topelement: Et.Element) -> None:
        """

        Parameters
        ----------
        topelement: Et.Element
            De body van het soap bericht. Aan deze body worden velden toegevoegd.

        Returns
        -------
        None
        Deze functie voegt de fields layer toe aan het topelement.
        """

        fields_xml = Et.SubElement(topelement, "fields")

        if self.fields is not None:
            for field in self.fields:
                Et.SubElement(fields_xml, f"urn:{field}")

    def add_parameters(self, topelement: Et.Element) -> None:
        """

        Parameters
        ----------
        topelement: Et.Element
            De body van het soap bericht. Aan deze body worden velden toegevoegd.

        Returns
        -------
        None
        Deze functie voegt de parameters layer toe aan het topelement.
        """
        parameters_xml = Et.SubElement(topelement, "parameters")

        if self.parameters is not None:
            for name, value in self.parameters.items():
                parameter = Et.SubElement(parameters_xml, name)
                parameter.text = str(value)

    def add_body(self, topelement: Et.Element) -> Et.SubElement:
        """

        Parameters
        ----------
        topelement: Et.Element
            De soap envelope van het bericht (toplaag).

        Returns
        -------
        module: Et.SubElement
            Deze functie maakt de body aan onder de soap envelope layer.
        """

        body = Et.SubElement(topelement, "soapenv:Body")
        module = Et.SubElement(body, f"urn:{self.naam}")
        license_xml = Et.SubElement(module, "urn:license")
        license_xml.text = self.api_key

        return module

    def create_envelope(self) -> Et.Element:
        """

        Returns
        -------
        xml_request: Et.Element
            Het volledige XML bericht dat verstuurd dient te worden.
        """

        xml_request = Et.Element("soapenv:Envelope")
        xml_request.set("xmlns:soapenv", "http://schemas.xmlsoap.org/soap/envelope/")
        xml_request.set("xmlns:urn", f"urn:{self.urn}")
        Et.SubElement(xml_request, "soapenv:Header")

        return xml_request

    def build_soap_request(self) -> str:
        """

        Returns
        -------
        xml_request_str: str
            Het XML bericht in string format
        """
        xml_request = self.create_envelope()
        body = self.add_body(xml_request)
        self.add_parameters(body)
        self.add_fields(body)

        xml_request_str = Et.tostring(xml_request).decode("utf-8")

        return xml_request_str

    def post_request(self) -> pd.DataFrame:
        """

        Returns
        -------
        df: pd.DataFrame
            resultaat van de uitvraag in de vorm van een pandas dataframe.
        """

        xml_request = self.build_soap_request()
        response = requests.post(url=self.endpoint, data=xml_request, headers=self.headers)
        content = Et.fromstring(response.content.decode("utf-8")).find("ds:Body", self.ns)
        self.check_for_errors(content)
        df = self.parse_all_data(content)

        return df
