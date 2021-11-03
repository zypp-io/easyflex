import logging
import os
from datetime import datetime
from typing import Union
from xml.etree import ElementTree as Et

import pandas as pd
import requests

from easyflex.exceptions import ServiceNotKnownError


class OperatieParameters:
    def __init__(
        self,
        api_key: str,
        adm_code: str,
        naam: str,
        parameters: dict,
        fields: list,
        runcount: int,
        service: str,
        max_rows: int = 5000,
        inherit_datatypes=True,
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
        runcount: int
            De teller die het aantal verzoeken bijhoud. De eerste keer is deze 0
        service: str
            De dataservices die is gekozen
        max_rows: int
            het pagina limiet van de module. Deze staan genoemd in de Easyflex documentatie. Deze
            is in de meeste gevallen 5000
        """

        self.api_key = api_key
        self.adm_code = adm_code
        self.naam = naam
        self.runcount = runcount
        self.max_rows = max_rows
        self.parameters = self.create_parameters(parameters)  # vanaf en totenmet parameters
        self.fields = fields
        self.inherit_datatypes = inherit_datatypes

        self.headers = {"content-type": "text/xml", "charset": "utf8"}

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
        additional_params["vanaf"] = 1 + (self.max_rows * self.runcount)
        additional_params["totenmet"] = self.max_rows + (self.max_rows * self.runcount)

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

    @staticmethod
    def cast_datatypes(kolomnaam: str, kolomwaarde: str, datatype: str) -> Union[int, float, datetime, str]:
        """
        Deze functie converteerd de kolomwaardes naar de juiste data types. De datatypes worden
        in het xml attribuut benoemd. Omdat deze datatypes gekoppeld moeten worden aan de python
        datatypes is deze functie geschreven.

        Parameters
        ----------
        kolomnaam: str
            kolomnaam van de record
        kolomwaarde: str
            kolomwaarde van de record
        datatype: str
            het datatype van de kolom

        Returns
        -------
        kolomwaarde: Union[int, float, datetime, str]
            de kolomwaarde, omgezet van tekst naar het juiste datatype
        """

        if datatype == "xsd:int":
            kolomwaarde = int(kolomwaarde)
        elif datatype == "xsd:float":
            kolomwaarde = float(kolomwaarde)
        elif datatype == "xsd:long":
            kolomwaarde = int(kolomwaarde)
        elif datatype == "xsd:positiveInteger":
            kolomwaarde = int(kolomwaarde)
        elif datatype == "xsd:dateTime":
            kolomwaarde = datetime.strptime(kolomwaarde, "%Y-%m-%dT%H:%M:%S")
        elif datatype == "xsd:date":
            kolomwaarde = datetime.strptime(kolomwaarde, "%Y-%m-%d")
        elif datatype == "xsd:time":
            kolomwaarde = str(kolomwaarde)
        elif datatype == "xsd:base64Binary":
            kolomwaarde = str(kolomwaarde)
        elif datatype == "xsd:boolean":
            kolomwaarde = bool(kolomwaarde)
        elif datatype == "xsd:string":
            kolomwaarde = str(kolomwaarde)
        elif datatype is None:
            return kolomwaarde
        elif datatype.find("Array") != -1:
            return kolomwaarde
        else:
            logging.info(f"datatype {datatype} van veld {kolomnaam} niet gecast")

        return kolomwaarde

    def parse_array(self, items: Et.SubElement):
        """
        Soms zit er een array met informatie bijgevoegd, op 1 record van de response. Omdat we voor
        iedere module van de dataservices willen zorgen dat 1 record ook 1 record blijft, worden de
        arrays nu als list met dictionaries toegevoegd aan 1 cel van de dataframe.

        Parameters
        ----------
        items: Et.SubElement
            Et.SubElement waar de array in staat. Deze lijst bevat alle informatie van de array

        Returns
        -------

        """

        all_data = []  # lijst waar de waardes in worden gezet
        for item in items:  # iterate over alle elementen in de array

            information = dict()  # maak een dictionary aan voor iedere item in de array

            for x in item:  # zorg dat ieder item wordt opgeslagen in de dictionary
                information[x.tag.replace(self.ns_txt.get("urn"), "")] = x.text

            all_data.append(information)  # append de dictionary aan de dataset

        return all_data

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

        kolomnamen, kolomwaarden = list(), list()

        for content in rec:
            array_data = [x for x in content.attrib.values() if x.find("Array") != -1]
            if array_data:  # array data wordt anders behandeld, zie docstring parse_array.
                kolomwaarde = self.parse_array(content)
                kolomnaam = content.tag.replace(self.ns_txt["urn"], "")
            else:
                kolomwaarde = content.text  # kolomwaarde uitgedrukt in tekst
                kolomnaam = content.tag.replace(self.ns_txt["urn"], "")

                if self.inherit_datatypes:
                    datatype = content.attrib.get(f"{self.ns_txt.get('schema')}type")  # datatype vh veld
                    kolomwaarde = self.cast_datatypes(kolomnaam, kolomwaarde, datatype)  # pas kolomtype aan

            kolomnamen.append(kolomnaam)
            kolomwaarden.append(kolomwaarde)

        data = dict(zip(kolomnamen, kolomwaarden))

        return data

    @staticmethod
    def remove_common_prefix(df: pd.DataFrame) -> pd.DataFrame:
        """
        Deze functie zorgt ervoor dat de prefix van de kolomnamen wordt bepaald, en vervolgens
        verwijderd. vaak is de prefix gelijk aan de modulenaam, bijvoorbeeld ds_wm_medewerkers.
        Omdat dit niet altijd het geval is, bepalen we met os.path.commonprefix wat de prefix is.

        Parameters
        ----------
        df: pd.DataFrame
            dataset met oorspronkelijke kolomnamen

        Returns
        -------
        df: pd.DataFrame
            dataset met de nieuwe kolomnamen, zonder de overeenkomende voorvoegsel.
        """

        original_columns = df.columns.to_list()
        df.columns = df.columns.str.replace(os.path.commonprefix(original_columns), "")

        return df

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
        df = self.remove_common_prefix(df)
        # df = df.convert_dtypes()  # convert to best possible dtypes

        return df

    def parse_errors(self, content: Et.Element) -> pd.DataFrame:
        """

        Parameters
        ----------
        content: Et.Element
            De root van het XML bericht. Deze wordt gechecked op eventuele fouten

        Returns
        -------
        fault_df: pd.DataFrame
            Deze functie geeft een dataframe terug met fout gegevens.
        """

        fault = content.find("ds:Fault/detail/detail", self.ns)
        if fault is not None:
            logging.warning(f"{self.adm_code}: foutcode van easyflex server: {fault.text}")
            fault_df = pd.DataFrame([{**self.parameters, **{"foutcode": fault.text}}])
        else:
            fault_df = pd.DataFrame()

        return fault_df

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

    @staticmethod
    def add_array(parameter: Et.SubElement, items: list):
        """
        add_array kan gebruikt worden om arrays toe te voegen aan de soap requests.
        De arrays moeten in de vorm van items worden aangeboden. Deze items moeten aan de xml laag worden toegevoegd.
        Het is belangrijk dat de opbouw van de parameter in de vorm van een dictionary is. Zie voorbeeld.

        voorbeeld van correcte vorm van het 'parameters' argument in de functie query():
        parameters = {"rf_declidnrs": [{"rf_decl_idnr": "258176109"}]}

        Parameters
        ----------
        parameter: Et.SubElement
            Et element van de array.
        items: list
            lijst met dictionaries die de items vormen in de request.

        Returns
        -------

        """

        for item in items:

            # iedere item krijgt zijn eigen subelement 'item'
            item_xml = Et.SubElement(parameter, "item")

            if not isinstance(item, dict):
                logging.error("Array moet in de vorm van dictionary zijn.")

            for item_name, item_value in item.items():
                # vervolgens worden hier subelementen aantoegevoegd die in de dictionary zijn opgenomen.
                array_item_xml = Et.SubElement(item_xml, item_name)
                array_item_xml.text = item_value

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
                if isinstance(value, list):
                    self.add_array(parameter, items=value)
                else:
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

        xml_request_str = Et.tostring(xml_request, encoding="utf8")

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
        content = Et.fromstring(response.content.decode("utf8")).find("ds:Body", self.ns)
        errors = self.parse_errors(content)
        if not errors.empty:
            return errors

        df = self.parse_all_data(content)
        df["werkmaatschappij_code"] = self.adm_code

        return df
