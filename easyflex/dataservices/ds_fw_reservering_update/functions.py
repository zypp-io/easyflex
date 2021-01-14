import xml.etree.ElementTree as ET

import pandas as pd


class DataServicesParameters:
    """Toelichting:
    \tDeze class is voor het toewijzen van administratie specifieke parameters.
    \t**********************
    \tapikey: de sleutel die je via je easyflex portal kan aanvragen.
    \tadm_code: deze wordt aan de dataset toegevoegd, zodat meerdere adm. tergelijkertijd kunnen worden ingelezen.
    \tadm_naam: naam van de easyflex administratie
    \tprojectdir: waar moeten de resultaten worden opgeslagen
    \tmodule: welke module van de dataservices wordt gebruikt?
    \t**********************
    """

    def __init__(self, apikey, adm_code, adm_naam, projectdir, module):
        self.apikey = apikey
        self.adm_code = adm_code
        self.adm_naam = adm_naam
        self.projectdir = projectdir
        self.module = module


class StatementParameters:
    """Toelichting:
    \tDeze class maakt een insert statement aan volgens de easyflex dataservices handleiding.

    verwijzing easyflex documentatie:
    \tAlle velden in de easyflex dataservices kunnen gebruikt worden.
    \tDe easyflex dataservices is te downloaden in easyflex.
    De class wordt in het SOAP bericht geparsed in het noodzakelijke format.
    """

    def __init__(self, **kwargs):  # Het aantal attributen is variabel.
        for attr in kwargs.keys():
            if str(kwargs[attr]) != "nan":
                self.__dict__[attr] = kwargs[attr]
            else:
                continue


def parse_error(
    error_msg,
):  # functie om de foutmelding die terug komt uit de dataservices te ontsluiten
    df = pd.DataFrame(index=[0])
    df["status"] = "niet ingelezen"

    for child in error_msg.find("detail"):

        if child.tag == "detail":
            df["foutbericht"] = child.text
        else:
            continue
    return df


def parse_success(
    success_msg,
):  # functie om de informatie te ontsluiten die terugkomt na een succesvolle insert
    df = pd.DataFrame(index=[0])
    df["status"] = "succesvol ingelezen"

    ns, ns_txt = get_namespaces()
    for child in success_msg:
        if child.text != None:
            df[child.tag.replace(ns_txt["urn"], "")] = child.text
        else:
            continue

    return df


def get_namespaces():
    ns_txt = {
        "env": "{http://schemas.xmlsoap.org/soap/envelope/}",
        "urn": "{urn:EfDataService}",
    }
    ns = {
        "env": "http://schemas.xmlsoap.org/soap/envelope/",
        "urn": "urn:EfDataService",
    }

    return ns, ns_txt


def read_results(result):
    ns, ns_txt = get_namespaces()
    root = ET.fromstring(result)
    body = root.find("env:Body", ns)

    return body


def make_xml_msg(
    statement,
):  # maakt op basis van de dictionary een xml bericht dat verstuurd gaat worden naar de easyflex endpoint

    xmlstrings = list()

    for k, v in statement.items():

        if k != "Id":
            urn = f"<urn:{k}>{v}</urn:{k}>"  # conform documentatie easyflex
            xmlstrings.append(urn)

        else:
            continue

    xml_msg = "".join(xmlstrings)

    return xml_msg
