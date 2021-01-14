import numpy as np
import pandas as pd
import requests

from .functions import get_namespaces
from .functions import make_xml_msg
from .functions import parse_error
from .functions import parse_success
from .functions import read_results


def parse_results(body, run_params):
    ns, ns_txt = get_namespaces()

    if body[0].tag.replace(ns_txt["env"], "") == "Fault":
        df = parse_error(body.find("env:Fault", ns))
    else:
        success_msg = body.find(
            f"urn:{run_params.module}_result/urn:fields/urn:item", ns
        )  # soms met een laag item erin

        if success_msg:
            df = parse_success(success_msg)

        else:
            success_msg = body.find(
                f"urn:{run_params.module}_result/urn:fields", ns
            )  # andere keren zonder item
            df = parse_success(success_msg)

    return df


def soap_request_body(run_params, statements):
    l = list()
    for (
        statement
    ) in (
        statements
    ):  # hier worden de juiste waardes uit de dataframe geplaatst in het format van de insert statement

        body_parameters = make_xml_msg(statement)

        body = f""" 
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:EfDataService">    
        <soapenv:Header/>    
            <soapenv:Body>       
                <urn:{run_params.module}>          
                <urn:license>{run_params.apikey}</urn:license>          
                    <urn:parameters>
                       {body_parameters}                  
                    </urn:parameters>               
                </urn:{run_params.module}>    
            </soapenv:Body> 
        </soapenv:Envelope>
        """

        l.append(body)

    return l


def soap_request_builder(
    run_params, statements
):  # template of the soap request, needed for the insert statement.

    print(
        "\n",
        f"started soap request {run_params.module}",
        "Werkmaatschappij:",
        run_params.adm_naam,
    )
    responsedict = dict()

    for (
        statement
    ) in (
        statements
    ):  # hier worden de juiste waardes uit de dataframe geplaatst in het format van de insert statement

        Id = statement["Id"]
        print(Id)
        apikey = run_params.apikey
        body_parameters = make_xml_msg(statement)

        url = "https://www.easyflex.net/dataservice/"
        headers = {"content-type": "text/xml"}

        body = f""" 
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:EfDataService">    
        <soapenv:Header/>    
            <soapenv:Body>       
                <urn:{run_params.module}>          
                <urn:license>{apikey}</urn:license>          
                    <urn:parameters>
                       {body_parameters}                  
                    </urn:parameters>               
                </urn:{run_params.module}>    
            </soapenv:Body> 
        </soapenv:Envelope>
        """
        # send the insert request.
        # print(body)
        response = requests.post(url, data=body, headers=headers).content.decode("utf-8")
        responsedict[Id] = response

    return responsedict


def send_request(run_params, statements):
    resultdict = soap_request_builder(run_params, statements)

    df = pd.DataFrame()

    for Id, result in resultdict.items():
        body = read_results(result)
        status = parse_results(body, run_params)
        status["Id"] = Id

        if "foutbericht" not in df.columns:
            df["foutbericht"] = np.nan
        else:
            None

        df = pd.concat([df, status], axis=0, sort=False, ignore_index=True)

    return df
