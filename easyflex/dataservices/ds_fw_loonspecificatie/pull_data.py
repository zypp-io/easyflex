import logging
from tqdm import tqdm
from easyflex.functions import cat_modules
from .azure import upload_to_blob
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import base64
import os


def save_pdf_loonspec(run_params, module_name):

    logging.info("let op: dit script gebruikt data uit de module ds_fw_loonspecificatiegegevens.")

    try:
        df = cat_modules(
            run_params, "{}_ds_fw_loonspecificatiegegevens.pkl".format(run_params.adm_code)
        )
    except:
        return logging.info(f"geen gegevens voor WM {run_params.adm_code}")
    starttime = datetime.now()

    for k, v in tqdm(df.iterrows(), total=df.shape[0]):

        registratienummer = v["lsg_registratienummer"]
        loonspecificatienummer = v["lsg_loonspecificatienummer"]
        werkmaatschappij = v["werkmaatschappij"]

        url = "https://www.easyflex.net/dataservice/"
        headers = {"content-type": "text/xml"}

        body = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:EfDataService">
                    <soapenv:Header/>
                        <soapenv:Body>
                            <urn:{module_name}>
                                 <urn:license>{run_params.apikey}</urn:license>
                                <urn:parameters>
                                    <urn:loonspecificatienummer>{loonspecificatienummer}</urn:loonspecificatienummer>
                                </urn:parameters>
                                <urn:fields>
                                </urn:fields>
                            </urn:{module_name}>
                        </soapenv:Body>
                    </soapenv:Envelope>"""

        response = requests.post(url, data=body, headers=headers).content.decode("utf-8")

        root = ET.fromstring(response)

        namespaces = {
            "ds": "http://schemas.xmlsoap.org/soap/envelope/",
            "urn": "urn:EfDataService",
        }  # namespaces in de xml

        Body = root.find("ds:Body", namespaces)  # lees de response uit

        base64pdf = Body.find(
            "urn:ds_fw_loonspecificatie_result/urn:fields/urn:ds_fw_loonspecificatie", namespaces
        ).text

        base64pdf = base64pdf.encode(
            "ascii"
        )  # De base 64 data moet worden omgezet naar ascii format (encoden)
        # Zet het om naar een PDF bestand
        filename = f"wm_{werkmaatschappij}_fw_{registratienummer}_ls_{loonspecificatienummer}.pdf"
        file_path = os.path.join(run_params.inputdir, filename)
        with open(file_path, "wb") as f:
            f.write(base64.decodebytes(base64pdf))
        upload_to_blob(file_path, filename)
        logging.debug(f"{filename} succesvol opgeslagen. runtime:{datetime.now() - starttime}")
