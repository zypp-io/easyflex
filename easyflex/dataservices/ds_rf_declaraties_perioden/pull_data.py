import pandas as pd
import logging

from easyflex.dataservices.soap_request import get_data
from easyflex.functions import cat_modules, export_module

from .fields import pull_fields
from .format import format_data


class OperatieParameters:
    def __init__(self, naam, output_prefix, parameters):

        self.naam = naam
        self.runcount = 1

        self.parameters_xml = self.parameters_xml(parameters)
        self.fields_xml = """			<urn:fields>
			</urn:fields>"""
        self.output_prefix = output_prefix

    def parameters_xml(self, parameters):
        xml_lines = list()

        for parameter_name, parameter_value in parameters.items():
            xml_lines.append(
                "<urn:{}>{}</urn:{}>".format(parameter_name, parameter_value, parameter_name)
            )

        xml_lines = "\n".join(xml_lines)
        xml_format = """<urn:parameters>\n{}\n</urn:parameters>""".format(xml_lines)

        return xml_format


def pull_module(run_params, module_name):
    data = pd.DataFrame()
    velden = pull_fields()

    regnrs = cat_modules(run_params, "ds_fw_persoonsgegevens_all")
    regnrs = regnrs[regnrs["werkmaatschappij"] == run_params.adm_code]
    regnrs = regnrs.fw_registratienummer.to_list()

    logging.info("declaratie periodes opvragen voor {} flexwerkers".format(len(regnrs)))
    for regnr in regnrs:
        runcount = 0
        run = True
        logging.info("start uitvraag voor regnr {}".format(regnr))
        while run:
            parameters = {"rf_decl_fwregnr": regnr, "rf_decl_status": 2}
            operatie = OperatieParameters(
                naam="ds_rf_declaraties_perioden",
                output_prefix="rl_",
                parameters=parameters,
            )

            df = get_data(run_params, operatie)

            data = pd.concat([data, df], sort=False, ignore_index=True)
            run = False
            runcount += 1

    data = format_data(data)
    if len(data):
        export_module(data, module_name, run_params)
