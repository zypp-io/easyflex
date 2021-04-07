from easyflex.run_settings import get_client_settings
from easyflex.dataservices.ds_bi_declaratieregels import pull_data as ds_bi_declaratieregels
from easyflex.dataservices.ds_fw_arbeidscontract import pull_data as ds_fw_arbeidscontract
from easyflex.dataservices.ds_fw_fase_details import pull_data as ds_fw_fase_details
from easyflex.dataservices.ds_fw_fases import pull_data as ds_fw_fases
from easyflex.dataservices.ds_fw_huidige_fase import pull_data as ds_fw_huidige_fase
from easyflex.dataservices.ds_fw_journaalposten import pull_data as ds_fw_journaalposten
from easyflex.dataservices.ds_fw_loonjournaal import pull_data as ds_fw_loonjournaal
from easyflex.dataservices.ds_fw_memovelden import pull_data as ds_fw_memovelden
from easyflex.dataservices.ds_fw_persoonsgegevens_all import (
    pull_data as ds_fw_persoonsgegevens_all,
)
from easyflex.dataservices.ds_fw_persoonsgegevens_comm import (
    pull_data as ds_fw_persoonsgegevens_comm,
)
from easyflex.dataservices.ds_fw_reserveringen import pull_data as ds_fw_reserveringen
from easyflex.dataservices.ds_fw_wwpremie import pull_data as ds_fw_wwpremie
from easyflex.dataservices.ds_rf_declaraties_perioden import (
    pull_data as ds_rf_declaraties_perioden,
)
from easyflex.dataservices.ds_rl_bedrijfsgegevens import pull_data as ds_rl_bedrijfsgegevens
from easyflex.dataservices.ds_rl_fw_uren import pull_data as ds_rl_fw_uren
from easyflex.dataservices.ds_rl_kostenplaatsen import pull_data as ds_rl_kostenplaatsen
from easyflex.dataservices.ds_rl_plaatsing_looncomponent import (
    pull_data as ds_rl_plaatsing_looncomponent,
)
from easyflex.dataservices.ds_rl_plaatsingen import pull_data as ds_rl_plaatsingen
from easyflex.dataservices.ds_wm_locaties import pull_data as ds_wm_locaties
from easyflex.dataservices.ds_wm_medewerkers import pull_data as ds_wm_medewerkers
from easyflex.dataservices.ds_wm_notificaties import pull_data as ds_wm_notificaties
from easyflex.dataservices.ds_fw_loonspecificatiegegevens import (
    pull_data as ds_fw_loonspecificatiegegevens,
)
from easyflex.dataservices.ds_fw_loonspecificatie import pull_data as ds_fw_loonspecificatie
from easyflex.dataservices.ds_rf_declaraties_all import pull_data as ds_rf_declaraties_all

from easyflex.dataservices.functions import EasyflexParameters
import logging


def query_all(run_params, module, incremental, day_offset, years, debug):

    werkmaatschappijen = get_client_settings()["administraties"]

    for wm in werkmaatschappijen:
        run_params = EasyflexParameters(
            client=run_params.client,
            years=years,
            wm=wm,
            incremental=incremental,
            day_offset=day_offset,
            debug=debug,
        )

        query_dataservices(run_params, module)


def query_dataservices(run_params, module):
    if module == "ds_wm_medewerkers":
        ds_wm_medewerkers.pull_module(run_params, "ds_wm_medewerkers")
    elif module == "ds_bi_declaratieregels":
        ds_bi_declaratieregels.pull_module(run_params, "ds_bi_declaratieregels")
    elif module == "bi_matchkeys":
        ds_bi_declaratieregels.pull_module(run_params, "bi_matchkeys")
    elif module == "ds_rl_kostenplaatsen":
        ds_rl_kostenplaatsen.pull_module(run_params, "ds_rl_kostenplaatsen")
    elif module == "ds_fw_fases":
        ds_fw_fases.pull_module(run_params, "ds_fw_fases")
    elif module == "ds_fw_huidige_fase":
        ds_fw_huidige_fase.pull_module(run_params, "ds_fw_huidige_fase")
    elif module == "ds_fw_fase_details":
        ds_fw_fase_details.pull_module(run_params, "ds_fw_fase_details")
    elif module == "ds_rl_bedrijfsgegevens":
        ds_rl_bedrijfsgegevens.pull_module(run_params, "ds_rl_bedrijfsgegevens")
    elif module == "ds_wm_locaties":
        ds_wm_locaties.pull_module(run_params, "ds_wm_locaties")
    elif module == "ds_rl_plaatsingen":
        ds_rl_plaatsingen.pull_module(run_params, "ds_rl_plaatsingen")
    elif module == "ds_rl_plaatsing_looncomponent":
        ds_rl_plaatsing_looncomponent.pull_module(run_params, "ds_rl_plaatsing_looncomponent")
    elif module == "ds_fw_loonjournaal":
        ds_fw_loonjournaal.pull_module(run_params, "ds_fw_loonjournaal")
    elif module == "ds_fw_journaalposten":
        ds_fw_journaalposten.pull_module(run_params, "ds_fw_journaalposten")
    elif module == "ds_rl_fw_uren":
        ds_rl_fw_uren.pull_module(run_params, "ds_rl_fw_uren")
    elif module == "ds_fw_arbeidscontract":
        ds_fw_arbeidscontract.pull_module(run_params, "ds_fw_arbeidscontract")
    elif module == "ds_fw_memovelden":
        ds_fw_memovelden.pull_module(run_params, "ds_fw_memovelden")
    elif module == "ds_fw_persoonsgegevens_all":
        ds_fw_persoonsgegevens_all.pull_module(run_params, "ds_fw_persoonsgegevens_all")
    elif module == "ds_fw_persoonsgegevens_comm":
        ds_fw_persoonsgegevens_comm.pull_module(run_params, "ds_fw_persoonsgegevens_comm")
    elif module == "ds_fw_wwpremie":
        ds_fw_wwpremie.pull_module(run_params, "ds_fw_wwpremie")
    elif module == "ds_rf_declaraties_perioden":
        ds_rf_declaraties_perioden.pull_module(run_params, "ds_rf_declaraties_perioden")
    elif module == "ds_fw_reserveringen":
        ds_fw_reserveringen.pull_module(run_params, "ds_fw_reserveringen")
    elif module == "ds_wm_notificaties":
        ds_wm_notificaties.pull_module(run_params, "ds_wm_notificaties")
    elif module == "ds_fw_loonspecificatiegegevens":
        ds_fw_loonspecificatiegegevens.pull_module(run_params, "ds_fw_loonspecificatiegegevens")
    elif module == "ds_fw_loonspecificatie":
        ds_fw_loonspecificatie.save_pdf_loonspec(run_params, "ds_fw_loonspecificatie")
    elif module == "ds_rf_declaraties_all":
        ds_rf_declaraties_all.pull_module(run_params, "ds_rf_declaraties_all")
    else:
        logging.info(f"module {module} onbekend.")
