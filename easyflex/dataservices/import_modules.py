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


def query_all(run_params, modules, incremental, day_offset, years):

    werkmaatschappijen = get_client_settings()["administraties"]

    for wm in werkmaatschappijen:
        run_params = EasyflexParameters(
            client=run_params.client,
            years=years,
            wm=wm,
            incremental=incremental,
            day_offset=day_offset,
        )

        query_dataservices(run_params, modules)


def query_dataservices(run_params, modules):
    if "ds_wm_medewerkers" in modules:
        ds_wm_medewerkers.pull_module(run_params, "ds_wm_medewerkers")
    if "ds_bi_declaratieregels" in modules:
        ds_bi_declaratieregels.pull_module(run_params, "ds_bi_declaratieregels")
    if "bi_matchkeys" in modules:
        ds_bi_declaratieregels.pull_module(run_params, "bi_matchkeys")
    if "ds_rl_kostenplaatsen" in modules:
        ds_rl_kostenplaatsen.pull_module(run_params, "ds_rl_kostenplaatsen")
    if "ds_fw_fases" in modules:
        ds_fw_fases.pull_module(run_params, "ds_fw_fases")
    if "ds_fw_huidige_fase" in modules:
        ds_fw_huidige_fase.pull_module(run_params, "ds_fw_huidige_fase")
    if "ds_fw_fase_details" in modules:
        ds_fw_fase_details.pull_module(run_params, "ds_fw_fase_details")
    if "ds_rl_bedrijfsgegevens" in modules:
        ds_rl_bedrijfsgegevens.pull_module(run_params, "ds_rl_bedrijfsgegevens")
    if "ds_wm_locaties" in modules:
        ds_wm_locaties.pull_module(run_params, "ds_wm_locaties")
    if "ds_rl_plaatsingen" in modules:
        ds_rl_plaatsingen.pull_module(run_params, "ds_rl_plaatsingen")
    if "ds_rl_plaatsing_looncomponent" in modules:
        ds_rl_plaatsing_looncomponent.pull_module(run_params, "ds_rl_plaatsing_looncomponent")
    if "ds_fw_loonjournaal" in modules:
        ds_fw_loonjournaal.pull_module(run_params, "ds_fw_loonjournaal")
    if "ds_fw_journaalposten" in modules:
        ds_fw_journaalposten.pull_module(run_params, "ds_fw_journaalposten")
    if "ds_rl_fw_uren" in modules:
        ds_rl_fw_uren.pull_module(run_params, "ds_rl_fw_uren")
    if "ds_fw_arbeidscontract" in modules:
        ds_fw_arbeidscontract.pull_module(run_params, "ds_fw_arbeidscontract")
    if "ds_fw_memovelden" in modules:
        ds_fw_memovelden.pull_module(run_params, "ds_fw_memovelden")
    if "ds_fw_persoonsgegevens_all" in modules:
        ds_fw_persoonsgegevens_all.pull_module(run_params, "ds_fw_persoonsgegevens_all")
    if "ds_fw_persoonsgegevens_comm" in modules:
        ds_fw_persoonsgegevens_comm.pull_module(run_params, "ds_fw_persoonsgegevens_comm")
    if "ds_fw_wwpremie" in modules:
        ds_fw_wwpremie.pull_module(run_params, "ds_fw_wwpremie")
    if "ds_rf_declaraties_perioden" in modules:
        ds_rf_declaraties_perioden.pull_module(run_params, "ds_rf_declaraties_perioden")
    if "ds_fw_reserveringen" in modules:
        ds_fw_reserveringen.pull_module(run_params, "ds_fw_reserveringen")
    if "ds_wm_notificaties" in modules:
        ds_wm_notificaties.pull_module(run_params, "ds_wm_notificaties")
    if "ds_fw_loonspecificatiegegevens" in modules:
        ds_fw_loonspecificatiegegevens.pull_module(run_params, "ds_fw_loonspecificatiegegevens")
    if "ds_fw_loonspecificatie" in modules:
        ds_fw_loonspecificatie.save_pdf_loonspec(run_params, "ds_fw_loonspecificatie")
    if "ds_rf_declaraties_all" in modules:
        ds_rf_declaraties_all.pull_module(run_params, "ds_rf_declaraties_all")
