import logging

from keyvault import get_keyvault_secrets

from easyflex import Easyflex

"""
Test suite voor Easyflex dataservices

"""


def test_ds_fw_loonjournaalposten():
    api_keys = get_keyvault_secrets(keyvault_name="easyflex-tests")
    ef = Easyflex(api_keys, service="dataservice")
    data = ef.query(module="ds_fw_journaalposten", parameters={"jaar": 2021, "week": 2})
    logging.info(f"end of test! imported {data.shape[0]} records")


def test_ds_wm_medewerkers():
    api_keys = get_keyvault_secrets(keyvault_name="easyflex-tests")
    ef = Easyflex(api_keys, service="dataservice")
    data = ef.query(module="ds_wm_medewerkers")
    logging.info(f"end of test! imported {data.shape[0]} records")


def test_ds_rf_declaratie_regels():

    api_keys = get_keyvault_secrets(keyvault_name="easyflex-tests")
    ef = Easyflex(api_keys, service="dataservice")

    data = ef.query(
        "ds_rf_declaratie_regels",
        parameters={"rf_decl_plaatsingnummer": "8174729", "rf_decl_idnrs": [{"rf_decl_idnr": "258174536"}]},
    )

    logging.info(f"end of test! imported {data.shape[0]} records")


def test_ds_fw_persoonsgegevens_memovelden():
    api_keys = get_keyvault_secrets(keyvault_name="easyflex-tests")
    ef = Easyflex(api_keys, service="dataservice")
    data = ef.query(
        module="ds_fw_persoonsgegevens_all",
        parameters={"memorubrieken": "true", "registratienummer": "5477778"},
        velden=[
            "fw_persoonsgegevens_all_registratienummer",
            "fw_persoonsgegevens_all_achternaam",
            "fw_persoonsgegevens_all_memorubrieken",
        ],
    )
    logging.info(f"end of test! imported {data.shape[0]} records")


def test_ds_wm_locaties():
    api_keys = get_keyvault_secrets(keyvault_name="easyflex-tests")
    ef = Easyflex(api_keys, service="dataservice")
    data = ef.query(
        module="ds_wm_locaties",
        parameters={"status": 21690},
        velden=["wm_locatie_nummer", "wm_locatie_code", "wm_locatie_naam"],
    )

    logging.info(f"end of test! imported {data.shape[0]} records")


if __name__ == "__main__":
    # test_ds_fw_persoonsgegevens_memovelden()
    # test_ds_fw_loonjournaalposten()
    # test_ds_wm_medewerkers()
    # test_ds_wm_locaties()
    test_ds_rf_declaratie_regels()
