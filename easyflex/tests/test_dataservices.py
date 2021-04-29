from easyflex import Easyflex
import logging
from keyvault import get_keyvault_secrets

"""
Test suite voor Easyflex dataservices

"""


def test_ds_wm_medewerkers():
    api_keys = get_keyvault_secrets(keyvault_name="easyflex-tests")
    ef = Easyflex(api_keys, service="dataservice")
    data = ef.query(module="ds_wm_medewerkers")
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
    test_ds_wm_medewerkers()
    test_ds_wm_locaties()
