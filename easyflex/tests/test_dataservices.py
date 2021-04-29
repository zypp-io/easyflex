from easyflex import Easyflex
import logging
from keyvault import get_keyvault_secrets


def test_ds_wm_medewerkers(api_keys):
    ef = Easyflex(api_keys, service="dataservice")
    data = ef.query(module="ds_wm_medewerkers")
    logging.info(f"end of script! imported {data.shape[0]} records")


if __name__ == "__main__":
    test_api_keys = get_keyvault_secrets(keyvault_name="easyflex-tests")
    test_ds_wm_medewerkers(test_api_keys)
