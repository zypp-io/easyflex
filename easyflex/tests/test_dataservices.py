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


def test_ds_fw_persoonsgegevens_update():
    api_keys = get_keyvault_secrets(keyvault_name="staffing-easyflex-test")
    api_keys = {"9036": api_keys.get("9036")}
    ef = Easyflex(api_keys, service="dataservice")
    data = ef.query(
        module="ds_fw_persoonsgegevens_update",
        parameters={
            "voornaam": "Ötzi",
            "achternaam": "Önderhetîjs",
            "inschrijver": "28694",
            "relatiebeheerder": "28694",
            "locatie": 4846,
            "voorletters": "O",
            "geslacht": "20092",
            "plaats": "ARNHEM",
            "straat": "Kemperbergerweg",
            "huisnummer": "771",
            "land": "NL",
            "woonplaats": "ARNHEM",
            "woonstraat": "Kemperbergerweg",
            "woonhuisnummer": "771",
            "woonland": "NL",
            "woonpostcode": "6816RW",
            "postcode": "6816RW",
            "versie": "Easyflex test",
            "registratienummer": "6201913",
            "communicatie": [
                {
                    "commvolgnr": "0",
                    "commtype": "20016",
                    "commwaarde": "0612345678",
                    "commpersoonlijk": "0",
                    "commcollectief": "0",
                }
            ],
        },
    )

    logging.info(f"end of test! imported {data.shape[0]} records")

    return data


def test_ds_fw_persoonsgegevens():
    api_keys = get_keyvault_secrets(keyvault_name="staffing-easyflex-test")
    api_keys = {"9036": api_keys.get("9036")}
    ef = Easyflex(api_keys, service="dataservice")
    data = ef.query(module="ds_fw_persoonsgegevens", parameters={"registratienummer": "6201913"})

    logging.info(f"end of test! imported {data.shape[0]} records")

    return data


def test_ascii():
    test_ds_fw_persoonsgegevens_update()
    read = test_ds_fw_persoonsgegevens()

    assert read.iloc[0]["voornaam"] == "Ötzi"
    assert read.iloc[0]["achternaam"] == "Önderhetîjs"


if __name__ == "__main__":
    test_ds_fw_persoonsgegevens_memovelden()
    test_ds_fw_loonjournaalposten()
    test_ds_wm_medewerkers()
    test_ds_wm_locaties()
    test_ds_rf_declaratie_regels()
    test_ascii()
