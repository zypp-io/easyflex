# Changelog

Alle wijzigingen in dit project worden hier gedocumenteerd.

## 0.0.21 - 2021-11-03

- Fix bug where latin1 characters are not properly encoded
- added test for inserting latin1 characters (e.g. Umlaut)

## 0.0.20 - 2021-09-06

- Fix bug where we check for empty dict instead of dict type

## 0.0.19 - 2021-08-17

- Set requirements version to specific value

## 0.0.18 - 2021-08-12

- Toevoegen van array ondersteuning aan parameters argument.Wanneer een list aan dictionaries wordt opgegeven in de value van de parameter, zullen deze items als array worden toegevoegd aan de request. Dit is noodzakelijk voor bijvoorbeeld veld rf_decl_idnrs in module ds_rf_declaratie_regels

    ```python
    ef.query(module="ds_rf_declaratie_regels",
             parameters={
                    "rf_decl_plaatsingnummer": "8174729",
                    "rf_decl_idnrs": [{"rf_decl_idnr": "258174536"}]
                    }
    ```

## 0.0.17 - 2021-08-06

-  Zet argument `inherit_datatypes` op default `False` in plaats van `True`.
