# Changelog

Alle wijzigingen in dit project worden hier gedocumenteerd.

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
