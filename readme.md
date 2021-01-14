# Easyflex dataservices API voor python
Dit project bevat python scripts voor het ontsluiten van data uit de easyflex API.
De datasets zijn groot en worden met een .csv bestand weggeschreven naar Azure Blob Storage.
In Azure Data Factory gaat een process trigger af en de data wordt in de database geladen.

## De aanpak:
1. Pas de klant settings aan op basis van de template `yml/default/template_client_settings.yml` <br>
    ```
    - client_name: naam van de klant
    - administraties: lijst met administraties die in de routine gebruikt moeten worden
    - api_keys: lijst van alle API Keys
     ```
   
2. Kies de modules en analyses op basis van de template `yml/default/default_run_settings.yml`
   ```
    - refresh: True voor verversen van API data
    - upload: True voor uploaden naar Azure
    - years: lijst met jaren voor het ophalen van historische data
    - debug: True voor testruns
    - modules: lijst met gekozen module nummers (zie `yml/catalog/default/` voor keuzes)
    - analysis: lijst met gekozen analyse nummers (zie `yml/catalog/default/` voor keuzes)
    - incremental: Optie om alleen nieuwe records binnen te halen
    - day_offset: aantal dagen sinds vandaag: bedoeld om records binnen te halen vanaf deze datum
   ```
3. Plaats de 2 yaml bestanden in de map `yml/custom/`
4. run het script `main.py`
5. voor crontab: gebruik main.py `yml/custom/<run_settings_file>.yml` om specifieke parameters te gebruiken.


## Onderhouden door:

- [Melvin Folkers](https://github.com/melvinfolkers)
