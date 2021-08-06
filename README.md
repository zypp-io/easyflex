<p align="center"><img alt="logo" src="https://www.zypp.io/static/assets/img/logos/zypp/black/500px.png" width="200"></p>
<br>

[![Downloads](https://pepy.tech/badge/easyflex)](https://pepy.tech/project/easyflex)
![PyPI](https://img.shields.io/pypi/v/easyflex)
[![Open Source](https://badges.frapsoft.com/os/v1/open-source.svg?v=103)](https://opensource.org/)

# Easyflex dataservices API voor python
> Dit project bevat python scripts voor het ontsluiten van data uit de easyflex API.

## Project documentatie
- [Introductie](#introductie)
- [Hoe moet je dit project gebruiken?](#hoe-moet-je-dit-project-gebruiken?)
    - [Simpel voorbeeld](#simpel-voorbeeld-zonder-velden-of-parameters)
    - [Voorbeeld met parameters en velden](#voorbeeld-met-parameters-en-velden)

# Introductie
Easyflex heeft een API ontwikkeld voor het ontsluiten van data. Op basis van de [Easyflex web- en dataservices documentatie](https://confluence.easyflex.net/display/WEBDATAKLNT/Web-+en+dataservice) is dit python project ontstaan.
Het doel van het project is om snel en efficient data te ontsluiten van 1 of meerdere Easyflex administraties.

# Hoe moet je dit project gebruiken?
In twee stappen is het mogelijk om data te ontsluiten. De eerste stap is het initialiseren van de class `Easyflex`. In deze class registreren worden de API keys geregistreerd die gebruikt worden bij de uitvraag.
De tweede stap is het uitvragen van de dataservices of webservices. Hier moet een module naam worden opgegeven. Vervolgens kunnen de parameters en velden worden opgegeven, conform de [documentatie van de modules](https://confluence.easyflex.net/display/WEBDATAKLNT/1.2+ds_wm_medewerkers).

### Simpel voorbeeld zonder velden of parameters
```python
from easyflex import Easyflex
api_keys = {"<YOUR_ADM_CODE>": "<YOUR_API_KEY>","<YOUR_ADM_CODE_2>": "<YOUR_API_KEY_2>"}

ef = Easyflex(api_keys, service="dataservice")
data = ef.query(module="ds_wm_medewerkers")
```

### Voorbeeld met parameters en velden
```python
from easyflex import Easyflex
api_keys = {"<YOUR_ADM_CODE>": "<YOUR_API_KEY>","<YOUR_ADM_CODE_2>": "<YOUR_API_KEY_2>"}

ef = Easyflex(api_keys, service="dataservice")
data = ef.query(module="ds_wm_locaties",
                parameters={"status": 21690},
                velden=["wm_locatie_nummer", "wm_locatie_code", "wm_locatie_naam"])
```

## Onderhouden door:

- [Melvin Folkers - Zypp](https://github.com/zypp-io)
