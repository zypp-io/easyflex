import logging

import pandas as pd
from tqdm import tqdm

from easyflex.api import OperatieParameters


class Easyflex:
    def __init__(self, api_keys: dict, service: str = "dataservice"):
        """
        Deze class betrekt zich tot het authenticeren met de Easyflex API. bij het initialiseren van
        deze class worden de API keys vastgezet voor de latere uitvragen.

        Parameters
        ----------
        api_keys: dict
            De keys zijn administratie codes en de values zijn de API codes
        service: str
            dataservice of webservice. De service bepaald naar welke endpoints de verzoeken
            worden verstuurd.
        """

        logging.info(f"{len(api_keys)} administraties geselecteerd.")
        self.api_keys = api_keys
        self.administraties = api_keys.keys()
        self.service = service

    def request_data(
        self,
        module: str,
        administratie: str,
        parameters: dict = None,
        velden: list = None,
        max_rows: int = 5000,
        inherit_datatypes: bool = True,
    ):
        """

        Parameters
        ----------
        module: str
            Modulenaam van de uit te vragen data.
        administratie: str
            administratiecode die gebruikt wordt voor de uitvraag.
        parameters: dict
            parameters voor het verzoek. De keys zijn veldnamen en de values zijn de filterwaarden.
        velden: list
            lijst met velden die opgevraagd moeten worden. Als deze leeg is worden alle velden
            uitgevraagd.
        max_rows: int
           maximale aantal rijen dat kan worden uitgevraagd. Default = 5000
        inherit_datatypes: bool
           default = True. geeft aan of de datatypes van de Easyflex dataservices worden aangehouden
           Als deze op False staat, worden geen datatypes omgezet (alles string).

        Returns
        -------

        """

        api_key = self.api_keys.get(administratie)
        runcount = 0
        data_list = []
        run = True

        while run:  # blijf uitvragen totdat de laatste pagina is bereikt.
            operatie = OperatieParameters(
                api_key=api_key,
                adm_code=administratie,
                naam=module,
                parameters=parameters,
                fields=velden,
                runcount=runcount,
                service=self.service,
                max_rows=max_rows,
                inherit_datatypes=inherit_datatypes,
            )  # iedere request worden vanaf en totenmet parameters aangepast.

            df = operatie.post_request()
            data_list.append(df)

            if not len(df) == operatie.max_rows:  # als de dataset < max aantal regels dan stoppen.
                run = False
            runcount += 1

        data = pd.concat(data_list, axis=0, sort=False, ignore_index=True)

        return data

    def query(
        self,
        module: str,
        parameters: dict = None,
        velden: list = None,
        max_rows: int = 5000,
        inherit_datatypes: bool = False,
    ) -> pd.DataFrame:
        """
        De query functie bevraagt de easyflex API op basis van de gekozen module.
        Het resultaat wordt weggeschreven naar een tmp folder.

        Parameters
        ----------
        module: str
            modulenaam van uit te vragen data
        parameters: dict
            dictionary met parameters voor de specifiek gekozen module.
        velden: list
            lijst met velden die uitgevraagd moeten worden.
        max_rows: int
            het aantal records dat per keer uitgevraagd kan worden voor de module.
        inherit_datatypes: bool
           default = True. geeft aan of de datatypes van de Easyflex dataservices worden aangehouden
           Als deze op False staat, worden geen datatypes omgezet (alles string).

        Returns
        -------
        df: pd.DataFrame
            dataframe met de resultaten van de query.

        """

        data_list = []

        for administratie in tqdm(self.administraties, desc=f"importing {module}"):
            df = self.request_data(module, administratie, parameters, velden, max_rows, inherit_datatypes)
            data_list.append(df)
        data = pd.concat(data_list, axis=0, sort=False, ignore_index=True)

        return data
