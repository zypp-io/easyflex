import tempfile
import pandas as pd
import os
import logging
from easyflex.api import OperatieParameters
from easyflex.utils import create_dir
from tqdm import tqdm


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
        self.directory = self.set_directories()
        self.service = service

    @staticmethod
    def set_directories() -> tempfile.TemporaryDirectory:
        """

        Returns
        -------
        directory: tempfile.TemporaryDirectory
            de temp directory die is aangemaakt voor het opslaan van tijdelijke bestanden.
        """
        directory = tempfile.TemporaryDirectory(suffix="_easyflex")
        folders = ["pickles"]
        for folder in folders:
            create_dir(os.path.join(directory.name, folder))

        return directory

    def cat_modules(self, module: str) -> pd.DataFrame:
        """

        Parameters
        ----------
        module: str
            De naam van de module, bijvoorbeeld ds_wm_medewerkers.

        Returns
        -------
        data: pd.DataFrame
            dataset met alle geimporteerde data van alle werkmaatschappijen.
        """

        all_pickles = list()
        pickle_dir = os.path.join(self.directory.name, "pickles")
        files = [x for x in os.listdir(pickle_dir) if x.find(module) != -1]

        for file in files:
            filepath = os.path.join(pickle_dir, file)
            df = pd.read_pickle(filepath)
            all_pickles.append(df)

        if all_pickles:
            data = pd.concat(all_pickles, axis=0, sort=False)
        else:
            data = pd.DataFrame()

        return data

    def export_module(self, operatie: OperatieParameters, data: pd.DataFrame) -> None:
        """

        Parameters
        ----------
        operatie: OperatieParameters
            De class met de Operatie parameters. Zie documentatie van OperatieParameters.
        data: pd.DataFrame
            dataframe met de informatie van 1 administratie. Deze dataset worden voor ieder
            afzonderlijk verzoek opgeslagen in een tijdelijk .pkl bestand.

        Returns
        -------
        None
        """

        filename = f"{operatie.adm_code}_{operatie.naam}.pkl"
        data.to_pickle(os.path.join(self.directory.name, "pickles", filename))
        logging.debug("{} geexporteerd! ({} records)".format(filename, len(data)))

    def request_data(
        self,
        module: str,
        administratie: str,
        parameters: dict = None,
        velden: list = None,
        limit: int = 5000,
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
        limit: int
            lijst met velden die opgevraagd moeten worden. Als deze leeg is worden alle velden
            uitgevraagd.

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
                limit=limit,
            )  # iedere request worden vanaf en totenmet parameters aangepast.

            df = operatie.post_request()
            data_list.append(df)

            if not len(df) == operatie.limit:  # als de dataset < max aantal regels dan stoppen.
                run = False
            runcount += 1

        data = pd.concat(data_list, axis=0, sort=False, ignore_index=True)

        if not data.empty:  # exporteer de dataset naar een pickle bestand.
            self.export_module(operatie, data)  # noqa

    def query(
        self,
        module: str,
        parameters: dict = None,
        velden: list = None,
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

        Returns
        -------
        df: pd.DataFrame
            dataframe met de resultaten van de query.

        """

        # self.directory.cleanup()

        for administratie in tqdm(self.administraties, desc=f"importing {module}"):
            self.request_data(module, administratie, parameters, velden)

        df = self.cat_modules(module)

        return df
