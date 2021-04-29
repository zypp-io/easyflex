import tempfile
import pandas as pd
import os
import logging
from easyflex.api import OperatieParameters
from easyflex.utils import create_dir
from tqdm import tqdm
from easyflex.exceptions import NoDataToImport


class Easyflex:
    def __init__(self, api_keys, service: str = "dataservice"):
        logging.info(f"{len(api_keys)} administraties geselecteerd.")
        self.api_keys = api_keys
        self.administraties = api_keys.keys()
        self.directory = self.set_directories()
        self.service = service

    @staticmethod
    def set_directories():
        directory = tempfile.TemporaryDirectory(suffix="_easyflex")
        folders = ["pickles"]
        for folder in folders:
            create_dir(os.path.join(directory.name, folder))

        return directory

    def cat_modules(self, module):

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
            raise NoDataToImport("geen data beschikbaar.")

        return data

    def export_module(self, operatie, data):

        filename = f"{operatie.adm_code}_{operatie.naam}.pkl"
        data.to_pickle(os.path.join(self.directory.name, "pickles", filename))
        logging.debug("{} geexporteerd! ({} records)".format(filename, len(data)))

    def request_data(self, module, parameters, velden, administratie):
        api_key = self.api_keys.get(administratie)
        runcount = 0
        data_list = []
        run = True

        while run:
            operatie = OperatieParameters(
                api_key=api_key,
                adm_code=administratie,
                naam=module,
                parameters=parameters,
                fields=velden,
                limit=5000,
                runcount=runcount,
                service=self.service,
            )

            df = operatie.post_request()
            data_list.append(df)

            if not len(df) == operatie.limit:
                run = False
            runcount += 1

        data = pd.concat(data_list, axis=0, sort=False, ignore_index=True)

        if not data.empty:
            self.export_module(operatie, data)  # noqa

    def query(
        self,
        module: str = None,
        parameters: dict = None,
        velden: list = None,
    ) -> pd.DataFrame:
        """
        De query functie bevraagt de easyflex API op basis van de gekozen module en jaren.
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
            self.request_data(module, parameters, velden, administratie)

        df = self.cat_modules(module)

        return df
