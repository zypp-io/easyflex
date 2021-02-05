import logging
import os
import yaml


class RunParameters:
    def __init__(
        self,
        client,
    ):  # the constructor of the class (makes the instance)
        self.homedir = os.path.join(os.path.expanduser("~"), "tmp", "easyflex")

        self.datadir = self.create_dir(destination=os.path.join(self.homedir, "data", client))
        self.logdir = self.create_dir(destination=os.path.join(self.datadir, "log"))
        self.inputdir = self.create_dir(destination=os.path.join(self.datadir, "input"))
        self.pickledir = self.create_dir(destination=os.path.join(self.datadir, "pickles"))
        self.stagingdir = self.create_dir(destination=os.path.join(self.datadir, "staging"))

        self.client = client

    @staticmethod
    def create_dir(destination):

        try:
            if not os.path.exists(destination):
                os.makedirs(destination)
        except OSError:
            logging.warning("Error Creating directory. " + destination)
        return destination


class EasyflexParameters(RunParameters):
    def __init__(
        self,
        client: str,
        years: list,
        wm: str,
        incremental: bool,
        day_offset: int,
        debug: bool,
        parameters: dict,
    ):
        """

        Parameters
        ----------
        client: str
            Identifier voor de organisatie, bijvoorbeeld 'company-name'
        years: list
            list van jaren die uitgevraagd moeten worden (alleen indien noodzakelijk)
        wm: str
            Easyflex werkmaatschappij van de uitvraag.
        incremental: bool
            incremental=True is te gebruiken in combinatie met day_offset. alleen nieuwe records
            worden uitgevraagd. incremental=False (default) geeft alle records terug.
        day_offset: int
            cutoff waarde. Deze waarde wordt gebruikt om in geval van een incremental update een
            knip te zetten op records die zijn aangemaakt tussen nu en 'day_offset' dagen geleden.
        debug: bool
            debug=True voor testen. default=False
        parameters: dict
            additionele parameters die gebruikt worden in de request. Bijvoorbeeld in module
            ds_wm_notificaties waar op basis van onderwerp een uitvraag wordt gedaan.
        """

        super().__init__(
            client,
        )

        self.years = years
        self.incremental = incremental
        self.day_offset = day_offset
        self.debug = debug
        self.parameters = parameters

        client_settings = get_settings(os.environ.get("client_settings_path"))
        API_KEYS = client_settings["api_keys"]
        api_record = [api_key for api_key in API_KEYS if api_key[1] == wm][0]

        self.adm_code = wm
        self.adm_nummer = api_record[0]
        self.adm_naam = api_record[2]
        self.apikey = api_record[3]


def get_settings(file_name):
    with open(file_name, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
