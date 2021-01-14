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
    def __init__(self, client, years, wm, incremental, day_offset, debug):

        super().__init__(
            client,
        )

        self.years = years
        self.incremental = incremental
        self.day_offset = day_offset
        self.debug = debug

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
