import os
import yaml
from dotenv import load_dotenv
from easyflex.dataservices.functions import RunParameters

import pandas as pd

pd.set_option("mode.chained_assignment", None)


def get_settings(file_name):
    with open(file_name, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


def get_client_settings():

    client_settings = get_settings(os.environ.get("client_settings_path"))

    return client_settings


def set_run_parameters():
    load_dotenv(override=True)

    client_settings_path = os.environ.get("client_settings_path")
    client_settings = get_settings(client_settings_path)
    client_name = client_settings["client_name"]

    run_params = RunParameters(client=client_name)

    return run_params
