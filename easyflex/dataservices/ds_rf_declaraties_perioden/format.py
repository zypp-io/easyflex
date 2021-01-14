from datetime import datetime
import pandas as pd
import numpy as np


def format_data(df):
    pass
    # df = convert_stapsysteem(df)
    # df = convert_to_date(df)
    # df = correct_open_enddate(df)

    return df


def convert_stapsysteem(df):
    if "stapsysteem" in df.columns:
        waardes = {
            "22620": "NBBU Fase 1 (AOW plus)",
            "22621": "NBBU Fase 2 (AOW plus)",
            "22622": "NBBU Fase 3 (AOW plus)",
            "22623": "‘NBBU Fase 3 (AOW plus)",
            "22624": "NBBU Fase 4 (AOW plus)",
            "22630": "Niet ingedeeld",
            "22631": "Bepaalde tijd",
            "22632": "Onbepaalde tijd",
            "22640": "Niet ingedeeld",
            "22641": "ABU Fase A",
            "22642": "ABU Fase B",
            "22643": "ABU Fase C",
            "22650": "Niet ingedeeld",
            "22651": "NBBU Fase 1",
            "22652": "NBBU Fase 2",
            "22653": "NBBU Fase 3",
            "22654": "NBBU Fase 4",
            "22660": "Niet ingedeeld",
            "22661": "Uitzendbeding",
            "22669": "Ketensysteem",
        }

        df["stapsysteem"] = df.stapsysteem.map(waardes)  # mapped de velden om naar textwaarden

    return df


def convert_to_date(df):
    columns = ["stapaanvang", "stapeinde"]

    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d")

    return df


def correct_open_enddate(df):
    today = datetime.now().date()
    if "stapeinde" in df.columns:
        df.loc[df["stapeinde"].dt.date == today, "stapeinde"] = np.nan

    return df
