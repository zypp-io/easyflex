import pandas as pd


def format_data(df):

    df = ww_type(df)
    df["ww_datum"] = pd.to_datetime(df["ww_datum"], format="%Y-%m-%d")
    df["ww_gewijzigd"] = pd.to_datetime(df["ww_gewijzigd"], format="%Y-%m-%dT%H:%M:%S")

    return df


def ww_type(df):
    d = {
        "27340": "Hoge premie",
        "27341": "Lage premie",
        "27342": "Hoge premie - correctie",
        "27343": "Hoge premie - herzien",
        "27344": "Lage premie - correctie",
    }

    if "ww_type" in df.columns:
        df["ww_type"] = df["ww_type"].map(d)

    return df
