import pandas as pd


def format_data(df):

    df = convert_to_float(df)
    df = convert_to_date(df)

    return df


def convert_to_float(df):
    columns = ["jpg_debet", "jpg_credit", "jpg_aantal"]

    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype("float")

    return df


def convert_to_date(df):
    columns = ["jpg_bdatum", "jpg_jdatum"]

    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d")

    return df
