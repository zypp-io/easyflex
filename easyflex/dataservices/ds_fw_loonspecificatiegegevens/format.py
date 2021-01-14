import pandas as pd


def format_data(df):

    df.columns = [x.replace("ds_", "") for x in df.columns]

    return df
