import pandas as pd


def format_data(df):

    df["mw_naam"] = (
        df["mw_medewerkerroepnaam"]
        + " "
        + (df["mw_medewerkervoorvoegsels"] + " ").fillna("")
        + df["mw_medewerkerachternaam"]
    )

    return df
