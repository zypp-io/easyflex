import pandas as pd


def format_data(df):

    if "kp_kostenplaatsstatus" in df.columns:
        waardes = {"21720": "actief", "21721": "passief"}

        df["kp_kostenplaatsstatus"] = df.kp_kostenplaatsstatus.map(
            waardes
        )  # mapped de velden om naar textwaarden

    return df
