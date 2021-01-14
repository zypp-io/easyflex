import logging

import pandas as pd


def format_data(df):
    df = contractsoort(df)
    df = contractperiode(df)
    df = contractreden(df)
    df = contractvorm(df)
    df = arbeidsverhouding(df)
    df = convert_to_float(df)
    df = convert_to_date(df)

    return df


def contractsoort(df):
    if "ct_contractsoort" in df.columns:
        waardes = {
            "22610": "Voor bepaalde tijd",
            "22611": "Voor de duur van het project",
            "22612": "Onbepaalde tijd",
        }

        df["ct_contractsoort"] = df.ct_contractsoort.map(
            waardes
        )  # mapped de velden om naar textwaarden

    return df


def contractperiode(df):
    if "ct_contractperiode" in df.columns:
        waardes = {"22062": "Week", "22063": "4 weken", "22064": "Maand"}

        df["ct_contractperiode"] = df.ct_contractperiode.map(
            waardes
        )  # mapped de velden om naar textwaarden

    return df


def contractreden(df):
    if "ct_contractreden" in df.columns:
        waardes = {
            "23230": "Aangegaan dienstverband",
            "23231": "Opvolgend werkgeverschap",
            "23239": "Automatisch gegenereerd dienstverband",
        }

        df["ct_contractreden"] = df.ct_contractreden.map(
            waardes
        )  # mapped de velden om naar textwaarden

    return df


def contractvorm(df):
    if "ct_contractvorm" in df.columns:
        waardes = {
            "29610": "Overeenkomst ABU A (Uitzendbeding)",
            "29611": "Overeenkomst ABU AB uldb (Bepaalde tijd)",
            "29612": "Overeenkomst ABU AB (Bepaalde tijd)",
            "29613": "Overeenkomst ABU ABC (Onbepaalde tijd)",
            "29630": "Overeenkomst NBBU 1 (Uitzendbeding)",
            "29631": "Overeenkomst NBBU 2 (Uitzendbeding)",
            "29632": "Overeenkomst NBBU 3 (Bepaalde tijd)",
            "29633": "Overeenkomst NBBU 4 (Onbepaalde tijd)",
            "29634": "Overeenkomst NBBU ½ uldb (Bepaalde tijd)",
            "29635": "Overeenkomst NBBU ½ (Bepaalde tijd)",
            "29650": "Overeenkomst (Wettelijk beding)",
            "29651": "Overeenkomst (Bepaalde tijd)",
            "29652": "Overeenkomst (Onbepaalde tijd)",
            "29653": "Overeenkomst uldb (Bepaalde tijd)",
            "29654": "Overeenkomst uldb (Onbepaalde tijd)",
            "29670": "Overeenkomst VPO Bepaalde tijd uldb",
            "29671": "Overeenkomst VPO Bepaalde tijd",
            "29672": "Overeenkomst VPO Onbepaalde tijd uldb",
            "29673": "Overeenkomst VPO Onbepaalde tijd",
        }

        df["ct_contractvorm"] = df.ct_contractvorm.map(
            waardes
        )  # mapped de velden om naar textwaarden

    return df


def arbeidsverhouding(df):
    if "ct_arbeidsverhouding" in df.columns:
        waardes = {
            "22101": "Arbeidsovereenkomst",
            "22107": "Stageovereenkomst",
            "22108": "Uitzendkracht",
            "22109": "Payrollovereenkomst",
        }

        df["ct_arbeidsverhouding"] = df.ct_arbeidsverhouding.map(
            waardes
        )  # mapped de velden om naar textwaarden

    return df


def convert_to_float(df):
    columns = ["ct_contractloondagen", "ct_contractloon", "ct_contractuurloon"]

    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype("float")
        else:
            None

    return df


def convert_to_date(df):
    columns = ["ct_contractaanvang", "ct_contracteinde"]

    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d")
        else:
            None

    return df
