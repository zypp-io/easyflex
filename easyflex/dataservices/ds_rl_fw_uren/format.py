import pandas as pd


def format_data(df):

    df = lctype(df)
    df = convert_to_float(df)
    df = convert_to_int(df)
    df = convert_to_date(df)

    return df


def convert_to_float(df):
    columns = [
        "uren_fwpercentage",
        "uren_rlpercentage",
        "uren_fwprestatietoeslag",
        "uren_fwuurloon",
        "uren_rlpercentage",
        "uren_rltarief",
    ]

    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype("float")

    return df


def convert_to_int(df):
    columns = ["uren_fwminuten", "uren_rlminuten"]

    for col in columns:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype("int")

    return df


def convert_to_date(df):
    columns = ["uren_datum"]

    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d")

    return df


def lctype(df):
    lctype = {
        "20810": "Loon normale uren",
        "20811": "Loon overwerk",
        "20812": "Loon onregelmatige diensten",
        "20813": "Loon verlaagd",
        "20814": "Loon verhoogd",
        "20815": "Vast loon",
        "20817": "Loon leegloop",
        "20818": "Loon arbeidsongeschiktheid",
        "20819": "Wachtdagencompensatie",
        "20820": "Kort verzuim",
        "20821": "Feestdagen",
        "20822": "Vakantiedagen",
        "20823": "Vakantiegeld",
        "20825": "Arbeidsduurverkorting",
        "20826": "P.o.b. (belast)",
        "20827": "Tijd voor tijd",
        "20829": "Extra reserveringen",
    }

    if "uren_looncomponenttype" in df.columns:
        df["uren_looncomponenttype"] = df.uren_looncomponenttype.map(lctype)

    else:
        None

    return df
