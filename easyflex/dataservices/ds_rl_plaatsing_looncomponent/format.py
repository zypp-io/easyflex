import pandas as pd


def format_data(df):
    df = lctype1(df)
    df = lctype2(df)
    df = toeslagrekenmodel(df)
    df = basisrekenmodel(df)
    df = eenheid(df)
    df = btw(df)
    df = wijzig_rl(df)
    df = wijzig_fw(df)

    df = convert_to_float(df)
    df = convert_to_date(df)

    return df


def convert_to_float(df):
    columns = [
        "plc_bedrag_fw",
        "plc_percentage_fw",
        "plc_percentage_rl",
        "plc_basisfactor",
        "plc_basismargebedrag",
        "plc_basismargepercentage",
        "plc_basisfactuurtarief",
        "plc_toeslagfactor",
        "plc_toeslagmargebedrag",
        "plc_toeslagmargepercentage",
        "plc_toeslagfactuurtarief",
        "plc_bedrag_rl",
        "plc_belast",
        "plc_onbelast",
    ]

    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype("float")

    return df


def convert_to_date(df):
    columns = ["plc_startdatum", "plc_einddatum", "plc_gewijzigd"]

    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d")

    return df


def lctype1(df):
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
        "20828": "ORT reservering",
        "20829": "Extra reserveringen",
        "20830": "Vergoedingen belast",
        "20831": "Reiskostenvergoedingen belast/onbelast",
        "20832": "Vergoedingen belast/onbelast",
        "20833": "Vergoedingen in natura",
        "20860": "Vergoedingen onbelast",
        "20861": "Inhoudingen",
        "20864": "Vergoeding (eindheffing enkelvoudig tarief)",
        "20865": "Vergoeding (eindheffing gebruteerd tarief)",
        "20877": "Reiskostenvergoeding onbelast",
    }

    if "plc_looncomponenttype" in df.columns:
        df["plc_looncomponenttype"] = df.plc_looncomponenttype.map(lctype)

    else:
        None

    return df


def lctype2(df):
    lc_type = {
        "24550": "Uren",
        "24551": "Bruto vergoedingen en inhoudingen",
        "24552": "Netto vergoedingen en inhoudingen",
    }

    if "plc_type" in df.columns:
        df["plc_type"] = df.plc_type.map(lc_type)

    else:
        None

    return df


def basisrekenmodel(df):
    mapping = {
        "24110": "Factor",
        "24111": "Marge",
        "24112": "Tarief",
        "24129": "N.v.t.",
    }

    if "plc_basisrekenmodel" in df.columns:
        df["plc_basisrekenmodel"] = df.plc_basisrekenmodel.map(mapping)

    else:
        None

    return df


def toeslagrekenmodel(df):
    mapping = {
        "24110": "Factor",
        "24111": "Marge",
        "24112": "Tarief",
        "24129": "N.v.t.",
    }

    if "plc_toeslagrekenmodel" in df.columns:
        df["plc_toeslagrekenmodel"] = df.plc_toeslagrekenmodel.map(mapping)

    else:
        None

    return df


def eenheid(df):
    mapping = {
        "24170": "Uur",
        "24171": "Dag (ma t/m vr)",
        "24172": "Gewerkte dag",
        "24173": "Stuk",
        "24174": "Kilometer",
        "24175": "Loontijdvak",
        "24176": "Declaratietijdvak",
        "24177": "Kalenderdag",
        "24178": "Gewerkt uur",
        "24179": "Bedrag",
    }

    if "plc_eenheid" in df.columns:
        df["plc_eenheid"] = df.plc_eenheid.map(mapping)

    else:
        None

    return df


def btw(df):
    mapping = {
        "17720": "Levering BTW hoog in Nederland",
        "17721": "Levering BTW laag in Nederland",
        "17722": "Levering BTW vrij in Nederland",
        "17723": "Levering in Nederland (BTW verlegd)",
        "17724": "Levering naar landen binnen de EU",
        "17725": "Levering naar landen buiten de EU",
    }

    if "plc_btw" in df.columns:
        df["plc_btw"] = df.plc_btw.map(mapping)

    else:
        None

    return df


def wijzig_rl(df):
    mapping = {"0": "Wijzigen is niet toegestaan", "1": "Wijzigen is toegestaan"}

    if "plc_wijzig_rl" in df.columns:
        df["plc_wijzig_rl"] = df.plc_wijzig_rl.map(mapping)

    else:
        None

    return df


def wijzig_fw(df):
    mapping = {"0": "Wijzigen is niet toegestaan", "1": "Wijzigen is toegestaan"}

    if "plc_wijzig_fw" in df.columns:
        df["plc_wijzig_fw"] = df.plc_wijzig_fw.map(mapping)

    else:
        None

    return df
