import pandas as pd


def format_data(df):
    df = lj_soort(df)
    df = lj_direct(df)
    df = lj_ops(df)
    df = convert_to_float(df)
    df = convert_to_date(df)

    return df


def lj_soort(df):
    waardes = {"12000": "Flexwerker", "12001": "Vaste medewerker"}

    if "lj_soort" in df.columns:
        df["lj_soort"] = df.lj_soort.map(waardes)

    return df


def lj_direct(df):
    waardes = {
        "23600": "Omzetrekening leveringen btw laag tarief(1)",
        "23601": "Afdrachtrekening leveringen btw laag tarief(1)",
        "23602": "Omzetrekening leveringen btw laag tarief(2)",
        "23603": "Afdrachtrekening leveringen btw laag tarief(2)",
        "23604": "Omzetrekening leveringen btw laag tarief(3)",
        "23605": "Afdrachtrekening leveringen btw laag tarief(3)",
        "23606": "Omzetrekening leveringen btw hoog tarief(1)",
        "23607": "Afdrachtrekening leveringen btw hoog tarief(1)",
        "23608": "Omzetrekening leveringen btw hoog tarief(2)",
        "23609": "Afdrachtrekening leveringen btw hoog tarief(2)",
        "23610": "Omzetrekening leveringen btw hoog tarief(3)",
        "23611": "Afdrachtrekening leveringen btw hoog tarief(3)",
        "23612": "Omzetrekening leveringen btw 0% tarief",
        "23613": "Omzetrekening leveringen btw verlegd",
        "23614": "Omzetrekening leveringen binnen EU",
        "23615": "Omzetrekening leveringen buiten EU",
        "23616": "Tegenrekening btw leveringen",
        "23620": "Tegenrekening sectoren",
        "23621": "Tegenrekening relaties",
        "23622": "Tegenrekening projecten",
        "23623": "Tegenrekening flexwerkers",
        "23624": "Tegenrekening loonbelasting",
        "23625": "Tegenrekening sociale verzekeringen",
        "23630": "Rekening debiteuren",
        "23631": "Rekening kredietbeperking",
        "23632": "Uitwijkrekening",
        "23633": "Rekening loonbeslagen",
        "23634": "Inkoopnota's zzp",
        "23635": "Inkoopnota's doorlenen",
        "23636": "Rekening aansluiting voor loon SV",
        "23637": "Kostenplaats aansluiting voor loon SV",
        "23638": "Kostensoort aansluiting voor loon SV",
        "23639": "Rekening aansluiting voor loon LB",
        "23640": "Kostenplaats aansluiting voor loon LB",
        "23641": "Kostensoort aansluiting voor loon LB",
    }

    if "lj_direct" in df.columns:
        df["lj_direct"] = df.lj_direct.map(waardes)

    return df


def lj_ops(df):
    waardes = {
        "1": "Wachtgeld",
        "11": "Kort verzuim",
        "12": "Feestdagen",
        "13": "Vakantiedagen",
        "21": "Vakantiegeld",
        "22": "ORT reservering",
        "30": "Arbeidsduurverkorting",
        "50": "Reservering diversen",
        "80": "Persoonlijk opleiding budget (belast)",
        "90": "Pensioenen",
        "91": "SER afdracht (algemene voorziening looncomponent)",
        "95": "Scholingsfonds (algemene voorziening looncomponent)",
        "96": "Sociaal fonds (algemene voorziening looncomponent)",
        "99": "Werkgeverslasten reservering (werkgeverslasten reservering)",
        "900": "Tijd-voor-tijd",
    }

    if "lj_ops" in df.columns:
        df["lj_ops"] = df.lj_ops.map(waardes)

    return df


def convert_to_float(df):
    columns = ["lj_debet", "lj_credit", "lj_aantal"]

    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype("float")

    return df


def convert_to_date(df):
    columns = ["lj_bdatum", "lj_jdatum"]

    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d")

    return df
