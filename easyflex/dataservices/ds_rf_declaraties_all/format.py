import pandas as pd


def format_data(df):
    df = rf_decl_periodesoort(df)
    df = rf_decl_boeking(df)
    df = rf_decl_status(df)
    df = rf_decl_status_fw(df)
    df = rf_decl_status_rl(df)
    df = rf_decl_tvt(df)
    df = rf_decl_tvt_onr(df)
    df = rf_decl_job_accorderen(df)
    df = convert_dates(df)
    df = convert_datetimes(df)

    return df


def rf_decl_periodesoort(df):
    mapping = {"29770": "Week", "29771": "4-weken", "29772": "Maand"}

    if "rf_decl_periodesoort" in df.columns:
        df["rf_decl_periodesoort"] = df.rf_decl_periodesoort.map(mapping)

    return df


def rf_decl_boeking(df):
    mapping = {"10860": "Opboeking", "10861": "Afboeking"}

    if "rf_decl_boeking" in df.columns:
        df["rf_decl_boeking"] = df.rf_decl_boeking.map(mapping)

    return df


def rf_decl_status(df):
    mapping = {
        "24840": "Urendeclaratie ontbreekt",
        "24841": "Accordering onvolledig",
        "24842": "Gereed voor eindaccordering",
        "24843": "Wachten",
        "24844": "Geaccordeerd voor verwerking",
        "24845": "In bewerking",
        "24846": "Vervallen",
        "24849": "Afgewikkeld",
    }

    if "rf_decl_status" in df.columns:
        df["rf_decl_status"] = df.rf_decl_status.map(mapping)

    return df


def rf_decl_status_fw(df):
    mapping = {
        "29790": "Nieuw (nieuwe declaratie, nog niets ingevuld)",
        "29791": "Bezig (bezig met invullen, al uren ingevuld)",
        "29792": "Gereed (klaar met invullen)",
        "29793": "Akkoord (declaratie gereed gemeld)",
        "29799": "Verwerkt (declaratie verloond/gefactureerd)",
    }

    if "rf_decl_status_fw" in df.columns:
        df["rf_decl_status_fw"] = df.rf_decl_status_fw.map(mapping)

    return df


def rf_decl_status_rl(df):
    mapping = {
        "29790": "Nieuw (nieuwe declaratie, nog niets ingevuld)",
        "29791": "Bezig (bezig met invullen, al uren ingevuld)",
        "29792": "Gereed (klaar met invullen)",
        "29793": "Akkoord (declaratie gereed gemeld)",
        "29799": "Verwerkt (declaratie verloond/gefactureerd)",
    }

    if "rf_decl_status_rl" in df.columns:
        df["rf_decl_status_rl"] = df.rf_decl_status_rl.map(mapping)

    return df


def rf_decl_tvt(df):
    mapping = {"24970": "Uitbetalen", "24971": "Alles sparen", "24972": "Alleen toeslagdeel sparen"}

    if "rf_decl_tvt" in df.columns:
        df["rf_decl_tvt"] = df.rf_decl_tvt.map(mapping)

    return df


def rf_decl_tvt_onr(df):
    mapping = {"24970": "Uitbetalen", "24971": "Alles sparen", "24972": "Alleen toeslagdeel sparen"}

    if "rf_decl_tvt_onr" in df.columns:
        df["rf_decl_tvt_onr"] = df.rf_decl_tvt_onr.map(mapping)

    return df


def rf_decl_job_accorderen(df):
    mapping = {
        "20470": "Niet op papier",
        "20471": "Ondertekenen relatie en flexwerker",
        "20472": "Ondertekenen relatie",
        "20473": "Ondertekenen flexwerker",
        "20480": "Niet digitaal",
        "20481": "Accorderen relatie en flexwerker",
        "20482": "Accorderen relatie",
        "20483": "Accorderen flexwerker",
        "20490": "Standaardinstelling van de relatie",
    }

    if "rf_decl_job_accorderen" in df.columns:
        df["rf_decl_job_accorderen"] = df.rf_decl_job_accorderen.map(mapping)

    return df


def convert_dates(df):
    columns = [
        "rf_decl_startdatum",
        "rf_decl_einddatum",
        "rf_decl_job_startdatum",
        "rf_decl_job_einddatum",
    ]

    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d")

    return df


def convert_datetimes(df):
    columns = [
        "rf_decl_gereed_fw",
        "rf_decl_gereed_rl",
        "rf_decl_loon_klaar",
        "rf_decl_fact_klaar",
        "rf_decl_accoord_op",
        "rf_decl_datumtijdgewijzigd",
    ]

    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M:%S")

    return df
