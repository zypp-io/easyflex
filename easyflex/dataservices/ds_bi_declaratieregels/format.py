import logging

import pandas as pd
import numpy as np


def format_data(df):

    if "bi_declid" not in df.columns:
        return df
    df = leveringswijze(df)
    df = status(df)
    df = bi_type(df)
    df = lctype(df)
    df = eenheid(df)
    df = convert_to_float(df)
    df = convert_to_date(df)

    df = verdeel_omzet_kosten_locaties(df)
    df = bruto_loon(df)

    return df


def bruto_loon(df):
    try:
        df["bi_brutoloon_c"] = df.apply(lambda x: bereken_brutoloon(x), axis=1)
    except:
        df["bi_brutoloon_c"] = np.nan
    return df


def bereken_brutoloon(x):
    if (
        x["bi_type"] == "uren"
        and isinstance(x["bi_fwpercentage"], (int, float))
        and isinstance(x["bi_fwuurloon"], (int, float))
        and isinstance(x["bi_fwuren"], (int, float))
    ):
        bu = round(
            round(x["bi_fwuurloon"] * (x["bi_fwpercentage"] / 100), 2) * x["bi_fwuren"],
            2,
        )
    else:
        bu = 0.0

    return bu


def eenheid(df):
    if "bi_eenheid" in df.columns:
        waardes = {
            "0": "",
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

        df["bi_eenheid"] = df.bi_eenheid.map(waardes)  # mapped de velden om naar textwaarden

    else:
        None

    return df


def leveringswijze(df):
    if "bi_leveringswijze" in df.columns:
        waardes = {
            "23320": "Uitzendwerk",
            "23321": "Vakantiewerk",
            "23322": "Detacheren",
            "23323": "Payrol",
            "23324": "Werving & selectie",
            "23325": "Doorlenen",
            "23326": "ZZP’er",
            "23328": "Onbekend",
        }

        df["bi_leveringswijze"] = df.bi_leveringswijze.map(
            waardes
        )  # mapped de velden om naar textwaarden

    else:
        None

    return df


def status(df):
    if "bi_status" in df.columns:
        # transform bi_status

        waardes = {
            "24840": "Urendeclaratie ontbreekt",
            "24841": "Accordering onvolledig",
            "24842": "Gereed voor eindaccordering",
            "24843": "Wachten",
            "24844": "Geaccordeerd voor verwerking",
            "24845": "In bewerking",
            "24846": "Vervallen",
            "24849": "Afgewikkeld",
        }

        df["bi_status"] = df.bi_status.map(waardes)  # mapped de velden om naar textwaarden
    else:
        None

    return df


def bi_type(df):
    if "bi_type" in df.columns:
        # opties bi_type

        waardes = {"1": "uren", "2": "vergoeding/inhouding", "4": "handmatige omzet"}  #

        df["bi_type"] = df.bi_type.map(waardes)  # mapped de velden om naar textwaarden
    else:
        None

    return df


def lctype(df):
    lctype = {
        "20810": "Loon normale uren ",
        "20811": "Loon overwerk ",
        "20812": "Loon onregelmatige diensten ",
        "20813": "Loon verlaagd ",
        "20814": "Loon verhoogd",
        "20815": "Vast loon",
        "20817": "Loon leegloop",
        "20818": "Loon arbeidsongeschiktheid ",
        "20819": "Wachtdagencompensatie ",
        "20820": "Kort verzuim",
        "20821": "Feestdagen",
        "20822": "Vakantiedagen",
        "20823": "Vakantiegeld",
        "20825": "Arbeidsduurverkorting",
        "20826": "P.o.b. (belast)",
        "20827": "Tijd voor tijd ",
        "20828": "ORT reservering ",
        "20829": "Extra reserveringen ",
        "20830": "Vergoedingen belast",
        "20831": "Reiskostenvergoedingen belast/onbelast",
        "20832": "Vergoedingen belast/onbelast",
        "20833": "Vergoedingen in natura ",
        "20860": "Vergoedingen onbelast",
        "20861": "Inhoudingen ",
        "20864": "Vergoeding (eindheffing enkelvoudig tarief)",
        "20865": "Vergoeding (eindheffing gebruteerd tarief)",
        "20877": "Reiskostenvergoeding onbelast",
    }

    if "bi_lctype" in df.columns:
        df["bi_lctype"] = df.bi_lctype.map(lctype)

    else:
        None

    return df


def convert_to_float(df):
    columns = [
        "bi_fwaantal",
        "bi_fwuren",
        "bi_fwuurloon",
        "bi_loonbelast",
        "bi_loononbelast",
        "bi_loonbelastbuitenstat",
        "bi_loononbelastbuitenstat",
        "bi_omzet",
        "bi_omzetbuitenstat",
        "bi_omzethandmatig",
        "bi_omzethandmatigbuitenstat",
        "bi_fwbeh_perc_omzet_kosten",
        "bi_fwbeh_perc_uren",
        "bi_fwloc_perc_omzet_kosten",
        "bi_fwloc_perc_uren",
        "bi_fwpercentage",
        "bi_rlbeh_perc_omzet_kosten",
        "bi_rlbeh_perc_uren",
        "bi_rlloc_perc_omzet_kosten",
        "bi_rlloc_perc_uren",
        "bi_rlpercentage",
        "bi_fwgeldbelast",
        "bi_fwgeldonbelast",
        "bi_rlaantal",
        "bi_rltarief",
        "bi_rluren",
    ]

    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype("float")
        else:
            None

    return df


def convert_to_date(df):
    columns = [
        "bi_decldatum",
        "bi_decleinddatum",
        "bi_declstartdatum",
        "bi_factureerdatum",
        "bi_factuurdatum",
        "bi_verloondatum",
    ]

    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d")
        else:
            None

    return df


def add_required_columns(df):
    cols = ["bi_fwlocatie", "bi_rllocatie", "bi_type"]
    for col in cols:
        if col not in df.columns:
            df[col] = ""
        else:
            continue

    cols = ["bi_rlloc_perc_omzet_kosten", "bi_fwloc_perc_omzet_kosten"]
    for col in cols:
        if col not in df.columns:
            df[col] = 50
        else:
            continue

    return df


def kies_velden():
    omzetvelden = [
        "bi_omzet",
        "bi_omzetbuitenstat",
        "bi_omzethandmatig",
        "bi_omzethandmatigbuitenstat",
    ]
    loonkostenvelden = [
        "bi_loonbelast",
        "bi_loonbelastbuitenstat",
        "bi_loononbelast",
        "bi_loononbelastbuitenstat",
    ]
    vergoedingvelden = ["bi_fwgeldbelast", "bi_fwgeldonbelast"]
    aantalvelden = ["bi_fwuren", "bi_fwaantal", "bi_rluren", "bi_rlaantal"]
    velden = omzetvelden + loonkostenvelden + vergoedingvelden + aantalvelden

    return velden


def zelfde_locatie(df):
    df = df[(df.bi_fwlocatie == df.bi_rllocatie)]
    df.rename(
        columns={
            "bi_fwlocatie": "bi_locatie",
            "bi_fwloc_perc_omzet_kosten": "bi_perc_locatie",
        },
        inplace=True,
    )
    df.drop(["bi_rllocatie", "bi_rlloc_perc_omzet_kosten"], axis=1, inplace=True)

    return df


def handmatige_omzet(df):
    df = df[(df.bi_type == "handmatige omzet")]
    df.rename(
        columns={
            "bi_rllocatie": "bi_locatie",
            "bi_rlloc_perc_omzet_kosten": "bi_perc_locatie",
        },
        inplace=True,
    )
    df.drop(["bi_fwlocatie", "bi_fwloc_perc_omzet_kosten"], axis=1, inplace=True)

    return df


def verdeel_locaties(df):

    df_fw = df.drop(["bi_rllocatie", "bi_rlloc_perc_omzet_kosten"], axis=1)
    df_rl = df.drop(["bi_fwlocatie", "bi_fwloc_perc_omzet_kosten"], axis=1)
    df_fw.rename(
        columns={
            "bi_fwlocatie": "bi_locatie",
            "bi_fwloc_perc_omzet_kosten": "bi_perc_locatie",
        },
        inplace=True,
    )
    df_rl.rename(
        columns={
            "bi_rllocatie": "bi_locatie",
            "bi_rlloc_perc_omzet_kosten": "bi_perc_locatie",
        },
        inplace=True,
    )

    df_new = pd.concat([df_rl, df_fw], axis=0, sort=False)

    return df_new


def berekening_uitvoeren(df_new):
    te_transformeren_velden = kies_velden()
    df_new[te_transformeren_velden] = df_new[te_transformeren_velden].multiply(
        df_new.bi_perc_locatie / 100, axis="index"
    )
    df_new = df_new[df_new.bi_perc_locatie != 0]

    return df_new


def verdeel_omzet_kosten_locaties(df):
    logging.info("aantal records voor correctie locaties: {}".format(len(df)))

    df = add_required_columns(df)

    df_zelfde_loc = zelfde_locatie(df)
    df = df[(df.bi_fwlocatie != df.bi_rllocatie)]

    df_handmatige_omzet = handmatige_omzet(df)
    df = df[(df.bi_type != "handmatige omzet")]

    df_new = verdeel_locaties(df)
    df_new = berekening_uitvoeren(df_new)

    df_all = pd.concat([df_new, df_zelfde_loc, df_handmatige_omzet], axis=0, sort=False)

    logging.info("aantal records na correctie locaties: {}".format(len(df_all)))

    return df_all
