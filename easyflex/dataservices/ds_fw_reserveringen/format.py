import pandas as pd


def format_data(df):

    df = lc_type(df)
    df["fw_res_saldo_geld"] = df["fw_res_saldo_geld"].astype(float)
    df["fw_res_saldo_tijd"] = df["fw_res_saldo_tijd"].astype(float)
    df["fw_res_gewijzigd"] = pd.to_datetime(df["fw_res_gewijzigd"], format="%Y-%m-%dT%H:%M:%S")

    return df


def lc_type(df):
    d = {
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

    if "fw_res_lc_type" in df.columns:
        df["fw_res_lc_type"] = df["fw_res_lc_type"].map(d)

    return df
