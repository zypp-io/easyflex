def format_data(df):

    df = df.select_dtypes(object).replace(r"\r", "", regex=True)  # remove character

    df = plaatsingsoortwerk(df)
    df = plaatsingafspraakloonschaal(df)
    df = plaatsingafspraakbeloningsregeling(df)
    df = fwstapsysteem(df)

    return df


def plaatsingsoortwerk(df):
    if "pl_plaatsingsoortwerk" in df.columns:

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

        df["pl_plaatsingsoortwerk"] = df.pl_plaatsingsoortwerk.map(waardes)
    else:
        None

    return df


def plaatsingafspraakloonschaal(df):
    if "pl_plaatsingafspraakloonschaal" in df.columns:

        waardes = {
            "20150": "Minimum loon",
            "20151": "minimum loon (week)",
            "20152": "minimum loon (maand)",
            "20153": "Instroomtabel",
            "20154": "Normtabel",
            "20155": "Inlenersbeloning",
            "20156": "Loongebouw allocatie",
            "20157": "Loongebouw overige",
        }

        # transform plaatsingafspraakloonschaal

        df["pl_plaatsingafspraakloonschaal"] = df.pl_plaatsingafspraakloonschaal.map(
            waardes
        )  # mapped de velden om naar textwaarden

    else:
        None

    return df


def plaatsingafspraakbeloningsregeling(df):
    if "pl_plaatsingafspraakbeloningsregeling" in df.columns:

        waardes = {
            "23340": "minimum loon",
            "23341": "Inlenersbeloning",
            "23342": "uitzend cao",
            "23343": "N.v.t.",
        }

        df["pl_plaatsingafspraakbeloningsregeling"] = df.pl_plaatsingafspraakbeloningsregeling.map(
            waardes
        )

    else:
        None

    return df


def fwstapsysteem(df):
    if "pl_fwstapsysteem" in df.columns:
        waardes = {
            "22620": "NBBU Fase 1 (AOW plus)",
            "22621": "NBBU Fase 2 (AOW plus)",
            "22622": "NBBU Fase 3 (AOW plus)",
            "22623": "NBBU Fase 3 (AOW plus)",
            "22624": "NBBU Fase 4 (AOW plus)",
            "22630": "Niet ingedeeld (VPO)",
            "22631": "Bepaalde tijd",
            "22632": "Onbepaalde tijd",
            "22640": "Niet ingedeeld (ABU)",
            "22641": "ABU fase A",
            "22642": "ABU Fase B",
            "22643": "ABU fase 3",
            "22650": "Niet ingedeeld (NBBU)",
            "22651": "NBBU Fase",
            "12652": "NBBU Fase 2",
            "22653": "NBBU Fase 3",
            "22654": "NBBU Fase 4",
            "22660": "Niet ingedeeld (Niet aangesloten)",
            "22661": "Uitzendbeding",
            "22669": "Ketensysteem",
        }

        df["pl_fwstapsysteem"] = df.pl_fwstapsysteem.map(waardes)
    else:
        None

    return df
