def format_data(df):

    if "fw_briefhoofd" in df.columns:
        waardes = {"24980": "Jij", "24981": "U"}
        df["fw_briefhoofd"] = df.fw_briefhoofd.map(waardes)

    if "fw_geslacht" in df.columns:
        waardes = {"20091": "Man", "20092": "Vrouw"}

        df["fw_geslacht"] = df.fw_geslacht.map(waardes)

    if (
        "fw_voornaam" in df.columns
        and "fw_voorvoegsels" in df.columns
        and "fw_achternaam" in df.columns
    ):
        df["fw_naam"] = (
            df.fw_voornaam.fillna("")
            + " "
            + df.fw_voorvoegsels.fillna("")
            + " "
            + df.fw_achternaam.fillna("")
        ).str.strip()

    if "fw_commtype" in df.columns:
        waardes = {
            "20010": "Vanuit conversie",
            "20012": "Telefoon (intern)",
            "20013": "Skype",
            "20014": "Telefoon (naamnummer)",
            "20015": "Telefoon",
            "20016": "Mobiel",
            "20017": "E-mail",
            "20018": "Website",
            "20019": "Fax",
        }

        df["fw_commtype"] = df.fw_commtype.map(waardes)

    if "fw_type" in df.columns:
        waardes = {"10040": "Flexwerker", "10041": "Vaste medewerker"}

        df["fw_type"] = df.fw_type.map(waardes)

    if "fw_burgerlijkestaat" in df.columns:
        waardes = {
            "21660": "Ongehuwd",
            "21661": "Gehuwd",
            "21662": "Samenwonend",
            "21663": "Gescheiden",
            "21664": "Verweduwd ",
            "21665": "Onbekend",
        }

        df["fw_burgerlijkestaat"] = df.fw_burgerlijkestaat.map(waardes)

    if "fw_autorisatiestatus" in df.columns:
        waardes = {"21480": "Wit", "21481": "Groen", "21482": "Geel", "21483": "Rood"}

        df["fw_autorisatiestatus"] = df.fw_autorisatiestatus.map(waardes)

    return df
