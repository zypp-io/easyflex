def format_data(df):
    if "fwcomm_commtype" in df.columns:
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

        df["fwcomm_commtype"] = df.fwcomm_commtype.map(
            waardes
        )  # mapped de velden om naar textwaarden

    return df
