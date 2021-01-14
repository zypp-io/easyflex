def format_data(df):
    if "fw_stapsysteem" in df.columns:
        waardes = {
            "22620": "NBBU Fase 1 (AOW plus)",
            "22621": "NBBU Fase 2 (AOW plus)",
            "22622": "NBBU Fase 3 (AOW plus)",
            "22623": "‘NBBU Fase 3 (AOW plus)",
            "22624": "NBBU Fase 4 (AOW plus)",
            "22630": "Niet ingedeeld",
            "22631": "Bepaalde tijd",
            "22632": "Onbepaalde tijd",
            "22640": "Niet ingedeeld",
            "22641": "ABU Fase A",
            "22642": "ABU Fase B",
            "22643": "ABU Fase C",
            "22650": "Niet ingedeeld",
            "22651": "NBBU Fase 1",
            "22652": "NBBU Fase 2",
            "22653": "NBBU Fase 3",
            "22654": "NBBU Fase 4",
            "22660": "Niet ingedeeld",
            "22661": "Uitzendbeding",
            "22669": "Ketensysteem",
        }

        df["fw_stapsysteem"] = df.fw_stapsysteem.map(
            waardes
        )  # mapped de velden om naar textwaarden

    return df
