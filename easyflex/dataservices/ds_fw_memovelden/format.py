def format_data(df):

    if "fw_memonummer" in df.columns:
        df = df[df.fw_memonummer.isna() == False]

    return df
