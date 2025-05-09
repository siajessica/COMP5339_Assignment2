def cleaning(df):
    # Drop rows with any missing values
    df = df.dropna()

    # Drop 'code' column if identical to 'stationcode'
    if 'code' in df.columns and (df['code'] == df['stationcode']).all():
        df = df.drop(columns=['code'])

    # Drop exact duplicate rows on selected subset
    df = df.drop_duplicates(subset=['brand', 'stationid', 'address', 'fueltype', 'lastupdated'])

    # Convert date column format
    df['lastupdated'] = pd.to_datetime(df['lastupdated'], dayfirst=True)

    # Remove rows where 'price' is non-positive
    df = df[df['price'] > 0]

    return df
