def curve_drop(distance_miles: float) -> float:
    # globe earth curve drop in inches
    drop_inches = (distance_miles ** 2) * 8 / 12
    return drop_inches


def compound_interest(principle: float, rate: float, peroids: int):
    # Calculates compound interest
    total_return = principle * (pow((1 + rate / 100), peroids))
    print("Total Interest $:", round(total_return, 2))
    print("Anualized Peroid %", round(total_return / principle, 1) * 100)


def read_matching_files(glob_string: str, reader=pd.read_csv) -> pd.DataFrame:
    from glob import glob
    from os import path

    return pd.concat(map(reader, glob(path.join('', glob_string))), ignore_index=True)


def cyclic_transform(df: pd.DataFrame, col: str):
    df = df.copy()
    unq_values = len(df[col].unique())
    df[col + '_sin'] = np.sin(df[col] * (2.0 * np.pi / 7))
    df[col + '_cos'] = np.cos(df[col] * (2.0 * np.pi / 7))
    return df
