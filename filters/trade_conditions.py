import pandas as pd


def irregular_conditions_filter(tdf: pd.DataFrame) -> pd.DataFrame:
    irregular_conditions = [2, 5, 7, 10, 13, 15, 16, 20, 21, 22, 29, 33, 38, 52, 53]
    irreg_idx = []
    for row in tdf.itertuples():
        irreg_flag = pd.Series(row.conditions).isin(irregular_conditions).any()
        irreg_idx.append(irreg_flag)

    tdf['irregular'] = irreg_idx
    return tdf
