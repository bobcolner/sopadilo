import pandas as pd
from statsmodels.stats.weightstats import DescrStatsW


def weighted_mean(values: pd.Series, weights: pd.Series) -> pd.Series:
    return (values * weights).sum() / weights.sum()


def weighted_median(df: pd.DataFrame, val: 'str', weight: 'str') -> pd.DataFrame:
    df_sorted = df.sort_values(val)
    cumsum = df_sorted[weight].cumsum()
    cutoff = df_sorted[weight].sum() / 2.0
    return df_sorted[cumsum >= cutoff][val].iloc[0]


def weighted_quantile(values: list, weights: list, probs: list=[.05, .25, .5, .75, .95]) -> list:
    dsw = DescrStatsW(data=values, weights=weights)
    return dsw.quantile(probs).values


def cyclic_transform(df: pd.DataFrame, col: str):
    df = df.copy()
    unq_values = len(df[col].unique())
    df[col+'_sin'] = np.sin(df[col] * (2 * np.pi / unq_values))
    df[col+'_cos'] = np.cos(df[col] * (2 * np.pi / unq_values))
    return df
