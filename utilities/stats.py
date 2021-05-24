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
