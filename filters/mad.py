import numpy as np
import pandas as pd


def mad_filter_df(df: pd.DataFrame, col: str, value_winlen: int, devations_winlen: int,
    k: int, center: bool, diff: str) -> pd.DataFrame:
    
    df = df.copy()
    df[col+'_median'] = df[col].rolling(value_winlen, min_periods=value_winlen, center=center).median()
    if diff == 'simple':
        df[col+'_median_diff'] = abs(df[col] - df[col+'_median'])
    elif diff == 'pct':
        df[col+'_median_diff'] = abs((df[col] - df[col+'_median']) / df[col+'_median']) * 100

    df[col+'_median_diff_median'] = df[col+'_median_diff'].rolling(devations_winlen, min_periods=value_winlen, center=False).median()
    if diff == 'simple':
        sim_min = 0.005
        sim_max = 0.05
        df.loc[df[col+'_median_diff_median'] < sim_max, col+'_median_diff_median'] = sim_min  # enforce min bound
        df.loc[df[col+'_median_diff_median'] > sim_min, col+'_median_diff_median'] = sim_max  # enforce max bound
    elif diff == 'pct':
        pct_min = 0.001
        pct_max = 0.03
        df.loc[df[col+'_median_diff_median'] < pct_min, col+'_median_diff_median'] = pct_min  # enforce min bound
        df.loc[df[col+'_median_diff_median'] > pct_max, col+'_median_diff_median'] = pct_max  # enforce max bound

    df['mad_outlier'] = df[col+'_median_diff'] > (df[col+'_median_diff_median'] * k)
    print(df.mad_outlier.value_counts() / len(df))
    return df


class MADFilter:
    
    def __init__(self, value_winlen: int=22, deviation_winlen: int=1111, k: int=11):
        self.value_winlen = value_winlen
        self.deviation_winlen = deviation_winlen
        self.k = k
        self.values = []
        self.deviations = []
        self.status = 'mad_warmup'

    def update(self, next_value) -> float:
        self.values.append(next_value)
        self.values = self.values[-self.value_winlen:]  # only keep winlen
        self.value_median = np.median(self.values)
        self.diff = next_value - self.value_median
        if diff=='simple':
            self.abs_diff = abs(self.diff)
        elif diff=='pct':
            self.abs_diff = abs(self.diff / self.value_median) * 100
        
        self.deviations.append(self.abs_diff)
        self.deviations = self.deviations[-self.deviation_winlen:]  # only keep winlen
        self.deviations_median = np.median(self.deviations)
        self.deviations_median = 0.005 if self.deviations_median < 0.005 else self.deviations_median  # enforce lower limit
        self.deviations_median = 0.05 if self.deviations_median > 0.05 else self.deviations_median  # enforce upper limit
        # final tick status logic
        if len(self.values) < (self.deviation_winlen):
            self.status = 'mad_warmup'
        elif self.abs_diff > (self.deviations_median * self.k):
            self.status = 'mad_outlier'    
        else:
            self.status = 'mad_clean'

        return self.value_median
