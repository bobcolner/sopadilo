import numpy as np
import pandas as pd
# https://towardsdatascience.com/outlier-detection-with-hampel-filter-85ddf523c73d


class MADFilter:
    
    def __init__(self, value_winlen: int, deviation_winlen: int, k: int):
        self.value_winlen = value_winlen
        self.deviation_winlen = deviation_winlen
        self.k = k
        self.values = []
        self.deviations = []
        self.mad = []
        self.status = 'mad_warmup'

    def update(self, next_value) -> float:
        self.values.append(next_value)
        self.values = self.values[-self.value_winlen:]  # only keep winlen
        self.value_median = np.median(self.values)
        self.diff = next_value - self.value_median
        diff_type = 'simple'
        if diff_type is 'simple':
            self.abs_diff = abs(self.diff)
        elif diff_type == 'pct':
            self.abs_diff = abs(self.diff / self.value_median) * 100
        
        self.deviations.append(self.abs_diff)
        self.deviations = self.deviations[-self.deviation_winlen:]  # only keep winlen
        self.deviations_median = np.median(self.deviations)
        diff_min, diff_max = get_diff_minmax(diff_type='simple')
        self.deviations_median = diff_min if self.deviations_median < diff_min else self.deviations_median  # enforce lower limit
        self.deviations_median = diff_max if self.deviations_median > diff_max else self.deviations_median  # enforce upper limit
        self.mad.append(self.deviations_median)
        # final tick status logic
        if len(self.deviations) < (self.value_winlen):
            self.status = 'mad_warmup'
        elif self.abs_diff > (self.deviations_median * self.k):
            self.status = 'mad_outlier'
        else:
            self.status = 'mad_clean'

        return self.value_median


def get_diff_minmax(diff_type: str='simple') -> tuple:

    if diff_type == 'simple':
        diff_min = 0.0025
        diff_max = 0.05
    elif diff_type == 'pct':
        diff_min = 0.001
        diff_max = 0.03

    return diff_min, diff_max


def mad_filter_df(df: pd.DataFrame, col: str, value_winlen: int, deviation_winlen: int,
    k: int, center: bool=False, diff_type: str='simple') -> pd.DataFrame:
    
    df = df.copy()
    df[col+'_median'] = df[col].rolling(value_winlen, min_periods=value_winlen, center=center).median()
    if diff_type == 'simple':
        df[col+'_median_diff'] = abs(df[col] - df[col+'_median'])
    elif diff_type == 'pct':
        df[col+'_median_diff'] = abs((df[col] - df[col+'_median']) / df[col+'_median']) * 100

    df[col+'_median_diff_median'] = df[col+'_median_diff'].rolling(deviation_winlen, min_periods=(value_winlen * 3), center=False).median()

    diff_min, diff_max = get_diff_minmax(diff_type)

    df.loc[df[col+'_median_diff_median'] < diff_min, col+'_median_diff_median'] = diff_min  # enforce min bound
    df.loc[df[col+'_median_diff_median'] > diff_max, col+'_median_diff_median'] = diff_max  # enforce max bound
    df['mad_outlier'] = df[col+'_median_diff'] > (df[col+'_median_diff_median'] * k)  # outlier check 

    return df
