from utilities.globals import DATA_LOCAL_PATH, DATA_S3_PATH

print(DATA_LOCAL_PATH)
print(DATA_S3_PATH)

import datetime as dt
import numpy as np
import pandas as pd
from bar_sampler import meta, sampler, stacked


thresh = {
    # meta params
    'symbol': 'GLD',
    'start_date': '2020-01-10',
    'end_date': '2020-01-15',
    # filter
    'mad_value_winlen': 22,
    'mad_deviation_winlen': 1111,
    'mad_k': 11,
    'jma_winlen': 7,
    'jma_power': 2,
    # time batcher
    'batch_freq': '3s',
    # bar sampler params
    'renko_return': 'price_jma_return',
    'renko_size': 0.1,
    'renko_reveral_multiple': 2,
    'renko_range_frac': 22,
    'max_duration_td': dt.timedelta(minutes=33),
    'min_duration_td': dt.timedelta(seconds=33),
    'min_tick_count': 33,
    # label params
    'add_label': True,
    'reward_ratios': list(np.arange(2, 11, 0.5)),
}

print(thresh)

# bd = meta.get_bar_date(thresh, date='2019-01-02')

# print(bd.keys())
