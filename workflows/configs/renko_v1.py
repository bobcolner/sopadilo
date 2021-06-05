import datetime as dt
import numpy as np
from data_layer import data_access

prefix_data = '/data/trades'

all_syms = data_access.list_sd_data(prefix=prefix_data)

config = {
    'meta': {
        'symbol_list': all_syms,
        'start_date': '2019-01-01',
        'end_date': '2021-01-28',
        'config_id': 'renko_v1',
        'presist_destination': 'both',
        'on_ray': True,
    },
    'filter': {
        'mad_value_winlen': 22,
        'mad_deviation_winlen': 1111,
        'mad_k': 17,
        'jma_winlen': 7,
        'jma_power': 2,
    },
    'sampler': {
        'renko_return': 'price_jma_return',
        'renko_reveral_multiple': 2,
        'renko_range_frac': 22,
        'renko_range_min_pct_value': 0.04,  # % of symbol value enforced as min renko size
        'max_duration_td': dt.timedelta(minutes=33),
        'min_duration_td': dt.timedelta(seconds=33),
        'min_tick_count': 33,
        'add_label': True,
        'reward_ratios': list(np.arange(2, 11, 0.5)),
    }
}