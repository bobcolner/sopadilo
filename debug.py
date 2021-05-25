import datetime as dt
import numpy as np
import pandas as pd
from data_model import arrow_dataset, s3_backend
from tick_filter import streaming_tick_filter
from tick_sampler import streaming_tick_sampler, stacked, meta, meta_ray
from utilities import pickle

config = {
    'meta': {
        'symbol': 'GORO',
        'start_date': '2020-11-10',
        'end_date': '2020-11-16',
    },
    'filter': {
        'mad_value_winlen': 22,
        'mad_deviation_winlen': 1111,
        'mad_k': 17,
        'jma_winlen': 7,
        'jma_power': 2,
        # 'batch_freq': '2s',
    },
    'sampler': {
        'renko_return': 'price_jma_return',
        'renko_size': 0.1,  # for simple runs
        'renko_reveral_multiple': 2,
        'renko_range_frac': 22,
        'renko_range_min_pct_value': 0.03,  # X% of symbol value to enforc min renko size
        'max_duration_td': dt.timedelta(minutes=33),
        'min_duration_td': dt.timedelta(seconds=33),
        'min_tick_count': 33,
        'add_label': True,
        'reward_ratios': list(np.arange(2, 11, 0.5)),
    }
}

import ray
ray.init(dashboard_port=1111, ignore_reinit_error=True)

bds = meta_ray.sample_dates(config)

# bar dates stats
stacked.bar_dates_stats(bds)

# bds = pickle.pickle_dump(bds, 'tmp/bds.pickle')
# bds = pickle.pickle_load('tmp/bds.pickle')

stacked_df = stacked.fill_gaps_dates(bds, fill_col='price_close')
