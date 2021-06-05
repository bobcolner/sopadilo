import datetime as dt
import numpy as np
import pandas as pd
import ray

from tick_filter import streaming_tick_filter
from tick_sampler import streaming_tick_sampler, daily_stats
from workflows import sampler_task, sampler_flow, read_flow
from utilities import project_globals as g
from data_layer import data_access, arrow_dataset


prefix_data = '/data/trades'

all_syms = data_access.list_sd_data(prefix=prefix_data)

config = {
    'meta': {
        'symbol': 'EGO',
        'symbol_list': all_syms,
        'start_date': '2019-01-01',
        'end_date': '2021-01-28',
        'config_id': 'renko_v2',
        'presist_destination': 'both',
        'ray_on': True,
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
        'renko_size': 0.1,  # for simple runs
        'renko_reveral_multiple': 2,
        'renko_range_frac': 17,
        'renko_range_min_pct_value': 0.07,  # % of symbol value enforced as min renko size
        'max_duration_td': dt.timedelta(minutes=33),
        'min_duration_td': dt.timedelta(seconds=33),
        'min_tick_count': 33,
        'add_label': True,
        'reward_ratios': list(np.arange(2, 11, 0.5)),
    }
}

prefix_2 = f"/bars/{config['meta']['config_id']}/meta"

prefix_3 = f"/bars/{config['meta']['config_id']}/df"

ray.init(dashboard_host='0.0.0.0', dashboard_port=1111, ignore_reinit_error=True)
# ray.shutdown()

bds = sampler_flow.run(config)
