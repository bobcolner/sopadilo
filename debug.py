import datetime as dt
import pickle
import numpy as np
import pandas as pd
# import pandas_bokeh
# pandas_bokeh.output_file("/tmp/bokeh_output.html")
import ray

from data_layer import arrow_dataset, storage_adaptor
from tick_filter import streaming_tick_filter
from tick_sampler import streaming_tick_sampler, daily_stats
from workflows import sampler_task, sampler_flow
from utilities import date_fu, project_globals as g
from data_layer import storage_adaptor, fsspec_factory, data_access


config = {
    'meta': {
        'symbol': 'AU',
        'start_date': '2019-01-01',
        'end_date': '2019-02-01',
        'config_id': 'renko_v1',
        'presist_destination': 'remote',
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
        'renko_range_frac': 22,
        'renko_range_min_pct_value': 0.03,  # % of symbol value enforced as min renko size
        'max_duration_td': dt.timedelta(minutes=33),
        'min_duration_td': dt.timedelta(seconds=33),
        'min_tick_count': 33,
        'add_label': True,
        'reward_ratios': list(np.arange(2, 11, 0.5)),
    }
}

prefix_1 = f"/tick_samples/{config['meta']['config_id']}/bar_date"

prefix_2 = f"/tick_samples/{config['meta']['config_id']}/bars_df"

prefix_3 = '/data/trades'


ray.init(dashboard_port=1111, ignore_reinit_error=True)
# ray.shutdown()

data_access.list('MAG', prefix_1, show_storage=True, source='remote')

bar_dates = sampler_flow.run(config, ray_on=True)

