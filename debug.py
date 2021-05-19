import datetime as dt
import numpy as np

thresh = {
    # meta params
    'symbol': 'VTI',
    'start_date': '2020-01-02',
    'end_date': '2020-01-07',
    # bar sampler params
    'renko_return': 'price_return',
    'renko_size': 0.1,
    'renko_reveral_multiple': 2,
    'renko_range_frac': 22,
    'max_duration_td': dt.timedelta(minutes=33),
    'min_duration_td': dt.timedelta(seconds=33),
    'min_tick_count': 33,
    # label params
    'add_label': True,
    'reward_ratios': list(np.arange(2.5, 11, 0.5)),
    # filter
    'mad_value_winlen': 22,
    'mad_deviation_winlen': 1111,
    'mad_k': 11,
    'jma_winlen': 7,
    'jma_power': 2,
}

from bar_sampler.meta import get_bar_date, get_bar_dates

# bd = get_bar_date(thresh, date='2020-01-02')
bds = get_bar_dates(thresh)

from bar_sampler.stacked import fill_gaps_dates

# fill daily gaps
stacked_df = fill_gaps_dates(bds, fill_col='price_vwap')
