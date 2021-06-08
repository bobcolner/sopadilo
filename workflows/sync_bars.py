from data_layer import data_access
from sample_features import ticks_to_bar

prefix_df = '/bars/renko_v2/df'
prefix_meta = '/bars/renko_v2/meta'

sym_leaders = data_access.list_sd_data(prefix=prefix_df, source='local')
sym_leaders = ['VTI', 'GLD']
sym_followers = ['AU','SAND']

for sym_follow in sym_followers:
    date_list = data_access.list_sd_data(sym_follow, prefix=prefix_df, source='local')
    # date_list = ['2020-01-02', '2020-01-03']
    for date in date_list:
        bars_df = data_access.fetch_sd_data(sym_follow, date, prefix=prefix_df)

        for sym_leader in sym_leaders:
            # get sym pair ticks
            ticks_df = data_access.fetch_sd_data(sym_leader, date, prefix='/data/trades')
            # get precomputed filter idx
            bar_date = data_access.fetch_sd_data(sym_leader, date, prefix=prefix_meta)
            fidx = bar_date['filtered_df']
            tdf = ticks_df.drop(index=fidx.index)
            tdf = tdf.set_index('sip_dt').tz_localize('UTC').tz_convert('America/New_York')
            # for each bar in sym_follower
            bars = []
            for bar in bars_df.itertuples():
                bar_ticks = tdf.loc[(tdf.index >= bar.open_at) & (tdf.index <= bar.close_at)]
                bdf = ticks_to_bar.ticks_to_bar(price=bar_ticks['price'], volume=bar_ticks['size'], close_at=bar_ticks.index)
                bdf.update({'symbol': sym_leader, 'date': date})
                bars.append(bdf)

            data_access.presist_sd_data(pd.DataFrame(bars), sym_leader, date, prefix=f"/sync_bars/{sym_follow}", destination='both')
