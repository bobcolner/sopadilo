import ray
from utilities import date_fu
from data_layer import data_access
from workflows.syncbar_task import sync_symbol_date


config_id = 'renko_v2'
base_symbol = 'SAND'
sync_symbol = 'AU'
date = '2020-01-02'
config_id = 'renko_v2'

sync_symbol_date(base_symbol, sync_symbol, date, config_id)

def syncbar_flow(symbol_list: list, start_date: str, end_date: str, on_ray: bool=False):

    sym_leaders, sym_followers = segment_symbols(symbol_list, start_date, end_date)

    if on_ray:
        sync_bars_ray = ray.remote(sync_bars)

    for sym_follower in sym_followers:
        _, requested_remaining_dates = get_remaining_dates(
            symbol=sym_follower,
            prefix=f"/bars/{config_id}/sync_bars/clock_symbol={sym_follower}", 
            start_date=start_date, 
            end_date=end_date
            )
        for date in requested_remaining_dates:

            for sym_leader in sym_leaders:
                if sym_leader == sym_follower:
                    continue

                if on_ray:
                    sync_bars_ray.remote(bars_df, sym_follower, sym_leader, date, prefix)
                else:
                    sync_bars(bars_df, sym_follower, sym_leader, date, prefix)


def get_remaining_dates(symbol: str, prefix: str, start_date: str, end_date: str) -> tuple:
    # find open market dates
    requested_dates = date_fu.get_open_market_dates(start_date, end_date)
    # all avialiable tick backfill dates
    existing_dates = data_access.list_sd_data(symbol, prefix, source='remote')
    # requested & avialiable dates
    requested_existing_dates = list(set(requested_dates).intersection(set(existing_dates)))
    # requested & remaiing dates
    requested_remaining_dates = list(set(requested_dates).difference(set(existing_dates)))
    return requested_existing_dates, requested_remaining_dates


def segment_symbols(sym_list: list, start_date: str, end_date: str) -> tuple:

    df = data_access.fetch_market_daily(start_date, end_date)
    mdf = df.loc[df.symbol.isin(sym_list)]
    mdf['price_range'] = mdf['high'] - mdf['low']
    mdf['range_pct_value'] = mdf['price_range'] / mdf['vwap']
    gdf = mdf.groupby('symbol').mean()
    gdf_pct = gdf.describe(percentiles=[.2,.5,.8])
    large_symbols = gdf[gdf.dollar_total > gdf_pct.loc['80%', 'dollar_total']]
    mid_symbols = gdf[
        (gdf.dollar_total > gdf_pct.loc['20%', 'dollar_total']) & 
        (gdf.dollar_total < gdf_pct.loc['50%', 'dollar_total']) &
        (gdf.dollar_total > gdf_pct.loc['50%', 'range_pct_value'])
    ]
    return large_symbols.index.to_list(), mid_symbols.index.to_list()
