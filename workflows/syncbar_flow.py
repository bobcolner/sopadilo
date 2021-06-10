import ray
from utilities import date_fu
from data_layer import data_access
from workflows.syncbar_task import sync_symbol_date

start_date ='2019-01-01'
end_date ='2019-03-01'


def syncbar_flow(config_id: str, on_ray: bool=False):

    all_symbols = data_access.list_sd_data(prefix=f"/bars/{config_id}/df", source='local')
    large_caps, mid_caps = segment_symbols(all_symbols)
    if on_ray:
        sync_symbol_date_ray = ray.remote(sync_symbol_date)
        futures = []

    for sym_midcap in mid_caps:
        for sym_largecap in large_caps:
            print('syncing symbols:', sym_midcap, sym_largecap)
            if sym_largecap == sym_midcap:
                continue
            
            dates = get_dates(
                symbol=sym_largecap,
                prefix = f"/bars/{config_id}/sync_bars/clock_symbol={sym_midcap}",
                type='remaining',
                source='local',
            )
            print('scheduled', len(dates), 'dates')  # logging
            for date in dates:
                if on_ray:
                    future = sync_symbol_date_ray.remote(
                        clock_symbol=sym_midcap, 
                        sync_symbol=sym_largecap, 
                        date=date, 
                        config_id=config_id,
                        )
                    futures.append(future)
                else:
                    sync_symbol_date(clock_symbol=sym_midcap, sync_symbol=sym_largecap, date=date, config_id=config_id)
    if on_ray:
        ray.get(futures)


def get_dates(symbol: str, prefix: str, type: str='remaining', source: str='local') -> tuple:
    # find open market dates
    requested_dates = date_fu.get_open_market_dates(start_date, end_date)
    # all existing dates
    existing_dates = data_access.list_sd_data(symbol, prefix, source)
    if type == 'existing':
        # requested & avialiable dates
        dates = list(set(requested_dates).intersection(set(existing_dates)))
    elif type == 'remaining':
        # requested & remaiing dates
        dates = list(set(requested_dates).difference(set(existing_dates)))

    if len(dates) > 0:
        dates.sort()

    return dates


def segment_symbols(sym_list: list) -> tuple:

    df = data_access.fetch_market_daily(start_date, end_date)
    mdf = df.loc[df.symbol.isin(sym_list)]
    mdf['price_range'] = mdf['high'] - mdf['low']
    mdf['range_pct_value'] = mdf['price_range'] / mdf['vwap']
    gdf = mdf.groupby('symbol').mean()
    gdf_pct = gdf.describe(percentiles=[0.2, 0.5, 0.8])
    large_symbols = gdf[gdf.dollar_total > gdf_pct.loc['80%', 'dollar_total']]
    mid_symbols = gdf[
        (gdf.dollar_total > gdf_pct.loc['20%', 'dollar_total']) & 
        (gdf.dollar_total < gdf_pct.loc['50%', 'dollar_total']) &
        (gdf.dollar_total > gdf_pct.loc['50%', 'range_pct_value'])
    ]
    return large_symbols.index.to_list(), mid_symbols.index.to_list()
