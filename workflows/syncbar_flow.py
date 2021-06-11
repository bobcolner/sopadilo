import ray
from utilities import date_fu
from data_layer import data_access
from workflows.syncbar_task import sync_symbol_date
from tenacity import retry


@retry
def run(config: dict):

    all_symbols = data_access.list_sd_data(prefix=f"/bars/{config['config_id']}/df", source=config['source'])
    large_caps, mid_caps = segment_symbols(all_symbols, start_date=config['start_date'], end_date=config['end_date'])
    if config['on_ray']:
        sync_symbol_date_ray = ray.remote(sync_symbol_date)
        futures = []

    for sym_midcap in mid_caps:
        for sym_largecap in large_caps:
            if sym_largecap == sym_midcap:
                continue

            dates = get_dates(
                symbol=sym_largecap,
                prefix = f"/bars/{config['config_id']}/sync_bars/clock_symbol={sym_midcap}",
                start_date=config['start_date'],
                end_date=config['end_date'],
                type='remaining',
                source=config['source'],
            )
            print('syncing symbols:', sym_midcap, sym_largecap, 'scheduled', len(dates), 'dates')  # logging
            for date in dates:
                if config['on_ray']:
                    future = sync_symbol_date_ray.remote(
                        clock_symbol=sym_midcap,
                        sync_symbol=sym_largecap,
                        date=date,
                        config=config,
                        )
                    futures.append(future)
                else:
                    sync_symbol_date(clock_symbol=sym_midcap, sync_symbol=sym_largecap, date=date, config=config)
    if config['on_ray']:
        ray.get(futures)


def get_dates(symbol: str, prefix: str, start_date: str, end_date: str, type: str, source: str) -> tuple:
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


def segment_symbols(sym_list: list, start_date: str, end_date: str) -> tuple:

    df = data_access.fetch_market_daily(start_date, end_date)
    mdf = df.loc[df.symbol.isin(sym_list)]
    mdf.loc[:, 'price_range'] = mdf['high'] - mdf['low']
    mdf.loc[:, 'range_pct_value'] = mdf['price_range'] / mdf['vwap']
    gdf = mdf.groupby('symbol').mean()
    gdf_pct = gdf.describe(percentiles=[0.2, 0.5, 0.8])
    large_symbols = gdf[gdf.dollar_total > gdf_pct.loc['80%', 'dollar_total']]
    mid_symbols = gdf[
        (gdf.dollar_total > gdf_pct.loc['20%', 'dollar_total']) & 
        (gdf.dollar_total < gdf_pct.loc['50%', 'dollar_total']) &
        (gdf.dollar_total > gdf_pct.loc['50%', 'range_pct_value'])
    ]
    return large_symbols.index.to_list(), mid_symbols.index.to_list()
