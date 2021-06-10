import pandas as pd
from data_layer import data_access
from sample_features import ticks_to_bar


def sync_symbol_date(clock_symbol: str, sync_symbol: str, date: str, config_id: str):
    # get base symbol bars
    base_bars = data_access.fetch_sd_data(clock_symbol, date, prefix=f"/bars/{config_id}/df")
    # get sync symbol ticks
    sync_ticks = fetch_clean_ticks(sync_symbol, date, config_id)
    # build syncbar for each base symbol bar
    sync_bars = []
    for bar in base_bars.itertuples():
        syncbar_ticks = sync_ticks.loc[(sync_ticks.index > bar.open_at) & (sync_ticks.index <= bar.close_at)]
        if len(syncbar_ticks) < 1:
            print(clock_symbol, sync_symbol, date, 'zero tick syncbar')
            continue

        syncbar = ticks_to_bar.ticks_to_bar(
            price=syncbar_ticks['price'],
            volume=syncbar_ticks['size'],
            close_at=syncbar_ticks.index,
            )
        syncbar.update({'symbol': sync_symbol, 'date': date})
        sync_bars.append(syncbar)

    print('clock:', clock_symbol, 'sync:', sync_symbol, date)  # logging
    data_access.presist_sd_data(pd.DataFrame(sync_bars).reset_index(drop=True),
        symbol=sync_symbol,
        date=date,
        prefix=f"/bars/{config_id}/sync_bars/clock_symbol={clock_symbol}",
        destination='local',
        )


def fetch_clean_ticks(symbol: str, date: str, config_id: str):
    # get symbol-date ticks
    ticks_df = data_access.fetch_sd_data(symbol, date, prefix="/data/trades")
    # get precomputed filter idx
    bar_date = data_access.fetch_sd_data(symbol, date, prefix=f"/bars/{config_id}/meta")
    fidx = bar_date['filtered_df']
    ticks_df = ticks_df.drop(index=fidx.index)
    ticks_df = ticks_df.set_index('sip_dt').tz_localize('UTC').tz_convert('America/New_York')
    return ticks_df
