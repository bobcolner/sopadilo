from typing import Union
import pandas as pd
from data_layer import storage_adaptor
from utilities import project_globals as g


fs_local = storage_adaptor.StorageAdaptor('local', root_path=g.DATA_LOCAL_PATH)
fs_remote = storage_adaptor.StorageAdaptor('s3', root_path=g.DATA_S3_PATH)


def list(symbol: str=None, prefix: str='', source: str='remote', show_storage: bool=False) -> Union[list, dict]:
    if symbol:
        if source == 'local':
            results = fs_local.ls_symbol_dates(symbol, prefix, show_storage)
        elif source == 'remote':
            results = fs_remote.ls_symbol_dates(symbol, prefix, show_storage)
    else:
        if source == 'local':
            results = fs_local.ls_symbols(prefix, show_storage)
        elif source == 'remote':
            results = fs_remote.ls_symbols(prefix, show_storage)

    return results


def fetch_sd_data(symbol: str, date: str, prefix: str) -> object:
    try:
        sd_date = fs_local.read_sd_data(symbol, date, prefix)
    except FileNotFoundError:
        sd_date = fs_remote.read_sd_data(symbol, date, prefix)

    return sd_date


def presist_sd_data(sd_data: Union[object, pd.DataFrame], symbol: str, date: str, prefix: str, destination: str='remote'):
    if destination in ['local', 'both']:
        fs_local.write_sd_data(sd_data, symbol, date, prefix)
    elif destination in ['remote', 'both']:
        fs_remote.write_sd_data(sd_data, symbol, date, prefix)


def fetch_polygon_data(symbol: str, date: str, prefix: str='/data/trades') -> pd.DataFrame:
    try:
        print(symbol, date, 'trying to get fetch data local or remote...')
        sdf = fetch_sd_data(symbol, date, prefix)
    except:
        from data_api import polygon_df
        print(symbol, date, 'getting data from polygon API...')
        sdf = polygon_df.get_date_df(symbol, date, tick_type='trades')
        print(symbol, date, 'saving data to local & remote')
        presist_sd_data(sdf, symbol, date, prefix, source='both')

    return sdf


def fetch_market_daily(start_date: str=None, end_date: str=None) -> pd.DataFrame:
    try:
        mdf = fs_local.read_df_from_fs('/data/daily_agg/data.feather')
    except:
        mdf = fs_remote.read_df_from_fs('/data/daily_agg/data.feather')
    
    if (start_date and end_date):
        mask = (mdf['date'] >= start_date) & (mdf['date'] <= end_date)
        return mdf[mask]
    else:
        return mdf
