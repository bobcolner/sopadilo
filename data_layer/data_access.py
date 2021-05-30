from typing import Union
import pandas as pd
from data_layer import storage_adaptor
from utilities import globals_unsafe as g


fs_remote = storage_adaptor.StorageAdaptor('s3_filecache', root_path=g.DATA_S3_PATH)

fs_local = storage_adaptor.StorageAdaptor('local', root_path=g.DATA_LOCAL_PATH)


def list_symbol_storage(symbol: str, prefix: str, source: str='remote'):

    if source == 'local':
        results = fs_local.list_symbol_storage(symbol, prefix)
    elif source == 'remote':
        results = fs_remote.list_symbol_storage(symbol, prefix)
    elif source == 'both':
        results = {
            'local': fs_local.list_symbol_storage(symbol, prefix),
            'remote': fs_remote.list_symbol_storage(symbol, prefix),
        }
    return results


def list_symbol_dates(symbol: str, prefix: str, source: str='remote'):

    if source == 'local':
        results = fs_local.list_symbol_dates(symbol, prefix)
    elif source == 'remote':
        results = fs_remote.list_symbol_dates(symbol, prefix)

    return results


def fetch_sd_data(symbol: str, date: str, prefix: str) -> object:
    try:  # try local
        try:  # try load dataframe
            sd_obj = fs_local.read_sdf(symbol, date, prefix)
        except: # try load pickle
            sd_obj = fs_local.read_sdpickle(symbol, date, prefix)
    except FileNotFoundError:  # try remote
        try:
            sd_obj = fs_remote.read_sdf(symbol, date, prefix)
        except:
            sd_obj = fs_remote.read_sdpickle(symbol, date, prefix)

    return sd_obj


def presist_sd_data(sd_data: Union[object, pd.DataFrame], symbol: str, date: str, prefix: str):

    if type(sd_data) == pd.DataFrame:
        fs_local.write_sdf(sd_data, symbol, date, prefix)
        fs_remote.write_sdf(sd_data, symbol, date, prefix)
    else:
        fs_local.write_sdpickle(sd_data, symbol, date, prefix)
        fs_remote.write_sdpickle(sd_data, symbol, date, prefix)


def fetch_polygon_data(symbol: str, date: str, tick_type: str='trades', prefix: str='/data') -> pd.DataFrame:

    try:
        print(symbol, date, 'trying to get data from local file...')
        sdf = fs_local.read_sdf(symbol, date, prefix=f"{prefix}/{tick_type}")
    except FileNotFoundError:
        try:
            print(symbol, date, 'trying to get data from s3/b2...')
            sdf = fs_remote.read_sdf(symbol, date, prefix=f"{prefix}/{tick_type}")
        except FileNotFoundError:
            print(symbol, date, 'getting data from polygon API...')
            sdf = polygon_df.get_date_df(symbol, date, tick_type)
            print(symbol, date, 'saving data to S3/B2...')
            fs_remote.write_sdf(sdf, symbol, date, prefix=f"{prefix}/{tick_type}")
        finally:
            print(symbol, date, 'saving data to local file')
            fs_local.write_sdf(sdf, symbol, date, prefix=f"{prefix}/{tick_type}")

    return sdf


def fetch_market_daily(start_date: str, end_date: str) -> pd.DataFrame:
    try:
        mdf = fs_local.read_df_from_fs('/data/daily_agg/data.feather')
    except:
        mdf = fs_remote.read_df_from_fs('/data/daily_agg/data.feather')
    
    mask = (mdf['date'] >= start_date) & (mdf['date'] <= end_date)
    return mdf[mask]
