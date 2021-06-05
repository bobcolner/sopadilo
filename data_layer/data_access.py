from typing import Union
from pathlib import Path
import pandas as pd
from data_layer import storage_adaptor
from utilities import project_globals as g


# local data source
(Path.cwd() / 'tmp/local_data').mkdir(parents=True, exist_ok=True)
fs_local = storage_adaptor.StorageAdaptor('local', root_path=g.DATA_LOCAL_PATH)

# remote data source with local filecache
(Path.cwd() / 'tmp/fsspec_cache').mkdir(parents=True, exist_ok=True)
fs_remote = storage_adaptor.StorageAdaptor('s3_filecache', root_path=g.DATA_S3_PATH)


def walk_prefix(prefix: str, source: str='remote') -> list:
    if source == 'local':
        walks = fs_local.fs.walk(g.DATA_LOCAL_PATH+prefix)
    elif source == 'remote':
        walks = fs_remote.fs.walk(g.DATA_S3_PATH+prefix)

    df = pd.DataFrame(walks, columns=['dirpath','dirnames','filenames'])
    df['filenames'] = df['filenames'].astype('string').str.strip("['']")
    if source == 'remote':
        df['dirpath'] = df['dirpath'].str.replace(g.DATA_S3_PATH, '')
    elif source == 'local':
        df['dirpath'] = df['dirpath'].str.replace(g.DATA_LOCAL_PATH, '')

    files_df = df[df.filenames != '']
    paths = files_df.dirpath + '/' + files_df.filenames

    return paths.to_list()


def list_sd_data(symbol: str=None, prefix: str='', source: str='remote', show_storage: bool=False) -> Union[list, dict]:
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


def fetch_sd_data(symbol: str, date: str, prefix: str, source: str='local_then_remote') -> object:
    if source == 'local_then_remote':
        try:
            sd_date = fs_local.read_sd_data(symbol, date, prefix)
        except FileNotFoundError:
            sd_date = fs_remote.read_sd_data(symbol, date, prefix)
    elif source == 'remote':
        sd_date = fs_remote.read_sd_data(symbol, date, prefix)
    elif source == 'local':
        sd_date = fs_local.read_sd_data(symbol, date, prefix)

    return sd_date


def presist_df(df: pd.DataFrame, fs_path: str, destination: str='remote'):
    if destination in ['local', 'both']:
        fs_local.write_df_to_fs(df, fs_path)

    if destination in ['remote', 'both']:
        fs_remote.write_df_to_fs(df, fs_path)


def presist_sd_data(sd_data: Union[object, pd.DataFrame], symbol: str, date: str, prefix: str, destination: str='remote'):
    if destination in ['local', 'both']:
        fs_local.write_sd_data(sd_data, symbol, date, prefix)

    if destination in ['remote', 'both']:
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
