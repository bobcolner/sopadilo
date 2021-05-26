from io import BytesIO
from tempfile import NamedTemporaryFile
from pathlib import Path
import pandas as pd
import fsspec
from data_api import polygon_df
from utilities import pickle
from utilities.globals_unsafe import DATA_LOCAL_PATH, DATA_S3_PATH, B2_ACCESS_KEY_ID, B2_SECRET_ACCESS_KEY, B2_ENDPOINT_URL


def get_s3fs_client(fs_type: str) -> fsspec.filesystem:
    if fs_type == 's3_cached':
        # https://filesystem-spec.readthedocs.io/
        s3fs = fsspec.filesystem(
            protocol='filecache',
            target_protocol='s3',
            target_options={
                'key': B2_ACCESS_KEY_ID,
                'secret': B2_SECRET_ACCESS_KEY,
                'client_kwargs': {'endpoint_url': B2_ENDPOINT_URL}
                },
            cache_storage='./tmp/fsspec_cache'
            )
    elif fs_type == 's3':
        s3fs = fsspec.filesystem(
            protocol='s3', 
            key=B2_ACCESS_KEY_ID, 
            secret=B2_SECRET_ACCESS_KEY, 
            client_kwargs={'endpoint_url': B2_ENDPOINT_URL}
            )
    elif fs_type == 'local':
        s3fs = fsspec.filesystem(protocol='file')

    return s3fs


s3fs = get_s3fs_client(fs_type='s3_cached')


def list_symbol_dates(symbol: str, tick_type: str) -> str:
    paths = s3fs.ls(path=DATA_S3_PATH + f"/{tick_type}/symbol={symbol}/", refresh=True)
    return [path.split('date=')[1] for path in paths]


def list_symbols(tick_type: str) -> str:
    paths = s3fs.ls(path=DATA_S3_PATH + f"/{tick_type}", refresh=True)
    return [path.split('symbol=')[1] for path in paths]


def list_path(s3_path: str) -> str:
    return s3fs.ls(path=DATA_S3_PATH + f"{s3_path}", refresh=True)


def ls(path: str) -> str:
    return s3fs.ls(path, refresh=True)


def remove_symbol(symbol: str, tick_type: str):
    path = DATA_S3_PATH + f"/{tick_type}/symbol={symbol}/"
    s3fs.rm(path, recursive=True)


def show_symbol_storage_used(symbol: str, tick_type: str) -> dict:
    path = DATA_S3_PATH + f"/{tick_type}/symbol={symbol}/"
    return s3fs.du(path)


def get_date_df_from_s3(symbol: str, date: str, tick_type: str, columns: list=None) -> pd.DataFrame:
    byte_data = s3fs.cat(DATA_S3_PATH + f"/{tick_type}/symbol={symbol}/date={date}/data.feather")
    if columns:
        df = pd.read_feather(BytesIO(byte_data), columns=columns)
    else:
        df = pd.read_feather(BytesIO(byte_data))
    return df


def get_df_from_s3(path: str, columns: list=None) -> pd.DataFrame:
    byte_data = s3fs.cat(DATA_S3_PATH + path)
    if columns:
        df = pd.read_feather(BytesIO(byte_data), columns=columns)
    else:
        df = pd.read_feather(BytesIO(byte_data))
    return df


def get_pickle_from_s3(path: str) -> object:
    byte_data = s3fs.cat(DATA_S3_PATH + path)
    obj = pickle.load(byte_data)
    return obj


def put_date_df_to_s3(df: pd.DataFrame, symbol: str, date: str, tick_type: str):
    with NamedTemporaryFile(mode='w+b') as tmp_ref1:
        df.to_feather(path=tmp_ref1.name, version=2)
        s3fs.put(tmp_ref1.name, DATA_S3_PATH + f"/{tick_type}/symbol={symbol}/date={date}/data.feather")


def put_df_to_s3(df: pd.DataFrame, s3_path: str):
    with NamedTemporaryFile(mode='w+b') as tmp_ref1:
        df.to_feather(path=tmp_ref1.name, version=2)
        s3fs.put(tmp_ref1.name, DATA_S3_PATH + f"/{s3_path}/data.feather")


def put_pickle_to_s3(obj, s3_path: str):
    with NamedTemporaryFile(mode='w+b') as tmp_ref1:
        pickle.file_dump(object=obj, file_name=tmp_ref1.name)
        s3fs.put(tmp_ref1.name, DATA_S3_PATH + f"/{s3_path}/object.pickle")


def put_file_path(local_path: str, s3_path: str):
    s3fs.put(local_path, DATA_S3_PATH + f"/{s3_path}/")


def date_df_to_file(df: pd.DataFrame, symbol:str, date:str, tick_type: str) -> str:
    path = DATA_LOCAL_PATH + f"/{tick_type}/symbol={symbol}/date={date}/"
    Path(path).mkdir(parents=True, exist_ok=True)
    df.to_feather(path+'data.feather', version=2)
    return path + 'data.feather'


def get_and_save_date_df(symbol: str, date: str, tick_type: str) -> pd.DataFrame:
    print(symbol, date, 'getting data fron polygon api')
    df = polygon_df.get_date_df(symbol, date, tick_type)
    print(symbol, date, 'putting data to S3/B2')
    put_date_df_to_s3(df, symbol, date, tick_type)
    print(symbol, date, 'saving data to local file')
    path = date_df_to_file(df, symbol, date, tick_type)
    return df


def fetch_date_df(symbol: str, date: str, tick_type: str) -> pd.DataFrame:
    try:
        print(symbol, date, 'trying to get data from local file...')
        df = pd.read_feather(DATA_LOCAL_PATH + f"/{tick_type}/symbol={symbol}/date={date}/data.feather")
    except FileNotFoundError:
        try:
            print(symbol, date, 'trying to get data from s3/b2...')
            df = get_date_df_from_s3(symbol, date, tick_type)
        except FileNotFoundError:
            print(symbol, date, 'getting data from polygon API...')
            df = polygon_df.get_date_df(symbol, date, tick_type)
            print(symbol, date, 'saving data to S3/B2...')
            put_date_df_to_s3(df, symbol, date, tick_type)
        finally:
            print(symbol, date, 'saving data to local file')
            path = date_df_to_file(df, symbol, date, tick_type)

    return df
