from io import BytesIO
from typing import Union
import pickle
import pandas as pd
from data_layer import fsspec_factory


class StorageAdaptor:

    def __init__(self, fs_type: str, root_path: str):
        self.fs = fsspec_factory.get_filesystem(fs_type)
        self.fs_type = fs_type
        self.root_path = root_path

    def ls_fs_path(self, fs_path: str, refresh: bool=True) -> list:
        try:
            output = self.fs.ls(self.root_path + fs_path, refresh=refresh)
        except FileNotFoundError:
            output = []
        return output

    def du_fs_path(self, fs_path: str) -> dict:
        byte_len = self.fs.du(self.root_path + fs_path)
        if byte_len < 10 ** 3:
            humanized_size = {'Bytes': byte_len}
        elif byte_len < 10 ** 6:
            humanized_size = {'KB': round(byte_len / 10 ** 3, 1)}
        elif byte_len < 10 ** 9:
            humanized_size = {'MB': round(byte_len / 10 ** 6, 1)}
        else:
            humanized_size = {'GB': round(byte_len / 10 ** 9, 2)}

        return humanized_size

    def download_fs_path(self, fs_path: str, local_path: str, recursive: bool=False):
        self.fs.get(rpath=self.root_path + fs_path, lpath=local_path, recursive=recursive)

    def upload_local_path(self, local_path: str, fs_path: str, recursive: bool=False):
        self.fs.put(lpath=local_path, rpath=self.root_path + fs_path, recursive=recursive)

    def rm_fs_path(self, fs_path: str, recursive: bool=False):
        self.fs.rm(self.root_path + fs_path, recursive)

    def read_pickle_from_fs(self, fs_path: str) -> object:
        byte_data = self.fs.cat(self.root_path + fs_path)
        return pickle.loads(byte_data)

    def read_df_from_fs(self, fs_path: str, columns: list=None) -> pd.DataFrame:
        byte_data = self.fs.cat(self.root_path + fs_path)
        return pd.read_feather(BytesIO(byte_data), columns=columns)

    def write_df_to_fs(self, df: pd.DataFrame, fs_path: str):
        # do not include trailing slash in path
        with self.fs.open(self.root_path + f"{fs_path}/data.feather", 'wb') as fio:
            df.to_feather(path=fio, version=2)

    def write_pickle_to_fs(self, obj: object, fs_path: str):
        # do not include trailing slash in path
        with self.fs.open(self.root_path + f"{fs_path}/object.pickle", 'wb') as fio:
            pickle.dump(obj, file=fio, protocol=4)  # protocol 5 only supported in python 3.8+ and not needed here

### high-level functions: /{ROOT_PATH}/{prefix}/symbol={symbol}/date={date}/data.feather
    
    def list_symbols(self, prefix: str, show_storage: bool=False) -> Union[list, dict]:
        if show_storage:
            output = self.du_fs_path(prefix)
        else:    
            paths = self.ls_fs_path(prefix)
            output = [path.split('symbol=')[1] for path in paths if path.split('/')[-1].startswith('symbol=')]
        return output

    def list_symbol_dates(self, symbol: str, prefix: str, show_storage: bool=False) -> Union[list, dict]:
        if show_storage:
            output = self.du_fs_path(f"{prefix}/symbol={symbol}")
        else:    
            paths = self.ls_fs_path(f"{prefix}/symbol={symbol}")
            output = [path.split('date=')[1] for path in paths if path.split('/')[-1].startswith('date=')]
        return output

    def remove_symbol(self, symbol: str, prefix: str):
        self.rm_fs_path(f"{prefix}/symbol={symbol}/", recursive=True)

    def remove_symbol_date(self, symbol: str, date: str, prefix: str):
        self.rm_fs_path(f"{prefix}/symbol={symbol}/date={date}/", recursive=True)

    def read_sdf(self, symbol: str, date: str, prefix: str, columns: list=None) -> pd.DataFrame:
        return self.read_df_from_fs(f"{prefix}/symbol={symbol}/date={date}/data.feather", columns)

    def read_sdpickle(self, symbol: str, date: str, prefix: str) -> object:
        return self.read_pickle_from_fs(f"{prefix}/symbol={symbol}/date={date}/object.pickle")

    def read_sd_data(self, symbol: str, date: str, prefix: str) -> Union[pd.DataFrame, object]:
        try:
            sd_data = self.read_sdf(symbol, date, prefix)
        except:
            sd_data = self.read_sdpickle(symbol, date, prefix)
        return sd_data

    def write_sdf(self, sdf: pd.DataFrame, symbol: str, date: str, prefix: str):
        self.write_df_to_fs(sdf, fs_path=f"{prefix}/symbol={symbol}/date={date}")

    def write_sdpickle(self, sd_obj: object, symbol: str, date: str, prefix: str):
        self.write_pickle_to_fs(sd_obj, fs_path=f"{prefix}/symbol={symbol}/date={date}")

    def write_sd_data(self, sd_data: object, symbol: str, date: str, prefix: str):
        if type(sd_data) == pd.DataFrame:
            self.write_sdf(sd_data, symbol, date, prefix)
        else:
            self.write_sdpickle(sd_data, symbol, date, prefix)
