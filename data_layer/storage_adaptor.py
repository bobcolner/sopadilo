from io import BytesIO
from tempfile import NamedTemporaryFile
import pickle
import pandas as pd
from data_layer import fsspec_factory


class StorageAdaptor:

    def __init__(self, fs_type: str='s3_filecache', root_path: str='polygon-equities'):
        self.fs = fsspec_factory.get_filesystem(fs_type)
        self.fs_type = fs_type
        self.root_path = root_path
    
    def list_fs_path(self, fs_path: str) -> str:
        return self.fs.ls(self.root_path + fs_path, refresh=True)

    def list_fs_path_storage(self, fs_path: str) -> dict:

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

    def remove_fs_path(self, fs_path: str, recursive: bool=False):
        self.fs.rm(self.root_path + fs_path, recursive)

    def read_pickle_from_fs(self, fs_path: str) -> object:
        byte_data = self.fs.cat(self.root_path + fs_path)
        return pickle.loads(byte_data)

    def write_pickle_to_fs(self, obj: object, fs_path: str):
        # do not include trailing slash in path
        with self.fs.open(self.root_path + f"{fs_path}/object.pickle", 'wb') as fio:
            pickle.dump(obj, file=fio, protocol=4)  # protocol 5 only supported in python 3.8+ and not needed here

    def read_df_from_fs(self, fs_path: str, columns: list=None) -> pd.DataFrame:
        byte_data = self.fs.cat(self.root_path + fs_path)
        return pd.read_feather(BytesIO(byte_data), columns=columns)

    def write_df_to_fs(self, df: pd.DataFrame, fs_path: str):
        # do not include trailing slash in path
        with NamedTemporaryFile(mode='w+b') as tmp_ref1:
            df.to_feather(path=tmp_ref1.name, version=2)
            self.fs.put(lpath=tmp_ref1.name, rpath=self.root_path + f"{fs_path}/data.feather")

### high-level functions: /{ROOT_PATH}/{prefix}/symbol={symbol}/date={date}/data.feather

    def list_symbol_dates(self, symbol: str, prefix: str) -> str:
        paths = self.list_fs_path(f"{prefix}/symbol={symbol}")
        return [path.split('date=')[1] for path in paths if path.split('/')[-1].startswith('date=')]

    def list_symbols(self, prefix: str) -> str:
        paths = self.list_fs_path(prefix)
        return [path.split('symbol=')[1] for path in paths if path.split('/')[-1].startswith('symbol=')]

    def list_symbol_storage(self, symbol: str, prefix: str) -> dict:
        return self.list_fs_path_storage(f"{prefix}/symbol={symbol}/")

    def remove_symbol(self, symbol: str, prefix: str):
        self.remove_fs_path(f"{prefix}/symbol={symbol}/", recursive=True)

    def remove_symbol_date(self, symbol: str, date: str, prefix: str):
        self.remove_fs_path(f"{prefix}/symbol={symbol}/date={date}/", recursive=True)

    def read_sdf(self, symbol: str, date: str, prefix: str, columns: list=None) -> pd.DataFrame:
        return self.read_df_from_fs(f"{prefix}/symbol={symbol}/date={date}/data.feather", columns)

    def read_sdpickle(self, symbol: str, date: str, prefix: str) -> object:
        return self.read_pickle_from_fs(f"{prefix}/symbol={symbol}/date={date}/object.pickle")

    def write_sdpickle(self, sd_obj: object, symbol: str, date: str, prefix: str):
        self.write_pickle_to_fs(sd_obj, fs_path=f"{prefix}/symbol={symbol}/date={date}")

    def write_sdf(self, sdf: pd.DataFrame, symbol: str, date: str, prefix: str):
        self.write_df_to_fs(sdf, fs_path=f"{prefix}/symbol={symbol}/date={date}")
