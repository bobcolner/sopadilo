from io import BytesIO
from tempfile import NamedTemporaryFile
import pandas as pd
from data_layer import fsspec_factory
from utilities import easy_pickle


class StorageAdaptor:

    def __init__(self, fs_type: str='s3_filecache', root_path: str='polygon-equities'):
        self.fss = fsspec_factory.get_filesystem(fs_type)
        self.fs_type = fs_type
        self.root_path = root_path
    
    def list_fs_path(self, fs_path: str) -> str:
        return self.fss.ls(self.root_path + fs_path, refresh=True)

    def list_fs_path_storage_used(self, fs_path: str) -> dict:
        byte_len = self.fss.du(self.root_path + fs_path)
        return {
            'MB': round(byte_len / 10 ** 6, 1),
            'GB': round(byte_len / 10 ** 9, 2),
        }

    def download_fs_path(self, fs_path: str, local_path: str, recursive: bool=False):
        self.fss.get(rpath=self.root_path + fs_path, lpath=local_path, recursive=recursive)

    def upload_local_path(self, local_path: str, fs_path: str, recursive: bool=False):
        self.fss.put(lpath=local_path, rpath=self.root_path + fs_path, recursive=recursive)

    def remove_fs_path(self, fs_path: str, recursive: bool=False):
        self.fss.rm(self.root_path + fs_path, recursive)

    def read_pickle_from_fs(self, fs_path: str) -> object:
        byte_data = self.fss.cat(self.root_path + fs_path)
        return easy_pickle.load(byte_data)

    def read_df_from_fs(self, fs_path: str, columns: list=None) -> pd.DataFrame:
        byte_data = self.fss.cat(self.root_path + fs_path)
        return pd.read_feather(BytesIO(byte_data), columns=columns)

    def write_pickle_to_fs(self, obj: object, fs_path: str):
        with NamedTemporaryFile(mode='w+b') as tmp_ref1:
            easy_pickle.file_dump(obj, file_name=tmp_ref1.name)
            self.fss.put_file(tmp_ref1.name, self.root_path + f"{fs_path}/object.pickle")

    def write_df_to_fs(self, df: pd.DataFrame, fs_path: str):
        with NamedTemporaryFile(mode='w+b') as tmp_ref1:
            df.to_feather(path=tmp_ref1.name, version=2)
            self.fss.put(tmp_ref1.name, self.root_path + f"{fs_path}/data.feather")

### high-level functions: /{ROOT_PATH}/{prefix}/symbol={symbol}/date={date}/data.feather

    def list_symbol_dates(self, symbol: str, prefix: str) -> str:
        paths = self.list_fs_path(f"{prefix}/symbol={symbol}")
        return [path.split('date=')[1] for path in paths]

    def list_symbols(self, prefix: str) -> str:
        paths = self.list_fs_path(prefix)
        return [path.split('symbol=')[1] for path in paths]

    def list_symbol_storage_used(self, symbol: str, prefix: str) -> dict:
        return self.list_fs_path_storage_used(f"{prefix}/symbol={symbol}/")

    def remove_symbol(self, symbol: str, prefix: str):
        self.remove_fs_path(f"{prefix}/symbol={symbol}/", recursive=True)

    def remove_symbol_date(self, symbol: str, date: str, prefix: str):
        self.remove_fs_path(f"{prefix}/symbol={symbol}/date={date}/", recursive=True)

    def read_sdf(self, symbol: str, date: str, prefix: str, columns: list=None) -> pd.DataFrame:
        return self.read_df_from_fs(f"{prefix}/symbol={symbol}/date={date}/data.feather", columns)

    def read_sdpickle(self, symbol: str, date: str, prefix: str) -> object:
        return self.read_pickle_from_fs(f"{prefix}/symbol={symbol}/date={date}/data.feather")

    def write_sdpickle(self, sd_obj: object, symbol: str, date: str, prefix: str):
        self.write_pickle_to_fs(sd_obj, fs_path=f"{prefix}/symbol={symbol}/date={date}")

    def write_sdf(self, sdf: pd.DataFrame, symbol: str, date: str, prefix: str):
        self.write_df_to_fs(sdf, fs_path=f"{prefix}/symbol={symbol}/date={date}")
