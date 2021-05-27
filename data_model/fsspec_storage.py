from io import BytesIO
from tempfile import NamedTemporaryFile
import pandas as pd
import fsspec
from utilities import easy_pickle, globals_unsafe as g


def get_fsspec_fs(fs_type: str) -> fsspec.filesystem:

    if fs_type == 's3_cached':
        fs = fsspec.filesystem(
            protocol='filecache',
            target_protocol='s3',
            target_options={
                'key': g.B2_ACCESS_KEY_ID,
                'secret': g.B2_SECRET_ACCESS_KEY,
                'client_kwargs': {'endpoint_url': g.B2_ENDPOINT_URL}
                },
            cache_storage='./tmp/fsspec_cache'
            )
        ROOT_PATH = g.DATA_S3_PATH
    elif fs_type == 's3':
        fs = fsspec.filesystem(
            protocol='s3', 
            key=g.B2_ACCESS_KEY_ID, 
            secret=g.B2_SECRET_ACCESS_KEY, 
            client_kwargs={'endpoint_url': g.B2_ENDPOINT_URL}
            )
        ROOT_PATH = g.DATA_S3_PATH
    elif fs_type == 'local':
        fs = fsspec.filesystem(protocol='file')
        ROOT_PATH = g.DATA_LOCAL_PATH

    return fs, ROOT_PATH


fs, ROOT_PATH = get_fsspec_fs(fs_type='s3_cached')


def list_fs_path(fs_path: str) -> str:
    return fs.ls(path=ROOT_PATH + fs_path, refresh=True)


def list_fs_path_storage_used(fs_path: str) -> dict:
    path = ROOT_PATH + fs_path
    byte_len = fs.du(path)
    return {
        'MB': round(byte_len / 10 ** 6, 1),
        'GB': round(byte_len / 10 ** 9, 2),
    }


def download_fs_path(fs_path: str, local_path: str, recursive: bool=False):
    fs.get(rpath=ROOT_PATH + fs_path, lpath=local_path, recursive=recursive)


def upload_local_path(local_path: str, fs_path: str, recursive: bool=False):
    fs.put(lpath=local_path, rpath=ROOT_PATH + fs_path, recursive=recursive)


def remove_fs_path(fs_path: str, recursive: bool=False):
    fs.rm(ROOT_PATH + fs_path, recursive)


def read_pickle_from_fs(fs_path: str) -> object:
    byte_data = fs.cat(ROOT_PATH + fs_path)
    return pickle.load(byte_data)


def write_pickle_to_fs(obj: object, fs_path: str):
    with NamedTemporaryFile(mode='w+b') as tmp_ref1:
        pickle.file_dump(object=obj, file_name=tmp_ref1.name)
        fs.put_file(tmp_ref1.name, ROOT_PATH + f"{fs_path}/object.pickle")


def read_df_from_fs(fs_path: str, columns: list=None) -> pd.DataFrame:
    byte_data = fs.cat(ROOT_PATH + fs_path)
    if columns:
        df = pd.read_feather(BytesIO(byte_data), columns=columns)
    else:
        df = pd.read_feather(BytesIO(byte_data))
    return df


def write_df_to_fs(df: pd.DataFrame, fs_path: str):
    with NamedTemporaryFile(mode='w+b') as tmp_ref1:
        df.to_feather(path=tmp_ref1.name, version=2)
        fs.put(tmp_ref1.name, ROOT_PATH + f"{fs_path}/data.feather")
