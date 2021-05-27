from io import BytesIO
from tempfile import NamedTemporaryFile
import pandas as pd
import fsspec
from utilities import pickle, globals_unsafe as gus


def get_fsspec_fs(fs_type: str) -> fsspec.filesystem:

    if fs_type == 's3_cached':
        # https://filesystem-spec.readthedocs.io/
        fs = fsspec.filesystem(
            protocol='filecache',
            target_protocol='s3',
            target_options={
                'key': gus.B2_ACCESS_KEY_ID,
                'secret': gus.B2_SECRET_ACCESS_KEY,
                'client_kwargs': {'endpoint_url': gus.B2_ENDPOINT_URL}
                },
            cache_storage='./tmp/fsspec_cache'
            )
        ROOT_PATH = gus.DATA_S3_PATH
    elif fs_type == 's3':
        fs = fsspec.filesystem(
            protocol='s3', 
            key=gus.B2_ACCESS_KEY_ID, 
            secret=gus.B2_SECRET_ACCESS_KEY, 
            client_kwargs={'endpoint_url': gus.B2_ENDPOINT_URL}
            )
        ROOT_PATH = gus.DATA_S3_PATH
    elif fs_type == 'local':
        fs = fsspec.filesystem(protocol='file')
        ROOT_PATH = gus.DATA_LOCAL_PATH

    return fs, ROOT_PATH


fs, ROOT_PATH = get_fsspec_fs(fs_type='s3_cached')


### basic fs wrapper functions

def list_fs_path(fs_path: str) -> str:
    return fs.ls(path=ROOT_PATH + f"/{fs_path}", refresh=True)


def put_local_path_to_fs(local_path: str, fs_path: str, recursive: bool=False):
    fs.put(lpath=local_path, rpath=ROOT_PATH + f"/{fs_path}", recursive=recursive)


def get_fs_path(fs_path: str, local_path: str, recursive: bool=False):
    fs.get(rpath=ROOT_PATH + f"/{fs_path}", lpath=local_path, recursive=recursive)


def pickle_from_fs(fs_path: str) -> object:
    byte_data = fs.cat(ROOT_PATH + f"/{fs_path}")
    return pickle.load(byte_data)


def pickle_to_fs(obj, fs_path: str):
    with NamedTemporaryFile(mode='w+b') as tmp_ref1:
        pickle.file_dump(object=obj, file_name=tmp_ref1.name)
        fs.put_file(tmp_ref1.name, ROOT_PATH + f"/{fs_path}/object.pickle")


def df_from_fs(fs_path: str, columns: list=None) -> pd.DataFrame:
    byte_data = fs.cat(ROOT_PATH + f"/{fs_path}")
    if columns:
        df = pd.read_feather(BytesIO(byte_data), columns=columns)
    else:
        df = pd.read_feather(BytesIO(byte_data))
    return df


def df_to_fs(df: pd.DataFrame, fs_path: str):
    with NamedTemporaryFile(mode='w+b') as tmp_ref1:
        df.to_feather(path=tmp_ref1.name, version=2)
        fs.put(tmp_ref1.name, ROOT_PATH + f"/{fs_path}/data.feather")
