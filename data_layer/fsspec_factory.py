import fsspec
from utilities import project_globals as g


def get_filesystem(fs_type: str) -> fsspec.filesystem:

    if fs_type == 's3_filecache':
        fs = fsspec.filesystem(
            protocol='filecache',
            target_protocol='s3',
            target_options={
                'key': g.B2_ACCESS_KEY_ID,
                'secret': g.B2_SECRET_ACCESS_KEY,
                'client_kwargs': {'endpoint_url': g.B2_ENDPOINT_URL}
                },
            cache_storage='./tmp/fsspec_cache',
            auto_mkdir=True,
            )
    elif fs_type == 's3':
        fs = fsspec.filesystem(
            protocol='s3',
            key=g.B2_ACCESS_KEY_ID,
            secret=g.B2_SECRET_ACCESS_KEY,
            client_kwargs={'endpoint_url': g.B2_ENDPOINT_URL},
            )
    elif fs_type == 'local':
        fs = fsspec.filesystem(
            protocol='file',
            auto_mkdir=True,
            )

    return fs
