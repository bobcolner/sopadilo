import os
import json
from os.path import expanduser


try:
    with open('/tmp/secerets.json', mode='r') as fio:
        secerets_dict = json.load(fio)
except FileNotFoundError:
    secerets_dict = {}


def get_global_value(key: str, secerets_dict: dict=None, default: str='seceret') -> str:
    try:
        value = os.environ[key]
    except KeyError:
        value = secerets_dict.get(key, default)

    return value


# DATA_LOCAL_PATH = get_global_value('DATA_LOCAL_PATH', secerets_dict)
DATA_LOCAL_PATH = os.getcwd() + '/data'

DATA_S3_PATH = get_global_value('DATA_S3_PATH', secerets_dict, 'polygon-equities')

ALPHAVANTAGE_API_KEY = get_global_value('ALPHAVANTAGE_API_KEY', secerets_dict)

TIINGO_API_KEY = get_global_value('TIINGO_API_KEY', secerets_dict)

B2_ACCESS_KEY_ID = get_global_value('B2_ACCESS_KEY_ID', secerets_dict)

B2_SECRET_ACCESS_KEY = get_global_value('B2_SECRET_ACCESS_KEY', secerets_dict)

B2_ENDPOINT_URL = get_global_value('B2_ENDPOINT_URL', secerets_dict)
