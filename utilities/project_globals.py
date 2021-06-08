import os
import json


try:
    with open('./tmp/secerets.json', mode='r') as fio:
        secerets_dict = json.load(fio)
except FileNotFoundError:
    secerets_dict = {}


def get_global_value(key: str, secerets_dict: dict=None, default: str='seceret') -> str:
    try:
        value = os.environ[key]
    except KeyError:
        value = secerets_dict.get(key, default)

    return value


DATA_LOCAL_PATH = os.getcwd() + '/tmp/local_data'

DATA_S3_PATH = get_global_value('DATA_S3_PATH', secerets_dict, 'polygon-equities')

ALPHAVANTAGE_API_KEY = get_global_value('ALPHAVANTAGE_API_KEY', secerets_dict)

TIINGO_API_KEY = get_global_value('TIINGO_API_KEY', secerets_dict)

B2_APPLICATION_KEY_ID = get_global_value('B2_APPLICATION_KEY_ID', secerets_dict)

B2_APPLICATION_KEY = get_global_value('B2_APPLICATION_KEY', secerets_dict)

B2_ENDPOINT_URL = get_global_value('B2_ENDPOINT_URL', secerets_dict)
