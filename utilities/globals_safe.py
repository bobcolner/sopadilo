from os import environ


try:
    DATA_LOCAL_PATH = environ['DATA_LOCAL_PATH']
except:
    DATA_LOCAL_PATH = '~/data'

try:
    DATA_S3_PATH = environ['DATA_S3_PATH']
except:
    DATA_S3_PATH = 'polygon-equities/data'

try:
    POLYGON_API_KEY = environ['POLYGON_API_KEY']
except:
    POLYGON_API_KEY = 'seceret'

try:
    ALPHAVANTAGE_API_KEY = environ['ALPHAVANTAGE_API_KEY']
except:
    ALPHAVANTAGE_API_KEY = 'seceret'

try:
    TIINGO_API_KEY = environ['TIINGO_API_KEY']
except:
    TIINGO_API_KEY = 'seceret'

try:
    B2_ACCESS_KEY_ID = environ['B2_ACCESS_KEY_ID']
except:
    B2_ACCESS_KEY_ID = 'seceret'

try:
    B2_SECRET_ACCESS_KEY = environ['B2_SECRET_ACCESS_KEY']
except:
    B2_SECRET_ACCESS_KEY = 'seceret'

try:
    B2_ENDPOINT_URL = environ['B2_ENDPOINT_URL']
except:
    B2_ENDPOINT_URL = 'seceret'
