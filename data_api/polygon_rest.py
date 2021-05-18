import os
import requests


BASE_URL = 'https://api.polygon.io'

POLYGON_API_KEY = os.environ['POLYGON_API_KEY']


def validate_response(response: str):
    if response.status_code == 200:
        return response.json()['results']
    else:
        response.raise_for_status()


def get_market_date(date: str, locale: str='us', market: str='stocks') -> list:
    url = BASE_URL + f"/v2/aggs/grouped/locale/{locale}/market/{market}/{date}?apiKey={POLYGON_API_KEY}"
    response = requests.get(url)
    return validate_response(response)


def get_markets() -> list:
    url = BASE_URL + f"/v2/reference/markets?apiKey={POLYGON_API_KEY}"
    response = requests.get(url)
    return validate_response(response)


def get_locales() -> list:
    url = BASE_URL + f"/v2/reference/locales?apiKey={POLYGON_API_KEY}"
    response = requests.get(url)
    return validate_response(response)


def get_types() -> list:
    url = BASE_URL + f"/v2/reference/types?apiKey={POLYGON_API_KEY}"
    response = requests.get(url)
    return validate_response(response)


def get_tickers_page(page: int=1, stock_type: str='etp') -> list:
    path = BASE_URL + f"/v2/reference/tickers"
    params = {}
    params['apiKey'] = POLYGON_API_KEY
    params['page'] = page
    params['active'] = 'true'
    params['perpage'] = 50
    params['market'] = 'stocks'
    params['locale'] = 'us'
    params['type'] = stock_type
    response = get(path, params)
    if response.status_code != 200:
        response.raise_for_status()
    tickers_list = response.json()['tickers']
    return tickers_list


def get_all_tickers(start_page: int=1, end_page: int=None, stock_type: str='etp') -> list:
    # stock_type: [cs, etp]
    run = True
    page_num = start_page
    all_tickers = []
    while run == True:
        print('getting page #: ', page_num)
        tickers = get_tickers_page(page=page_num, stock_type=stock_type)
        all_tickers = all_tickers + tickers
        page_num = page_num + 1
        if len(tickers) < 50:
            run = False
        if end_page and page_num >= end_page:
            run = False
    
    return all_tickers


def get_ticker_details(symbol: str):
    url = BASE_URL + f"/v1/meta/symbols/{symbol}/company?apiKey={POLYGON_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()


def get_market_status():
    url = BASE_URL + f"/v1/marketstatus/now?apiKey={POLYGON_API_KEY}"
    response = requests.get(url)
    return validate_response(response)


def get_stock_ticks_batch(symbol: str, date: str, tick_type: str, timestamp_first: int=None, 
    timestamp_limit: int=None, reverse: bool=False, limit: int=50000) -> list:

    if tick_type == 'quotes':
        path = BASE_URL + f"/v2/ticks/stocks/nbbo/{symbol}/{date}"
    elif tick_type == 'trades':
        path = BASE_URL + f"/v2/ticks/stocks/trades/{symbol}/{date}"

    params = {}
    params['apiKey'] = POLYGON_API_KEY
    if timestamp_first is not None:
        params['timestamp'] = timestamp_first

    if timestamp_limit is not None:
        params['timestampLimit'] = timestamp_limit

    if reverse is not None:
        params['reverse'] = reverse

    if limit is not None:
        params['limit'] = limit

    response = get(path, params)
    return validate_response(response)


def get_stocks_ticks_date(symbol: str, date: str, tick_type: str) -> list:
    last_tick = None
    limit = 50000
    ticks = []
    batch = 0
    run = True
    while run == True:
        batch += 1
        ticks_batch = get_stock_ticks_batch(symbol, date, tick_type, timestamp_first=last_tick, limit=limit)
        if len(ticks_batch) < 1: # empty tick batch
            print(symbol, date, 'empty batch!', batch)
            run = False
            continue
        print(symbol, date, 'tick batch:', batch, 'downloaded:', len(ticks_batch), 'ticks')
        # update last_tick timestamp
        last_tick = ticks_batch[-1]['t'] # sip ts
        ticks = ticks + ticks_batch # append batch to ticks list
        if len(ticks_batch) < limit: # check if we are done pulling ticks
            run = False
        elif len(ticks_batch) == limit:
            del ticks[-1] # drop last row to avoid dups

    return ticks
