import pandas as pd
from data_api import polygon_rest


def market_daily_to_df(daily: list) -> pd.DataFrame: 
    df = pd.DataFrame(daily, columns=['T', 'v', 'o', 'c', 'h', 'l', 'vw', 't'])
    df = df.rename(columns={'T': 'symbol',
                            'v': 'volume',
                            'o': 'open',
                            'c': 'close',
                            'h': 'high',
                            'l': 'low',
                           'vw': 'vwap',
                            't': 'epoch'})
    # remove symbols with non-ascii characters
    ascii_mask = df.symbol.apply(lambda x: x.isascii())
    df = df.loc[ascii_mask].reset_index(drop=True)
    # remove binary null characters "\x00" from the symbol string
    df.loc[:, 'symbol'] = df.symbol.apply(lambda x: x.replace("\x00", ""))
    # add datetime column
    df['date_time'] = pd.to_datetime(df['epoch'] * 10**6).dt.normalize()
    df = df.drop(columns='epoch')
    # add midprice
    df.loc[:, 'midprice'] = df.apply(lambda row: (row['high'] + row['low'] + row['open'] + row['close']) / 4, axis=1)
    # fix vwap
    mask = ~(df.vwap.between(df.low, df.high)) # vwap outside the high/low range
    df.loc[mask, 'vwap'] = df.loc[mask, 'midprice'] # replace bad vwap with midprice
    # add dollar total
    df['dollar_total'] = df['vwap'] * df['volume']
    # optimze datatypes
    df['symbol'] = df['symbol'].astype('string')
    df['volume'] = df['volume'].astype('uint64')
    for col in ['dollar_total', 'vwap', 'midprice', 'open', 'close', 'high', 'low']:
        df[col] = df[col].astype('float32')
    return df


def ticks_to_df(ticks: list, tick_type: str) -> pd.DataFrame:
    if tick_type == 'trades':
        df = pd.DataFrame(ticks, columns=['t', 'y', 'q', 'i', 'x', 'p', 's', 'c', 'z'])
        df = df.rename(columns={'p': 'price',
                                's': 'size',
                                'x': 'exchange_id',
                                't': 'sip_epoch',
                                'y': 'exchange_epoch',
                                'q': 'sequence',
                                'i': 'trade_id',
                                'c': 'conditions',
                                'z': 'tape'
                                })
        # optimize datatypes
        df['price'] = df['price'].astype('float32')
        df['size'] = df['size'].astype('uint32')
        df['exchange_id'] = df['exchange_id'].astype('uint8')
        df['trade_id'] = df['trade_id'].astype('string')

    elif tick_type == 'quotes':
        df = pd.DataFrame(ticks, columns=['t', 'y', 'q', 'x', 'X', 'p', 'P', 's', 'S', 'z'])
        df = df.rename(columns={'p': 'bid_price',
                                'P': 'ask_price',
                                's': 'bid_size',
                                'S': 'ask_size',
                                'x': 'bid_exchange_id',
                                'X': 'ask_exchange_id',
                                't': 'sip_epoch',
                                'y': 'exchange_epoch',
                                'q': 'sequence',
                                'c': 'conditions',
                                'i': 'indicators',
                                'z': 'tape'
                                })
        # optimze datatypes
        df['bid_price'] = df['bid_price'].astype('float32')
        df['ask_price'] = df['ask_price'].astype('float32')
        df['bid_size'] = df['bid_size'].astype('uint32')
        df['ask_size'] = df['ask_size'].astype('uint32')
        df['bid_exchange_id'] = df['bid_exchange_id'].astype('uint8')
        df['ask_exchange_id'] = df['ask_exchange_id'].astype('uint8')
    
    # cast datetimes (for both trades+quotes -don't use tz b/c of arrow/parquet issues)
    df['sequence'] = df['sequence'].astype('uint32')
    df['sip_dt'] = pd.to_datetime(df['sip_epoch'], unit='ns')
    df['exchange_dt'] = pd.to_datetime(df['exchange_epoch'], unit='ns')
    # drop columns
    df = df.drop(columns=['tape', 'sip_epoch', 'exchange_epoch'])
    return df.reset_index(drop=True)


def validate_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        raise ValueError('df is NoneType')
    if len(df) < 1:
        raise ValueError('zero row df')
    elif any(df.count() == 0):
        raise ValueError('df has fields with no values. Recent historic data may not be ready for consumption')
    else:
        return df


def get_ticks_date_df(symbol: str, date: str, tick_type: str='trades') -> pd.DataFrame:
    ticks = polygon_rest.get_stocks_ticks_date(symbol, date, tick_type)
    df = ticks_to_df(ticks, tick_type)
    return validate_df(df)


def get_market_date_df(date: str) -> pd.DataFrame:
    daily = polygon_rest.get_market_date(locale='us', market='stocks', date=date)
    if len(daily) == 0:
        raise ValueError('get_market_date returned zero rows')

    return market_daily_to_df(daily)


def get_date_df(symbol: str, date: str, tick_type: str) -> pd.DataFrame:
    if (symbol == 'market') or (tick_type == 'daily'):
        df = get_market_date_df(date)
    else:
        df = get_ticks_date_df(symbol, date, tick_type)
    return df


def get_symbol_details_df(symbols: list) -> pd.DataFrame:
    results = []
    for symbol in symbols:
        print(symbol)
        dets = polygon_rest.get_ticker_details(symbol)
        if dets:
            results.append(dets)
    results = [i for i in results if i]  # remove empty/null items from list
    return pd.DataFrame(results)
