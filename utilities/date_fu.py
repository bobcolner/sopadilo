from pandas_market_calendars import get_calendar
from data_layer import data_access


def get_open_market_dates(start_date: str, end_date: str, market_name: str='NYSE') -> list:    
    market = get_calendar(market_name)
    schedule = market.schedule(start_date=start_date, end_date=end_date)
    dates = [i.date().isoformat() for i in schedule.index]
    return dates


def query_dates(symbol: str, prefix: str, start_date: str, end_date: str, query_type: str, source: str) -> list:
    # find open market dates
    requested_dates = get_open_market_dates(start_date, end_date)
    # all existing dates
    existing_dates = data_access.list_sd_data(symbol, prefix, source)
    if query_type == 'existing':
        # requested & avialiable dates
        dates = list(set(requested_dates).intersection(set(existing_dates)))
    elif query_type == 'remaining':
        # requested & remaiing dates
        dates = list(set(requested_dates).difference(set(existing_dates)))

    if len(dates) > 0:
        dates.sort()

    return dates
