import datetime as dt
from pathlib import Path
import pandas as pd
from utilities import globals_unsafe as g


def get_open_market_dates(start_date: str, end_date: str) -> list:
    from pandas_market_calendars import get_calendar
    market = get_calendar('NYSE')
    schedule = market.schedule(start_date=start_date, end_date=end_date)
    dates = [i.date().isoformat() for i in schedule.index]
    return dates


def list_dates_from_local_path(symbol: str, tick_type: str) -> list:
    from os import listdir
    # assumes 'hive' {date}={yyyy-mm-dd}/data.{format} filename template
    dates_path = f"{g.DATA_LOCAL_PATH}/{tick_type}/symbol={symbol}"
    if Path(dates_path).exists():    
        file_list = listdir(dates_path)
        if '.DS_Store' in file_list:
            file_list.remove('.DS_Store')
        existing_dates = [i.split('=')[1] for i in file_list]
    else:
        existing_dates = []
    return existing_dates


def find_remaining_dates(request_dates: list, existing_dates: list) -> list:
    existing_dates_set = set(existing_dates)
    remaining_dates = [x for x in request_dates if x not in existing_dates_set and x < dt.date.today().isoformat()]
    remaining_dates = pd.Series(remaining_dates).sort_values(ascending=True).to_list()  # sort dates
    return remaining_dates
