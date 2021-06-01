from pandas_market_calendars import get_calendar


def get_open_market_dates(start_date: str, end_date: str, market_name: str='NYSE') -> list:    
    market = get_calendar(market_name)
    schedule = market.schedule(start_date=start_date, end_date=end_date)
    dates = [i.date().isoformat() for i in schedule.index]
    return dates
