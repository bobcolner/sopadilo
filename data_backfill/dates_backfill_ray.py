import ray
import utils_dates
import polygon_s3 as ps3


@ray.remote
def backfill_task(symbol: str, date: str, tick_type: str) -> bool:
    df = ps3.get_and_save_date_df(symbol, date, tick_type)
    return True


def backfill(start_date: str, end_date: str, symbols: list, tick_type: str) -> list:

    request_dates = utils_dates.get_open_market_dates(start_date, end_date)
    futures = []
    for symbol in symbols:
        existing_dates = ps3.list_symbol_dates(symbol, tick_type)
        remaining_dates = utils_dates.find_remaining_dates(request_dates, existing_dates)
        for date in remaining_dates:
            result = backfill_task.remote(symbol, date, tick_type)
            futures.append(result)

    return ray.get(futures)
