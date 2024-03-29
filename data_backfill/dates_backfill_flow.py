from datetime import timedelta, datetime, date as dt
from psutil import cpu_count
from prefect import Flow, Parameter, task, unmapped
from prefect.executors import DaskExecutor, LocalExecutor

from utilities.dates import get_open_market_dates, find_remaining_dates
from data_model.fsspec_backend import list_symbol_dates, get_and_save_date_df


@task(max_retries=2, retry_delay=timedelta(seconds=2))
def get_remaining_symbol_dates(start_date: str, end_date: str, symbols: list[str], tick_type: str) -> list[tuple]:
    request_dates = get_open_market_dates(start_date, end_date)
    symbol_dates = []
    for symbol in symbols:
        existing_dates = list_symbol_dates(symbol, tick_type)
        remaining_dates = find_remaining_dates(request_dates, existing_dates)
        for date in remaining_dates:
            symbol_dates.append((symbol, date))
    return symbol_dates


@task(max_retries=2, retry_delay=timedelta(seconds=2))
def backfill_date_task(symbol_date: tuple, tick_type: str):
    df = get_and_save_date_df(
        symbol=symbol_date[0], 
        date=symbol_date[1], 
        tick_type=tick_type
        )
    return True


def get_flow():
    with Flow(name='backfill-flow') as flow:
        start_date = Parameter('start_date', default='2020-01-01')
        end_date = Parameter('end_date', default='2020-02-01')
        tick_type = Parameter('tick_type', default='trades')
        symbols = Parameter('symbols', default=['GLD'])

        symbol_date_list = get_remaining_symbol_dates(start_date, end_date, symbols, tick_type)

        backfill_date_task_result = backfill_date_task.map(
            symbol_date=symbol_date_list,
            tick_type=unmapped(tick_type)
        )
    return flow


def run_flow(symbols: list[str], tick_type: str, start_date: str, 
    # end_date: str=(datetime.utcnow() - timedelta(days=1)).isoformat().split('T')[0],  # yesterday utc
    end_date: str=(dt.today() - timedelta(days=1)).isoformat(),  # yesterday local tz
    n_workers: int=(cpu_count(logical=False)), 
    threads_per_worker: int=8,
    processes: bool=False):
    
    if type(symbols) != list:
        raise ValueError('symbols expects a list type')

    flow = get_flow()
    
    executor = DaskExecutor(
        cluster_kwargs={
            'n_workers': n_workers,
            'processes': processes,
            'threads_per_worker': threads_per_worker,
        })
    # executor = LocalExecutor()
    
    flow_state = flow.run(
        executor=executor,
        symbols=symbols,
        tick_type=tick_type,
        start_date=start_date,
        end_date=end_date,
    )
    return flow_state


if __name__ == '__main__':

    flow_state = run_flow(
        symbols=['GLD', 'GOLD'], 
        tick_type='trades', 
        start_date='2020-01-01'
        )
