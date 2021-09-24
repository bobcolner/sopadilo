import datetime as dt
import numpy as np
import pandas as pd


def bdf_to_list(bdf: pd.DataFrame) -> list:
    bar_dates = []
    for date in bdf.date.unique():
        bar_date = bdf[bdf.date==date].to_dict(orient='records')
        bar_dates.append(bar_date)

    return bar_dates


def fill_gap(bar_1: dict, bar_2: dict, fill_size: float, fill_col: str) -> list:
    
    num_steps = round(abs(bar_1[fill_col] - bar_2[fill_col]) / fill_size)
    fill_values = list(np.linspace(start=bar_1[fill_col], stop=bar_2[fill_col], num=num_steps))
    fill_values.append(bar_2[fill_col])  # add extra buffer bar
    fill_dict = {
        'bar_trigger': 'gap_filler',
        fill_col: fill_values,
    }

    return pd.DataFrame(fill_dict).to_dict(orient='records')


def fill_gaps_dates(bdf: pd.DataFrame, fill_col: str) -> pd.DataFrame:
    
    bar_dates = bdf_to_list(bdf)

    for idx, bar_date in enumerate(bar_dates):
        if idx == 0:
            continue

        gap_fill = fill_gap(
            bar_1=bar_dates[idx-1][-1],  # previous date, last bar
            bar_2=bar_dates[idx][0],     # current date, first bar
            fill_size=max(abs(pd.DataFrame(bar_date)['price_return']).median()/2.0, 0.005),
            fill_col=fill_col,
        )
        bar_dates[idx-1] = bar_dates[idx-1] + gap_fill

    # build continous 'stacked' bars df
    stacked = []
    for bar_date in bar_dates:
        stacked = stacked + bar_date

    return pd.DataFrame(stacked)


def stacked_df_stats(stacked_df: pd.DataFrame) -> pd.DataFrame:

    bars_df = stacked_df[stacked_df['bar_trigger'] != 'gap_filler'].reset_index(drop=True)
    bars_df.loc[:, 'date'] = bars_df['close_at'].dt.date.astype('string')
    bars_df.loc[:, 'duration_min'] = bars_df['duration_td'].dt.seconds / 60
    
    dates_df = bars_df.groupby('date').agg(
        bar_count=pd.NamedAgg(column="price_close", aggfunc="count"),
        duration_min_median=pd.NamedAgg(column="duration_min", aggfunc="median"),
        price_range_mean=pd.NamedAgg(column="price_range", aggfunc="mean"),
        first_bar_open=pd.NamedAgg(column="open_at", aggfunc="min"),
        last_bar_close=pd.NamedAgg(column="close_at", aggfunc="max"),
    ).reset_index()

    return dates_df


def bar_dates_stats(bar_dates: list) -> pd.DataFrame:
    
    results = []
    for bar_date in bar_dates:
        bars_df = bar_date['bars_df'].copy()
        out = (bar_date['ticks_df'].status.value_counts() / bar_date['ticks_df'].shape[0]).to_dict()
        out.update({'date': bar_date['date']})
        rrr = (abs(bars_df.label_rrr).value_counts() / bars_df.shape[0]).to_dict()
        out.update(rrr)
        results.append(out)
        bar_date['bars_df']['open_at'].min()

    return pd.DataFrame(results)
