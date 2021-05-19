import numpy as np
import pandas as pd


def fill_gap(bar_1: dict, bar_2: dict, renko_size: float, fill_col: str) -> dict:

    num_steps = round(abs(bar_1[fill_col] - bar_2[fill_col]) / (renko_size / 2))
    fill_values = list(np.linspace(start=bar_1[fill_col], stop=bar_2[fill_col], num=num_steps))
    fill_values.insert(-1, bar_2[fill_col])
    fill_values.insert(-1, bar_2[fill_col])
    fill_dt = pd.date_range(
        start=bar_1['close_at'] + timedelta(hours=1),
        end=bar_2['open_at'] - timedelta(hours=1),
        periods=num_steps + 2,
        )
    fill_dict = {
        'bar_trigger': 'gap_filler',
        'close_at': fill_dt,
        fill_col: fill_values,
    }
    return pd.DataFrame(fill_dict).to_dict(orient='records')


def fill_gaps_dates(bar_dates: list, fill_col: str) -> pd.DataFrame:

    for idx, date in enumerate(bar_dates):
        if idx == 0:
            continue
        import pudb; pudb.set_trace()
        try:
            gap_fill = fill_gap(
                bar_1=bar_dates[idx-1]['bars'][-1],
                bar_2=bar_dates[idx]['bars'][0],
                renko_size=bar_dates[idx]['thresh']['renko_size'],
                fill_col=fill_col,
            )
            bar_dates[idx-1]['bars'] = bar_dates[idx-1]['bars'] + gap_fill

        except:
            print(date['date'])
            continue
    # build continous 'stacked' bars df
    stacked = []
    for date in bar_dates:
        stacked = stacked + date['bars']

    return pd.DataFrame(stacked)


def process_bar_dates(bar_dates: list, imbalance_thresh: float=0.95) -> pd.DataFrame:

    results = []
    for date_d in bar_dates:
        bdf = pd.DataFrame(date_d['bars'])
        results.append({
            'date': date_d['date'], 
            'bar_count': len(date_d['bars']),
            'imbalance_thresh': bdf.volume_imbalance.quantile(q=imbalance_thresh),
            'duration_min_mean': bdf.duration_min.mean(),
            'duration_min_median': bdf.duration_min.median(),
            'price_range_mean': bdf.price_range.mean(),
            'price_range_median': bdf.price_range.median(),
            'thresh': date_d['thresh']
            })
    daily_bar_stats_df = jma_filter_df(pd.DataFrame(results), 'imbalance_thresh', length=5, power=1)
    daily_bar_stats_df.loc[:, 'imbalance_thresh_jma_lag'] = daily_bar_stats_df['imbalance_thresh_jma'].shift(1)
    daily_bar_stats_df = daily_bar_stats_df.dropna()

    return daily_bar_stats_df


def stacked_df_stats(stacked_df: pd.DataFrame) -> pd.DataFrame:

    bars_df = stacked_df[stacked_df['bar_trigger'] != 'gap_filler'].reset_index(drop=True)
    bars_df.loc[:, 'date'] = bars_df['close_at'].dt.date.astype('string')
    bars_df.loc[:, 'duration_min'] = bars_df['duration_td'].dt.seconds / 60
    
    dates_df = bars_df.groupby('date').agg(
        bar_count=pd.NamedAgg(column="price_close", aggfunc="count"),
        duration_min_median=pd.NamedAgg(column="duration_min", aggfunc="median"),
        jma_range_mean=pd.NamedAgg(column="jma_range", aggfunc="mean"),
        first_bar_open=pd.NamedAgg(column="open_at", aggfunc="min"),
        last_bar_close=pd.NamedAgg(column="close_at", aggfunc="max"),
    ).reset_index()

    return dates_df
