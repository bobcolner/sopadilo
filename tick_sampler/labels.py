import statsmodels.api as sm
import pandas as pd
from pandas._libs.tslibs.timestamps import Timestamp


def get_trend_outcome(label_prices: pd.DataFrame) -> dict:

    if len(label_prices) < 30:
        return {}

    df = label_prices.copy()
    df['const'] = 1
    df = df.reset_index()
    model = sm.OLS(endog=df['price'], exog=df[['const', 'index']])
    results = model.fit()
    trend = {
        'label_trend_slope': results.params[1],
        'label_trend_tvalue': results.tvalues[0],
        'label_trend_r2': results.rsquared,
    }
    return trend


def get_tb_outcome(reward_ratio: float, risk_level: float, side: str, label_prices: pd.DataFrame,
                    goal: str='profit', price_col: str='price_jma') -> dict:

    first_price = label_prices['price'].values[0]
    if side=='long':
        if goal=='profit':
            target_price = first_price + (risk_level * reward_ratio)
            target_at = label_prices[label_prices[price_col] >= target_price].min()['nyc_dt']
        elif goal=='stop':
            target_price = first_price - risk_level
            target_at = label_prices[label_prices[price_col] < target_price].min()['nyc_dt']
            reward_ratio = -1
    elif side=='short':
        if goal=='profit':
            target_price = first_price - (risk_level * reward_ratio)
            target_at = label_prices[label_prices[price_col] <= target_price].min()['nyc_dt']
        elif goal=='stop':
            target_price = first_price + risk_level
            target_at = label_prices[label_prices[price_col] > target_price].min()['nyc_dt']
            reward_ratio = -1

        reward_ratio = reward_ratio * -1

    outcome = {
        'label_side': side,
        'label_outcome': goal,
        'label_rrr': reward_ratio,
        'label_outcome_at': target_at,
    }
    return outcome


def triple_barrier_outcomes(label_prices: pd.DataFrame, risk_level: float, reward_ratios: list) -> list:

    first_price = label_prices['price_jma'].values[0]
    tb_outcomes = []
    for side in ['long', 'short']:
        stop_outcome = get_tb_outcome(None, risk_level, side, label_prices, goal='stop')
        tb_outcomes.append(stop_outcome)
        for reward in reward_ratios:
            profit_outcome = get_tb_outcome(reward, risk_level, side, label_prices, goal='profit')
            tb_outcomes.append(profit_outcome)
    tb_df = pd.DataFrame(tb_outcomes).sort_values('label_outcome_at')

    return tb_df


def signed_outcomes_to_label(outcomes: pd.DataFrame, label_end_at: Timestamp) -> dict:

    outcomes = outcomes.dropna()
    if outcomes.shape[0] == 0:  # no outcomes
        label = [{
                    'label_side': 'neutral',
                    'label_outcome': 'neutral',
                    'label_rrr': 0,
                    'label_outcome_at': label_end_at,
                }]
    elif outcomes[outcomes['label_outcome']=='stop'].shape[0] > 0:  # stop-loss found
        idx = outcomes[outcomes['label_outcome']=='stop'].index.values[0]  # index of stop
        if idx == 0:  # stop-loss is first outcome
            label = outcomes.head(1).to_dict(orient='records')
        elif idx > 0:
            multi_label = outcomes[outcomes.index < idx]  # profit outcomes before stop-out
            label = multi_label[multi_label['label_rrr'].abs()==multi_label['label_rrr'].abs().max()].to_dict(orient='records')
    else:
        label = outcomes[outcomes['label_rrr'].abs()==outcomes['label_rrr'].abs().max()].to_dict(orient='records')  # get max reward

    return label[0]


def outcomes_to_label(outcomes: pd.DataFrame, label_end_at: Timestamp) -> dict:

    long_outcomes = outcomes.loc[outcomes['label_side']=='long'].reset_index(drop=True)
    short_outcomes = outcomes.loc[outcomes['label_side']=='short'].reset_index(drop=True)
    long_label = signed_outcomes_to_label(long_outcomes, label_end_at)
    short_label = signed_outcomes_to_label(short_outcomes, label_end_at)
    if (long_label['label_outcome'] == 'profit') and (short_label['label_outcome'] == 'stop'):
        label = long_label
    elif (short_label['label_outcome'] == 'profit') and (long_label['label_outcome'] == 'stop'):
        label = short_label
    elif (short_label['label_outcome'] in ['neutral', 'stop']) and (long_label['label_outcome'] in ['neutral', 'stop']):
        try:
            label_end_at = max(long_label['label_outcome_at'], short_label['label_outcome_at'])
        except:
            label_end_at = label_end_at

        label = {
            'label_side': 'neutral',
            'label_outcome': 'neutral',
            'label_rrr': 0,
            'label_outcome_at': label_end_at,
            }
    else:
        label = {'label_outcome': 'unknown'}

    return label


def get_label_ticks(ticks_df: pd.DataFrame, label_start_at: Timestamp, horizon_mins: int) -> pd.DataFrame:

    delayed_label_start_at = label_start_at + pd.Timedelta(value=3, unit='seconds')  # inference+network latency compensation
    label_end_at = label_start_at + pd.Timedelta(value=horizon_mins, unit='minutes')
    label_prices = ticks_df.loc[(ticks_df['nyc_dt'] >= delayed_label_start_at) & (ticks_df['nyc_dt'] < label_end_at)]

    return label_prices,  label_end_at


def label_bars(bars: list, ticks_df: pd.DataFrame, risk_level: float, horizon_mins: int, reward_ratios: list) -> list:

    for idx, row in enumerate(bars):
        label_prices, label_end_at = get_label_ticks(ticks_df, label_start_at=row['close_at'], horizon_mins=horizon_mins)
        label_duration = label_end_at - row['close_at']
        if label_duration < pd.Timedelta(minutes=5):
            print('Dropping label, less then 5min from bar close_at:', row['close_at'])
            continue

        if len(label_prices) < 1:
            print('Dropping label, only', len(label_prices['price']), 'trades;' 'start at:', row['close_at'])
            continue

        outcomes = triple_barrier_outcomes(label_prices, risk_level, reward_ratios)
        label = outcomes_to_label(outcomes, label_end_at)
        label.update({
            'label_start_at': row['close_at'],
            'label_end_at': label_end_at,
            })
        bars[idx].update(label)

    return bars
