from collections import namedtuple
import pandas as pd
from utilities import stats


def state_to_bar(state: dict) -> dict:    
    new_bar = {}
    if state['stat']['tick_count'] < 11:
        return new_bar

    price_vol_df = pd.DataFrame({'price': state['trades']['price'], 'volume': state['trades']['volume']})
    price_vwap = stats.weighted_mean(price_vol_df.price, price_vol_df.volume)
    wquants = stats.weighted_quantile(price_vol_df.price, price_vol_df.volume)
    new_bar = {
        'bar_trigger': state['stat']['bar_trigger'],
        'open_at': state['trades']['nyc_dt'][0],
        'close_at': state['trades']['nyc_dt'][-1],
        'duration_td': state['trades']['nyc_dt'][-1] - state['trades']['nyc_dt'][0],
        'tick_count': state['stat']['tick_count'],
        'volume': state['stat']['volume'],
        'dollars': state['stat']['dollars'],
        'tick_imbalance': state['stat']['tick_imbalance'],
        'volume_imbalance': state['stat']['volume_imbalance'],
        'price_high': state['stat']['price_high'],
        'price_low': state['stat']['price_low'],
        'price_open': price_vol_df.price.values[0],
        'price_close': price_vol_df.price.values[-1],
        'price_range': state['stat']['price_range'],
        'price_return': state['stat']['price_return'],
        'price_vwap': price_vwap,
        'price_wq05': wquants[0],
        'price_wq25': wquants[1],
        'price_wq50': wquants[2],
        'price_wq75': wquants[3],
        'price_wq95': wquants[4],
    }
    return new_bar


def reset_state(thresh: dict={}) -> dict:
    state = {}    
    state['thresh'] = thresh
    # tick event log
    state['trades'] = {}
    state['trades']['nyc_dt'] = []
    state['trades']['tick_count'] = []
    state['trades']['volume'] = []
    state['trades']['side'] = []
    state['trades']['price'] = []
    state['trades']['price_jma'] = []
    state['trades']['price_high'] = []
    state['trades']['price_low'] = []
    # full update batches log (includes zero trade batches)
    state['batches'] = {}
    state['batches']['nyc_dt'] = []
    # 'streaming' metrics
    state['stat'] = {}
    state['stat']['duration_td'] = None
    state['stat']['price_low'] = 10 ** 5
    state['stat']['price_high'] = 0
    state['stat']['price_range'] = 0
    state['stat']['price_return'] = 0
    state['stat']['price_jma_return'] = 0
    state['stat']['tick_count'] = 0
    state['stat']['volume'] = 0
    state['stat']['dollars'] = 0
    state['stat']['tick_imbalance'] = 0
    state['stat']['volume_imbalance'] = 0
    state['stat']['dollar_imbalance'] = 0
    # trigger status
    state['stat']['bar_trigger'] = 'waiting'
    return state


def imbalance_runs(state: dict) -> dict:
    if len(state['trades']['side']) >= 2:
        if state['trades']['side'][-1] == state['trades']['side'][-2]:
            state['stat']['tick_run'] += 1        
            state['stat']['volume_run'] += state['trades']['volume'][-1]
            state['stat']['dollar_run'] += state['trades']['price'][-1] * state['trades']['volume'][-1]
        else:
            state['stat']['tick_run'] = 0
            state['stat']['volume_run'] = 0
            state['stat']['dollar_run'] = 0
    return state


def check_bar_thresholds(state: dict) -> dict:

    def get_next_renko_thresh(renko_size: float, last_bar_return: float, reversal_multiple: float) -> tuple:
        if last_bar_return >= 0:
            thresh_renko_bull = renko_size
            thresh_renko_bear = -renko_size * reversal_multiple
        elif last_bar_return < 0:
            thresh_renko_bull = renko_size * reversal_multiple
            thresh_renko_bear = -renko_size
        return thresh_renko_bull, thresh_renko_bear

    if 'renko_size' in state['thresh']:
        try:
            state['thresh']['renko_bull'], state['thresh']['renko_bear'] = get_next_renko_thresh(
                renko_size=state['thresh']['renko_size'],
                last_bar_return=state['stat']['last_bar_return'],
                reversal_multiple=state['thresh']['renko_reveral_multiple']
            )
        except:
            state['thresh']['renko_bull'] = state['thresh']['renko_size']
            state['thresh']['renko_bear'] = -state['thresh']['renko_size']

        if state['stat'][state['thresh']['renko_return']] >= state['thresh']['renko_bull']:
            state['stat']['bar_trigger'] = 'renko_up'
        if state['stat'][state['thresh']['renko_return']] < state['thresh']['renko_bear']:
            state['stat']['bar_trigger'] = 'renko_down'

    if 'volume_imbalance' in state['thresh'] and abs(state['stat']['volume_imbalance']) >= state['thresh']['volume_imbalance']:
        state['stat']['bar_trigger'] = 'volume_imbalance'
    
    if 'max_duration_td' in state['thresh'] and state['stat']['duration_td'] > state['thresh']['max_duration_td']:
        state['stat']['bar_trigger'] = 'duration'

    # over-ride newbar trigger with 'minimum' thresholds
    if 'min_duration_td' in state['thresh'] and state['stat']['duration_td'] < state['thresh']['min_duration_td']:
        state['stat']['bar_trigger'] = 'waiting'

    if 'min_tick_count' in state['thresh'] and state['stat']['tick_count'] < state['thresh']['min_tick_count']:
        state['stat']['bar_trigger'] = 'waiting'

    return state


def update_bar_state(tick: namedtuple, state: dict, bars: list) -> tuple:

    # basic duration update (always available)
    state['batches']['nyc_dt'].append(tick.close_at)
    state['stat']['duration_td'] = state['batches']['nyc_dt'][-1] - state['batches']['nyc_dt'][0]

    if tick.tick_count > 0:
        # append tick
        state['trades']['nyc_dt'].append(tick.close_at)
        state['trades']['tick_count'].append(tick.tick_count)
        state['trades']['volume'].append(tick.volume)
        state['trades']['side'].append(tick.side)
        state['trades']['price'].append(tick.price)
        state['trades']['price_high'].append(tick.price_high)
        state['trades']['price_low'].append(tick.price_low)
        state['trades']['price_jma'].append(tick.price_jma)
        # 'stats'
        state['stat']['tick_count'] += tick.tick_count
        state['stat']['volume'] += tick.volume
        state['stat']['dollars'] += tick.price * tick.volume
        # price
        state['stat']['price_low'] = tick.price_low if tick.price_low < state['stat']['price_low'] else state['stat']['price_low']
        state['stat']['price_high'] = tick.price_high if tick.price_high > state['stat']['price_high'] else state['stat']['price_high']
        state['stat']['price_range'] = state['stat']['price_high'] - state['stat']['price_low']
        state['stat']['price_return'] = tick.price - state['trades']['price'][0]
        state['stat']['price_jma_return'] = tick.price_jma - state['trades']['price_jma'][0]
        state['stat']['last_bar_return'] = bars[-1]['price_return'] if len(bars) > 0 else 0
        # imbalances
        state['stat']['tick_imbalance'] += tick.side
        state['stat']['volume_imbalance'] += (tick.side * tick.volume)
        state['stat']['dollar_imbalance'] += (tick.side * tick.volume * tick.price)

    # check state tirggered sample threshold
    state = check_bar_thresholds(state)
    if state['stat']['bar_trigger'] != 'waiting':
        new_bar = state_to_bar(state)
        bars.append(new_bar)
        state = reset_state(state['thresh'])

    return state, bars


class BarSampler:

    def __init__(self, thresh: dict):
        self.state = reset_state(thresh)
        self.bars = []

    def update(self, next_tick: namedtuple):
        self.state, self.bars = update_bar_state(next_tick, self.state, self.bars)

    def batch(self, ticks_df: pd.DataFrame):
        for tick in ticks_df.itertuples():
            self.update(next_tick=tick)

        return self.bars
