import pandas as pd
from utilities import stats


def ticks_to_bar(price: pd.Series, volume: pd.Series, close_at: pd.Series) -> pd.DataFrame:

    price_vwap = stats.weighted_mean(price, volume)
    wquants = stats.weighted_quantile(price, volume)
    new_bar = {
        'bar_trigger': 'sync',
        'open_at': close_at[0],
        'close_at': close_at[-1],
        'duration_td': close_at[-1] - close_at[0],
        'tick_count': len(price),
        'volume': volume.sum(),
        'dollar_value': volume.sum() * len(price),
        # 'tick_imbalance':
        # 'volume_imbalance':
        # 'jma_features':
        'price_high': price.max(),
        'price_low': price.min(),
        'price_open': price[0],
        'price_close': price[-1],
        'price_range': price.max() - price.min(),
        'price_return': price[0] - price[-1],
        'price_vwap': price_vwap,
        'price_wq05': wquants[0],
        'price_wq25': wquants[1],
        'price_wq50': wquants[2],
        'price_wq75': wquants[3],
        'price_wq95': wquants[4],
    }
    return new_bar
