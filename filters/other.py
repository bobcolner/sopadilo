import numpy as np
import pandas as pd


def rema_filter_update(series_last: float, rema_last: float, winlen: int=14, lamb: float=0.5) -> float:
    # regularized ema
    alpha = 2 / (winlen + 1)
    rema = (rema_last + alpha * (series_last - rema_last) + 
        lamb * (2 * rema_last - rema[2])) / (lamb + 1)

    return rema


def rema_filter(series: pd.Series, winlen: int, lamb: float) -> list:
    rema_next = series.values[0]
    rema = []
    for value in series:
        rema_next = rema_filter_update(
            series_last=value, rema_last=rema_next, winlen=winlen, lamb=lamb
        )
        rema.append(rema_next)

    return rema


def supersmoother(x: list, n: int=10) -> np.ndarray:
    from math import exp, cos, radians
    assert (n > 0) and (n < len(x))
    a = exp(-1.414 * 3.14159 / n)
    b = cos(radians(1.414 * 180 / n))
    c2 = b
    c3 = -a * a
    c1 = 1 - c2 - c3
    ss = np.zeros(len(x))
    for i in range(3, len(x)):
        ss[i] = c1 * (x[i] + x[i-1]) / 2 + c2 * ss[i-1] + c3 * ss[i-2]

    return ss
