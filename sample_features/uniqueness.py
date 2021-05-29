import pandas as pd
from mlfinlab.sampling.bootstrapping import get_ind_matrix, get_ind_mat_average_uniqueness
from mlfinlab.sampling.concurrent import get_av_uniqueness_from_triple_barrier


def get_concurrent_stats(lbars_df: pd.DataFrame) -> dict:

    samples_info_sets = lbars_df[['label_start_at', 'label_outcome_at']]
    samples_info_sets = samples_info_sets.set_index('label_start_at')
    samples_info_sets.columns = ['t1'] # t1 = label_outcome_at
    price_bars = lbars_df[['open_at', 'close_at', 'price_close']]
    price_bars = price_bars.set_index('close_at')
    label_avg_unq = get_av_uniqueness_from_triple_barrier(samples_info_sets, price_bars, num_threads=1)
    ind_mat = get_ind_matrix(samples_info_sets, price_bars)
    avg_unq_ind_mat = get_ind_mat_average_uniqueness(ind_mat)
    results = {
        'label_avg_unq': label_avg_unq,
        'grand_avg_unq': label_avg_unq['tW'].mean(),
        'ind_mat': ind_mat,
        'ind_mat_avg_unq': avg_unq_ind_mat
    }
    return results
