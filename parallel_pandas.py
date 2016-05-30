# -*- coding: utf-8 -*-

from multiprocessing import Pool
import pandas as pd
from functools import partial

def call_return_series(fn, series, **kwargs):
    res = fn(series, **kwargs)
    if not isinstance(res, pd.Series):
        return pd.Series(res, name=series.name)
    else:
        return res

def apply_p(df, fn, threads=2, **kwargs):
    """Parallel apply for pandas DataFrame.

    Argument fn cannot be a lambda function, but it can be generated with
    functools.partial.

    """
    p = Pool(threads)
    index_names = df.index.names
    result = p.map(partial(call_return_series, fn, **kwargs),
                   [row for _, row in df.reset_index().iterrows()])
    return pd.DataFrame(result).set_index(index_names)
