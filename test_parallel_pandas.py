# -*- coding: utf-8 -*-

from parallel_pandas import apply_p
import pandas as pd
from functools import partial

def mult_sum(series, y=1):
    return series.append(pd.Series({'c': (series.a + series.b) * y}))

def test_apply_p():
    df = pd.DataFrame({'a': range(4)}, index=['e', 'f', 'g', 'h'])
    df.index.name = 'ind'
    df['b'] = df.a * 2
    df = apply_p(df, partial(mult_sum, y=10))
    assert df.loc['h', 'c'] == 90

def test_apply_p_kwargs():
    df = pd.DataFrame({'a': range(4)}, index=['e', 'f', 'g', 'h'])
    df.index.name = 'ind'
    df['b'] = df.a * 2
    df = apply_p(df, mult_sum, y=10)
    assert df.loc['h', 'c'] == 90
