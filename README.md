Right now, `parallel_pandas` exists so that you can apply a function to the rows
of a pandas DataFrame across multiple processes using multiprocessing.

`apply_p(df, fn, threads=2, **kwargs)`

- df: The pandas DataFrame
- fn: A function to apply. The first argument is a series (a row of df), and the
  kwargs are passed as additional arguments. The function cannot be a lambda
  function, but it can be generated with `functools.partial`.
- threads: The number of processes to use.
