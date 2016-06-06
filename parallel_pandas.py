# -*- coding: utf-8 -*-

from multiprocessing import Pool
import pandas as pd
from functools import partial
from os import system
from os.path import join, abspath

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

# Slurm functions for NERSC

SLURM_TEMPLATE = """#!/bin/bash
#SBATCH --partition=shared
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --time=01:00:00
"""

CMD_TEMPLATE = """python -c "import {0},pandas; {0}.{1}(pandas.read_json('{2}', typ='series'))" """

def apply_slurm(df, package_name, function_name, data_directory):
    """Parallel apply for pandas DataFrame using slurm.

    Argument fn cannot be a lambda function, but it can be generated with
    functools.partial.

    Does not return anything.

    """
    index_names = df.index.names
    for _, row in df.reset_index().iterrows():
        job_name = '%s_%s' % (function_name, '_'.join(row[x] for x in index_names))
        job_file = join(data_directory, '%s.sl' % job_name)
        json_file = abspath(join(data_directory, '%s.json' % job_name))
        row.to_json(json_file)
        with open(job_file, 'w') as outfile:
            outfile.write(SLURM_TEMPLATE)
            outfile.write('#SBATCH --output=slurmout_%s\n' % job_name)
            outfile.write('#SBATCH --job-name=%s\n\n' % job_name)
            outfile.write(CMD_TEMPLATE.format(package_name, function, json_file))
            outfile.write('\n')
        system('sbatch ' + job_file)
