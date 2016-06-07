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

CMD_TEMPLATE = """
python -c "import {0},pandas; {0}.{1}(pandas.read_json('{2}', typ='series'), '{3}')"
"""

def apply_slurm(df, package_name, function_name, data_directory):
    """Parallel apply for pandas DataFrame using slurm. Does not return anything.

    Arguments
    ---------

    df: A DataFrame to iterate through.

    package_name: The name of the package containing function_name.

    function_name: The name of a function in the package given by
    package_name. The function should take two argumes: a pandas series and an
    output directory.

    data_directory: A directory to store all the data files.

    """
    index_names = df.index.names
    for _, row in df.reset_index().iterrows():
        job_name = '%s_%s' % (function_name, '_'.join(str(row[x]) for x in index_names))
        job_file = join(data_directory, '%s.sl' % job_name)
        json_file = abspath(join(data_directory, '%s_input.json' % job_name))
        row.to_json(json_file)
        with open(job_file, 'w') as outfile:
            outfile.write(SLURM_TEMPLATE)
            outfile.write('#SBATCH --output=slurmout_%s\n' % job_name)
            outfile.write('#SBATCH --job-name=%s\n\n' % job_name)
            outfile.write(CMD_TEMPLATE.format(package_name, function_name,
                                              json_file, data_directory))
            outfile.write('\n')
        system('chmod +x ' + job_file)
        system('sbatch ' + job_file)
