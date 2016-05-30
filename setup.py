try:
    from setuptools import setup, Command
except:
    from distutils.core import setup, Command

setup(
    name='parallel_pandas',
    author='Zachary King',
    py_modules=['parallel_pandas'],
)
