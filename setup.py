import setuptools
from distutils.core import setup
from Cython.Build import cythonize

setup(
  name = 'fasttrips',
  ext_modules = cythonize("fasttrips/*.pyx"),
)
