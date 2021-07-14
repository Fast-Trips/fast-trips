import os,sys
from setuptools import setup, Extension
import numpy

# read the contents of your README file
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

### Settings for Extension Building
#compile_args = sysconfig.get_config_var('CFLAGS').split()
compile_args=["-std=c++11"]#+compile_args

if sys.platform == 'darwin':
    compile_args+=["-mmacosx-version-min=10.9"]

extension = Extension('_fasttrips',
                      sources=['src/fasttrips.cpp',
                               'src/hyperlink.cpp',
                               'src/access_egress.cpp',
                               'src/path.cpp',
                               'src/pathfinder.cpp',
                               ],
                      extra_compile_args = compile_args,
                      include_dirs=[numpy.get_include()],
                      )

setup(name          = 'fasttrips',
      version       = '1.0b2',
      author        = 'MTC, SFCTA & PSRC',
      author_email  = 'lzorn@bayareametro.gov',
      description   = 'Dynamic Transit Assignment Model. Given a transit network and a list of transit demand, finds a pathset and chooses a path for each traveler.',
      long_description_content_type='text/markdown',
      long_description=long_description,
      packages      = ['fasttrips'],
      url           = 'http://fast-trips.mtc.ca.gov/',
      license       = 'Apache',
      classifiers   = [# How mature is this project?
                       'Development Status :: 4 - Beta',

                       # Indicate who your project is intended for
                       'Intended Audience :: Other Audience',
                       'Topic :: Scientific/Engineering',

                       # Pick your license as you wish (should match "license" above)
                        'License :: OSI Approved :: Apache Software License',

                       # Specify the Python versions you support here. In particular, ensure
                       # that you indicate whether you support Python 2, Python 3 or both.
                       'Programming Language :: Python :: 3',
                       'Programming Language :: Python :: 3.6',
                       'Programming Language :: Python :: 3.7',
                       'Programming Language :: Python :: 3.8',
                       'Programming Language :: Python :: 3.9'],
      keywords      = 'transit model dynamic passenger assignment simulation',
      install_requires = ['numpy>=1.15',
                          'pandas',
                          'partridge',
                          'future',
                          'configparser',
                          'psutil',
                          'pytest'],
      package_dir   = { 'fasttrips':'fasttrips' },
      package_data  = { 'fasttrips':['Examples/*',
                                     'tests/*.py'] },
      entry_points  = { 'console_scripts': ['run_fasttrips=fasttrips.Run:main']},
      scripts       = [ 'scripts/create_tableau_path_map.py',
                        'scripts/run_example.py'],
      ext_modules   = [extension]
      )
