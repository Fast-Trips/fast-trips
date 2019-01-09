from setuptools import setup, Extension
import os,sys
import numpy

### Settings for Extension Building
if sys.platform == 'darwin':
    compile_args=['-stdlib=libc++', "-mmacosx-version-min=10.9"]
else:
    compile_args=['-std=libc++']

extension = Extension('_fasttrips',
                      sources=['src/fasttrips.cpp',
                               'src/hyperlink.cpp',
                               'src/access_egress.cpp',
                               'src/path.cpp',
                               'src/pathfinder.cpp',
                               ],
                      language='c++',
                      extra_compile_args = compile_args,
                      include_dirs=[numpy.get_include()],
                      )

setup(name          = 'fasttrips',
      version       = '1.0a14',
      author        = 'MTC, SFCTA & PSRC',
      author_email  = 'lzorn@mtc.ca.gov',
      description   = 'Dynamic Transit Assignment Model. Given a transit network and a list of transit demand, finds a pathset and chooses a path for each traveler.',
      long_description = 'See https://github.com/MetropolitanTransportationCommission/fast-trips',
      packages      = ['fasttrips'],
      url           = 'http://fast-trips.mtc.ca.gov/',
      license       = 'Apache',
      classifiers   = [# How mature is this project?
                       'Development Status :: 3 - Alpha',

                       # Indicate who your project is intended for
                       'Intended Audience :: Other Audience',
                       'Topic :: Scientific/Engineering',

                       # Pick your license as you wish (should match "license" above)
                        'License :: OSI Approved :: Apache Software License',

                       # Specify the Python versions you support here. In particular, ensure
                       # that you indicate whether you support Python 2, Python 3 or both.
                       'Programming Language :: Python :: 2',
                       'Programming Language :: Python :: 2.7',
                       'Programming Language :: Python :: 3',
                       'Programming Language :: Python :: 3.6'],
      keywords      = 'transit model dynamic passenger assignment simulation',
      install_requires = ['functools32;python_version<="2.7"',
                          'numpy',
                          'pandas==0.22',
                          'partridge==0.6.0.dev1',
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
