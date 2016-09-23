from setuptools import setup, Extension
import sys
import numpy

setup(name          = 'fasttrips',
      version       = '1.0',
      author        = 'MTC, SFCTA & PSRC',
      description   = 'Dynamic Transit Assignment',
      packages      = ['fasttrips'],
      url           = 'http://fast-trips.mtc.ca.gov/',
      ext_modules   = [Extension('_fasttrips',
                                 sources=['src/fasttrips.cpp',
                                          'src/hyperlink.cpp',
                                          'src/path.cpp',
                                          'src/pathfinder.cpp',
                                          ],
                                 include_dirs=[numpy.get_include()],
                                 libraries=['psapi'] if sys.platform=='win32' else []
                                 )
                      ],
      )