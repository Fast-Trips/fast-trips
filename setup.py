from setuptools import setup, Extension

setup(name          = 'fasttrips',
      version       = '1.0',
      author        = 'MTC, SFCTA & PSRC',
      description   = 'Dynamic Transit Assignment',
      packages      = ['fasttrips'],
      url           = 'http://fast-trips.mtc.ca.gov/',
      ext_modules   = [Extension('_fasttrips',
                                 sources=['src/fasttrips.cpp'],
                                 )
                      ],
      )