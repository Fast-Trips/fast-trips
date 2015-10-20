# fast-trips
Dynamic transit assignment tool

 * Project Website: http://fast-trips.mtc.ca.gov/
 * Full Technical Documentation (API): http://metropolitantransportationcommission.github.io/fast-trips/

Steps:

1.  git clone https://github.com/MetropolitanTransportationCommission/fast-trips
2.  Install [Microsoft Visual C++ Compiler for Python 2.7](http://www.microsoft.com/en-us/download/details.aspx?id=44266)
3.  Install the python package [transitfeed](https://github.com/google/transitfeed/wiki/TransitFeed) for reading GTFS
4.  To build, in the fast-trips dir run:  `python setup.py build_ext --inplace` in the fast-trips directory
5.  To run using example input, run `scripts\runAllTests.bat` on Windows, or just run the commands listed there.
