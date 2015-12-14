# fast-trips
fast-trips is a Dynamic Transit Assignment tool written in Python and supplemented by code in C++. For more information about this visit the following links:
 * Project Website: http://fast-trips.mtc.ca.gov/
 * Full Technical Documentation (API): http://metropolitantransportationcommission.github.io/fast-trips/

### Setup
Follow the steps below to setup up fast-trips:
*  Install [Git][git-url] and clone the fast-trips repository (https://github.com/MetropolitanTransportationCommission/fast-trips.git) to a local directory: `<fast-trips-dir>`. If the user plans on making changes to the code, it is recommended that the repository be [forked][git-fork-url] before cloning.
*  Switch to the `develop` branch of the repository.
*  Download and install a *data analytics* Python 2.7 distribution: [Anaconda][anaconda-url].
*  Install [Microsoft Visual C++ Compiler for Python 2.7][python-vcpp-url].
*  Install the python package [transitfeed][python-transitfeed-url] for reading GTFS.
*  To build, in the fast-trips directory `<fast-trips-dir>`, run the following in a command prompt:  `python setup.py build_ext --inplace`.

### Test Sample Input
Sample input files have been provided in `<fast-trips-dir>\Examples\test_network` to test the setup and also assist with the creation of new fast-trips runs. The input files have been created from a small hypothetical test network and example transit demand.
To quickly test the setup, run fast-trips on sample input using the following steps:
*  Add `<fast-trips-dir>` to the `PYTHONPATH` environment variable in *Advanced system settings*.
*  Run `\scripts\runAllTests.bat` from within `<fast-trips-dir>` in a command prompt. This will run several "preset" scenarios. The user can alternatively run each scenario individually using the commands listed in the batch file. Details about the test scenarios are provided in subsequent sections.
`output` will contain output files from running fast-trips with the sample input data provided.

##### Test Network
A hypothetical 5-zone test network was developed to help code development. It has a total of three transit routes (one rail and two bus) with two or three stops each. There are also two park-and-ride (PnR) locations.

![alt text](/Examples/test_network/input/test_network.png) 

`input` contains the supply-side/network inputs files prepared from the test network. More information about network input files and formats can be found in the [GTFS-Plus Data Standards Repository][network-standard-url].

##### Test Demand
`input` also contains two seperate scenarios with sample demand for fast-trips and [documentation](/Examples/test_network/demand_reg/Readme.md). Similar to network data standards, there also exists a [Demand Data Standards Repository][demand-standard-url]. 

[git-url]: <https://git-scm.com/>
[git-fork-url]: <https://help.github.com/articles/fork-a-repo/>
[python-vcpp-url]: <http://www.microsoft.com/en-us/download/details.aspx?id=44266>
[anaconda-url]: <https://www.continuum.io/downloads>
[python-transitfeed-url]: <https://github.com/google/transitfeed/wiki/TransitFeed>
[git-repo-url]: <https://github.com/MetropolitanTransportationCommission/fast-trips.git>
[network-standard-url]: <https://github.com/osplanning-data-standards/GTFS-PLUS>
[demand-standard-url]: <https://github.com/osplanning-data-standards/dyno-demand>