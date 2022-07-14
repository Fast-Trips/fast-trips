**Build status**

Master Branch: [![Master branch build status](https://travis-ci.org/BayAreaMetro/fast-trips.svg?branch=master)](https://travis-ci.org/BayAreaMetro/fast-trips)  

Develop Branch [![Develop branch build status: ](https://travis-ci.org/BayAreaMetro/fast-trips.svg?branch=develop)](https://travis-ci.org/BayAreaMetro/fast-trips)

# fast-trips
Fast-Trips is a Dynamic Transit Passenger Assignment tool written in Python and supplemented by code in C++. For more information about this visit the following links:  

 * Documentation  : http://fast-trips.github.io/fast-trips/
 * Implementaiton Project Website: http://fast-trips.mtc.ca.gov/

 **Use Cases**  
 Fast-trips can be used for analyzing short-term effects as a stand-along tool as well as long range planning when linked up with a travel demand modeling tool:
  - An analyst who wants to study the effect of a on service reliability of a schedule change.
  - An analyst who wants to evaluate a service plan for a special event.
  - A modeler who wants to include capacity constraints and reliability as a performance metric for long-range planning investments as evaluated in a long range transportation plan.

## installing

**Requirements**
Fast-Trips should work on OSX, Linux (Ubuntu tested) and Windows with Python 2.7 and Python 3.6+ installed.  We also recommend using a virtual environment manager such as [Conda](www.conda.io).

**Stable Release**

`pip install fasttrips`

**Bleeding Edge**

`pip install git+https://github.com/bayareametro/fast-trips.git@develop#egg=fasttrips`

## Running Fast-Trips

Fast-Trips can be run from the command line or by calling it from within a Python script or an iPython notebook using the `Run.run_fasttrips()` function.

There are six required parameters that need to either be passed from the command line or the function call:  

  * `input_network_dir` = directory for input networks can be found  
  * `input_demand_dir`  = directory where input demand can be found  
  * `input_weights`     = file where path weights can be found  
  * `run_config` = file where run configurations can be found  
  * `iters`      = Number of global iterations  
  * `output_dir` = directory where output folder is created  
  * `pathfinding_type` = either `deterministic` or `stochastic`

All the other parameters described in the [configuration options](#configuration-options-fasttrips) can also be passed as keywords.  

**NOTE: Any parameters passed in at run-time from the command line or via the script will overwrite any parameters read in from the `run_config` file.**

### Running the Springfield Example

Sample input files have been provided in `<fast-trips-dir>\Examples\Springfield` to test the setup and also assist with the creation of new fast-trips runs. The input files include network files created from a small hypothetical network and also example transit demand data.

#### From a Script
```python

# Examples\Springfield\run_springfield.py

import os
from fasttrips import Run

# DIRECTORY LOCATIONS
EXAMPLE_DIR         = os.path.abspath(os.path.dirname(__file__))

INPUT_NETWORK       = os.path.join(EXAMPLE_DIR, 'networks', 'vermont')
INPUT_DEMAND        = os.path.join(EXAMPLE_DIR, 'demand', 'general')
INPUT_CONFIG        = os.path.join(EXAMPLE_DIR, 'configs', 'A')
OUTPUT_DIR          = os.path.join(EXAMPLE_DIR, 'output')
OUTPUT_FOLDER       = "general_run"

# INPUT FILE LOCATIONS
CONFIG_FILE         = os.path.join(INPUT_CONFIG, 'config_ft.txt')
INPUT_WEIGHTS       = os.path.join(INPUT_CONFIG, 'pathweight_ft.txt')

print "Running Fast-Trips in %s" % (ex_dir.split(os.sep)[-1:])

Run.run_fasttrips(
    input_network_dir= INPUT_NETWORK,
    input_demand_dir = INPUT_DEMAND,
    run_config       = CONFIG_FILE,
    input_weights    = INPUT_WEIGHTS,
    output_dir       = OUTPUT_DIR,
    output_folder    = OUTPUT_FOLDER,
    pathfinding_type = "stochastic",
    overlap_variable = "count",
    overlap_split_transit = True,
    iters            = 1,
    utils_conversion_factor = 10,
    dispersion       = 0.50)
```

To run the example:

*  Make sure your `<fast-trips-dir>` is in your `PYTHONPATH` environment variable in *Advanced system settings* [Win] or terminal [OSX].
*  Run `python Examples/Springfield/run_springfield.py` from within `<fast-trips-dir>\scripts` in a command prompt [ Win ] or terminal [ OSX ].

Output files from running fast-trips with the sample input data provided can be found in the `Springfield/output` directory.

#### From Command Line  

The same example can be run from the command line by using the command from within the `<fast-trips-dir>` directory:

```bat
C:\Users\lzorn\Documents\fast-trips>rem See usage and forgive my use of windows
C:\Users\lzorn\Documents\fast-trips>rem If using installed version, use 'run_fasttrips' instead of 'python fasttrips\Run.py'
C:\Users\lzorn\Documents\fast-trips>python fasttrips\Run.py -h
usage:

  Run Fast-Trips from the command line with required inputs as command line parameters.

positional arguments:
  {deterministic,stochastic,file}
                        Type of pathfinding
  iters                 Number of iterations to run
  run_config            The run configuration file
  input_network_dir     Location of the input network
  input_demand_dir      Location of the input demand
  input_weights         Location of the pathweights file
  output_dir            Location to write fasttrips output

optional arguments:
  -h, --help            show this help message and exit
  -t, --trace_only      Run only the trace persons?
  -n NUM_TRIPS, --num_trips NUM_TRIPS
                        Number of person trips to run, to run a subset of the
                        whole demand.
  -d DISPERSION, --dispersion DISPERSION
                        Stochastic dispersion parameter
  -m MAX_STOP_PROCESS_COUNT, --max_stop_process_count MAX_STOP_PROCESS_COUNT
                        Max times to process a stop in stochastic pathfinding
  -c, --capacity        Enable capacity constraint
  -o OUTPUT_FOLDER, --output_folder OUTPUT_FOLDER
                        Directory within output_loc to write fasttrips
                        outtput. If none specified, will construct one.
  --debug_output_columns
                        Include debug columns in output
  --overlap_variable {None,count,distance,time}
                        Variable to use for overlap penalty calculation
  --overlap_split_transit
                        Split transit for path overlap penalty calculation
  --transfer_fare_ignore_pathfinding
                        In path-finding, suppress trying to adjust fares using
                        transfer rules. For performance.
  --transfer_fare_ignore_pathenum
                        In path-enumeration, suppress trying to adjust fares
                        using transfer rules. For performance.

C:\Users\lzorn\Documents\fast-trips>rem Run it with Springfield Example scenario
C:\Users\lzorn\Documents\fast-trips>rem If using installed version, use 'run_fasttrips' instead of 'python fasttrips\Run.py'

C:\Users\lzorn\Documents\fast-trips>python fasttrips\Run.py stochastic 1 fasttrips\Examples\Springfield\configs\A\config_ft.txt fasttrips\Examples\Springfield\networks\vermont fasttrips\Examples\Springfield\demand\general fasttrips\Examples\Springfield\configs\A\pathweight_ft.txt fasttrips\Examples\test_scenario\output
```

## Example Scenarios

Fast-Trips comes with a handful of scenarios in the `fasttrips/Examples` directory to use as examples or get you started. They can be viewed at a high-level using the [jupyter notebooks](http://jupyter.org/) contained in that directory.  Note that these notebooks may require you to install additional Python packages such as [jupyter](http://jupyter.org/), [ipywidgets](https://ipywidgets.readthedocs.io/en/latest/), and [bokeh](https://bokeh.pydata.org/en/latest/).

### Springfield
The Springfield scenario is what many of our tests use and is meant to be a generic example with enough complexity and modes to flex Fast-Trips muscles, but not too complex to understand what is going on.

#### Springfield Network

The hypothetical 5-zone example network was developed to help code development. It has a total of three transit routes (one rail and two bus) with two or three stops each. There are also two park-and-ride (PnR) locations.

![alt text](/fasttrips/Examples/Springfield/networks/vermont/test_network.png "Transit Example Network")

Transit vehicles commence at 3:00 PM and continue until 6:00 PM. There are 152 transit trips that make a total of 384 station stops. The `input` folder contains all the supply-side/network input files prepared from the test network. More information about network input file standards can be found in the [GTFS-Plus Data Standards Repository][network-standard-url].

#### Springfield Demand
Two versions of sample demand have been prepared:
*  `general` contains regular demand that consists only of a transit trip list. Demand starts at 3:15 PM and ends at 5:15 PM.One trip occurs every 10 seconds. More information is available in [documentation](https://github.com/BayAreaMetro/fast-trips/blob/master/fasttrips/Examples/Springfield/Readme.md).
*  `simpson_zorn` represents demand for two user classes that can use different sets of path weights. Household and person attribute files are present in addition to the trip list to model user heterogeneity and multiple user classes.

Similar to network data standards, there also exists a [Demand Data Standards Repository][demand-standard-url].

#### Springfield Configs
There are several configurations for the Springfield setup, which are generally grouped as:  
* `A` which doesn't use user classes, and
* `B` which uses user classes and thus needs to use the `simpson_zorn` demand

## Tests
There are a couple dozen tests that are stored in `\tests`.  They can be run by installing the [PyTest](https://docs.pytest.org/en/latest/) library (`pip install pytest`and executing the command `pytest` from the command line within your `<fast-trips-dir>`.  

Most of the tests use test scenarios that can be found in the `fasttrips/Examples` directory.

Many (but not all) of the tests can be individually run by giving the command `pytest tests/test_<TESTNAME>.py`.  

Test output defaults to the folder `fasttrips/Examples/output`

