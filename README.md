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

### Input
The input to fast-trips consists of:
*  A Transit Network directory, including schedules, access, egress and transfer information, specified by the [GTFS-Plus Data Standards Repository][network-standard-url]
*  A Transit Demand directory, including persons, households and trips, specified by the [Demand Data Standards Repository][demand-standard-url]
*  fast-trips Configuration, specified below

Configuration is specified in the following files:

#### `config_ft.txt`

This is a *required* python file and may be included in both the Transit Supply and Transit Demand input directories.
If the same options are specified in both, then the version specified in the Transit Demand input directory will be used.
(Two versions may be specified because some configuration options are more relevant to demand and some are more relevant
to network inputs.)

The configuration files are parsed by python's [ConfigParser module](https://docs.python.org/2/library/configparser.html#module-ConfigParser) and therefore
adhere to that format, with two possible sections: *fasttrips* and *pathfinding*.
(See [Network Example](Examples/test_network/input/config_ft.txt) ) (See [Demand Example](Examples/test_network/demand_twopaths/config_ft.txt) )

**fasttrips configuration options**

Option Name                         | Type   | Default | Description
-----------                         | ----   | --------| -------------------------
`bump_buffer`                       | float  | 5       | Not really used yet.
`bump_one_at_a_time`                | bool   | False   |
`capacity_constraint`               | bool   | False   | Hard capacity constraint.  When True, fasttrips forces everyone off overcapacity vehicles and
                                                         disallows them from finding a new path using an overcapacity vehicle.
`create_skims`                      | bool   |
`debug_num_trips`                   | int    |
`debug_trace_only`                  | bool   |
`iterations`                        | int    |
`output_passenger_trajectories`     | bool   |
`number_of_processes`               | int    |
`pathfinding_type`                  | string |
`prepend_route_id_to_trip_id`       | bool   |
`simulation`                        | bool   |
`skim_end_time`                     | string |
`skim_start_time`                   | string |
`stochastic_dispersion`             | float  |
`stochastic_max_stop_process_count` | int    |
`stochastic_pathset_size`           | int    |
`time_window`                       | float  |
`trace_person_ids`                  | string |

**pathfinding configuration options**

Option Name                         | Type   | Default | Description
-----------                         | ----   | --------| -----------
`user_class_function`               | string |

#### `config_ft.py`

This is an *optional* python file in the Transit Demand input directory containing functions that are evaluated.
This could be used to programmatically define user classes based on person, household and/or trip attributes.
To use a function in this file, specify it in the *pathfinding* configuration as the `user_class_function`.
(See [Example](Examples/test_network/demand_twopaths/config.py) )

####  `pathweight_ft.txt`

### Test Sample Input
Sample input files have been provided in `<fast-trips-dir>\Examples\test_network` to test the setup and also assist with the creation of new fast-trips runs. The input files include network files created from a small hypothetical test network and also example transit demand data.
To quickly test the setup, run fast-trips on sample input using the following steps:
*  Add `<fast-trips-dir>` to the `PYTHONPATH` environment variable in *Advanced system settings*.
*  Run `\scripts\runAllTests.bat` from within `<fast-trips-dir>` in a command prompt. This will run several "preset" parameter combinations. The user can alternatively run each parameter combination individually using the commands listed in the batch file. Details about the test runs are provided in subsequent sections.
Output files from running fast-trips with the sample input data provided can be found in the `output` directory.

##### Test Network
A hypothetical 5-zone test network was developed to help code development. It has a total of three transit routes (one rail and two bus) with two or three stops each. There are also two park-and-ride (PnR) locations.

![alt text](/Examples/test_network/input/test_network.png "Transit Test Network") 

Transit vehicles commence at 3:00 PM and continue until 6:00 PM. There are 152 transit trips that make a total of 384 station stops. `input` folder contains all the supply-side/network input files prepared from the test network. More information about network input file standards can be found in the [GTFS-Plus Data Standards Repository][network-standard-url].

##### Test Demand
Two versions of sample demand have been prepared:
*  `demand_reg` contains regular demand that consists only of a transit trip list. There are no multiple user classes and all trips use a single set of path weights (`pathweight_ft.txt`). Demand starts at 3:15 PM and ends at 5:15 PM.One trip occurs every 10 seconds. More information is available in [documentation](/Examples/test_network/demand_reg/Readme.md).
*  `demand_twopaths` represents demand for two user classes that use different sets of path weights. Household and person attribute files are present in addition to the trip list to model user heterogeneity and multiple user classes.

Similar to network data standards, there also exists a [Demand Data Standards Repository][demand-standard-url]. 

##### Test Runs
There are a total of six test runs in `\scripts\runAllTests.bat`. Type of assignment, capacity constraint, and number of iterations are varied in addition to the demand.

| Sno   | Demand  | Assignment Type | Iterations | Capacity Constraint |
|------:|:-------:|:---------------:|-----------:|:-------------------:|
| 1 | Multi-class | Deterministic   | 2 | On  |
| 2 | Multi-class | Stochastic      | 1 | Off |
| 3 | Multi-class | Stochastic      | 2 | On  |
| 4 | Regular     | Deterministic   | 2 | On  |
| 5 | Regular     | Stochastic      | 1 | Off |
| 6 | Regular     | Stochastic      | 2 | On  |

Type of Assignment:
 *  "Deterministic" indicates use of a deterministic trip-based shortest path search algorithm
 *  "Stochastic" indicates use of a stochastic hyperpath-finding algorithm

[git-url]: <https://git-scm.com/>
[git-fork-url]: <https://help.github.com/articles/fork-a-repo/>
[python-vcpp-url]: <http://www.microsoft.com/en-us/download/details.aspx?id=44266>
[anaconda-url]: <https://www.continuum.io/downloads>
[python-transitfeed-url]: <https://github.com/google/transitfeed/wiki/TransitFeed>
[git-repo-url]: <https://github.com/MetropolitanTransportationCommission/fast-trips.git>
[network-standard-url]: <https://github.com/osplanning-data-standards/GTFS-PLUS>
[demand-standard-url]: <https://github.com/osplanning-data-standards/dyno-demand>