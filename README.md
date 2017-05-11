# fast-trips

fast-trips is a Dynamic Transit Assignment tool written in Python and supplemented by code in C++. For more information about this visit the following links:
 * Project Website: http://fast-trips.mtc.ca.gov/
 * Full Technical Documentation (API): http://data.mtc.ca.gov/fast-trips/

## Contents
* [Setup](#setup)
* [Input](#input)
  * [`config_ft.txt`](#config_fttxt)
    * [Configuration Options: fasttrips](#configuration-options-fasttrips)
    * [Configuration Options: pathfinding](#configuration-options-pathfinding)
    * [More on Overlap Path Size Penalites](#more-on-overlap-path-size-penalties)
  * [`config_ft.py`](#config_ftpy)
  * [`pathweight_ft.txt`](#pathweight_fttxt)
* [Fares](#fares)
* [Test Sample Input](#test-sample-input)
  * [Test Network](#test-network)  
  * [Test Demand](#test-demand) 
* [Running Fast-Trips](#running-fast-trips)  
* [Tests](#tests)  
* [Summarizing Results](#summarizing-results)
* [Frequently Asked Questions](#frequently-asked-questions)
* [References](#references)
* [Changelog](#changelog)

## Setup
Follow the steps below to setup up fast-trips:
*  Install [Git][git-url] and clone the fast-trips repository (https://github.com/MetropolitanTransportationCommission/fast-trips.git) to a local directory: `<fast-trips-dir>`. If the user plans on making changes to the code, it is recommended that the repository be [forked][git-fork-url] before cloning.
*  Switch to the `develop` branch of the repository.
*  Download and install [numpy][numpy-url] and [pandas][pandas-url].  One option is to install a *data analytics* Python 2.7 distribution which bundles these, like [Anaconda][anaconda-url].  Windows users can also find package installers [here][python-packages-windows-url].
*  If compiling on Windows, install [Microsoft Visual C++ Compiler for Python 2.7][python-vcpp-url].  On Linux, install the python-dev package.
*  Install the python package [transitfeed][python-transitfeed-url] for reading GTFS.
*  Set the `PYTHONPATH` environment variable to the location of your fast-trips repo, which we're calling `<fast-trips-dir>`.
*  To build, in the fast-trips directory `<fast-trips-dir>`, run the following in a command prompt:  `python setup.py build_ext --inplace`.

## Input
The input to fast-trips consists of:
*  A Transit Network directory, including schedules, access, egress and transfer information, specified by the [GTFS-Plus Data Standards Repository][network-standard-url]
*  A Transit Demand directory, including persons, households and trips, specified by the [Demand Data Standards Repository][demand-standard-url]
*  fast-trips Configuration, specified below

Configuration is specified in the following files:

### `config_ft.txt`

This is a *required* python file and may be included in both the Transit Supply and Transit Demand input directories.
If the same options are specified in both, then the version specified in the Transit Demand input directory will be used.
(Two versions may be specified because some configuration options are more relevant to demand and some are more relevant
to network inputs.)

The configuration files are parsed by python's [ConfigParser module](https://docs.python.org/2/library/configparser.html#module-ConfigParser) and therefore
adhere to that format, with two possible sections: *fasttrips* and *pathfinding*.
(See [Network Example](Examples/test_network/input/config_ft.txt) ) (See [Demand Example](Examples/test_network/demand_twopaths/config_ft.txt) )

#### Configuration Options: fasttrips

Option Name                         | Type   | Default | Description
-----------                         | ----   | --------| -------------------------
`bump_buffer`                       | float  | 5       | Not really used yet.
`bump_one_at_a_time`                | bool   | False   |
`capacity_constraint`               | bool   | False   | Hard capacity constraint.  When True, fasttrips forces everyone off overcapacity vehicles and disallows them from finding a new path using an overcapacity vehicle.
`create_skims`                      | bool   | False   | Not implemented yet.
`debug_num_trips`                   | int    | -1      | If positive, will truncate the trip list to this length.
`debug_trace_only`                  | bool   | False   | If True, will only find paths and simulate the person ids specified in `trace_person_ids`.
`debug_output_columns`              | bool   | False   | If True, will write internal & debug columns into output.
`fare_zone_symmetry`                | bool   | False   | If True, will assume fare zone symmetry.  That is, if fare_id X is configured from origin zone A to destination zone B, and there is no fare configured from zone B to zone A, we'll assume that fare_id X also applies.
`max_iterations`                    | int    | 1       | Maximum number of pathfinding iterations to run.
`number_of_processes`               | int    | 0       | Number of processes to use for path finding.
`output_passenger_trajectories`     | bool   | True    | Write chosen passenger paths?  TODO: deprecate.  Why would you ever not do this?
`output_pathset_per_sim_iter`       | bool   | False   | Output pathsets for each simulation iteration?  If false, just outputs once per path-finding iteration.
`prepend_route_id_to_trip_id`       | bool   | False   | This is for readability in debugging; if True, then route ids will be prepended to trip ids.
`simulation`                        | bool   | True    | Simulate transit vehicles?  After path-finding, should fast-trips update vehicle times and put passengers on vehicles?  If False, fast-trips still calculates costs and probabilities and chooses paths, but the vehicle times will not be updated from those read in from the input network, and passengers will not be loaded onto vehicles.  This is useful for debugging path-finding and verifying that pathfinding calculations are consistent with cost/fare calculations done outside pathfinding.
`skim_start_time`                   | string | 5:00    | Not implemented yet.
`skim_end_time`                     | string | 10:00   | Not implemented yet.
`skip_person_ids`                   | string | 'None'  | A list of person IDs to skip.
`trace_ids`                         | string | 'None'  | A list of tuples, (person ID, person trip ID), for whom to output verbose trace information.

#### Configuration Options: pathfinding

Option Name                         | Type   | Default | Description
-----------                         | ----   | --------| -----------
`max_num_paths`                     | int    | -1      | If positive, drops paths after this IF probability is less than ``
`min_path_probability`              | float  | 0.005   | Paths with probability less than this get dropped IF `max_num_paths` specified AND hit.
`min_transfer_penalty`              | float  | 1       | Minimum transfer penalty. Safeguard against having no transfer penalty which can result in terrible paths with excessive transfers.
`overlap_chunk_size`                | int    | 500     | How many person's trips to process at a time in overlap calculations in python simulation (more means faster but more memory required.)
`overlap_scale_parameter`           | float  | 1       | Scale parameter for overlap path size variable.
`overlap_split_transit`             | bool   | False   | For overlap calcs, split transit leg into component legs (A to E becauses A-B-C-D-E)
`overlap_variable`                  | string | 'count' | The variable upon which to base the overlap path size variable.  Can be one of `None`, `count`, `distance`, `time`.
`pathfinding_type`                  | string | 'stochastic' | Pathfinding method.  Can be `stochastic`, `deterministic`, or `file`.
`pathweights_fixed_width`           | bool   | False   | If true, read the pathweights file as a fixed width, left-justified table (as opposed to a CSV, which is the default).
`stochastic_dispersion`             | float  | 1.0     | Stochastic dispersion parameter. TODO: document this further.
`stochastic_max_stop_process_count` | int    | -1      | In path-finding, how many times should we process a stop during labeling?  Specify -1 for no max.
`stochastic_pathset_size`           | int    | 1000    | In path-finding, how many paths (not necessarily unique) determine a pathset?
`time_window`                       | float  | 30      | In path-finding, the max time a passenger would wait at a stop.
`transfer_fare_ignore_pathfinding`  | bool   | False   | In path-finding, suppress trying to adjust fares using transfer rules.  For performance.
`transfer_fare_ignore_pathenum`     | bool   | False   | In path-enumeration, suppress trying to adjust fares using transfer rules.  For performance.
`user_class_function`               | string | 'generic_user_class' | A function to generate a user class string given a user record.

#### More on Overlap Path Size Penalties

The path size overlap penalty is formulated by Ramming and discussed in Hoogendoorn-Lanser et al. (see [References](#references) ).

When the pathsize overlap is penalized (pathfinding `overlap_variable` is not `None`), then the following equation is used to calculate the path size overlap penalty:

![Path Overlap Penalty Equation](/doc/overlap_function.png "Path Overlap Penalty Equation")

Where
  * *i* is the path alternative for individual *n*
  * &Gamma;<sub>*i*</sub> is the set of legs of path alternative *i*
  * *l<sub>a</sub>* is the value of the `overlap_variable` for leg *a*.  So it is either 1, the distance or the time of leg *a* depending of if `overlap_scale_parameter` is `count`, `distance` or `time`, respectively.
  * *L<sub>i</sub> is the total sum of the `overlap_variable` over all legs *l<sub>a</sub>* that make up path alternative *i*
  * *C<sub>in</sub> is the choice set of path alternatives for individual *n* that overlap with alternative *i*
  * &gamma; is the `overlap_scale_parameter`
  * &delta;<sub>*ai*</sub> = 1 and &delta;<sub>*aj*</sub> = 0 &forall; *j* &ne; *i*

From Hoogendoor-Lanser et al.:

> Consequently, if leg *a* for alternative *i* is unique, then [the denominator is equal to 1] and the path size contribution of leg *a* is equal to its proportional length *l<sub>a</sub>/L<sub>i</sub>*. If leg *l<sub>a</sub>* is also used by alternative *j*, then the contribution of leg *l<sub>a</sub>* to path size PS<sub>*i*</sub> is smaller than *l<sub>a</sub>/L<sub>i</sub>*. If &gamma; = 0 or if routes *i* and *j* have equal length, then the contribution of leg *a* to PS<sub>*i*</sub> is equal to *l<sub>a</sub>/2L<sub>i</sub>*. If &gamma; &gt; 0 and routes *i* and *j* differ in length, then the contribution of leg *a* to PS<sub>*i*</sub> depends on the ratio of *L<sub>i</sub>* to *L<sub>j</sub>*. If route *i* is longer than route *j* and &gamma; &gt; 1, then the contribution of leg *a* to PS<sub>*i*</sub> is larger than *l<sub>a</sub>/2L<sub>i</sub>*; otherwise, the contribution is smaller than *l<sub>a</sub>/2L<sub>i</sub>*. If &gamma; &gt; 1 in the exponential path size formulation, then long routes are penalized in favor of short routes. The use of parameter &gamma; is questionable if overlapping routes have more or less equal length and should therefore be set to 0. Overlap between those alternatives should not affect their choice probabilities differently. The degree to which long routes should be penalized might be determined by estimating &gamma;. If &gamma; is not estimated, then an educated guess with respect to &gamma; should be made. To this end, differences in route length between alternatives in a choice set should be considered.

### `config_ft.py`

This is an *optional* python file containing functions that are evaluated to ascertain items such as user classes. 
This could be used to programmatically define user classes based on person, household and/or trip attributes.

The function name for user class is specified in the *pathfinding* input parameter `user_class_function`

__Example:__

```python
def user_class(row_series):
    """
    Defines the user class for this trip list.

    This function takes a single argument, the pandas.Series with person, household and
    trip_list attributes, and returns a user class string.
    """
    if row_series["hh_id"] == "simpson":
        return "not_real"
    return "real"
```


###  `pathweight_ft.txt`

The pathweight file is a *required* file that tells Fast-Trips how much to value each attribute of a path.  

The file can be a csv or fixed-format.  If you use a fixed-format, make sure
`pathweights_fixed_width = True` in the run configuration file (e.g., `config_ft.txt`).

`pathweight_ft.txt` **must** have the following columns:

Column Name        | Type  | Description
-----------        | ----  | -----------
`user_class`       | Str   | Config functions can use trip list, person, and household attributes to return a user class string to the trip.  <br><br>The string that is returned determines the set of path weights that are used. <br><br>( ??? is default if no user class function ? )  
`purpose`          | St    | This should match the trip purpose as specified in `trip_list.txt`  
`demand_mode_type` | Str   | One of <br>- `transfer`, <br>- `access`, <br>- `egress`, or <br>-`transit`  
`demand_mode`      | Str   | One of: <br>- `transfer`, <br>- a string specified as **access/egress mode** in `trip_list.txt` demand file (i.e. `walk`, `PNR`)  , or <br>- a string specified as a **transit mode** in `trip_list.txt` demand file (i.e. `local_bus`, `commuter_rail`)  
`supply_mode`      | Str   | For `demand_mode_type=transit`, corresponds to the transit mode as defined in the [`GTFS-Plus`](https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/variables.md#mode) input network.  <br><br>For `demand_mode_type=transfer` and `demand_mode=transfer`, is one of `walk`, `wait`, or `transfer_penalty`.  <br><br>For `demand_mode_type = access`, is one of `walk_access`, `bike_access`, `pnr_access`, or `knr_access`. <br><br>For `demand_mode_type = egress`, is one of `walk_egress`, `bike_egress`, `pnr_egress`, or `knr_egress`. 
`weight_name`      | Str   | An attribute of the path link. See below for more details.
`weight_value`     | Float |  The multiplier for the attribute named `weight_name`

_Notes_: 

  1.   If demand mode X has supply mode Y, that means a trip specified as transit mode X in the `trip_list.txt` may use a transit link specified as Y in the network. Moreover, if the trip list were to specify that someone takes `commuter_rail` (like if the ABM chooses the primary mode for them as commuter rail), then they can still take a local bus or any lesser mode on their trip in addition to commuter rail. Often in this case, the weights are assumed to be higher for non-commuter rail modes and lower for commuter rail to induce them to ride. For example:
  
`demand_mode`	| `supply_mode`	| `weight_name`	        | `weight_value`
--------------  | ------------- | --------------------- | -----------
`commuter_rail` | `local_bus`	| `in_vehicle_time_min`	| 1.5
`commuter_rail`	| `heavy_rail`  | `in_vehicle_time_min` | 1.0

### `Weight_Name` Values  

The following is an example of a minimally specified `pathweight_ft.txt` : 

`demand_mode_type`| `demand_mode`	| `supply_mode`	| `weight_name`	       | `weight_value`
----------------  | --------------  | ------------- | -------------------- | -----------
`access`          | `walk`          | `walk_access` | `time_min`           | 2
`egress`          | `walk`          | `walk_egress` | `time_min`           | 2
`transit`         | `transit`       | `local_bus`   | `wait_time_min`      | 2
`transit`         | `transit`       | `local_bus`   | `in_vehicle_time_min`| 1
`transfer`        | `transfer`      | `transfer`    | `transfer_penalty`   | 5
`transfer`        | `transfer`      | `transfer`    | `time_min`           | 2

For most of the weights prefix mode is not needed. E.g. there is no need to label `weight_name` `time_min` for `supply_mode` `walk_access` as `walk_time_min`, because the fact that the `supply_mode` is `walk_access` means it is only assessed on walk links. The drive option (PNR/KNR access/egress), however, should have `walk_` and `drive_` prefixes, because the access can have both components: driving to the station from the origin and walking from the lot to the station. So for example, for `supply_mode` `pnr_access` there will be two weights associated with travel time: `walk_time_min` and `drive_time_min`. 

The following is a partial list of possible weight names base don the demand mode / supply mode combinations.


`demand_mode_type = access` / `demand_mode = walk` / `supply_mode = walk_access` 
  
  * `time_min`  
  * `elevation_gain` 
  * `preferred_delay_min`
  
`demand_mode_type = egress` / `demand_mode = walk` / `supply_mode = walk_egress` 
  
  * `time_min` 
  * `elevation_gain` 
  * `preferred_delay_min`
  
`demand_mode_type = access` / `demand_mode = PNR` / `supply_mode = pnr_access` 

  * `walk_time_min`  
  * `drive_time_min` 
  
`demand_mode_type = transfer` / `demand_mode = transfer` / `supply_mode = transfer`  

  * `transfer_penalty`
  * `time_min`
  * `wait_time_min`

`demand_mode_type = transit` / `demand_mode = transit` / `supply_mode = <pick a transit mode>` 

  * `in_vehicle_time_min`
  * `wait_time_min`
  
Note that the cost component is handled at the path level using the value of time column in `trip_list.txt`.  

## Fares

GTFS-plus fare inputs are similar to GTFS fare inputs but with additional fare periods for time period-based fares.

However, since the columns `route_id`, `origin_id`, `destination_id` and `contains_id` are all optional in [fare_rules.txt](https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_rules.md) and therefore may be specified in different combinations, fast-trips implements fares with the following rules:

* `contains_id` is not implemented in fast-trips, and its inclusion will result in an error
* Specifying `origin_id` and not `destination_id` or vice versa will result in an error.  Each fare rule must specify both or neither.
* These combinations of `route_id`, `origin_id`, and `destination_id` will be used to match a `fare_id` to a transit trip, in this order. The first match will win.
  * Matching `route_id`, `origin_id` and `destination_id`
  * Matching `route_id` only (no `origin_id` or `destination_id` specified)
  * Matching `origin_id` and `destination_id` only (no `route_id` specified)
  * No match (e.g. `fare_id` specified with no other columns)

Discount and free transfers specified in [fare_transfer_rules_ft.txt](https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_transfer_rules_ft.md) are applied to transfers from one fare period to another fare period, and these links need to be *back-to-back*.  So if a passenger transfers from A to B to C and the discount is specified for fare period A to fare period C, they will not receive the discount.

Free transfers are also specified *within* fare periods (possibly time-bounded) in [fare_attributes_ft.txt](https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_attributes_ft.md). These free transfers are applied *after* the discounts from [fare_transfer_rules_ft.txt](https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_transfer_rules_ft.md) and they do not need to be back-to-back.  So if a passenger transfers from A to B to A and fare period A has 1 free transfer specified, but a transfer from B to A has a transfer fare of $.50, the passenger will receive the free transfer since these rules are applied last (and override).

There are four places where fares factor into fast-trips.

1. During path-finding (C++ extension), fares get assessed as a cost onto links, which translate to generalized cost (minutes) via the traveler's value of time.  [Fare transfer rules](https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_transfer_rules_ft.md) here are complicated, because we don't know which is the next/previous fare, and we can only guess based on probabilities.  The fare is estimated using [`Hyperlink::getFareWithTransfer()`](src/hyperlink.cpp).

   Free transfers as configured in [fare attributes](https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_attributes_ft.md) are implemented here in a simplistic way; that is, a free transfer is assumed if the fare attributes have granted any free transfers without looking at `transfer_duration` or the number of transfers. Also, this transfer is required to be *back-to-back* also.  A future enhancement could include keeping a transfer count for each fare period so that the back-to-back requirement is not imposed, and also so that a certain number of free fares could be tallied, but at this time, a simpler approach is used because it's not clear if this kind of detail is helpful.

   Turn this off using configuration option `transfer_fare_ignore_pathfinding`.

2. During path-enumeration (C++ extension), when the paths are being constructed by choosing links from the hyperpath graph, at the point where each link is added to the path, the [fare transfer rules](https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_transfer_rules_ft.md) are applied to adjust fares with more certainty of the the path so far.  This is done in [`Hyperlink::setupProbabilities()`](src/hyperlink.cpp) which calls `Hyperlink::updateFare()` and updates the link cost as well if the fare is affected.  Free transfers as configured in [fare attributes](https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_attributes_ft.md) are looked at here as well, but without the transfer duration component.

3. During path-enumeration (C++ extension), after the path is constructed, the trip cost is re-calculated at the end using [`Path::calculateCost()`](src/path.cpp).  At this moment in the process, the path is complete and final, so the fare transfer rules are relatively easy to apply given that links are certain.  The initial fare and cost are saved and passed back to python to show the effect of step 1.

   Free transfers as configured in [fare attributes](https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_attributes_ft.md) are also addressed here.

   Turn this off using configuration option `transfer_fare_ignore_pathenum`.

4. During simulation (python), while the path is being adjusted due to vehicle times, the fares are calculated via [`Route.add_fares()`](fasttrips/Route.py).  This is unlikely to change anything unless the fare periods changed due to the slow-down of vehicles -- so consider deprecating this in favor of using the pathfinding results?  For now, it's a good test that the C++ code is working as expected; running with simulation off should result in identical fare and cost results from pathfinding and the (non-vehicle-updating) python simulation.

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

**NOTE: Any parameters passed in at run-time from the command line or via the script will overwrite any parameters read in from the `run_config` file.

### Running the Example from a Script

Sample input files have been provided in `<fast-trips-dir>\Examples\test_network` to test the setup and also assist with the creation of new fast-trips runs. The input files include network files created from a small hypothetical network and also example transit demand data.

```python

# \scripts\run_example.py

import os
from fasttrips import Run

EXAMPLES_DIR   = os.path.join(os.path.dirname(os.getcwd()),"fasttrips","Examples","test_scenario")

Run.run_fasttrips(
    input_network_dir     = os.path.join(EXAMPLES_DIR,"network"),
    input_demand_dir      = os.path.join(EXAMPLES_DIR,"demand_reg"),
    run_config            = os.path.join(EXAMPLES_DIR,"demand_reg","config_ft.txt"),
    input_weights         = os.path.join(EXAMPLES_DIR,"demand_reg","pathweight_ft.txt"),
    output_dir            = os.path.join(EXAMPLES_DIR,"output"),
    output_folder         = "example",
    pathfinding_type      = "stochastic",
    overlap_variable      = "count",
    overlap_split_transit = True,
    iters                 = 1,
    dispersion            = 0.50)
```

To run the example: 

*  Make sure your `<fast-trips-dir>` is in your `PYTHONPATH` environment variable in *Advanced system settings* [Win] or terminal [OSX].
*  Run `python run_example.py` from within `<fast-trips-dir>\scripts` in a command prompt [ Win ] or terminal [ OSX ]. 

Output files from running fast-trips with the sample input data provided can be found in the `output` directory.

### Running the Example from Command Line  

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

C:\Users\lzorn\Documents\fast-trips>rem Run it with Example test scenario and the demand_reg trip list
C:\Users\lzorn\Documents\fast-trips>rem If using installed version, use 'run_fasttrips' instead of 'python fasttrips\Run.py'

C:\Users\lzorn\Documents\fast-trips>python fasttrips\Run.py stochastic 1 fasttrips\Examples\test_scenario\demand_reg\config_ft.txt fasttrips\Examples\test_scenario\network fasttrips\Examples\test_scenario\demand_reg fasttrips\Examples\test_scenario\demand_reg\pathweight_ft.txt fasttrips\Examples\test_scenario\output_demand_reg
```

#### Example Network
The hypothetical 5-zone example network was developed to help code development. It has a total of three transit routes (one rail and two bus) with two or three stops each. There are also two park-and-ride (PnR) locations.

![alt text](/fasttrips/Examples/test_scenario/network/test_network.png "Transit Example Network")

Transit vehicles commence at 3:00 PM and continue until 6:00 PM. There are 152 transit trips that make a total of 384 station stops. `input` folder contains all the supply-side/network input files prepared from the test network. More information about network input file standards can be found in the [GTFS-Plus Data Standards Repository][network-standard-url].

#### Example Demand
Two versions of sample demand have been prepared:
*  `demand_reg` contains regular demand that consists only of a transit trip list. There are no multiple user classes and all trips use a single set of path weights (`pathweight_ft.txt`). Demand starts at 3:15 PM and ends at 5:15 PM.One trip occurs every 10 seconds. More information is available in [documentation](/Examples/test_network/demand_reg/Readme.md).
*  `demand_twopaths` represents demand for two user classes that use different sets of path weights. Household and person attribute files are present in addition to the trip list to model user heterogeneity and multiple user classes.

Similar to network data standards, there also exists a [Demand Data Standards Repository][demand-standard-url]. 


## Tests
There are multiple test runs in `\tests`.  They can be run by installing the [PyTest](https://docs.pytest.org/en/latest/) library and executing the command `pytest` from the command line within your `<fast-trips-dir>`.  

__Fares:__ `test_maxStopProcessCount.py`

Tests 10, 50, and 100 for the value of `max stop process count` – the maximum number of times you will re-processe a node (default: None)

 * **Overlap Variable:** `count`, `distance`, `time`   
 * **Overlap Split:** Boolean

__Fares:__ `test_fares.py`

Tests shortcuts in fare calculations

 * **Ignore Pathfinding**     
 * **Ignore Pathfinding and Path Enumeration** 
 
__Overlap Functions:__ `test_overlap.py`

Tests both overlap type and whether or not each transit segment is broken and compared to each of its parts.

 * **Overlap Variable:** `count`, `distance`, `time`   
 * **Overlap Split:** Boolean

__Feedback:__ `test_feedback.py`

 *  Runs full demand for three iterations with and without capacity constraint

__Dispersion Levels:__ `test_dispersion.py`

 *  Runs dispersion levels 0.1 to 1.0 at varying increments

__User Classes:__ `test_user_classes.py`

 *  Uses multiple user classes as defined in `config_ft.py`

__Assignment Type:__ `test_assignment_type.py`

 *  "Deterministic" indicates use of a deterministic trip-based shortest path search algorithm
 *  "Stochastic" indicates use of a stochastic hyperpath-finding algorithm

__Note:__ Multiprocessing is not tested because it is [incompatible with PyTest](https://github.com/pytest-dev/pytest/issues/958)  

## Summarizing Results

Fast-Trips will output results in the [dyno-path](https://github.com/osplanning-data-standards/dyno-path) format, which can be used to generate summary dashboards in Tableau or other reports.  


### Creating Tableau Dashboard



## Frequently Asked Questions

* How do I restart a run after pathfinding?

Use the option `pathfinding_type=file`, via [`runTest.py`](scripts/runTest.py) or in the configuration.  Then, drop the `pathsfound_paths.csv` and `pathsfound_links.csv` files in the output directory for your run, and they'll be read in instead of generated.

## References

 * Ramming, M. S. *Network Knowledge and Route Choice.* Ph.D. Thesis. Massachusetts Institute of Technology, Cambridge, Mass., 2002.

 * Hoogendoorn-Lanser, S., R. Nes, and P. Bovy. Path Size Modeling in Multinomial Route Choice Analysis. 27 In *Transportation Research Record: Journal of the transportation Research Board, No 1921*, 28 Transportation Research Board of the National Academies, Washington, D.C., 2005, pp. 27-34.

## Changelog

Major changes to fast-trips since the original FAST-TrIPs (https://github.com/MetropolitanTransportationCommission/FAST-TrIPs-1)

To be filled in further but including:
* Added pathfinding iterations to looping (so pathfinding_iteration=1 finds paths for everyone, and subsequently just find paths for people who don't have a valid path. Break when max or we don't find anymore)
* Added time-period based drive access links (10/2016)
* Added link distance to extension as part of StopState (10/2016)
* Implemented overlap pathsize correction (8/2016)
* Add purpose segmentation to cost weighting (7/2016)
* Output pathsets in addition to chosen paths (4/2016)
* Update transit trip vehicle times based on boards, alights and vehicle-configured accleration, deceleration and dwell formulas (4/2016)
* Output performance measures (pathfinding and path enumeration times, number of stops processed) (3/2016)
* Stop order update to pathfinding: when a stop state is updated, mark other reachable stops for reprocessing (3/2016) [details][stop-order-details-url]
* Support KNR and PNR access (11/2015)
* Read user-class based cost weighting (11/2015)
* Switch input format to GTFS-plus network (10/2015)
* Move path finding to C++ extension (9/2015)
* Parallelized path finding with multiprocessing (7/2015)
* Port original FAST-TrIPs codebase to python with debug tracing (5/2015)

[git-url]: <https://git-scm.com/>
[git-fork-url]: <https://help.github.com/articles/fork-a-repo/>
[python-vcpp-url]: <http://www.microsoft.com/en-us/download/details.aspx?id=44266>
[numpy-url]:  <http://www.numpy.org/>
[pandas-url]: <http://pandas.pydata.org/>
[anaconda-url]: <https://www.continuum.io/downloads>
[python-packages-windows-url]: <http://www.lfd.uci.edu/~gohlke/pythonlibs/>
[python-transitfeed-url]: <https://github.com/google/transitfeed/wiki/TransitFeed>
[git-repo-url]: <https://github.com/MetropolitanTransportationCommission/fast-trips.git>
[network-standard-url]: <https://github.com/osplanning-data-standards/GTFS-PLUS>
[demand-standard-url]: <https://github.com/osplanning-data-standards/dyno-demand>
[stop-order-details-url]: <https://github.com/MetropolitanTransportationCommission/fast-trips/pull/22>
