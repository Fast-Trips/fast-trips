"""
Functions to simplify running Fast-Trips.
"""

__copyright__ = "Copyright 2015-2017 Contributing Entities"
__license__   = """
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
"""
import argparse
import datetime
import os
import sys

import fasttrips


def run_setup(input_network_dir,
              input_demand_dir,
              input_weights,
              run_config,
              iters,
              output_dir,
              pathfinding_type  = 'stochastic',
              input_functions   = None,
              output_folder     = None,
              trace_only        = False,

              **kwargs):

    """
    Reads run configuration files from network and demand input directories.
    If additional parameters are input here, they will override the run configuration files.

    Named Keyword arguments:
        input_network_dir -- the input directory with GTFS-PLUS networks (required)
        input_demand_dir -- the input directory with the dyno-demand files (required)
        input_weights -- the file with the pathweight parameters (required)
        run_config -- the file with further run configs (required)
        iters -- number of global iterations, integer (required)
        output_dir -- where to put the output folder (required). Will be created if it doesn't already exist.

        pathfinding_type -- one of ['stochastic','deterministic','file'] (default: stochastic)
        input_functions -- the file with user functions
        output_folder -- where to put the outputs.  Will be created if it doesn't already exist.
        trace_only -- will only run demand for the trace.  Will not overwrite other output (default: False)


    Unnamed keyword arguments:
        num_trips -- if specified, will process this number of trips and ignore the rest
        pf_iters -- Integer. If specified, will set the maximum number of pathfinding iterations(default: 10)
        dispersion -- theta parameter; essentially the nesting parameter. Good value is between 0.5-1. (default: 1.0)
        max_stop_process_count = maximum number of times you will re-processe a node (default: 20)
        capacity -- Boolean to activate capacity constraints (default: False)

        overlap_variable -- One of ['None','count','distance','time']. Variable to use for overlap penalty calculation (default: 'count')
        overlap_split_transit -- Boolean.Split transit for path overlap penalty calculation (default: False)

        time_window = Integer. The time window in minutes where a passenger searches for a eminating transit route at each node.
        transfer_fare_ignore_pathfinding = Boolean. In path-finding, suppress trying to adjust fares using transfer rules.  For performance.
        transfer_fare_ignore_pathenum = Boolean. In path-enumeration, suppress trying to adjust fares using transfer rules.  For performance.
        number_of_processes = Integer. Number of processes to run at once (default: 1)
        output_pathset_per_sim_iter = Boolean. Output pathsets per simulation iteration?  (default: false)

        debug_output_columnns -- boolean to activate extra columns for debugging (default: False)
    """
    print kwargs

    if not input_network_dir:
        msg = "Must specify where to find input networks"
        FastTripsLogger.fatal(msg)
        raise fasttrips.ConfigurationError("external input", msg)

    if not input_demand_dir:
        msg = "Must specify where to find input demand"
        FastTripsLogger.fatal(msg)
        raise fasttrips.ConfigurationError("external input", msg)

    if not input_weights:
        msg = "Must specify where pathweight parameters are"
        FastTripsLogger.fatal(msg)
        raise fasttrips.ConfigurationError("external input", msg)

    if not run_config:
        msg = "Must specify file with run configurations"
        FastTripsLogger.fatal(msg)
        raise fasttrips.ConfigurationError("external input", msg)

    if pathfinding_type not in ['deterministic','stochastic','file']:
        msg = "pathfinding.type [%s] not defined. Expected values: %s" % (pathfinding_type,['deterministic','stochastic','file'])
        FastTripsLogger.fatal(msg)
        raise fasttrips.ConfigurationError("external override", msg)


    # Setup Output Directory
    if not output_folder:
        output_folder = "output_%s_iter%d_%s" % (pathfinding_type, iters, "cap" if (kwargs.has_key("capacity") and kwargs["capacity"]==True) else "nocap")

    # don't override full run results
    if trace_only:
        output_folder = "%s_trace" % output_folder

    if not output_dir:
        output_dir =  os.path.basename(input_demand_dir)

    # create folder if it doesn't already exist
    full_output_dir = os.path.join(output_dir, output_folder)

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    if not os.path.exists(full_output_dir):
        print "Creating full output dir [%s]" % full_output_dir
        os.mkdir(full_output_dir)

    # Create fast-trips instance
    ft = fasttrips.FastTrips(input_network_dir, input_demand_dir, input_weights, run_config, full_output_dir, input_functions=input_functions)

    # Read the configuration file and overwrite with any options called with the function call
    ft.read_configuration()

    if iters > 0:
        fasttrips.Assignment.MAX_ITERATIONS          = int(iters)

    if kwargs.has_key("pf_iters"):
            fasttrips.Assignment.MAX_PF_ITERATIONS = kwargs["pf_iters"]

    if kwargs.has_key("number_of_processes"):
        fasttrips.Assignment.NUMBER_OF_PROCESSES = kwargs["number_of_processes"]

    if "trace_ids" in kwargs.keys():
        fasttrips.Assignment.TRACE_IDS = kwargs["trace_ids"]

    if trace_only:
        if len(fasttrips.Assignment.TRACE_IDS) == 0:
            print "Trace only requested but no trace IDs are specified in configuration."
            sys.exit(2)
        fasttrips.Assignment.DEBUG_TRACE_ONLY    = True
        fasttrips.Assignment.NUMBER_OF_PROCESSES = 1

    if "pathfinding_type" in kwargs.keys():
        fasttrips.Assignment.PATHFINDING_TYPE        = kwargs["pathfinding_type"]

    if "max_stop_process_count" in kwargs.keys():
        fasttrips.Assignment.STOCH_MAX_STOP_PROCESS_COUNT = kwargs["max_stop_process_count"]

    if "debug_output_columns" in kwargs.keys():
        fasttrips.Assignment.DEBUG_OUTPUT_COLUMNS = kwargs["debug_output_columns"]

    if "overlap_variable" in kwargs.keys():
        if kwargs["overlap_variable"] not in ['None','count','distance','time']:
            msg = "pathfinding.overlap_variable [%s] not defined. Expected values: %s" % (kwargs["overlap_variable"], str(fasttrips.PathSet.OVERLAP_VARIABLE_OPTIONS))
            fasttrips.FastTripsLogger.fatal(msg)
            raise fasttrips.ConfigurationError("external override", msg)
        fasttrips.PathSet.OVERLAP_VARIABLE       = kwargs["overlap_variable"]

    if "overlap_split_transit" in kwargs.keys():
        fasttrips.PathSet.OVERLAP_SPLIT_TRANSIT  = kwargs["overlap_split_transit"]

    if "transfer_fare_ignore_pathfinding" in kwargs.keys():
        fasttrips.Assignment.TRANSFER_FARE_IGNORE_PATHFINDING = kwargs["transfer_fare_ignore_pathfinding"]

    if "transfer_fare_ignore_pathenum" in kwargs.keys():
        fasttrips.Assignment.TRANSFER_FARE_IGNORE_PATHENUM = kwargs["transfer_fare_ignore_pathenum"]

    if "time_window" in kwargs.keys():
        fasttrips.Assignment.TIME_WINDOW         = datetime.timedelta(minutes=float(kwargs["time_window"]))

    if "utils_conversion_factor" in kwargs.keys():
        fasttrips.Assignment.UTILS_CONVERSION    = kwargs["utils_conversion_factor"]

    if "dispersion" in kwargs.keys():
        fasttrips.Assignment.STOCH_DISPERSION    = kwargs["dispersion"]

    if "num_trips" in kwargs.keys():
        fasttrips.Assignment.DEBUG_NUM_TRIPS     = kwargs["num_trips"]

    if "capacity" in kwargs.keys():
        fasttrips.Assignment.CAPACITY_CONSTRAINT = kwargs["capacity"]

    if "output_pathset_per_sim_iter" in kwargs.keys():
        fasttrips.Assignment.OUTPUT_PATHSET_PER_SIM_ITER = kwargs["output_pathset_per_sim_iter"]

    if "user_class_function" in kwargs.keys():
        fasttrips.PathSet.USER_CLASS_FUNCTION    = kwargs["user_class_function"]

    return ft


def run_fasttrips(**kwargs):
    """
    Wrapper function to set up and run fast-trips.
    The fast trips parameters as a combination of parameters
    read from the control file, but overwritten with parameters passed into this function.
    """
    args_dict = kwargs
    # for key in args_dict.keys():
    #     print "%40s => %s" % (key, args_dict[key])

    # instantiate and read configuration
    ft = run_setup(**kwargs)
    # Read the networks and demand
    ft.read_input_files()
    # Run Fast-Trips
    r = ft.run_assignment(fasttrips.Assignment.OUTPUT_DIR)
    return r


USAGE = r"""

  Run Fast-Trips from the command line with required inputs as command line parameters.

"""

def main():
    """
    Does arg parsing for command line interface.
    """

    def str2bool(v):
        #susendberg's function
        return v.lower() in ("yes", "true", "t", "1")

    parser = argparse.ArgumentParser(usage=USAGE)
    parser.register('type','bool',str2bool)
    parser.add_argument('-t','--trace_only', action='store_true', help="Run only the trace persons?")
    parser.add_argument('-n','--num_trips',  type=int,  help="Number of person trips to run, to run a subset of the whole demand.")
    parser.add_argument('-d','--dispersion', type=float,help="Stochastic dispersion parameter")
    parser.add_argument('-m','--max_stop_process_count', type=int, help="Max times to process a stop in stochastic pathfinding")
    parser.add_argument('-c','--capacity',      action='store_true', help="Enable capacity constraint")
    parser.add_argument('-o','--output_folder', type=str,  help="Directory within output_loc to write fasttrips outtput.  If none specified, will construct one.")
    parser.add_argument('--debug_output_columns',             action='store_true', help="Include debug columns in output")
    parser.add_argument('--overlap_variable',                 choices=['None','count','distance','time'], help="Variable to use for overlap penalty calculation")
    parser.add_argument('--overlap_split_transit',            action='store_true', help="Split transit for path overlap penalty calculation")
    parser.add_argument('--transfer_fare_ignore_pathfinding', action='store_true', help="In path-finding, suppress trying to adjust fares using transfer rules.  For performance.")
    parser.add_argument('--transfer_fare_ignore_pathenum',    action='store_true', help="In path-enumeration, suppress trying to adjust fares using transfer rules.  For performance.")
    parser.add_argument("pathfinding_type",  choices=['deterministic','stochastic','file'], help="Type of pathfinding")
    parser.add_argument("iters",             type=int,  help="Number of iterations to run")
    parser.add_argument("run_config",        type=str,  help="The run configuration file")
    parser.add_argument("input_network_dir", type=str,  help="Location of the input network")
    parser.add_argument("input_demand_dir",  type=str,  help="Location of the input demand")
    parser.add_argument("input_weights",     type=str,  help="Location of the pathweights file")
    parser.add_argument("output_dir",        type=str,  help="Location to write fasttrips output")

    args = parser.parse_args(sys.argv[1:])

    # don't pass on items that aren't set
    args_dict = vars(args)
    for key in args_dict.keys():
        if args_dict[key]==None: del args_dict[key]

    # if config_ft.py exists in demand dir, specify it for input_functions
    func_file = os.path.join(args.input_demand_dir, "config_ft.py")
    if os.path.exists(func_file):
        args_dict["input_functions"] = func_file
    # print args_dict

    r = fasttrips.Run.run_fasttrips(**args_dict)

if __name__ == "__main__":
    main()
