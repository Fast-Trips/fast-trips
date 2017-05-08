import fasttrips
import os, pandas, re, sys
from .Error import ConfigurationError


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

        dispersion -- theta parameter; essentially the nesting parameter. Good value is between 0.5-1. (default: 1.0)
        max_stop_process_count = maximum number of times you will re-processe a node (default: None)
        capacity -- Boolean to activate capacity constraints (default: False)

        overlap_variable -- One of ['None','count','distance','time']. Variable to use for overlap penalty calculation (default: 'count')
        overlap_split_transit -- Boolean.Split transit for path overlap penalty calculation (default: False)

        transfer_fare_ignore_pathfinding = Boolean. In path-finding, suppress trying to adjust fares using transfer rules.  For performance.
        transfer_fare_ignore_pathenum = Boolean. In path-enumeration, suppress trying to adjust fares using transfer rules.  For performance.
        number_of_processes = Integer. Number of processes to run at once (default: 1)

        debug_output_columnns -- boolean to activate extra columns for debugging (default: False)
    """
    print kwargs

    if not input_network_dir:
        msg = "Must specify where to find input networks"
        FastTripsLogger.fatal(msg)
        raise ConfigurationError("external input", msg)

    if not input_demand_dir:
        msg = "Must specify where to find input demand"
        FastTripsLogger.fatal(msg)
        raise ConfigurationError("external input", msg)

    if not input_weights:
        msg = "Must specify where pathweight parameters are"
        FastTripsLogger.fatal(msg)
        raise ConfigurationError("external input", msg)

    if not run_config:
        msg = "Must specify file with run configurations"
        FastTripsLogger.fatal(msg)
        raise ConfigurationError("external input", msg)

    if pathfinding_type not in ['deterministic','stochastic','file']:
        msg = "pathfinding.type [%s] not defined. Expected values: %s" % (pathfinding_type,['deterministic','stochastic','file'])
        FastTripsLogger.fatal(msg)
        raise ConfigurationError("external override", msg)


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

    if kwargs.has_key("number_of_processes"):
        fasttrips.Assignment.NUMBER_OF_PROCESSES = kwargs["number_of_processes"]

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
            raise ConfigurationError("external override", msg)
        fasttrips.PathSet.OVERLAP_VARIABLE       = kwargs["overlap_variable"]

    if "overlap_split_transit" in kwargs.keys():
        fasttrips.PathSet.OVERLAP_SPLIT_TRANSIT  = kwargs["overlap_split_transit"]
        
    if "transfer_fare_ignore_pathfinding" in kwargs.keys():
        fasttrips.Assignment.TRANSFER_FARE_IGNORE_PATHFINDING = kwargs["transfer_fare_ignore_pathfinding"]
        
    if "transfer_fare_ignore_pathenum" in kwargs.keys():
        fasttrips.Assignment.TRANSFER_FARE_IGNORE_PATHENUM = kwargs["transfer_fare_ignore_pathenum"]

    if "dispersion" in kwargs.keys():
        fasttrips.Assignment.STOCH_DISPERSION    = kwargs["dispersion"]

    if "num_trips" in kwargs.keys():
        fasttrips.Assignment.DEBUG_NUM_TRIPS     = kwargs["num_trips"]

    if "capacity" in kwargs.keys():
        fasttrips.Assignment.CAPACITY_CONSTRAINT = kwargs["capacity"]

    return ft


def run_fasttrips(**kwargs):
    """
    Wrapper function to set up and run fast-trips.
    The fast trips parameters as a combination of parameters
    read from the control file, but overwritten with parameters passed into this function.
    """
    args_dict = kwargs
    for key in args_dict.keys():
        print "%40s => %s" % (key, args_dict[key])

    ft = run_setup(**kwargs)
    # Read the networks and demand
    ft.read_input_files()
    # Run Fast-Trips
    r = ft.run_assignment(fasttrips.Assignment.OUTPUT_DIR)
    return r
