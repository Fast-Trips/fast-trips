__copyright__ = "Copyright 2015 Contributing Entities"
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
import ConfigParser,Queue
import collections,datetime,math,multiprocessing,os,random,sys,traceback
import numpy,pandas
import _fasttrips

from .Error       import ConfigurationError
from .Logger      import FastTripsLogger, setupLogging
from .Passenger   import Passenger
from .PathSet     import PathSet
from .Performance import Performance
from .Stop        import Stop
from .TAZ         import TAZ
from .Transfer    import Transfer
from .Trip        import Trip
from .Util        import Util

class Assignment:
    """
    Assignment class.  Documentation forthcoming.

    """
    #: Configuration file for fasttrips
    CONFIGURATION_FILE              = 'config_ft.txt'
    #: Configuration functions
    CONFIGURATION_FUNCTIONS_FILE    = 'config_ft.py'

    #: Output copy of the configuration file in case anything got overridden
    #: (Hmm naming conventions are a bit awkward here)
    CONFIGURATION_OUTPUT_FILE       = 'ft_output_config.txt'

    #: Configuration: Maximum number of iterations to remove capacity violations. When
    #: the transit system is not crowded or when capacity constraint is
    #: relaxed the model will terminate after the first iteration
    ITERATION_FLAG                  = None

    ASSIGNMENT_TYPE_SIM_ONLY        = 'Simulation Only'
    ASSIGNMENT_TYPE_DET_ASGN        = 'Deterministic Assignment'
    ASSIGNMENT_TYPE_STO_ASGN        = 'Stochastic Assignment'
    #: Configuration: Assignment Type
    #: 'Simulation Only' - No Assignment (only simulation, given paths in the input)
    #: 'Deterministic Assignment'
    #: 'Stochastic Assignment'
    ASSIGNMENT_TYPE                 = None

    #: Configuration: Simulation flag. It should be True for iterative assignment. In a one shot
    #: assignment with simulation flag off, the passengers are assigned to
    #: paths but are not loaded to the network.  Boolean.
    SIMULATION_FLAG                 = None

    #: Configuration: Passenger trajectory output flag. Passengers' path and time will be
    #: reported if this flag is on. Note that the simulation flag should be on for
    #: passengers' time.  Boolean.
    OUTPUT_PASSENGER_TRAJECTORIES   = None

    #: Configuration: Path time-window. This is the time in which the paths are generated.
    #: E.g. with a typical 30 min window, any path within 30 min of the
    #: departure time will be checked.  A :py:class:`datetime.timedelta` instance.
    TIME_WINDOW                     = None

    #: Configuration: Create skims flag. This is specific to the travel demand models
    #: (not working in this version). Boolean.
    CREATE_SKIMS                    = None

    #: Configuration: Beginning of the time period for which the skim is required.
    #:  (specify as 'HH:MM'). A :py:class:`datetime.datetime` instance.
    SKIM_START_TIME                 = None

    #: Configuration: End of the time period for which the skim is required
    #: (specify as 'HH:MM'). A :py:class:`datetime.datetime` instance.
    SKIM_END_TIME                   = None

    #: Route choice configuration: Dispersion parameter in the logit function.
    #: Higher values result in less stochasticity. Must be nonnegative. 
    #: If unknown use a value between 0.5 and 1. Float.
    STOCH_DISPERSION                = None

    #: Route choice configuration: How many times max times should we process a stop
    #: during labeling?  Use -1 to specify no max.  Int.
    #: Setting this to a positive value may increase runtime but may decrease
    #: pathset quality. (Todo: test/quantify this.)
    STOCH_MAX_STOP_PROCESS_COUNT    = None

    #: Route choice configuration: How many stochastic paths will we generate
    #: (not necessarily unique) to define a path choice set?  Int.
    STOCH_PATHSET_SIZE              = None

    #: Route choice configuration: Use vehicle capacity constraints. Boolean.
    CAPACITY_CONSTRAINT             = None

    #: Debug mode: only run trace passengers
    DEBUG_TRACE_ONLY                = False

    #: Debug mode: only run this number of trips, -1 to run all. Int.
    DEBUG_NUM_TRIPS                 = -1

    #: Trace these passengers
    TRACE_PERSON_IDS                = None

    #: Prepend the route id to the trip id?  This is for readability in debugging, since
    #: route IDs are typically more readable and trip ids are inscrutable
    PREPEND_ROUTE_ID_TO_TRIP_ID     = False

    #: Number of processes to use for path finding (via :py:mod:`multiprocessing`)
    #: Set to 1 to run everything in this process
    #: Set to less than 1 to use the result of :py:func:`multiprocessing.cpu_count`
    #: Set to positive integer greater than 1 to set a fixed number of processes
    NUMBER_OF_PROCESSES             = None

    #: Extra time so passengers don't get bumped (?). A :py:class:`datetime.timedelta` instance.
    BUMP_BUFFER                     = None

    #: This is the only simulation state that exists across iterations
    #: It's a dictionary of (trip_id, stop_id) -> earliest time a bumped passenger started waiting
    bump_wait                       = {}
    bump_wait_df                    = None

    #: Simulation: bump one stop at a time (slower, more accurate)
    #:
    #: When addressing capacity constraints in simulation, we look at all the (trip, stop)-pairs
    #: where the boards are not allowed since vehicle is over capacity.  The faster way to address
    #: this is to bump all of those passengers, which means we call the assigned path bad and try
    #: to reassign.
    #:
    #: However, this could over-bump passengers, because if a passenger rides multiple
    #: crowded vehicles, then bumping her frees up space on other vehicles and so some other bumping
    #: may not be necessary.  Thus, the more accurate (but slower) method is to bump passengers from
    #: each (trip,stop) at a time, in order of the full vehicle arrival time, and then recalculate
    #: loads, and iterate until we have no capacity issues.  Boolean.
    BUMP_ONE_AT_A_TIME              = None

    #: MSA the results that affect the next iteration to avoid oscillation: boards, alights, overcap onboard at stops
    MSA_RESULTS                     = False

    #: Are we finding paths for everyone right now?  Or just un-arrived folks?
    PATHFINDING_EVERYONE            = True

    #: How many Simulation Iterations should we do before going back to path-finding?
    MAX_SIMULATION_ITERS            = 10

    #: Column names for simulation
    SIM_COL_PAX_BOARD_TIME          = 'board_time'
    SIM_COL_PAX_ALIGHT_TIME         = 'alight_time'
    SIM_COL_PAX_LINK_TIME           = 'linktime'
    SIM_COL_PAX_OVERCAP_FRAC        = 'overcap_frac'  # if board at an overcap stop, fraction of boards that are overcap
    SIM_COL_PAX_BUMP_ITER           = 'bump_iter'
    SIM_COL_PAX_BUMPSTOP_BOARDED    = 'bumpstop_boarded'
    SIM_COL_PAX_COST                = 'sim_cost'      # cannot be cost because it collides with TAZ.DRIVE_ACCESS_COLUMN_COST
    SIM_COL_PAX_PROBABILITY         = 'probability'
    SIM_COL_PAX_LOGSUM              = 'logsum'

    #: Is this link/path a missed transfer?
    #: Set in both pathset links and pathset paths, this is a 1 or 0
    SIM_COL_MISSED_XFER             = 'missed_xfer'

    #: Chosen status for path
    #: set to -1 for not chosen
    #: set to iteration + simulation_iteration/100 when a path is chosen
    #: set to -2 for chosen but then rejected
    SIM_COL_PAX_CHOSEN              = 'chosen'
    CHOSEN_NOT_CHOSEN_YET           = -1
    CHOSEN_REJECTED                 = -2

    def __init__(self):
        """
        This does nothing.  Assignment methods are static methods for now.
        """
        pass

    @staticmethod
    def read_configuration(input_network_dir, input_demand_dir, config_file=CONFIGURATION_FILE):
        """
        Read the configuration parameters.
        """
        pandas.set_option('display.width',      1000)
        # pandas.set_option('display.height',   1000)
        pandas.set_option('display.max_rows',   1000)
        pandas.set_option('display.max_columns', 100)

        # Functions are defined in here -- read this and eval it
        func_file = os.path.join(input_demand_dir, Assignment.CONFIGURATION_FUNCTIONS_FILE)
        if os.path.exists(func_file):
            my_globals = {}
            FastTripsLogger.info("Reading %s" % func_file)
            execfile(func_file, my_globals, PathSet.CONFIGURED_FUNCTIONS)
            FastTripsLogger.info("PathSet.CONFIGURED_FUNCTIONS = %s" % str(PathSet.CONFIGURED_FUNCTIONS))

        parser = ConfigParser.RawConfigParser(
            defaults={'iterations'                      :1,
                      'pathfinding_type'                :Assignment.ASSIGNMENT_TYPE_DET_ASGN,
                      'simulation'                      :'True',
                      'output_passenger_trajectories'   :'True',
                      'time_window'                     :30,
                      'create_skims'                    :'False',
                      'skim_start_time'                 :'5:00',
                      'skim_end_time'                   :'10:00',
                      'stochastic_dispersion'           :1.0,
                      'stochastic_max_stop_process_count':-1,
                      'stochastic_pathset_size'         :1000,
                      'capacity_constraint'             :'False',
                      'trace_person_ids'                :'None',
                      'debug_trace_only'                :'False',
                      'debug_num_trips'                 :-1,
                      'prepend_route_id_to_trip_id'     :'False',
                      'number_of_processes'             :0,
                      'bump_buffer'                     :5,
                      'bump_one_at_a_time'              :'False',
                      # pathfinding
                      'user_class_function'             :'generic_user_class'
                     })
        parser.read(os.path.join(input_network_dir, config_file))
        if input_demand_dir and (input_demand_dir != input_network_dir) and os.path.exists(os.path.join(input_demand_dir,  config_file)):
            parser.read(os.path.join(input_demand_dir, config_file))

        Assignment.ITERATION_FLAG                = parser.getint    ('fasttrips','iterations')
        Assignment.ASSIGNMENT_TYPE               = parser.get       ('fasttrips','pathfinding_type')
        assert(Assignment.ASSIGNMENT_TYPE in [Assignment.ASSIGNMENT_TYPE_SIM_ONLY, \
                                              Assignment.ASSIGNMENT_TYPE_DET_ASGN, \
                                              Assignment.ASSIGNMENT_TYPE_STO_ASGN])
        Assignment.SIMULATION_FLAG               = parser.getboolean('fasttrips','simulation')
        Assignment.OUTPUT_PASSENGER_TRAJECTORIES = parser.getboolean('fasttrips','output_passenger_trajectories')
        Assignment.TIME_WINDOW = datetime.timedelta(
                                         minutes = parser.getfloat  ('fasttrips','time_window'))
        Assignment.CREATE_SKIMS                  = parser.getboolean('fasttrips','create_skims')
        Assignment.SKIM_START_TIME = datetime.datetime.strptime(
                                                   parser.get       ('fasttrips','skim_start_time'),'%H:%M')
        Assignment.SKIM_END_TIME   = datetime.datetime.strptime(
                                                   parser.get       ('fasttrips','skim_end_time'),'%H:%M')
        Assignment.STOCH_DISPERSION              = parser.getfloat  ('fasttrips','stochastic_dispersion')
        Assignment.STOCH_PATHSET_SIZE            = parser.getint    ('fasttrips','stochastic_pathset_size')
        Assignment.STOCH_MAX_STOP_PROCESS_COUNT  = parser.getint    ('fasttrips','stochastic_max_stop_process_count')
        Assignment.CAPACITY_CONSTRAINT           = parser.getboolean('fasttrips','capacity_constraint')
        Assignment.TRACE_PERSON_IDS         = eval(parser.get       ('fasttrips','trace_person_ids'))
        Assignment.DEBUG_TRACE_ONLY              = parser.getboolean('fasttrips','debug_trace_only')
        Assignment.DEBUG_NUM_TRIPS               = parser.getint    ('fasttrips','debug_num_trips')
        Assignment.PREPEND_ROUTE_ID_TO_TRIP_ID   = parser.getboolean('fasttrips','prepend_route_id_to_trip_id')
        Assignment.NUMBER_OF_PROCESSES           = parser.getint    ('fasttrips','number_of_processes')
        Assignment.BUMP_BUFFER = datetime.timedelta(
                                         minutes = parser.getfloat  ('fasttrips','bump_buffer'))
        Assignment.BUMP_ONE_AT_A_TIME            = parser.getboolean('fasttrips','bump_one_at_a_time')

        # pathfinding
        PathSet.USER_CLASS_FUNCTION              = parser.get     ('pathfinding','user_class_function')
        if PathSet.USER_CLASS_FUNCTION not in PathSet.CONFIGURED_FUNCTIONS:
            msg = "User class function [%s] not defined.  Please check your function file [%s]" % (PathSet.USER_CLASS_FUNCTION, func_file)
            FastTripsLogger.fatal(msg)
            raise ConfigurationError(func_file, msg)

        weights_file = os.path.join(input_demand_dir, PathSet.WEIGHTS_FILE)
        if not os.path.exists(weights_file):
            FastTripsLogger.fatal("No path weights file %s" % weights_file)
            sys.exit(2)

        PathSet.WEIGHTS_DF = pandas.read_fwf(weights_file)
        FastTripsLogger.debug("Weights =\n%s" % str(PathSet.WEIGHTS_DF))
        FastTripsLogger.debug("Weight types = \n%s" % str(PathSet.WEIGHTS_DF.dtypes))

    @staticmethod
    def write_configuration(output_dir):
        """
        Write the configuration parameters to function as a record with the output.
        """
        parser = ConfigParser.SafeConfigParser()
        parser.add_section('fasttrips')
        parser.set('fasttrips','iterations',                    '%d' % Assignment.ITERATION_FLAG)
        parser.set('fasttrips','pathfinding_type',              Assignment.ASSIGNMENT_TYPE)
        parser.set('fasttrips','simulation',                    'True' if Assignment.ASSIGNMENT_TYPE else 'False')
        parser.set('fasttrips','output_passenger_trajectories', 'True' if Assignment.OUTPUT_PASSENGER_TRAJECTORIES else 'False')
        parser.set('fasttrips','time_window',                   '%f' % (Assignment.TIME_WINDOW.total_seconds()/60.0))
        parser.set('fasttrips','create_skims',                  'True' if Assignment.CREATE_SKIMS else 'False')
        parser.set('fasttrips','skim_start_time',               Assignment.SKIM_START_TIME.strftime('%H:%M'))
        parser.set('fasttrips','skim_end_time',                 Assignment.SKIM_END_TIME.strftime('%H:%M'))
        parser.set('fasttrips','stochastic_dispersion',         '%f' % Assignment.STOCH_DISPERSION)
        parser.set('fasttrips','stochastic_max_stop_process_count', '%d' % Assignment.STOCH_MAX_STOP_PROCESS_COUNT)
        parser.set('fasttrips','stochastic_pathset_size',       '%d' % Assignment.STOCH_PATHSET_SIZE)
        parser.set('fasttrips','capacity_constraint',           'True' if Assignment.CAPACITY_CONSTRAINT else 'False')
        parser.set('fasttrips','trace_person_ids',              '%s' % str(Assignment.TRACE_PERSON_IDS))
        parser.set('fasttrips','debug_trace_only',              'True' if Assignment.DEBUG_TRACE_ONLY else 'False')
        parser.set('fasttrips','debug_num_trips',               '%d' % Assignment.DEBUG_NUM_TRIPS)
        parser.set('fasttrips','prepend_route_id_to_trip_id',   'True' if Assignment.PREPEND_ROUTE_ID_TO_TRIP_ID else 'False')
        parser.set('fasttrips','number_of_processes',           '%d' % Assignment.NUMBER_OF_PROCESSES)
        parser.set('fasttrips','bump_buffer',                   '%f' % (Assignment.BUMP_BUFFER.total_seconds()/60.0))
        parser.set('fasttrips','bump_one_at_a_time',            'True' if Assignment.BUMP_ONE_AT_A_TIME else 'False')

        #pathfinding
        parser.add_section('pathfinding')
        parser.set('pathfinding','user_class_function',         '%s' % PathSet.USER_CLASS_FUNCTION)

        output_file = open(os.path.join(output_dir, Assignment.CONFIGURATION_OUTPUT_FILE), 'w')
        parser.write(output_file)
        output_file.close()

    @staticmethod
    def initialize_fasttrips_extension(process_number, output_dir, stop_times_df):
        """
        Initialize the C++ fasttrips extension by passing it the network supply.
        """
        FastTripsLogger.debug("Initializing fasttrips extension for process number %d" % process_number)

        # this may not be set yet if it is iter1
        overcap_col = Trip.SIM_COL_VEH_OVERCAP
        if Assignment.MSA_RESULTS:
            overcap_col = Trip.SIM_COL_VEH_MSA_OVERCAP

        if overcap_col not in list(stop_times_df.columns.values):
            stop_times_df[overcap_col] = 0

        FastTripsLogger.debug("initialize_fasttrips_extension() overcap sum: %d" % stop_times_df[overcap_col].sum())
        FastTripsLogger.debug("initialize_fasttrips_extension() STOPTIMES_COLUMN_DEPARTURE_TIME_MIN len: %d mean: %f" % \
                              (len(stop_times_df), stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN].mean()))

        _fasttrips.initialize_supply(output_dir, process_number,
                                     stop_times_df[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                    Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                                    Trip.STOPTIMES_COLUMN_STOP_ID_NUM]].as_matrix().astype('int32'),
                                     stop_times_df[[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN,
                                                    Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN,
                                                    overcap_col]].as_matrix().astype('float64'))

        _fasttrips.initialize_parameters(Assignment.TIME_WINDOW.total_seconds()/60.0,
                                         Assignment.BUMP_BUFFER.total_seconds()/60.0,
                                         Assignment.STOCH_PATHSET_SIZE,
                                         Assignment.STOCH_DISPERSION,
                                         Assignment.STOCH_MAX_STOP_PROCESS_COUNT)

    @staticmethod
    def set_fasttrips_bump_wait(bump_wait_df):
        """
        Sends the bump wait information to the fasttrips extension
        """
        # send a clear message?
        if type(bump_wait_df)==type(None): return

        if len(bump_wait_df) == 0: return

        _fasttrips.set_bump_wait(bump_wait_df[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                               Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                               Trip.STOPTIMES_COLUMN_STOP_ID_NUM]].as_matrix().astype('int32'),
                                 bump_wait_df[Passenger.PF_COL_PAX_A_TIME_MIN].values.astype('float64'))
    @staticmethod
    def write_vehicle_trips(output_dir, iteration, veh_trips_df):
        """
        """
        columns = ["iteration",                             # we'll add
                   Trip.TRIPS_COLUMN_DIRECTION_ID,
                   Trip.TRIPS_COLUMN_SERVICE_ID,
                   Trip.TRIPS_COLUMN_ROUTE_ID,
                   Trip.STOPTIMES_COLUMN_TRIP_ID,
                   Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                   Trip.STOPTIMES_COLUMN_STOP_ID,
                   Trip.STOPTIMES_COLUMN_ARRIVAL_TIME,
                   Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN,
                   Trip.STOPTIMES_COLUMN_DEPARTURE_TIME,
                   Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN,
                   Trip.STOPTIMES_COLUMN_TRAVEL_TIME_SEC,
                   Trip.STOPTIMES_COLUMN_DWELL_TIME_SEC,
                   Trip.VEHICLES_COLUMN_TOTAL_CAPACITY,
                   Trip.SIM_COL_VEH_BOARDS,
                   Trip.SIM_COL_VEH_ALIGHTS,
                   Trip.SIM_COL_VEH_ONBOARD,
                   Trip.SIM_COL_VEH_STANDEES,
                   Trip.SIM_COL_VEH_FRICTION,
                   Trip.SIM_COL_VEH_OVERCAP,
                   Trip.SIM_COL_VEH_MSA_BOARDS,
                   Trip.SIM_COL_VEH_MSA_ALIGHTS,
                   Trip.SIM_COL_VEH_MSA_ONBOARD,
                   Trip.SIM_COL_VEH_MSA_STANDEES,
                   Trip.SIM_COL_VEH_MSA_FRICTION,
                   Trip.SIM_COL_VEH_MSA_OVERCAP]

        # these may not be in there since they're optional
        for optional_col in [Trip.TRIPS_COLUMN_DIRECTION_ID,
                             Trip.VEHICLES_COLUMN_TOTAL_CAPACITY]:
            if optional_col not in veh_trips_df.columns.values:
                columns.remove(optional_col)

        veh_trips_df["iteration"] = iteration
        Util.write_dataframe(veh_trips_df[columns], "veh_trips_df", os.path.join(output_dir, "veh_trips.csv"), append=(iteration>0))
        veh_trips_df.drop("iteration", axis=1, inplace=True)

    @staticmethod
    def merge_pathsets(pathfind_trip_list_df, pathset_paths_df, pathset_links_df, new_pathset_paths_df, new_pathset_links_df):
        """
        Merge the given new pathset paths and links into the existing
        """
        FastTripsLogger.debug("merge_pathsets():     pathset_paths_df len=%d head=\n%s" % (len(    pathset_paths_df),     pathset_paths_df.head().to_string()))
        FastTripsLogger.debug("merge_pathsets(): new_pathset_paths_df len=%d head=\n%s" % (len(new_pathset_paths_df), new_pathset_paths_df.head().to_string()))
        FastTripsLogger.debug("merge_pathsets() dtypes=\n%s" % str(pathset_paths_df.dtypes))
        FastTripsLogger.debug("merge_pathsets():     pathset_links_df len=%d head=\n%s" % (len(    pathset_links_df),     pathset_links_df.head().to_string()))
        FastTripsLogger.debug("merge_pathsets(): new_pathset_links_df len=%d head=\n%s" % (len(new_pathset_links_df), new_pathset_links_df.head().to_string()))
        FastTripsLogger.debug("merge_pathsets() dtypes=\n%s" % str(pathset_links_df.dtypes))

        # TODO: This might be inefficient...

        # filter out the new pathset person trips from pathset_paths_df
        pathfind_trip_list_df["new"] = 1
        pathset_paths_df = pandas.merge(left =pathset_paths_df,
                                        right=pathfind_trip_list_df[[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,"new"]],
                                        how  ="left")
        pathset_paths_df = pathset_paths_df.loc[pandas.isnull(pathset_paths_df["new"])]
        pathset_paths_df.drop(["new"], axis=1, inplace=True)
        FastTripsLogger.debug("Filtered to %d pathset_paths_df rows" % len(pathset_paths_df))
        # TODO: error prone, make this cleaner with where it's initialized elsewhere
        new_pathset_paths_df[Assignment.SIM_COL_PAX_CHOSEN ] = Assignment.CHOSEN_NOT_CHOSEN_YET
        new_pathset_paths_df[Assignment.SIM_COL_MISSED_XFER] = 0
        # append
        pathset_paths_df = pandas.concat([pathset_paths_df, new_pathset_paths_df], axis=0)
        FastTripsLogger.debug("Concatenated so pathset_paths_df has %d rows" % len(pathset_paths_df))

        # filter out the new pathset person trips from pathset_links_df
        pathset_links_df = pandas.merge(left =pathset_links_df,
                                        right=pathfind_trip_list_df[[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,"new"]],
                                        how  ="left")
        pathset_links_df = pathset_links_df.loc[pandas.isnull(pathset_links_df["new"])]
        pathset_links_df.drop(["new"], axis=1, inplace=True)
        FastTripsLogger.debug("Filtered to %d pathset_links_df rows" % len(pathset_links_df))
        # append
        pathset_links_df = pandas.concat([pathset_links_df, new_pathset_links_df], axis=0)
        FastTripsLogger.debug("Concatenated so pathset_links_df has %d rows" % len(pathset_links_df))

        FastTripsLogger.debug("merge_pathsets():     pathset_paths_df len=%d head=\n%s\ntail=\n%s" % (len(pathset_paths_df), pathset_paths_df.head().to_string(),pathset_paths_df.tail().to_string()))
        FastTripsLogger.debug("merge_pathsets():     pathset_links_df len=%d head=\n%s\ntail=\n%s" % (len(pathset_links_df), pathset_links_df.head().to_string(),pathset_links_df.tail().to_string()))

        # done with this
        pathfind_trip_list_df.drop(["new"], axis=1, inplace=True)
        return (pathset_paths_df, pathset_links_df)

    @staticmethod
    def assign_paths(output_dir, FT):
        """
        Finds the paths for the passengers.
        """
        Assignment.write_configuration(output_dir)

        # write the initial load profile, iteration 0
        veh_trips_df     = FT.trips.get_full_trips()
        pathset_paths_df = None
        pathset_links_df = None

        # write 0-iter vehicle trips
        Assignment.write_vehicle_trips(output_dir, 0, veh_trips_df)

        for iteration in range(1,Assignment.ITERATION_FLAG+1):
            FastTripsLogger.info("***************************** ITERATION %d **************************************" % iteration)

            if Assignment.ASSIGNMENT_TYPE == Assignment.ASSIGNMENT_TYPE_SIM_ONLY and \
               os.path.exists(os.path.join(output_dir, Passenger.PASSENGERS_CSV % iteration)):
                FastTripsLogger.info("Simulation only")
                (num_paths_found, pathset_paths_df, pathset_links_df) = FT.passengers.read_assignment_results(output_dir, iteration)

            else:
                num_paths_found = Assignment.generate_pathsets(FT, pathset_paths_df, veh_trips_df, output_dir, iteration)
                (new_pathset_paths_df, new_pathset_links_df) = FT.passengers.setup_passenger_pathsets(iteration, FT.stops.stop_id_df, FT.trips.trip_id_df, FT.trips.trips_df, FT.routes.modes_df)

                # write performance info right away in case we crash, quit, etc
                FT.performance.write(output_dir, iteration)

            if Assignment.PATHFINDING_EVERYONE:
                pathset_paths_df = new_pathset_paths_df
                pathset_links_df = new_pathset_links_df
            else:
                (pathset_paths_df, pathset_links_df) = Assignment.merge_pathsets(FT.passengers.pathfind_trip_list_df, pathset_paths_df, pathset_links_df, new_pathset_paths_df, new_pathset_links_df)

            if Assignment.SIMULATION_FLAG == True:
                FastTripsLogger.info("****************************** SIMULATING *****************************")
                (num_passengers_arrived, pathset_paths_df, pathset_links_df, veh_trips_df) = \
                    Assignment.simulate(FT, output_dir, iteration, pathset_paths_df, pathset_links_df, veh_trips_df)

            # Set new schedule
            FT.trips.stop_times_df = veh_trips_df

            Assignment.write_vehicle_trips(output_dir, iteration, veh_trips_df)

            if Assignment.OUTPUT_PASSENGER_TRAJECTORIES:
                PathSet.write_path_times(Passenger.get_chosen_links(pathset_links_df), output_dir)

            # capacity gap stuff
            num_bumped_passengers = num_paths_found - num_passengers_arrived
            if num_paths_found > 0:
                capacity_gap = 100.0*num_bumped_passengers/num_paths_found
            else:
                capacity_gap = 100

            FastTripsLogger.info("")
            FastTripsLogger.info("  TOTAL ASSIGNED PASSENGERS: %10d" % num_paths_found)
            FastTripsLogger.info("  ARRIVED PASSENGERS:        %10d" % num_passengers_arrived)
            FastTripsLogger.info("  MISSED PASSENGERS:         %10d" % num_bumped_passengers)
            FastTripsLogger.info("  CAPACITY GAP:              %10.5f" % capacity_gap)

            if False and capacity_gap < 0.001:
                break

        # end for loop

    @staticmethod
    def filter_trip_list_to_not_arrived(trip_list_df, pathset_paths_df):
        """
        Filter the given trip list to only those that have not arrived according to *pathset_paths_df*.
        """
        FastTripsLogger.debug("filter_trip_list_to_not_arrived(): trip_list_df len=%d head()=\n%s"  % (len(trip_list_df), trip_list_df.head().to_string()))
        FastTripsLogger.debug("filter_trip_list_to_not_arrived(): pathset_paths_df len=%d head()=\n%s"  % (len(pathset_paths_df), pathset_paths_df.head().to_string()))

        # filter to only the chosen paths
        chosen_paths_df = pathset_paths_df.loc[pathset_paths_df[Assignment.SIM_COL_PAX_CHOSEN] >= 0, [Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM, Assignment.SIM_COL_PAX_CHOSEN]]

        # add chosen index
        trip_list_df_to_return = pandas.merge(left  =trip_list_df,
                                              right =chosen_paths_df,
                                              how   ="left")
        # use it to filter to null chosen
        trip_list_df_to_return = trip_list_df_to_return.loc[pandas.isnull(trip_list_df_to_return[Assignment.SIM_COL_PAX_CHOSEN])]
        # remove chosen column
        trip_list_df_to_return.drop([Assignment.SIM_COL_PAX_CHOSEN], axis=1, inplace=True)

        FastTripsLogger.debug("filter_trip_list_to_not_arrived(): trip_list_df_to_return len=%d head()=\n%s"  % (len(trip_list_df_to_return), trip_list_df_to_return.head().to_string()))
        return trip_list_df_to_return

    @staticmethod
    def generate_pathsets(FT, pathset_paths_df, veh_trips_df, output_dir, iteration):
        """
        Figures out which person trips for whom to generate_pathsets, stored in :py:attr:`Passenger.pathfind_trip_list_df`

        Generates paths sets for those person trips using deterministic trip-based shortest path (TBSP) or
        stochastic trip-based hyperpath (TBHP).

        Returns the number of pathsets found.
        """
        FastTripsLogger.info("**************************** GENERATING PATHS ****************************")
        start_time          = datetime.datetime.now()
        process_dict        = {}  # workernum -> {"process":process, "alive":alive bool, "done":done bool, "working_on":(person_id, trip_list_num)}
        todo_queue          = None
        done_queue          = None

        # We only need to do this once
        if iteration == 1:
            if Assignment.DEBUG_TRACE_ONLY:
                FT.passengers.trip_list_df = FT.passengers.trip_list_df.loc[FT.passengers.trip_list_df[Passenger.TRIP_LIST_COLUMN_PERSON_ID].isin(Assignment.TRACE_PERSON_IDS)]
            else:
                if Assignment.DEBUG_NUM_TRIPS > 0 and len(FT.passengers.trip_list_df) > Assignment.DEBUG_NUM_TRIPS:
                    FastTripsLogger.info("Truncating trip list to %d trips" % Assignment.DEBUG_NUM_TRIPS)
                    FT.passengers.trip_list_df = FT.passengers.trip_list_df.iloc[:Assignment.DEBUG_NUM_TRIPS]

        # these are the trips for which we'll find paths
        FT.passengers.pathfind_trip_list_df = FT.passengers.trip_list_df

        # test: on even iterations, try to find paths for the failed passengers, e.g. passengers with chosen==0
        if iteration % 2 == 0:
            Assignment.PATHFINDING_EVERYONE = False
            FastTripsLogger.info("Finding paths for trips for those that haven't arrived yet")
            FT.passengers.pathfind_trip_list_df = Assignment.filter_trip_list_to_not_arrived(FT.passengers.trip_list_df, pathset_paths_df)
        else:
            PATHFINDING_EVERYONE = True
            # we're starting over with empty vehicles
            Trip.reset_onboard(veh_trips_df)

        est_paths_to_find   = len(FT.passengers.pathfind_trip_list_df)
        FastTripsLogger.info("Finding pathsets for %d trips" % est_paths_to_find)
        if est_paths_to_find == 0:
            return 0

        info_freq           = pow(10, int(math.log(est_paths_to_find+1,10)-2))
        if info_freq < 1: info_freq = 1
        # info_freq = 1 # DEBUG CRASH

        num_processes       = Assignment.NUMBER_OF_PROCESSES
        if  Assignment.NUMBER_OF_PROCESSES < 1:
            num_processes   = multiprocessing.cpu_count()
        # it's not worth it unless each process does 3
        if num_processes > est_paths_to_find*3:
            num_processes = int(est_paths_to_find/3)

        # this is probalby time consuming... put in a try block
        try:
            # Setup multiprocessing processes
            if num_processes > 1:
                todo_queue      = multiprocessing.Queue()
                done_queue      = multiprocessing.Queue()
                for process_idx in range(1, 1+num_processes):
                    FastTripsLogger.info("Starting worker process %2d" % process_idx)
                    process_dict[process_idx] = {
                        "process":multiprocessing.Process(target=find_trip_based_paths_process_worker,
                            args=(iteration, process_idx, FT.input_network_dir, FT.input_demand_dir,
                                  FT.output_dir, todo_queue, done_queue,
                                  Assignment.ASSIGNMENT_TYPE==Assignment.ASSIGNMENT_TYPE_STO_ASGN,
                                  Assignment.bump_wait_df, veh_trips_df)),
                        "alive":True,
                        "done":False
                    }
                    process_dict[process_idx]["process"].start()
            else:
                Assignment.initialize_fasttrips_extension(0, output_dir, veh_trips_df)

            # process tasks or send tasks to workers for processing
            num_paths_found_prev  = 0
            num_paths_found_now   = 0
            path_cols             = list(FT.passengers.pathfind_trip_list_df.columns.values)
            for path_tuple in FT.passengers.pathfind_trip_list_df.itertuples(index=False):
                path_dict         = dict(zip(path_cols, path_tuple))
                trip_list_id      = path_dict[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM]
                person_id         = path_dict[Passenger.TRIP_LIST_COLUMN_PERSON_ID]
                trace_person      = person_id in Assignment.TRACE_PERSON_IDS

                if Assignment.DEBUG_TRACE_ONLY and not trace_person: continue

                # first iteration -- create path objects
                if iteration==1:
                    trip_pathset = PathSet(path_dict)
                    FT.passengers.add_pathset(trip_list_id, trip_pathset)
                else:
                    trip_pathset = FT.passengers.get_pathset(trip_list_id)

                if not trip_pathset.goes_somewhere(): continue

                # find pathsets for everyone -- dwell times have changed
                # if iteration > 1 and trip_list_id not in Assignment.bumped_trip_list_nums:
                #    num_paths_found_prev += 1
                #    continue

                if num_processes > 1:
                    todo_queue.put( trip_pathset )
                else:
                    if trace_person:
                        FastTripsLogger.debug("Tracing assignment of person_id %s" % str(person_id))

                    # do the work
                    (pathdict, perf_dict) = \
                        Assignment.find_trip_based_pathset(iteration, trip_pathset,
                                                        Assignment.ASSIGNMENT_TYPE==Assignment.ASSIGNMENT_TYPE_STO_ASGN,
                                                        trace=trace_person)
                    trip_pathset.pathdict = pathdict
                    FT.performance.add_info(iteration, trip_list_id, perf_dict)

                    if trip_pathset.path_found():
                        num_paths_found_now += 1

                    if num_paths_found_now % info_freq == 0:
                        time_elapsed = datetime.datetime.now() - start_time
                        FastTripsLogger.info(" %6d / %6d passenger paths found.  Time elapsed: %2dh:%2dm:%2ds" % (
                                             num_paths_found_now, est_paths_to_find,
                                             int( time_elapsed.total_seconds() / 3600),
                                             int( (time_elapsed.total_seconds() % 3600) / 60),
                                             time_elapsed.total_seconds() % 60))

            # multiprocessing follow-up
            if num_processes > 1:
                # we're done, let each process know
                for process_idx in process_dict.keys():
                    todo_queue.put('DONE')

                # get results
                done_procs = 0  # where done means not alive
                while done_procs < len(process_dict):

                    try:
                        result     = done_queue.get(True, 30)
                        worker_num = result[0]

                        # FastTripsLogger.debug("Received %s" % str(result))
                        if result[1] == "DONE":
                            FastTripsLogger.debug("Received done from process %d" % worker_num)
                            process_dict[worker_num]["done"] = True
                        elif result[1] == "STARTING":
                            process_dict[worker_num]["working_on"] = (result[2],result[3])
                        elif result[1] == "COMPLETED":
                            trip_list_id    = result[2]
                            pathset         = FT.passengers.get_pathset(trip_list_id)
                            pathset.pathdict= result[3]
                            perf_dict       = result[4]

                            FT.performance.add_info(iteration, trip_list_id, perf_dict)

                            if pathset.path_found():
                                num_paths_found_now += 1

                            if num_paths_found_now % info_freq == 0:
                                time_elapsed = datetime.datetime.now() - start_time
                                FastTripsLogger.info(" %6d / %6d passenger paths found.  Time elapsed: %2dh:%2dm:%2ds" % (
                                                     num_paths_found_now, est_paths_to_find,
                                                     int( time_elapsed.total_seconds() / 3600),
                                                     int( (time_elapsed.total_seconds() % 3600) / 60),
                                                     time_elapsed.total_seconds() % 60))

                            del process_dict[worker_num]["working_on"]
                        else:
                            print "Unexpected done queue contents: " + str(result)

                    except :
                        # FastTripsLogger.debug("Error: %s" % str(sys.exc_info()))
                        pass

                    # check if any processes are not alive
                    for process_idx in process_dict.keys():
                        if process_dict[process_idx]["alive"] and not process_dict[process_idx]["process"].is_alive():
                            FastTripsLogger.debug("Process %d is not alive" % process_idx)
                            process_dict[process_idx]["alive"] = False
                            done_procs += 1

                # join up my processes
                for process_idx in process_dict.keys():
                    process_dict[process_idx]["process"].join()

                # check if any processes crashed
                for process_idx in process_dict.keys():
                    if not process_dict[process_idx]["done"]:
                        if "working_on" in process_dict[process_idx]:
                            FastTripsLogger.info("Process %d appears to have crashed; it was working on %s" % \
                                                 (process_idx, str(process_dict[process_idx]["working_on"])))
                        else:
                            FastTripsLogger.info("Process %d appears to have crashed; see ft_debug_worker%02d.log" % (process_idx, process_idx))

        except (KeyboardInterrupt, SystemExit):
            exc_type, exc_value, exc_tb = sys.exc_info()
            FastTripsLogger.error("Exception caught: %s" % str(exc_type))
            error_lines = traceback.format_exception(exc_type, exc_value, exc_tb)
            for e in error_lines: FastTripsLogger.error(e)
            FastTripsLogger.error("Terminating processes")
            # terminating my processes
            for proc in process_dict:
                proc.terminate()
            sys.exit(2)
        except:
            # some other error
            exc_type, exc_value, exc_tb = sys.exc_info()
            error_lines = traceback.format_exception(exc_type, exc_value, exc_tb)
            for e in error_lines: FastTripsLogger.error(e)
            sys.exit(2)

        time_elapsed = datetime.datetime.now() - start_time
        FastTripsLogger.info("Finished finding %6d passenger paths.  Time elapsed: %2dh:%2dm:%2ds" % (
                                 num_paths_found_now,
                                 int( time_elapsed.total_seconds() / 3600),
                                 int( (time_elapsed.total_seconds() % 3600) / 60),
                                 time_elapsed.total_seconds() % 60))

        return num_paths_found_now + num_paths_found_prev


    @staticmethod
    def find_trip_based_pathset(iteration, pathset, hyperpath, trace):
        """
        Perform trip-based path set search.

        Will do so either backwards (destination to origin) if :py:attr:`PathSet.direction` is :py:attr:`PathSet.DIR_OUTBOUND`
        or forwards (origin to destination) if :py:attr:`PathSet.direction` is :py:attr:`PathSet.DIR_INBOUND`.

        Returns (pathdict,
                 performance_dict)

        Where pathdict maps {pathnum:{PATH_KEY_COST:cost, PATH_KEY_PROBABILITY:probability, PATH_KEY_STATES:[state list]}}

        Where performance_dict includes:
                 number of label iterations,
                 max number of times a stop was processed,
                 seconds spent in labeling,
                 seconds spend in enumeration

        :param iteration: The pathfinding iteration we're on
        :type  iteration: int
        :param pathset:   the path to fill in
        :type  pathset:   a :py:class:`PathSet` instance
        :param hyperpath: pass True to use a stochastic hyperpath-finding algorithm, otherwise a deterministic shortest path
                          search algorithm will be use.
        :type  hyperpath: boolean
        :param trace:     pass True if this path should be traced to the debug log
        :type  trace:     boolean

        """
        # FastTripsLogger.debug("C++ extension start")
        # send it to the C++ extension
        (ret_ints, ret_doubles, path_costs,
         label_iterations, max_label_process_count,
         seconds_labeling, seconds_enumerating) = \
            _fasttrips.find_pathset(iteration, pathset.person_id_num, pathset.trip_list_id_num, hyperpath,
                                 pathset.user_class, pathset.access_mode, pathset.transit_mode, pathset.egress_mode,
                                 pathset.o_taz_num, pathset.d_taz_num,
                                 1 if pathset.outbound() else 0, float(pathset.pref_time_min),
                                 1 if trace else 0)
        # FastTripsLogger.debug("C++ extension complete")
        # FastTripsLogger.debug("Finished finding path for person %s trip list id num %d" % (pathset.person_id, pathset.trip_list_id_num))
        pathdict = {}
        row_num  = 0

        for path_num in range(path_costs.shape[0]):

            pathdict[path_num] = {}
            pathdict[path_num][PathSet.PATH_KEY_COST       ] = path_costs[path_num, 0]
            pathdict[path_num][PathSet.PATH_KEY_PROBABILITY] = path_costs[path_num, 1]
            # List of (stop_id, stop_state)
            pathdict[path_num][PathSet.PATH_KEY_STATES     ] = []

            # print "path_num %d" % path_num

            # while we have unprocessed rows and the row is still relevant for this path_num
            while (row_num < ret_ints.shape[0]) and (ret_ints[row_num, 0] == path_num):
                # print row_num

                mode = ret_ints[row_num,2]
                # todo
                if mode == -100:
                    mode = PathSet.STATE_MODE_ACCESS
                elif mode == -101:
                    mode = PathSet.STATE_MODE_EGRESS
                elif mode == -102:
                    mode = PathSet.STATE_MODE_TRANSFER
                elif mode == -103:
                    mode = Passenger.MODE_GENERIC_TRANSIT_NUM

                if hyperpath:
                    pathdict[path_num][PathSet.PATH_KEY_STATES].append( (ret_ints[row_num, 1], [
                        ret_doubles[row_num,0],                                                          # label,
                        Util.SIMULATION_DAY_START + datetime.timedelta(minutes=ret_doubles[row_num,1]),  # departure/arrival time
                        mode,                                                                            # departure/arrival mode
                        ret_ints[row_num,3],                                                             # trip id
                        ret_ints[row_num,4],                                                             # successor/predecessor
                        ret_ints[row_num,5],                                                             # sequence
                        ret_ints[row_num,6],                                                             # sequence succ/pred
                        datetime.timedelta(minutes=ret_doubles[row_num,2]),                              # link time
                        ret_doubles[row_num,3],                                                          # cost
                        Util.SIMULATION_DAY_START + datetime.timedelta(minutes=ret_doubles[row_num,4])   # arrival/departure time
                    ] ) )
                else:
                    pathdict[path_num][PathSet.PATH_KEY_STATES].append( (ret_ints[row_num, 1], [
                        datetime.timedelta(minutes=ret_doubles[row_num,0]),                              # label,
                        Util.SIMULATION_DAY_START + datetime.timedelta(minutes=ret_doubles[row_num,1]),  # departure/arrival time
                        mode,                                                                            # departure/arrival mode
                        ret_ints[row_num,3],                                                             # trip id
                        ret_ints[row_num,4],                                                             # successor/predecessor
                        ret_ints[row_num,5],                                                             # sequence
                        ret_ints[row_num,6],                                                             # sequence succ/pred
                        datetime.timedelta(minutes=ret_doubles[row_num,2]),                              # link time
                        datetime.timedelta(minutes=ret_doubles[row_num,3]),                              # cost
                        Util.SIMULATION_DAY_START + datetime.timedelta(minutes=ret_doubles[row_num,4])   # arrival/departure time
                    ] ) )
                row_num += 1

        perf_dict = { \
            Performance.PERFORMANCE_COLUMN_LABEL_ITERATIONS      : label_iterations,
            Performance.PERFORMANCE_COLUMN_MAX_STOP_PROCESS_COUNT: max_label_process_count,
            Performance.PERFORMANCE_COLUMN_TIME_LABELING_MS      : seconds_labeling,
            Performance.PERFORMANCE_COLUMN_TIME_ENUMERATING_MS   : seconds_enumerating,
            Performance.PERFORMANCE_COLUMN_TRACED                : trace,
        }
        return (pathdict, perf_dict)

    @staticmethod
    def find_passenger_vehicle_times(pathset_links_df, veh_trips_df):
        """
        Given a dataframe of passenger links and a dataframe of vehicle trip links, adds two new columns to the passenger links for board and alight time.

        - Takes the trip links of pathset_links_df (columns: person_id, trip_list_id_num, pathnum, linkmode, trip_id_num, A_id_num, B_id_num, A_seq, B_seq, pf_A_time, pf_B_time, pf_linktime, A_id, B_id, trip_id)
        - Joins with vehicle trips on trip id num, A_id, A_seq to add:
          * board time   (Assignment.SIM_COL_PAX_BOARD_TIME)
          * overcap      (Trip.SIM_COL_VEH_OVERCAP)
          * overcap_frac (Assignment.SIM_COL_PAX_OVERCAP_FRAC)
        - Joins with vehicle trips on trip id num, B_id, B_seq to add:
          * alight time (Assignment.SIM_COL_PAX_ALIGHT_TIME)

        Returns the same dataframe but with four additional columns (replacing them if they're already there).
        """
        FastTripsLogger.debug("find_passenger_vehicle_times(): input pathset_links_df len=%d\n%s" % \
                              (len(pathset_links_df), pathset_links_df.head(20).to_string(formatters={Assignment.SIM_COL_PAX_LINK_TIME:Util.timedelta_formatter})))

        if Assignment.SIM_COL_PAX_BOARD_TIME in list(pathset_links_df.columns.values):
            pathset_links_df.drop([Assignment.SIM_COL_PAX_BOARD_TIME,
                                   Assignment.SIM_COL_PAX_ALIGHT_TIME,
                                   Trip.SIM_COL_VEH_OVERCAP], axis=1, inplace=True)
        if Assignment.SIM_COL_PAX_OVERCAP_FRAC in list(pathset_links_df.columns.values):
            pathset_links_df.drop([Assignment.SIM_COL_PAX_OVERCAP_FRAC], axis=1, inplace=True)

        # FastTripsLogger.debug("pathset_links_df:\n%s\n" % pathset_links_df.head().to_string())
        FastTripsLogger.debug("veh_trips_df:\n%s\n" % veh_trips_df.head().to_string())

        veh_trip_cols = [Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                         Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                         Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                         Trip.STOPTIMES_COLUMN_DEPARTURE_TIME,
                         Trip.SIM_COL_VEH_OVERCAP,                # TODO: what about msa_overcap?
                         Assignment.SIM_COL_PAX_OVERCAP_FRAC]

        # this one may not be here -- it's only present during capacity stuff
        if Assignment.SIM_COL_PAX_OVERCAP_FRAC not in list(veh_trips_df.columns.values):
            veh_trip_cols.remove(Assignment.SIM_COL_PAX_OVERCAP_FRAC)

        pathset_links_df = pandas.merge(
            left    =pathset_links_df,
            right   =veh_trips_df[veh_trip_cols],
            left_on =[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,'A_id_num','A_seq'],
            right_on=[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                      Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                      Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],
            how     ='left')
        pathset_links_df = pandas.merge(
            left    =pathset_links_df,
            right   =veh_trips_df[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                   Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                   Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                   Trip.STOPTIMES_COLUMN_ARRIVAL_TIME]],
            left_on =[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,'B_id_num','B_seq'],
            right_on=[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                      Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                      Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],
            how     ='left',
            suffixes=("_A","_B"))

        pathset_links_df.rename(columns={
            Trip.STOPTIMES_COLUMN_DEPARTURE_TIME:Assignment.SIM_COL_PAX_BOARD_TIME,   # transit vehicle depart time (at A) = board time for pax
            Trip.STOPTIMES_COLUMN_ARRIVAL_TIME  :Assignment.SIM_COL_PAX_ALIGHT_TIME,  # transit vehicle arrive time (at B) = alight time for pax
        }, inplace=True)

        # redundant with A_id, B_id, A_seq, B_seq, B_time is just alight time
        pathset_links_df.drop(['%s_A' % Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                               '%s_B' % Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                               '%s_A' % Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                               '%s_B' % Trip.STOPTIMES_COLUMN_STOP_SEQUENCE], axis=1, inplace=True)

        FastTripsLogger.debug("find_passenger_vehicle_times(): output pathset_links_df\n%s" % \
                              pathset_links_df.head(20).to_string(formatters={Assignment.SIM_COL_PAX_LINK_TIME:Util.timedelta_formatter}))

        return pathset_links_df

    @staticmethod
    def put_passengers_on_vehicles(iteration, bump_iter, pathset_paths_df, pathset_links_df, veh_trips_df):
        """
        Puts the chosen passenger trips specified in pathset_paths_df/pathset_links_df onto the transit vehicle trips specified by veh_trip_df.

        Returns veh_trips_df but with updated columns
          - :py:attr:`Trip.SIM_COL_VEH_BOARDS`
          - :py:attr:`Trip.SIM_COL_VEH_ALIGHTS`
          - :py:attr:`Trip.SIM_COL_VEH_ONBOARD`
        """
        # drop these -- we'll set them
        if Trip.SIM_COL_VEH_BOARDS in list(veh_trips_df.columns.values):
            veh_trips_df.drop([Trip.SIM_COL_VEH_BOARDS,
                               Trip.SIM_COL_VEH_ALIGHTS,
                               Trip.SIM_COL_VEH_ONBOARD], axis=1, inplace=True)

        veh_trips_df_len = len(veh_trips_df)

        passengers_df = Passenger.get_chosen_links(pathset_links_df)

        # Group to boards by counting trip_list_id_nums for a (trip_id, A_id as stop_id)
        passenger_trips_boards = passengers_df.loc[passengers_df[Assignment.SIM_COL_PAX_BUMP_ITER]==-1,  # unbumped passengers
                                                   [Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
                                                    Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,'A_id_num','A_seq']].groupby([Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,'A_id_num','A_seq']).count()
        passenger_trips_boards.index.names = [Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                              Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                              Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]

        # And alights by counting path_ids for a (trip_id, B_id as stop_id)
        passenger_trips_alights = passengers_df.loc[passengers_df[Assignment.SIM_COL_PAX_BUMP_ITER]==-1,
                                                    [Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
                                                     Trip.TRIPS_COLUMN_TRIP_ID_NUM,'B_id_num','B_seq']].groupby([Trip.TRIPS_COLUMN_TRIP_ID_NUM,'B_id_num','B_seq']).count()
        passenger_trips_alights.index.names = [Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                               Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                               Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]

        # Join them to the transit vehicle trips so we can put people on vehicles (boards)
        veh_loaded_df = pandas.merge(left        = veh_trips_df,
                                     right       = passenger_trips_boards,
                                     left_on     = [Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                    Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                                    Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],
                                     right_index = True,
                                     how         = 'left')
        veh_loaded_df.rename(columns={Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM:Trip.SIM_COL_VEH_BOARDS}, inplace=True)


        # Join for alights
        veh_loaded_df = pandas.merge(left        = veh_loaded_df,
                                     right       = passenger_trips_alights,
                                    left_on      = [Trip.TRIPS_COLUMN_TRIP_ID_NUM,
                                                    Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                                    Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],
                                    right_index  = True,
                                    how          ='left')
        veh_loaded_df.rename(columns={Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM:Trip.SIM_COL_VEH_ALIGHTS}, inplace=True)
        veh_loaded_df.fillna(value=0, inplace=True)
        assert(len(veh_loaded_df)==veh_trips_df_len)

        # these are ints, not floats
        veh_loaded_df[[Trip.SIM_COL_VEH_BOARDS, Trip.SIM_COL_VEH_ALIGHTS]] = \
            veh_loaded_df[[Trip.SIM_COL_VEH_BOARDS, Trip.SIM_COL_VEH_ALIGHTS]].astype(int)

        # MSA the boards and alights
        # TODO figure out how this works with the multiple levels of iterations
        if bump_iter==0:
            msa_lambda = 1.0/iteration
            veh_loaded_df[Trip.SIM_COL_VEH_MSA_BOARDS ] = msa_lambda*veh_loaded_df[Trip.SIM_COL_VEH_BOARDS ] + (1.0-msa_lambda)*veh_loaded_df[Trip.SIM_COL_VEH_MSA_BOARDS ]
            veh_loaded_df[Trip.SIM_COL_VEH_MSA_ALIGHTS] = msa_lambda*veh_loaded_df[Trip.SIM_COL_VEH_ALIGHTS] + (1.0-msa_lambda)*veh_loaded_df[Trip.SIM_COL_VEH_MSA_ALIGHTS]

        veh_loaded_df.set_index([Trip.TRIPS_COLUMN_TRIP_ID_NUM,Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],inplace=True)
        veh_loaded_df[Trip.SIM_COL_VEH_ONBOARD    ] = veh_loaded_df[Trip.SIM_COL_VEH_BOARDS    ] - veh_loaded_df[Trip.SIM_COL_VEH_ALIGHTS    ]
        veh_loaded_df[Trip.SIM_COL_VEH_MSA_ONBOARD] = veh_loaded_df[Trip.SIM_COL_VEH_MSA_BOARDS] - veh_loaded_df[Trip.SIM_COL_VEH_MSA_ALIGHTS]

        # on board is the cumulative sum of boards - alights
        trips_cumsum = veh_loaded_df[[Trip.SIM_COL_VEH_ONBOARD, Trip.SIM_COL_VEH_MSA_ONBOARD]].groupby(level=[0]).cumsum()
        veh_loaded_df.drop([Trip.SIM_COL_VEH_ONBOARD, Trip.SIM_COL_VEH_MSA_ONBOARD], axis=1, inplace=True) # replace with cumsum
        veh_loaded_df = pandas.merge(left        = veh_loaded_df,
                                     right       = trips_cumsum,
                                     left_index  = True,
                                     right_index = True,
                                     how         = 'left')

        assert(len(veh_loaded_df)==veh_trips_df_len)
        # print veh_trips_df.loc[5123368]
        veh_loaded_df.reset_index(inplace=True)

        FastTripsLogger.debug("veh_loaded_df with onboard>0: (showing head)\n" + \
                              veh_loaded_df.loc[veh_loaded_df[Trip.SIM_COL_VEH_ONBOARD]>0].head().to_string(formatters=\
               {Trip.STOPTIMES_COLUMN_ARRIVAL_TIME   :Util.datetime64_formatter,
                Trip.STOPTIMES_COLUMN_DEPARTURE_TIME :Util.datetime64_formatter}))

        return veh_loaded_df

    @staticmethod
    def flag_missed_transfers(pathset_paths_df, pathset_links_df):
        """
        Given passenger pathset links with the vehicle board_time and alight_time attached to trip links,
        this method will add columns to determine if there are missed transfers.

        This works on all paths in the pathset rather than just the chosen paths because then we can choose
        a path without missed transfers.

        The following columns are used in pathset_links_df:
        * Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
        * Passenger.PF_COL_PATH_NUM,
        * Passenger.PF_COL_LINK_NUM,
        * Assignment.SIM_COL_PAX_BOARD_TIME
        * Assignment.SIM_COL_PAX_ALIGHT_TIME
        * Passenger.PF_COL_PAX_B_TIME

        In particular, the following columns are added (or replaced if they're already there) to pathset_links_df:

        1) alight_delay_min   delay in alight_time from original pathfinding understanding of alight time
        2) new_A_time         new A_time given the trip board/alight times for the trip links
        3) new_B_time         new B_time given the trip board/alight times for the trip links
        4) new_linktime       new linktime from new_B_time-new_A_time
        5) new_waittime       new waittime given the trip board/alight times for the trip links
        6) missed_xfer        is this link a missed xfer

        And the following column is added to pathset_paths_df:
        *  missed_xfer        1 if a transfer is missed

        """
        # Drop these, we'll set them again
        if "alight_delay_min" in list(pathset_links_df.columns.values):
            pathset_links_df.drop(["alight_delay_min", "new_A_time", "new_B_time", "new_linktime", "new_waittime", Assignment.SIM_COL_MISSED_XFER], axis=1, inplace=True)

        # Set alight_delay_min
        FastTripsLogger.debug("flag_missed_transfers() pathset_links_df (%d):\n%s" % (len(pathset_links_df), pathset_links_df.head().to_string()))
        pathset_links_df["alight_delay_min"] = 0.0
        pathset_links_df.loc[pandas.notnull(pathset_links_df[Trip.TRIPS_COLUMN_TRIP_ID_NUM]), "alight_delay_min"] = \
            ((pathset_links_df[Assignment.SIM_COL_PAX_ALIGHT_TIME]-pathset_links_df[Passenger.PF_COL_PAX_B_TIME])/numpy.timedelta64(1, 'm'))

        #: todo: is there a more elegant way to take care of this?  some trips have times after midnight so they're the next day
        pathset_links_df.loc[pathset_links_df["alight_delay_min"]>22*60, Assignment.SIM_COL_PAX_BOARD_TIME ] = pathset_links_df[Assignment.SIM_COL_PAX_BOARD_TIME] - numpy.timedelta64(24, 'h')
        pathset_links_df.loc[pathset_links_df["alight_delay_min"]>22*60, Assignment.SIM_COL_PAX_ALIGHT_TIME] = pathset_links_df[Assignment.SIM_COL_PAX_ALIGHT_TIME] - numpy.timedelta64(24, 'h')
        pathset_links_df.loc[pathset_links_df["alight_delay_min"]>22*60, "alight_delay_min"] = \
            ((pathset_links_df[Assignment.SIM_COL_PAX_ALIGHT_TIME]-pathset_links_df[Passenger.PF_COL_PAX_B_TIME])/numpy.timedelta64(1, 'm'))

        max_alight_delay_min = pathset_links_df["alight_delay_min"].max()
        FastTripsLogger.debug("Biggest alight_delay = %f" % max_alight_delay_min)
        if max_alight_delay_min > 0:
            FastTripsLogger.debug("\n%s" % pathset_links_df.sort_values(by="alight_delay_min", ascending=False).head().to_string())

        # For trips, alight_time is the new B_time
        # Set A_time for links AFTER trip links by joining to next leg
        next_trips = pathset_links_df[[
            Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
            Passenger.PF_COL_PATH_NUM,
            Passenger.PF_COL_LINK_NUM,
            Assignment.SIM_COL_PAX_ALIGHT_TIME]].copy()
        next_trips[Passenger.PF_COL_LINK_NUM] = next_trips[Passenger.PF_COL_LINK_NUM] + 1
        next_trips.rename(columns={Assignment.SIM_COL_PAX_ALIGHT_TIME:"new_A_time"}, inplace=True)
        # Add it to passenger trips.  Now new_A_time is set for links after trip links (note this will never be a trip link)
        pathset_links_df = pandas.merge(left=pathset_links_df, right=next_trips, how="left", on=[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM, Passenger.PF_COL_PATH_NUM, Passenger.PF_COL_LINK_NUM])

        FastTripsLogger.debug(str(pathset_links_df.dtypes))

        # Set the new_B_time for those links -- link time for access/egress/xfer is travel time since wait times are in trip links
        pathset_links_df["new_B_time"] = pathset_links_df["new_A_time"] + pathset_links_df[Passenger.PF_COL_LINK_TIME]
        # For trip links, it's alight time
        pathset_links_df.loc[pathset_links_df[Passenger.PF_COL_LINK_MODE]==PathSet.STATE_MODE_TRIP, "new_B_time"] = pathset_links_df[Assignment.SIM_COL_PAX_ALIGHT_TIME]
        # For access links, it doesn't change from the original pathfinding result
        pathset_links_df.loc[pathset_links_df[Passenger.PF_COL_LINK_MODE]==PathSet.STATE_MODE_ACCESS, "new_A_time"] = pathset_links_df[Passenger.PF_COL_PAX_A_TIME]
        pathset_links_df.loc[pathset_links_df[Passenger.PF_COL_LINK_MODE]==PathSet.STATE_MODE_ACCESS, "new_B_time"] = pathset_links_df[Passenger.PF_COL_PAX_B_TIME]

        # Now we only need to set the trip link's A time from the previous link's new_B_time
        next_trips = pathset_links_df[[
            Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
            Passenger.PF_COL_PATH_NUM,
            Passenger.PF_COL_LINK_NUM,
            "new_B_time"]].copy()
        next_trips[Passenger.PF_COL_LINK_NUM] = next_trips[Passenger.PF_COL_LINK_NUM] + 1
        next_trips.rename(columns={"new_B_time":"new_trip_A_time"}, inplace=True)
        # Add it to passenger trips.  Now new_trip_A_time is set for trip links
        pathset_links_df = pandas.merge(left=pathset_links_df, right=next_trips, how="left", on=[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM, Passenger.PF_COL_PATH_NUM, Passenger.PF_COL_LINK_NUM])
        pathset_links_df.loc[pathset_links_df[Passenger.PF_COL_LINK_MODE]==PathSet.STATE_MODE_TRIP, "new_A_time"] = pathset_links_df["new_trip_A_time"]
        pathset_links_df.drop(["new_trip_A_time"], axis=1, inplace=True)

        pathset_links_df["new_linktime"] = pathset_links_df["new_B_time"] - pathset_links_df["new_A_time"]

        #: todo: is there a more elegant way to take care of this?  some trips have times after midnight so they're the next day
        #: if the linktime > 23 hours then the trip time is probably off by a day, so it's right after midnight -- back it up
        pathset_links_df.loc[pathset_links_df["new_linktime"] > numpy.timedelta64(22, 'h'), "new_B_time"  ] = pathset_links_df["new_B_time"] - numpy.timedelta64(24, 'h')
        pathset_links_df.loc[pathset_links_df["new_linktime"] > numpy.timedelta64(22, 'h'), "new_linktime"] = pathset_links_df["new_B_time"] - pathset_links_df["new_A_time"]

        # new wait time
        pathset_links_df.loc[pandas.notnull(pathset_links_df[Trip.TRIPS_COLUMN_TRIP_ID_NUM]), "new_waittime"] = pathset_links_df["board_time"] - pathset_links_df["new_A_time"]

        # invalid trips have negative wait time
        pathset_links_df[Assignment.SIM_COL_MISSED_XFER] = 0
        pathset_links_df.loc[pathset_links_df["new_waittime"]<numpy.timedelta64(0,'m'), Assignment.SIM_COL_MISSED_XFER] = 1

        # count how many are valid (sum of invalid = 0 for the trip list id + path)
        pathset_links_df_grouped = pathset_links_df.groupby([Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
                                                             Passenger.PF_COL_PATH_NUM]).aggregate({Assignment.SIM_COL_MISSED_XFER:"sum" })

        pathset_links_df_grouped.loc[pathset_links_df_grouped[Assignment.SIM_COL_MISSED_XFER]> 0, Assignment.SIM_COL_MISSED_XFER] = 1

        FastTripsLogger.info("          flag_missed_transfers found %d missed transfer trip legs for %d paths" % \
                             (pathset_links_df[Assignment.SIM_COL_MISSED_XFER].sum(),
                              pathset_links_df_grouped[Assignment.SIM_COL_MISSED_XFER].sum()))

        # add missed_xfer to pathset_paths_df (replacing if it was there already)
        if Assignment.SIM_COL_MISSED_XFER in list(pathset_paths_df.columns.values):
            pathset_paths_df.drop([Assignment.SIM_COL_MISSED_XFER], axis=1, inplace=True)

        pathset_paths_df = pandas.merge(left=pathset_paths_df, right=pathset_links_df_grouped.reset_index()[[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM, Passenger.PF_COL_PATH_NUM, Assignment.SIM_COL_MISSED_XFER]], how="left")
        FastTripsLogger.debug("flag_missed_transfers() pathset_paths_df (%d):\n%s" % (len(pathset_paths_df), pathset_paths_df.head(30).to_string()))

        return (pathset_paths_df, pathset_links_df)

    @staticmethod
    def flag_bump_overcap_passengers(iteration, simulation_iteration, bump_iter, pathset_paths_df, pathset_links_df, veh_loaded_df):
        """
        Check if we have boards on over-capacity vehicles.  Mark them and mark the boards.

        If :py:attr:`Assignment.CAPACITY_CONSTRAINT`, then bump off overcapacity passengers.

        The process is:

        1) Look at which vehicle links are over capacity, adding columns named :py:attr:`Trip.SIM_COL_VEH_OVERCAP`
           and py:attr:`Trip.SIM_COL_VEH_OVERCAP_FRAC` to *veh_loaded_df*

        2) Look at the stops where the first people board after we're at capacity (impossible boards) if any

        3) If :py:attr:`Assignment.BUMP_ONE_AT_A_TIME`, select the first such stop by arrival time
           Otherwise, select the first such stop for each vehicle trip

        4) Join these stops to pathset_links_df, so pathset_links_df now has column Assignment.SIM_COL_PAX_OVERCAP_FRAC

        5) If not :py:attr:`Assignment.CAPACITY_CONSTRAINT`, return (and drop the column named :py:attr:`Trip.SIM_COL_VEH_OVERCAP` from veh_loaded_df)

        6) Figure out which passenger trips are actually getting bumped.  Some people can get on at these stops, but not all, so let the first
           ones that arrive at the stop get on and filter to the ones we'll actually bump.  Update the column named :py:attr:`Assignmment.SIM_COL_PAX_BUMP_ITER`.
           If non-negative, this represents the iteration the passenger got bumped.

        Return (chosen_paths_bumped, pathset_paths_df, pathset_links_df, veh_loaded_df)
        """

        # 1) Look at which vehicle links are over capacity
        # overcap = how many people are problematic
        # overcap_frac = what percentage of boards are problematic
        veh_loaded_df[Trip.SIM_COL_VEH_OVERCAP           ] = veh_loaded_df[Trip.SIM_COL_VEH_ONBOARD] - veh_loaded_df[Trip.VEHICLES_COLUMN_TOTAL_CAPACITY]
        veh_loaded_df[Assignment.SIM_COL_PAX_OVERCAP_FRAC] = 0.0

        # Keep negatives - that means we have space
        # veh_loaded_df.loc[veh_loaded_df[Trip.SIM_COL_VEH_OVERCAP]<0, Trip.SIM_COL_VEH_OVERCAP           ] = 0  # negatives - don't care, set to zero
        veh_loaded_df.loc[veh_loaded_df[Trip.SIM_COL_VEH_BOARDS ]>0, Assignment.SIM_COL_PAX_OVERCAP_FRAC] = veh_loaded_df[Trip.SIM_COL_VEH_OVERCAP]/veh_loaded_df[Trip.SIM_COL_VEH_BOARDS]

        # only need to do this once
        # TODO: figure out MSA with iteration/simulation_iteration/bump_iter
        if iteration==1 and simulation_iteration==0 and bump_iter==0:
            veh_loaded_df[Trip.SIM_COL_VEH_MSA_OVERCAP] = veh_loaded_df[Trip.SIM_COL_VEH_MSA_ONBOARD] - veh_loaded_df[Trip.VEHICLES_COLUMN_TOTAL_CAPACITY]
            veh_loaded_df.loc[veh_loaded_df[Trip.SIM_COL_VEH_MSA_OVERCAP]<0, Trip.SIM_COL_VEH_MSA_OVERCAP] = 0  # negatives - don't care, set to zero

        # These are trips/stops over capacity
        overcap_df = veh_loaded_df.loc[veh_loaded_df[Trip.SIM_COL_VEH_OVERCAP] > 0]
        FastTripsLogger.debug("flag_bump_overcap_passengers() %d vehicle trip/stops over capacity: (showing head)\n%s" % \
                              (len(overcap_df), overcap_df.head().to_string()))

        # If none, we're done
        if len(overcap_df) == 0:
            FastTripsLogger.info("          No over-capacity vehicles")
            return (0, pathset_paths_df, pathset_links_df, veh_loaded_df)

        # 2) Look at the trip-stops where the *first people* board after we're at capacity (impossible boards) if any
        bump_stops_df = overcap_df.groupby([Trip.STOPTIMES_COLUMN_TRIP_ID]).aggregate('first').reset_index()
        FastTripsLogger.debug("flag_bump_overcap_passengers() bump_stops_df iter=%d sim_iter=%d bump_iter=%d (%d rows, showing head):\n%s" %
                              (iteration, simulation_iteration, bump_iter, len(bump_stops_df), bump_stops_df.head().to_string()))


        if Assignment.CAPACITY_CONSTRAINT:
            # One stop at a time -- slower but more accurate
            # 3) If Assignment.BUMP_ONE_AT_A_TIME, select the first such stop by arrival time
            #    Otherwise, select the first such stop for each vehicle trip
            if Assignment.BUMP_ONE_AT_A_TIME:
                bump_stops_df.sort_values(by=[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME], inplace=True)
                bump_stops_df = bump_stops_df.iloc[:1]

            FastTripsLogger.info("          Need to bump %d passengers from %d trip-stops" % (bump_stops_df.overcap.sum(), len(bump_stops_df)))

        # debug -- see the whole trip
        if True:
            FastTripsLogger.debug("flag_bump_overcap_passengers() Trips with bump stops:\n%s\n" % \
                pandas.merge(
                    left=veh_loaded_df[[Trip.STOPTIMES_COLUMN_TRIP_ID,
                                        Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                        Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                        Trip.STOPTIMES_COLUMN_STOP_ID,
                                        Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                        Trip.VEHICLES_COLUMN_TOTAL_CAPACITY,
                                        Trip.SIM_COL_VEH_BOARDS,
                                        Trip.SIM_COL_VEH_ALIGHTS,
                                        Trip.SIM_COL_VEH_ONBOARD,
                                        Trip.SIM_COL_VEH_OVERCAP,
                                        Assignment.SIM_COL_PAX_OVERCAP_FRAC]],
                    right=bump_stops_df[[Trip.STOPTIMES_COLUMN_TRIP_ID]],
                    how='inner').to_string())

        # 4) Join these stops to pathset_links_df, so pathset_links_df now has column Assignment.SIM_COL_PAX_OVERCAP_FRAC
        pathset_links_df = Assignment.find_passenger_vehicle_times(pathset_links_df, veh_loaded_df)

        # 5) If not Assignment.CAPACITY_CONSTRAINT, return (and drop the column named Trip.SIM_COL_VEH_OVERCAP from veh_loaded_df)
        #    (If we're not actually bumping passengers, we're done; the pathset_links_df have overcap and overcap_frac information)
        if not Assignment.CAPACITY_CONSTRAINT:

            veh_loaded_df.drop(Assignment.SIM_COL_PAX_OVERCAP_FRAC, axis=1, inplace=True)
            return (0, pathset_paths_df, pathset_links_df, veh_loaded_df)

        # join pathset links to bump_stops_df; now passenger links alighting at a bump stop will have Trip.STOPTIMES_COLUMN_STOP_SEQUENCE set
        pathset_links_df = pandas.merge(left    =pathset_links_df,
                                        left_on =[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM, "A_seq"],
                                        right   =bump_stops_df[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM, Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]],
                                        right_on=[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM, Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],
                                        how     ="left")
        FastTripsLogger.debug("flag_bump_overcap_passengers() pathset_links_df (%d rows, showing head):\n%s" % (len(pathset_links_df), pathset_links_df.head().to_string()))

        # bump candidates: alighting at bump stops, chosen paths, unbumped and overcap
        bumpstop_boards = pathset_links_df.loc[pandas.notnull(pathset_links_df[Trip.STOPTIMES_COLUMN_STOP_SEQUENCE])&        # alight at bump_stops_df stop
                                               (pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN]                 >=0)&        # chosen
                                               (pathset_links_df[Assignment.SIM_COL_PAX_BUMP_ITER]              < 0)].copy() # unbumped
        # unchosen bump candidates (to hedge against future choosing of paths with at-capacity links)
        unchosen_atcap_boards = pathset_links_df.loc[(pandas.notnull(pathset_links_df[Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]))&    # alight at bump_stops_df stop
                                                     (pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN]                    <0)&    # path not chosen (yet)
                                                     (pathset_links_df[Assignment.SIM_COL_PAX_BUMP_ITER]                 <0)]    # unbumped

        # bump off later arrivals, later trip_list_num
        bumpstop_boards.sort_values(by=[ \
            "new_A_time", # I think this is correct
            Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
            "A_seq",
            Passenger.PF_COL_PAX_A_TIME,
            Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM],
            ascending=[True, True, True, False, False], inplace=True)
        bumpstop_boards.reset_index(drop=True, inplace=True)

        # For each trip_id, stop_seq, stop_id, we want the first *overcap* rows
        # group to trip_id, stop_seq, stop_id and count off
        bpb_count = bumpstop_boards.groupby([Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                             "A_seq",
                                             "A_id_num"]).cumcount()
        bpb_count.name = 'bump_index'

        # Add the bump index to our passenger-paths/stops
        bumpstop_boards = pandas.concat([bumpstop_boards, bpb_count], axis=1)

        # 1 mean boarded, 0 means we got bumped
        bumpstop_boards["new_bumpstop_boarded"] = 1
        bumpstop_boards.loc[ bumpstop_boards["bump_index"] < bumpstop_boards[Trip.SIM_COL_VEH_OVERCAP], "new_bumpstop_boarded"] = 0  # these folks got bumped

        FastTripsLogger.debug("flag_bump_overcap_passengers() bumpstop_boards (%d rows, showing head):\n%s" % 
                              (len(bumpstop_boards), bumpstop_boards.head(50).to_string()))

        # filter to unique passengers/paths who got bumped
        bump_paths = bumpstop_boards.loc[ bumpstop_boards["new_bumpstop_boarded"] == 0,
            [Passenger.TRIP_LIST_COLUMN_PERSON_ID, Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM, Passenger.PF_COL_PATH_NUM]].drop_duplicates()
        chosen_paths_bumped = len(bump_paths)

        # if we have unchosen paths that board here, add those too
        if len(unchosen_atcap_boards) > 0:

            FastTripsLogger.debug("flag_bump_overcap_passengers() unchosen_atcap_boards (%d rows, showing head):\n%s" % \
                (len(unchosen_atcap_boards), unchosen_atcap_boards.head().to_string()))
            unchosen_atcap_boards = unchosen_atcap_boards[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                           Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
                                                           Passenger.PF_COL_PATH_NUM]].drop_duplicates()
            bump_paths = pandas.concat([bump_paths, unchosen_atcap_boards], axis=0)

        bump_paths['bump_iter_new'] = bump_iter

        FastTripsLogger.debug("flag_bump_overcap_passengers() bump_paths (%d rows, showing head):\n%s" % \
            (len(bump_paths), bump_paths.head().to_string()))

        # Kick out the bumped passengers -- update bump_iter on all pathset_links_df
        pathset_links_df = pandas.merge(left =pathset_links_df, right=bump_paths, how  ="left")
        pathset_links_df.loc[ pandas.notnull(pathset_links_df['bump_iter_new']), Assignment.SIM_COL_PAX_BUMP_ITER] = pathset_links_df['bump_iter_new']

        # Keep record of if they boarded at a bumpstop
        pathset_links_df = pandas.merge(left=pathset_links_df,
                                        right=bumpstop_boards[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                               Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
                                                               Passenger.PF_COL_PATH_NUM,
                                                               Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                               "A_seq","new_bumpstop_boarded"]],
                                        how="left")
        pathset_links_df.loc[ pandas.notnull(pathset_links_df['new_bumpstop_boarded']), Assignment.SIM_COL_PAX_BUMPSTOP_BOARDED] = pathset_links_df["new_bumpstop_boarded"]

        new_bump_wait = bumpstop_boards[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                           Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                           "A_id_num",
                                           Passenger.PF_COL_PAX_A_TIME]].groupby( \
                        [Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,"A_id_num"]).first()
        new_bump_wait.reset_index(drop=False, inplace=True)
        new_bump_wait.rename(columns={"A_id_num":Trip.STOPTIMES_COLUMN_STOP_ID_NUM}, inplace=True)

        FastTripsLogger.debug("new_bump_wait (%d rows, showing head):\n%s" %
            (len(new_bump_wait), new_bump_wait.head().to_string(formatters=\
           {Passenger.PF_COL_PAX_A_TIME:Util.datetime64_formatter})))

        # incorporate it into the bump wait df
        if type(Assignment.bump_wait_df) == type(None):
            Assignment.bump_wait_df = new_bump_wait
        else:
            Assignment.bump_wait_df = pandas.concat([Assignment.bump_wait_df, new_bump_wait], axis=0)

            FastTripsLogger.debug("flag_bump_overcap_passengers() bump_wait_df (%d rows, showing head):\n%s" %
                (len(Assignment.bump_wait_df), Assignment.bump_wait_df.head().to_string()))

            Assignment.bump_wait_df.drop_duplicates(subset=[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                            Trip.STOPTIMES_COLUMN_STOP_SEQUENCE], inplace=True)

        # drop unnecessary columns before returning
        pathset_links_df.drop([ \
            Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,  # adding this
            'bump_iter_new',
            'new_bumpstop_boarded'
            ], axis=1, inplace=True)

        veh_loaded_df.drop(Assignment.SIM_COL_PAX_OVERCAP_FRAC, axis=1, inplace=True)

        FastTripsLogger.debug("flag_bump_overcap_passengers(): return pathset_links_df.head():\n%s\n" % pathset_links_df.head().to_string())
        FastTripsLogger.debug("flag_bump_overcap_passengers(): return veh_loaded_df.head():\n%s\n" % veh_loaded_df.head().to_string())

        return (chosen_paths_bumped, pathset_paths_df, pathset_links_df, veh_loaded_df)


    @staticmethod
    def simulate(FT, output_dir, iteration, pathset_paths_df, pathset_links_df, veh_trips_df):
        """
        Given a pathset for each passenger, choose a path (if relevant) and then
        actually assign the passengers trips to the vehicles.

        Returns (valid_linked_trips, pathset_paths_df, pathset_links_df, veh_loaded_df)
        """
        simulation_iteration   = 0
        num_passengers_arrived = 0

        while True:
            FastTripsLogger.info("Simulation Iteration %d" % simulation_iteration)
            for trace_pax in Assignment.TRACE_PERSON_IDS:
                FastTripsLogger.debug("Initial pathset_links_df for %s\n%s" % \
                   (str(trace_pax), pathset_links_df.loc[pathset_links_df.person_id==trace_pax].to_string()))

                FastTripsLogger.debug("Initial pathset_paths_df for %s\n%s" % \
                   (str(trace_pax), pathset_paths_df.loc[pathset_paths_df.person_id==trace_pax].to_string()))

            ######################################################################################################
            FastTripsLogger.info("  Step 1. Find out board/alight times for all pathset links from vehicle times")

            # could do this just to chosen path links but let's do this to the whole pathset
            pathset_links_df = Assignment.find_passenger_vehicle_times(pathset_links_df, veh_trips_df)

            ######################################################################################################
            FastTripsLogger.info("  Step 2. Flag missed transfer links and paths in the pathsets")
            (pathset_paths_df, pathset_links_df) = Assignment.flag_missed_transfers(pathset_paths_df, pathset_links_df)

            ######################################################################################################
            FastTripsLogger.info("  Step 3. Calculate costs and probabilities for all pathset paths")
            (pathset_paths_df, pathset_links_df) = PathSet.calculate_cost(
                iteration, simulation_iteration, Assignment.STOCH_DISPERSION,
                pathset_paths_df, pathset_links_df, FT.passengers.trip_list_df,
                FT.transfers.transfers_df, FT.tazs.walk_df, FT.tazs.drive_df)

            ######################################################################################################
            FastTripsLogger.info("  Step 4. Choose a path for each passenger from their pathset")

            # Choose path for each passenger -- pathset_paths_df and pathset_links_df will now have
            # SIM_COL_PAX_CHOSEN >=0 for chosen paths/path links
            (num_chosen, pathset_paths_df, pathset_links_df) = Passenger.choose_paths(
                Assignment.PATHFINDING_EVERYONE and simulation_iteration==0,  # choose for everyone if we just re-found all paths
                iteration, simulation_iteration,
                pathset_paths_df, pathset_links_df)
            num_passengers_arrived += num_chosen

            ######################################################################################################
            FastTripsLogger.info("  Step 5. Put passenger paths on transit vehicles to get vehicle boards/alights/load")

            # no one is bumped yet
            bump_iter = 0
            pathset_links_df[Assignment.SIM_COL_PAX_OVERCAP_FRAC] = numpy.NaN

            if simulation_iteration==0:
                # For those we just found paths for, no one is bumped or going on overcap vehicles yet
                pathset_links_df.loc[pathset_links_df[Passenger.PF_COL_ITERATION]==iteration, Assignment.SIM_COL_PAX_BUMP_ITER ] = -1

            while True: # loop for capacity constraint

                # Put passengers on vehicles, updating the vehicle's boards, alights, onboard
                veh_trips_df = Assignment.put_passengers_on_vehicles(iteration, bump_iter, pathset_paths_df, pathset_links_df, veh_trips_df)

                if not FT.trips.has_capacity_configured():
                    # We can't do anything about capacity
                    break

                else:
                    ######################################################################################################
                    FastTripsLogger.info("  Step 6. Capacity constraints on transit vehicles.")

                    if bump_iter == 0:
                        FastTripsLogger.info("          Bumping one at a time? %s" % ("true" if Assignment.BUMP_ONE_AT_A_TIME else "false"))

                    # This needs to run at this point because the arrival times for the passengers are accurate here
                    (chosen_paths_bumped, pathset_paths_df, pathset_links_df, veh_trips_df) = \
                        Assignment.flag_bump_overcap_passengers(iteration, simulation_iteration, bump_iter,
                                                                pathset_paths_df, pathset_links_df, veh_trips_df)


                    FastTripsLogger.info("        -> completed loop bump_iter %d and bumped %d chosen paths" % (bump_iter, chosen_paths_bumped))

                    if chosen_paths_bumped == 0:
                        # do one final update of overcap to passengers
                        pathset_links_df = Assignment.find_passenger_vehicle_times(pathset_links_df, veh_trips_df)
                        break

                    num_passengers_arrived -= chosen_paths_bumped
                    bump_iter += 1


            if type(Assignment.bump_wait_df) == pandas.DataFrame and len(Assignment.bump_wait_df) > 0:
                Assignment.bump_wait_df[Passenger.PF_COL_PAX_A_TIME_MIN] = \
                    Assignment.bump_wait_df[Passenger.PF_COL_PAX_A_TIME].map(lambda x: (60.0*x.hour) + x.minute + (x.second/60.0))

            if type(Assignment.bump_wait_df) == pandas.DataFrame and len(Assignment.bump_wait_df) > 0:
                FastTripsLogger.debug("Bump_wait_df:\n%s" % Assignment.bump_wait_df.to_string(formatters=\
                    {Passenger.PF_COL_PAX_A_TIME :Util.datetime64_formatter}))

            ######################################################################################################
            FastTripsLogger.info("  Step 7. Update dwell and travel times for transit vehicles")
            # update the trip times -- accel/decel rates + stops affect travel times, and boards/alights affect dwell times
            veh_trips_df   = Trip.update_trip_times(veh_trips_df, Assignment.MSA_RESULTS)

            # write pathset paths and links
            Passenger.write_paths(output_dir, iteration, simulation_iteration, pathset_paths_df, False)
            Passenger.write_paths(output_dir, iteration, simulation_iteration, pathset_links_df, True )

            simulation_iteration += 1

            if num_chosen == 0: break

            if simulation_iteration > Assignment.MAX_SIMULATION_ITERS: break

        # write the final chosen paths for this iteration
        passengers_df = Passenger.get_chosen_links(pathset_links_df)
        passengers_df["iteration"] = iteration
        Util.write_dataframe(passengers_df, "passengers_df", os.path.join(output_dir, "ft_output_passenger_paths.csv"),
                             append=(iteration>1))
        passengers_df.drop(["iteration"], axis=1, inplace=True)

        return (num_passengers_arrived, pathset_paths_df, pathset_links_df, veh_trips_df)


def find_trip_based_paths_process_worker(iteration, worker_num, input_network_dir, input_demand_dir,
                                         output_dir, todo_pathset_queue, done_queue, hyperpath, bump_wait_df, stop_times_df):
    """
    Process worker function.  Processes all the paths in queue.

    todo_queue has (passenger_id, path object)
    """
    worker_str = "_worker%02d" % worker_num

    from .FastTrips import FastTrips
    setupLogging(infoLogFilename  = None,
                 debugLogFilename = os.path.join(output_dir, FastTrips.DEBUG_LOG % worker_str), 
                 logToConsole     = False,
                 append           = True if iteration > 1 else False)
    FastTripsLogger.info("Iteration %d Worker %2d starting" % (iteration, worker_num))

    # the child process doesn't have these set to read them
    Assignment.read_configuration(output_dir, input_demand_dir, Assignment.CONFIGURATION_OUTPUT_FILE)

    # this passes those read parameters and the stop times to the C++ extension
    Assignment.initialize_fasttrips_extension(worker_num, output_dir, stop_times_df)

    # the extension has it now, so we're done
    stop_times_df = None

    if iteration > 1:
        Assignment.set_fasttrips_bump_wait(bump_wait_df)

    while True:
        # go through my queue -- check if we're done
        todo = todo_pathset_queue.get()
        if todo == 'DONE':
            done_queue.put( (worker_num, 'DONE') )
            FastTripsLogger.debug("Received DONE from the todo_pathset_queue")
            return

        # do the work
        pathset = todo

        FastTripsLogger.info("Processing person %20s path %d" % (pathset.person_id, pathset.trip_list_id_num))
        # communicate it to the parent
        done_queue.put( (worker_num, "STARTING", pathset.person_id, pathset.trip_list_id_num ))

        trace_person = False
        if pathset.person_id in Assignment.TRACE_PERSON_IDS:
            FastTripsLogger.debug("Tracing assignment of person %s" % pathset.person_id)
            trace_person = True

        try:
            (pathdict, perf_dict) = Assignment.find_trip_based_pathset(iteration, pathset, hyperpath, trace=trace_person)
            done_queue.put( (worker_num, "COMPLETED", pathset.trip_list_id_num, pathdict, perf_dict) )
        except:
            FastTripsLogger.exception("Exception")
            # call it a day
            done_queue.put( (worker_num, "EXCEPTION", str(sys.exc_info()) ) )
            return
