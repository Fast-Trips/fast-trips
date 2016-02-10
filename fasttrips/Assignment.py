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

from .Logger    import FastTripsLogger, setupLogging
from .Passenger import Passenger
from .Path      import Path
from .Stop      import Stop
from .TAZ       import TAZ
from .Transfer  import Transfer
from .Trip      import Trip
from .Util      import Util

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

    #: Route choice configuration: How many stochastic paths will we generate
    #: (not necessarily unique) to define a path choice set?  Int.
    STOCH_PATHSET_SIZE              = None

    #: Route choice configuration: Use vehicle capacity constraints. Boolean.
    CAPACITY_CONSTRAINT             = None

    #: Use this as the date
    TODAY                           = datetime.date.today()

    #: Trace these passengers
    TRACE_PERSON_IDS                = None

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

    #: This is a :py:class:`set` of bumped passenger IDs.  For multiple-iteration assignment,
    #: this determines which passengers to assign.
    bumped_person_ids               = set()
    bumped_trip_list_nums           = set()

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

    #: assignment results - Passenger table
    PASSENGERS_CSV                  = r"passengers_df_iter%d.csv"

    #: Column names for simulation
    SIM_COL_PAX_BOARD_TIME              = 'board_time'
    SIM_COL_PAX_ALIGHT_TIME             = 'alight_time'
    SIM_COL_PAX_ARRIVE_TIME             = 'pax_arrive_time'
    SIM_COL_PAX_ARRIVE_TIME_MIN         = 'pax_arrive_time_min'
    SIM_COL_PAX_LINK_TIME               = 'linktime'

    def __init__(self):
        """
        This does nothing.  Assignment methods are static methods for now.
        """
        pass

    @staticmethod
    def read_configuration(input_network_dir, input_demand_dir):
        """
        Read the configuration parameters.
        """
        pandas.set_option('display.width', 1000)

        # Functions are defined in here -- read this and eval it
        func_file = os.path.join(input_demand_dir, Assignment.CONFIGURATION_FUNCTIONS_FILE)
        if os.path.exists(func_file):
            my_globals = {}
            FastTripsLogger.info("Reading %s" % func_file)
            execfile(func_file, my_globals, Path.CONFIGURED_FUNCTIONS)
            FastTripsLogger.info("Path.CONFIGURED_FUNCTIONS = %s" % str(Path.CONFIGURED_FUNCTIONS))

        parser = ConfigParser.RawConfigParser(
            defaults={'iterations'                      :1,
                      'pathfinding_type'                :Assignment.ASSIGNMENT_TYPE_DET_ASGN,
                      'simulation'                      :True,
                      'output_passenger_trajectories'   :True,
                      'time_window'                     :30,
                      'create_skims'                    :False,
                      'skim_start_time'                 :'5:00',
                      'skim_end_time'                   :'10:00',
                      'stochastic_dispersion'           :1.0,
                      'stochastic_pathset_size'         :1000,
                      'capacity_constraint'             :False,
                      'trace_person_ids'                :'None',
                      'number_of_processes'             :0,
                      'bump_buffer'                     :5,
                      'bump_one_at_a_time'              :True,
                      # pathfinding
                      'user_class_function'             :'generic_user_class'
                     })
        parser.read(os.path.join(input_network_dir, Assignment.CONFIGURATION_FILE))
        if input_demand_dir and os.path.exists(os.path.join(input_demand_dir,  Assignment.CONFIGURATION_FILE)):
            parser.read(os.path.join(input_demand_dir,  Assignment.CONFIGURATION_FILE))

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
        Assignment.CAPACITY_CONSTRAINT           = parser.getboolean('fasttrips','capacity_constraint')
        Assignment.TRACE_PERSON_IDS         = eval(parser.get       ('fasttrips','trace_person_ids'))
        Assignment.NUMBER_OF_PROCESSES           = parser.getint    ('fasttrips','number_of_processes')
        Assignment.BUMP_BUFFER = datetime.timedelta(
                                         minutes = parser.getfloat  ('fasttrips','bump_buffer'))
        Assignment.BUMP_ONE_AT_A_TIME            = parser.getboolean('fasttrips','bump_one_at_a_time')

        # pathfinding
        Path.USER_CLASS_FUNCTION                 = parser.get     ('pathfinding','user_class_function')
        if Path.USER_CLASS_FUNCTION not in Path.CONFIGURED_FUNCTIONS:
            FastTripsLogger.fatal("User class function [%s] not defined.  Please check your function file [%s]" % (Path.USER_CLASS_FUNCTION, func_file))
            raise

        weights_file = os.path.join(input_demand_dir, Path.WEIGHTS_FILE)
        if not os.path.exists(weights_file):
            FastTripsLogger.fatal("No path weights file %s" % weights_file)
            sys.exit(2)

        Path.WEIGHTS_DF = pandas.read_fwf(weights_file)
        FastTripsLogger.debug("Weights =\n%s" % str(Path.WEIGHTS_DF))
        FastTripsLogger.debug("Weight types = \n%s" % str(Path.WEIGHTS_DF.dtypes))

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
        parser.set('fasttrips','stochastic_pathset_size',       '%d' % Assignment.STOCH_PATHSET_SIZE)
        parser.set('fasttrips','capacity_constraint',           'True' if Assignment.CAPACITY_CONSTRAINT else 'False')
        parser.set('fasttrips','trace_person_ids',              '%s' % str(Assignment.TRACE_PERSON_IDS))
        parser.set('fasttrips','number_of_processes',           '%d' % Assignment.NUMBER_OF_PROCESSES)
        parser.set('fasttrips','bump_buffer',                   '%f' % (Assignment.BUMP_BUFFER.total_seconds()/60.0))
        parser.set('fasttrips','bump_one_at_a_time',            'True' if Assignment.BUMP_ONE_AT_A_TIME else 'False')

        #pathfinding
        parser.add_section('pathfinding')
        parser.set('pathfinding','user_class_function',         '%s' % Path.USER_CLASS_FUNCTION)

        output_file = open(os.path.join(output_dir, Assignment.CONFIGURATION_OUTPUT_FILE), 'w')
        parser.write(output_file)
        output_file.close()

    @staticmethod
    def initialize_fasttrips_extension(process_number, output_dir, FT):
        """
        Initialize the C++ fasttrips extension by passing it the network supply.
        """
        FastTripsLogger.debug("Initializing fasttrips extension for process number %d" % process_number)

        _fasttrips.initialize_supply(output_dir, process_number,
                                     FT.trips.stop_times_df[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                             Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                                             Trip.STOPTIMES_COLUMN_STOP_ID_NUM]].as_matrix().astype('int32'),
                                     FT.trips.stop_times_df[[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN,
                                                             Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN]].as_matrix().astype('float64'))

        _fasttrips.initialize_parameters(Assignment.TIME_WINDOW.total_seconds()/60.0,
                                         Assignment.BUMP_BUFFER.total_seconds()/60.0,
                                         Assignment.STOCH_PATHSET_SIZE,
                                         Assignment.STOCH_DISPERSION)

    @staticmethod
    def set_fasttrips_bump_wait(bump_wait_df):
        """
        Sends the bump wait information to the fasttrips extension
        """
        _fasttrips.set_bump_wait(bump_wait_df[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                               Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                               Trip.STOPTIMES_COLUMN_STOP_ID_NUM]].as_matrix().astype('int32'),
                                 bump_wait_df[Assignment.SIM_COL_PAX_ARRIVE_TIME_MIN].values.astype('float64'))


    @staticmethod
    def assign_paths(output_dir, FT):
        """
        Finds the paths for the passengers.
        """
        Assignment.write_configuration(output_dir)

        Assignment.bump_wait = {}
        for iteration in range(1,Assignment.ITERATION_FLAG+1):
            FastTripsLogger.info("***************************** ITERATION %d **************************************" % iteration)

            if Assignment.ASSIGNMENT_TYPE == Assignment.ASSIGNMENT_TYPE_SIM_ONLY and \
               os.path.exists(os.path.join(output_dir, Assignment.PASSENGERS_CSV % iteration)):
                FastTripsLogger.info("Simulation only")
                (num_paths_found, passengers_df) = Assignment.read_assignment_results(output_dir, iteration)

            else:
                num_paths_found    = Assignment.generate_paths(FT, output_dir, iteration)
                passengers_df      = Assignment.setup_passengers(FT, output_dir, iteration)

            veh_trips_df       = Assignment.setup_trips(FT)

            if Assignment.OUTPUT_PASSENGER_TRAJECTORIES:
                Path.write_paths(passengers_df, output_dir)

            if Assignment.SIMULATION_FLAG == True:
                FastTripsLogger.info("****************************** SIMULATING *****************************")
                (num_passengers_arrived,veh_trips_df,pax_exp_df) = Assignment.simulate(FT, passengers_df, veh_trips_df)

            if Assignment.OUTPUT_PASSENGER_TRAJECTORIES:
                Path.write_path_times(pax_exp_df, output_dir)

            # capacity gap stuff
            num_bumped_passengers = num_paths_found - num_passengers_arrived
            capacity_gap = 100.0*num_bumped_passengers/num_paths_found

            FastTripsLogger.info("")
            FastTripsLogger.info("  TOTAL ASSIGNED PASSENGERS: %10d" % num_paths_found)
            FastTripsLogger.info("  ARRIVED PASSENGERS:        %10d" % num_passengers_arrived)
            FastTripsLogger.info("  MISSED PASSENGERS:         %10d" % num_bumped_passengers)
            FastTripsLogger.info("  CAPACITY GAP:              %10.5f" % capacity_gap)

            if capacity_gap < 0.001 or Assignment.ASSIGNMENT_TYPE == Assignment.ASSIGNMENT_TYPE_STO_ASGN:
                break

        # end for loop
        FastTripsLogger.info("**************************** WRITING OUTPUTS ****************************")
        Assignment.print_load_profile(FT, veh_trips_df, output_dir)

    @staticmethod
    def generate_paths(FT, output_dir, iteration):
        """
        Generates paths ofr passengers using deterministic trip-based shortest path (TBSP) or
        stochastic trip-based hyperpath (TBHP).

        Returns the number of paths found.
        """
        FastTripsLogger.info("**************************** GENERATING PATHS ****************************")
        start_time          = datetime.datetime.now()
        process_list        = []
        todo_queue          = None
        done_queue          = None

        est_paths_to_find   = len(FT.passengers.trip_list_df)
        if iteration > 1:
            est_paths_to_find = len(Assignment.bumped_trip_list_nums)

        info_freq           = pow(10, int(math.log(est_paths_to_find+1,10)-2))
        if info_freq < 1: info_freq = 1

        num_processes       = Assignment.NUMBER_OF_PROCESSES
        if  Assignment.NUMBER_OF_PROCESSES < 1:
            num_processes   = multiprocessing.cpu_count()
        if num_processes > est_paths_to_find:
            num_processes = est_paths_to_find

        # this is probalby time consuming... put in a try block
        try:
            # Setup multiprocessing processes
            if num_processes > 1:
                todo_queue      = multiprocessing.Queue()
                done_queue      = multiprocessing.Queue()
                for process_idx in range(1, 1+num_processes):
                    FastTripsLogger.info("Starting worker process %2d" % process_idx)
                    process_list.append(multiprocessing.Process(target=find_trip_based_paths_process_worker,
                                                                args=(iteration, process_idx, FT.input_network_dir, FT.input_demand_dir,
                                                                      FT.output_dir, todo_queue, done_queue,
                                                                      Assignment.ASSIGNMENT_TYPE==Assignment.ASSIGNMENT_TYPE_STO_ASGN,
                                                                      Assignment.bump_wait_df)))
                    process_list[-1].start()
            else:
                Assignment.initialize_fasttrips_extension(0, output_dir, FT)

            # process tasks or send tasks to workers for processing
            num_paths_found_prev  = 0
            num_paths_found_now   = 0
            path_cols             = list(FT.passengers.trip_list_df.columns.values)
            for path_tuple in FT.passengers.trip_list_df.itertuples(index=False):
                path_dict         = dict(zip(path_cols, path_tuple))
                trip_list_id      = path_dict[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM]
                person_id         = path_dict[Passenger.TRIP_LIST_COLUMN_PERSON_ID]

                # first iteration -- create path objects
                if iteration==1:
                    trip_path = Path(path_dict)
                    FT.passengers.add_path(trip_list_id, trip_path)
                else:
                    trip_path = FT.passengers.get_path(trip_list_id)

                if not trip_path.goes_somewhere(): continue

                if iteration > 1 and trip_list_id not in Assignment.bumped_trip_list_nums:
                    num_paths_found_prev += 1
                    continue

                if num_processes > 1:
                    todo_queue.put( trip_path )
                else:
                    trace_person = False
                    if person_id in Assignment.TRACE_PERSON_IDS:
                        FastTripsLogger.debug("Tracing assignment of person_id %s" % str(person_id))
                        trace_person = True

                    # do the work
                    (cost, return_states) = Assignment.find_trip_based_path(iteration, FT, trip_path,
                                                                            Assignment.ASSIGNMENT_TYPE==Assignment.ASSIGNMENT_TYPE_STO_ASGN,
                                                                            trace=trace_person)
                    trip_path.states = return_states
                    trip_path.cost   = cost

                    if trip_path.path_found():
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
                for process_idx in range(len(process_list)):
                    todo_queue.put('DONE')

                # get results
                done_procs          = 0
                while done_procs < len(process_list):

                    result = done_queue.get()
                    if result == 'DONE':
                        FastTripsLogger.debug("Received done")
                        done_procs += 1

                    else:
                        trip_list_id    = result[0]
                        path            = FT.passengers.get_path(trip_list_id)
                        path.cost       = result[1]
                        path.states     = result[2]
                        if path.path_found():
                            num_paths_found_now += 1

                        if num_paths_found_now % info_freq == 0:
                            time_elapsed = datetime.datetime.now() - start_time
                            FastTripsLogger.info(" %6d / %6d passenger paths found.  Time elapsed: %2dh:%2dm:%2ds" % (
                                                 num_paths_found_now, est_paths_to_find,
                                                 int( time_elapsed.total_seconds() / 3600),
                                                 int( (time_elapsed.total_seconds() % 3600) / 60),
                                                 time_elapsed.total_seconds() % 60))

                # join up my processes
                for proc in process_list:
                    proc.join()

        except (KeyboardInterrupt, SystemExit):
            exc_type, exc_value, exc_tb = sys.exc_info()
            FastTripsLogger.error("Exception caught: %s" % str(exc_type))
            error_lines = traceback.format_exception(exc_type, exc_value, exc_tb)
            for e in error_lines: FastTripsLogger.error(e)
            FastTripsLogger.error("Terminating processes")
            # terminating my processes
            for proc in process_list:
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
    def find_trip_based_path(iteration, FT, path, hyperpath, trace):
        """
        Perform trip-based path search.

        Will do so either backwards (destination to origin) if :py:attr:`Path.direction` is :py:attr:`Path.DIR_OUTBOUND`
        or forwards (origin to destination) if :py:attr:`Path.direction` is :py:attr:`Path.DIR_INBOUND`.

        Returns (path cost, return_states).

        :param iteration: The pathfinding iteration we're on
        :type  iteration: int
        :param FT:        fasttrips data
        :type  FT:        a :py:class:`FastTrips` instance
        :param path:      the path to fill in
        :type  path:      a :py:class:`Path` instance
        :param hyperpath: pass True to use a stochastic hyperpath-finding algorithm, otherwise a deterministic shortest path
                          search algorithm will be use.
        :type  hyperpath: boolean
        :param trace:     pass True if this path should be traced to the debug log
        :type  trace:     boolean

        """
        # FastTripsLogger.debug("C++ extension start")
        # send it to the C++ extension
        (ret_ints, ret_doubles, path_cost) = \
            _fasttrips.find_path(iteration, path.person_id_num, path.trip_list_id_num, hyperpath,
                                 path.user_class, path.access_mode, path.transit_mode, path.egress_mode,
                                 path.o_taz_num, path.d_taz_num,
                                 1 if path.outbound() else 0, float(path.pref_time_min),
                                 1 if trace else 0)
        # FastTripsLogger.debug("C++ extension complete")

        # Put the results into an ordered dict statelist
        return_states = collections.OrderedDict()
        midnight = datetime.datetime.combine(Assignment.TODAY, datetime.time())

        for index in range(ret_ints.shape[0]):
            mode = ret_ints[index,1]
            # todo
            if mode == -100:
                mode = Path.STATE_MODE_ACCESS
            elif mode == -101:
                mode = Path.STATE_MODE_EGRESS
            elif mode == -102:
                mode = Path.STATE_MODE_TRANSFER
            elif mode == -103:
                mode = Passenger.MODE_GENERIC_TRANSIT_NUM

            if hyperpath:
                return_states[ret_ints[index, 0]] = [
                              ret_doubles[index,0],                                         # label,
                              midnight + datetime.timedelta(minutes=ret_doubles[index,1]),  # departure/arrival time
                              mode,                                                         # departure/arrival mode
                              ret_ints[index,2],                                            # trip id
                              ret_ints[index,3],                                            # successor/predecessor
                              ret_ints[index,4],                                            # sequence
                              ret_ints[index,5],                                            # sequence succ/pred
                              datetime.timedelta(minutes=ret_doubles[index,2]),             # link time
                              ret_doubles[index,3],                                         # cost
                              midnight + datetime.timedelta(minutes=ret_doubles[index,4])   # arrival/departure time
                              ]
            else:
                return_states[ret_ints[index, 0]] = [
                              datetime.timedelta(minutes=ret_doubles[index,0]),              # label,
                              midnight + datetime.timedelta(minutes=ret_doubles[index,1]),  # departure/arrival time
                              mode,                                                         # departure/arrival mode
                              ret_ints[index,2],                                            # trip id
                              ret_ints[index,3],                                            # successor/predecessor
                              ret_ints[index,4],                                            # sequence
                              ret_ints[index,5],                                            # sequence succ/pred
                              datetime.timedelta(minutes=ret_doubles[index,2]),             # link time
                              datetime.timedelta(minutes=ret_doubles[index,3]),             # cost
                              midnight + datetime.timedelta(minutes=ret_doubles[index,4])   # arrival/departure time
                              ]
        return (path_cost, return_states)

    @staticmethod
    def read_assignment_results(output_dir, iteration):
        """
        Reads assignment results from :py:attr:`Assignment.PASSENGERS_CSV`

        :param output_dir: Location of csv files to read
        :type output_dir: string
        :param iteration: The iteration label for the csv files to read
        :type iteration: integer
        :return: The number of paths assigned, the paths.  See :py:meth:`Assignment.setup_passengers`
                 for documentation on the passenger paths :py:class:`pandas.DataFrame`
        :rtype: a tuple of (int, :py:class:`pandas.DataFrame`)
        """

        # read existing paths
        passengers_df = pandas.read_csv(os.path.join(output_dir, Assignment.PASSENGERS_CSV % iteration),
                                        parse_dates=['A_time','B_time'])
        passengers_df['linktime'] = pandas.to_timedelta(passengers_df['linktime'])

        FastTripsLogger.info("Read %s" % os.path.join(output_dir, Assignment.PASSENGERS_CSV % iteration))
        FastTripsLogger.debug("passengers_df.dtypes=\n%s" % str(passengers_df.dtypes))

        uniq_pax = passengers_df[[Passenger.PERSONS_COLUMN_PERSON_ID,
                                  Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM]].drop_duplicates(subset=\
                                 [Passenger.PERSONS_COLUMN_PERSON_ID,
                                  Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM])
        num_paths_found = len(uniq_pax)

        return (num_paths_found, passengers_df)

    @staticmethod
    def setup_passengers(FT, output_dir, iteration):
        """
        Converts assignment results (which is stored in each Passenger :py:class:`Path`,
        in the :py:attr:`Path.states`) into a single :py:class:`pandas.DataFrame`.  Each row
        represents a link in the passenger's path.  The returned :py:class:`pandas.DataFrame`
        has the following columns:

        ==============  ===============  =====================================================================================================
        column name      column type     description
        ==============  ===============  =====================================================================================================
        `person_id               object  person unique ID
        `trip_list_id`            int64  trip list numerical ID
        `pathdir`                 int64  the :py:attr:`Path.direction`
        `pathmode`               object  the :py:attr:`Path.mode`
        `linkmode`               object  the mode of the link, one of :py:attr:`Path.STATE_MODE_ACCESS`, :py:attr:`Path.STATE_MODE_EGRESS`,
                                         :py:attr:`Path.STATE_MODE_TRANSFER` or :py:attr:`Path.STATE_MODE_TRIP`.  Paths will always start with
                                         access, followed by trips with transfers in between, and ending in an egress following the last trip.
        `trip_id`                object  the trip ID for trip links.  Set to :py:attr:`numpy.nan` for non-trip links.
        `trip_id_num`           float64  the numerical trip ID for trip links.  Set to :py:attr:`numpy.nan` for non-trip links.
        `A_id`                   object  the stop ID at the start of the link, or TAZ ID for access links
        `A_id_num`                int64  the numerical stop ID at the start of the link, or a numerical TAZ ID for access links
        `B_id`                   object  the stop ID at the end of the link, or a TAZ ID for access links
        `B_id_num`                int64  the numerical stop ID at the end of the link, or a numerical TAZ ID for access links
        `A_seq`,                  int64  the sequence number for the stop at the start of the link, or -1 for access links
        `B_seq`,                  int64  the sequence number for the stop at the start of the link, or -1 for access links
        `A_time`         datetime64[ns]  the time the passenger arrives at `A_id`
        `B_time`         datetime64[ns]  the time the passenger arrives at `B_id`
        `linktime`      timedelta64[ns]  the time spent on the link
        `cost`                  float64  the cost of the entire path
        ==============  ===============  =====================================================================================================

        Additionally, this method writes out the dataframe to a csv at :py:attr:`Assignment.PASSENGERS_CSV` in the given `output_dir`
        and labeled with the given `iteration`.
        """
        mylist = []
        for trip_list_id,path in FT.passengers.id_to_path.iteritems():
            if not path.goes_somewhere():   continue
            if not path.path_found():       continue

            # OUTBOUND passengers have states like this:
            #    stop:          label    departure   dep_mode  successor linktime
            # orig_taz                                 Access    b stop1
            #  b stop1                                  trip1    a stop2
            #  a stop2                               Transfer    b stop3
            #  b stop3                                  trip2    a stop4
            #  a stop4                                 Egress   dest_taz
            #
            #  stop:         label  dep_time    dep_mode   successor  seq  suc       linktime             cost  arr_time
            #   460:  0:20:49.4000  17:41:10      Access        3514   -1   -1   0:03:08.4000     0:03:08.4000  17:44:18
            #  3514:  0:17:41.0000  17:44:18     5131292        4313   30   40   0:06:40.0000     0:12:21.8000  17:50:59
            #  4313:  0:05:19.2000  17:50:59    Transfer        5728   -1   -1   0:00:19.2000     0:00:19.2000  17:51:18
            #  5728:  0:04:60.0000  17:57:00     5154302        5726   16   17   0:07:33.8000     0:03:02.4000  17:58:51
            #  5726:  0:01:57.6000  17:58:51      Egress         231   -1   -1   0:01:57.6000     0:01:57.6000  18:00:49

            # INBOUND passengers have states like this
            #   stop:          label      arrival   arr_mode predecessor linktime
            # dest_taz                                 Egress    a stop4
            #  a stop4                                  trip2    b stop3
            #  b stop3                               Transfer    a stop2
            #  a stop2                                  trip1    b stop1
            #  b stop1                                 Access   orig_taz
            #
            #  stop:         label  arr_time    arr_mode predecessor  seq pred       linktime             cost  dep_time
            #    15:  0:36:38.4000  17:30:38      Egress        3772   -1   -1   0:02:38.4000     0:02:38.4000  17:28:00
            #  3772:  0:34:00.0000  17:28:00     5123368        6516   22   14   0:24:17.2000     0:24:17.2000  17:05:50
            #  6516:  0:09:42.8000  17:03:42    Transfer        4766   -1   -1   0:00:16.8000     0:00:16.8000  17:03:25
            #  4766:  0:09:26.0000  17:03:25     5138749        5671    7    3   0:05:30.0000     0:05:33.2000  16:57:55
            #  5671:  0:03:52.8000  16:57:55      Access         943   -1   -1   0:03:52.8000     0:03:52.8000  16:54:03
            prev_linkmode = None
            prev_state_id = None
            if len(path.states) > 1:
                state_list = path.states.keys()
                if not path.outbound(): state_list = list(reversed(state_list))

                for state_index in range(len(state_list)):
                    state_id        = state_list[state_index]

                    state           = path.states[state_id]
                    linkmode        = state[Path.STATE_IDX_DEPARRMODE]
                    trip_id         = None
                    if linkmode not in [Path.STATE_MODE_ACCESS, Path.STATE_MODE_TRANSFER, Path.STATE_MODE_EGRESS]:
                        trip_id     = state[Path.STATE_IDX_TRIP]
                        linkmode    = Path.STATE_MODE_TRIP

                    if path.outbound():
                        a_id_num    = state_id
                        b_id_num    = state[Path.STATE_IDX_SUCCPRED]
                        a_seq       = state[Path.STATE_IDX_SEQ]
                        b_seq       = state[Path.STATE_IDX_SEQ_SUCCPRED]
                        b_time      = state[Path.STATE_IDX_ARRDEP]
                        a_time      = b_time - state[Path.STATE_IDX_LINKTIME]
                    else:
                        a_id_num    = state[Path.STATE_IDX_SUCCPRED]
                        b_id_num    = state_id
                        a_seq       = state[Path.STATE_IDX_SEQ_SUCCPRED]
                        b_seq       = state[Path.STATE_IDX_SEQ]
                        b_time      = state[Path.STATE_IDX_DEPARR]
                        a_time      = b_time - state[Path.STATE_IDX_LINKTIME]

                    # two trips in a row -- insert zero-walk transfer
                    if linkmode == Path.STATE_MODE_TRIP and prev_linkmode == Path.STATE_MODE_TRIP:
                        row = [path.person_id,
                               trip_list_id,
                               path.direction,
                               path.mode,
                               Path.STATE_MODE_TRANSFER,
                               None,
                               a_id_num,
                               a_id_num,
                               a_seq,
                               a_seq,
                               a_time,
                               a_time,
                               datetime.timedelta(),
                               path.cost,
                              ]
                        mylist.append(row)

                    row = [path.person_id,
                           trip_list_id,
                           path.direction,
                           path.mode,
                           linkmode,
                           trip_id,
                           a_id_num,
                           b_id_num,
                           a_seq,
                           b_seq,
                           a_time,
                           b_time,
                           state[Path.STATE_IDX_LINKTIME],
                           path.cost,
                           ]
                    mylist.append(row)

                    prev_linkmode = linkmode
                    prev_state_id = state_id

        df =  pandas.DataFrame(mylist,
                               columns=[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                        Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
                                        'pathdir',  # for debugging
                                        'pathmode', # for output
                                        'linkmode', 'trip_id_num',
                                        'A_id_num','B_id_num',
                                        'A_seq','B_seq',
                                        'A_time', 'B_time',
                                        'linktime', 'cost'])

        # get A_id and B_id and trip_id
        df = Util.add_new_id(  input_df=df,                          id_colname='A_id_num',                            newid_colname='A_id',
                             mapping_df=FT.stops.stop_id_df, mapping_id_colname=Stop.STOPS_COLUMN_STOP_ID_NUM, mapping_newid_colname=Stop.STOPS_COLUMN_STOP_ID)
        df = Util.add_new_id(  input_df=df,                          id_colname='B_id_num',                            newid_colname='B_id',
                             mapping_df=FT.stops.stop_id_df, mapping_id_colname=Stop.STOPS_COLUMN_STOP_ID_NUM, mapping_newid_colname=Stop.STOPS_COLUMN_STOP_ID)
        # get trip_id
        df = Util.add_new_id(  input_df=df,                          id_colname=Trip.TRIPS_COLUMN_TRIP_ID_NUM,         newid_colname=Trip.TRIPS_COLUMN_TRIP_ID,
                             mapping_df=FT.trips.trip_id_df, mapping_id_colname=Trip.TRIPS_COLUMN_TRIP_ID_NUM, mapping_newid_colname=Trip.TRIPS_COLUMN_TRIP_ID)

        FastTripsLogger.debug("Setup passengers dataframe:\n%s" % str(df.dtypes))
        df.to_csv(os.path.join(output_dir, Assignment.PASSENGERS_CSV % iteration), index=False)
        FastTripsLogger.info("Wrote passengers dataframe to %s" % os.path.join(output_dir, Assignment.PASSENGERS_CSV % iteration))
        return df

    @staticmethod
    def setup_trips(FT):
        """
        Sets up and returns a :py:class:`pandas.DataFrame` where each row contains a leg of a transit vehicle trip.
        # 2015-06-22 15:42:34 DEBUG Setup vehicle trips dataframe:
        # arrival_time          datetime64[ns]
        # departure_time        datetime64[ns]
        # stop_id                       object
        # stop_sequence                  int64
        # trip_id                       object
        # arrival_time_min             float64
        # departure_time_min           float64
        # stop_id_num                    int64
        # trip_id_num                    int64
        # route_id                      object
        # service_id                    object
        # vehicle_name                  object
        """
        # join with trips to get additional fields
        df = pandas.merge(left= FT.trips.stop_times_df, right= FT.trips.trips_df,
                          how='left',
                          on=[Trip.TRIPS_COLUMN_TRIP_ID, Trip.TRIPS_COLUMN_TRIP_ID_NUM])
        assert(len(FT.trips.stop_times_df) == len(df))
        return df

    @staticmethod
    def get_passenger_trips(passengers_df, veh_trips_df):
        """
        Creates a list of passenger trips with passenger arrive, board and alight times.

        - Takes the trip links of passengers_df
        - Joins with vehicle trips on trip id num, A_id, A_seq to get board time (Assignment.SIM_COL_PAX_BOARD_TIME)
        - Joins with vehicle trips on trip id num, B_id, B_seq to get alight time (Assignment.SIM_COL_PAX_ALIGHT_TIME)
        - Renames A_time to passenger arrival time (Assignment.SIM_COL_PAX_ARRIVE)
        returns DataFrame
        """
        passenger_trips = passengers_df.loc[passengers_df.linkmode=='Trip'].copy()
        FastTripsLogger.debug("       Have %d passenger trips" % len(passenger_trips))

        passenger_trips = pandas.merge(left    =passenger_trips,
                                       right   =veh_trips_df[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                              Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                                              Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                                              Trip.STOPTIMES_COLUMN_DEPARTURE_TIME]],
                                       left_on =[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,'A_id_num','A_seq'],
                                       right_on=[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                 Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                                 Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],
                                       how     ='left')
        passenger_trips = pandas.merge(left    =passenger_trips,
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

        passenger_trips.rename(columns=
           {Trip.STOPTIMES_COLUMN_DEPARTURE_TIME:Assignment.SIM_COL_PAX_BOARD_TIME,      # transit vehicle depart time (at A) = board time for pax
            'A_time'                            :Assignment.SIM_COL_PAX_ARRIVE_TIME,     # passenger arrival at the stop
            Trip.STOPTIMES_COLUMN_ARRIVAL_TIME  :Assignment.SIM_COL_PAX_ALIGHT_TIME,     # transit vehicle arrive time (at B) = alight time for pax
            }, inplace=True)

        # redundant with A_id, B_id, A_seq, B_seq
        passenger_trips.drop(['%s_A' % Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                              '%s_B' % Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                              '%s_A' % Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                              '%s_B' % Trip.STOPTIMES_COLUMN_STOP_SEQUENCE], axis=1, inplace=True)
        FastTripsLogger.debug("       Have %d passenger trips" % len(passenger_trips))

        # FastTripsLogger.debug("passenger_trips\n%s" % \
        #                (passenger_trips.to_string(formatters=\
        #                {Assignment.SIM_COL_PAX_BOARD_TIME   :Util.datetime64_min_formatter,
        #                 Assignment.SIM_COL_PAX_ARRIVE_TIME  :Util.datetime64_min_formatter,
        #                 Assignment.SIM_COL_PAX_ALIGHT_TIME  :Util.datetime64_min_formatter,
        #                 'B_time'                            :Util.datetime64_min_formatter,
        #                 Assignment.SIM_COL_PAX_LINK_TIME    :Util.timedelta_formatter})))
        return passenger_trips

    @staticmethod
    def simulate(FT, passengers_df, veh_trips_df):
        """
        Actually assign the passengers trips to the vehicles.
        """
        passengers_df_len       = len(passengers_df)
        veh_trips_df_len        = len(veh_trips_df)

        # veh_trips_df.set_index(['trip_id','stop_seq','stop_id'],verify_integrity=True,inplace=True)
        # FastTripsLogger.debug("veh_trips_df types = \n%s" % str(veh_trips_df.dtypes))
        FastTripsLogger.debug("veh_trips_df: \n%s" % veh_trips_df.head().to_string(formatters=
            {Trip.STOPTIMES_COLUMN_ARRIVAL_TIME   :Util.datetime64_formatter,
             Trip.STOPTIMES_COLUMN_DEPARTURE_TIME :Util.datetime64_formatter,
             'waitqueue_start_time'               :Util.datetime64_formatter}))

        for trace_pax in Assignment.TRACE_PERSON_IDS:
            FastTripsLogger.debug("Initial passengers_df for %s\n%s" % \
               (str(trace_pax),
                passengers_df.loc[passengers_df.person_id==trace_pax].to_string(formatters=\
               {'A_time'               :Util.datetime64_min_formatter,
                'B_time'               :Util.datetime64_min_formatter,
                'linktime'             :Util.timedelta_formatter})))

        ######################################################################################################
        FastTripsLogger.info("Step 1. Find out board/alight times for passengers from vehicle times")

        passenger_trips     = Assignment.get_passenger_trips(passengers_df, veh_trips_df)
        passenger_trips_len = len(passenger_trips)

        ######################################################################################################
        bump_iter = 0
        Assignment.bumped_person_ids.clear()
        Assignment.bumped_trip_list_nums.clear()
        while True: # loop for capacity constraint
            FastTripsLogger.info("Step 2. Put passenger paths on transit vehicles to get vehicle boards/alights/load")

            # Group to boards by counting trip_list_id_nums for a (trip_id, A_id as stop_id)
            passenger_trips_boards = passenger_trips[[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
                                                      Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,'A_id','A_seq']].groupby([Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,'A_id','A_seq']).count()
            passenger_trips_boards.index.names = [Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                  Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                                  Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]

            # And alights by counting path_ids for a (trip_id, B_id as stop_id)
            passenger_trips_alights = passenger_trips[[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
                                                       Trip.TRIPS_COLUMN_TRIP_ID_NUM,'B_id','B_seq']].groupby([Trip.TRIPS_COLUMN_TRIP_ID_NUM,'B_id','B_seq']).count()
            passenger_trips_alights.index.names = [Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                   Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                                   Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]

            # Join them to the transit vehicle trips so we can put people on vehicles
            veh_loaded_df = pandas.merge(left        = veh_trips_df,
                                         right       = passenger_trips_boards,
                                         left_on     = [Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                        Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                                        Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],
                                         right_index = True,
                                         how         = 'left')
            veh_loaded_df.rename(columns={Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM:'boards'}, inplace=True)

            veh_loaded_df = pandas.merge(left        = veh_loaded_df,
                                         right       = passenger_trips_alights,
                                        left_on      = [Trip.TRIPS_COLUMN_TRIP_ID_NUM,
                                                        Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                                        Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],
                                        right_index  = True,
                                        how          ='left')
            veh_loaded_df.rename(columns={Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM:'alights'}, inplace=True)
            veh_loaded_df.fillna(value=0, inplace=True)
            assert(len(veh_loaded_df)==veh_trips_df_len)

            # these are ints, not floats
            veh_loaded_df[['boards','alights']] = veh_loaded_df[['boards','alights']].astype(int)

            veh_loaded_df.set_index([Trip.TRIPS_COLUMN_TRIP_ID_NUM,Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],inplace=True)
            veh_loaded_df['onboard'] = veh_loaded_df.boards - veh_loaded_df.alights

            # on board is the cumulative sum of boards - alights
            trips_cumsum = veh_loaded_df[['onboard']].groupby(level=[0]).cumsum()
            veh_loaded_df.drop('onboard', axis=1, inplace=True) # replace with cumsum
            veh_loaded_df = pandas.merge(left        = veh_loaded_df,
                                         right       = trips_cumsum,
                                         left_index  = True,
                                         right_index = True,
                                         how         = 'left')
            assert(len(veh_loaded_df)==veh_trips_df_len)
            # print veh_trips_df.loc[5123368]
            veh_loaded_df.reset_index(inplace=True)

            FastTripsLogger.debug("veh_trips_loaded with onboard>0: (showing head)\n" + \
                                  veh_loaded_df.loc[veh_loaded_df.onboard>0].head().to_string(formatters=\
                   {Trip.STOPTIMES_COLUMN_ARRIVAL_TIME   :Util.datetime64_formatter,
                    Trip.STOPTIMES_COLUMN_DEPARTURE_TIME :Util.datetime64_formatter}))

            if not Assignment.CAPACITY_CONSTRAINT or not FT.trips.has_capacity_configured():
                # No need to loop
                break

            else:
                ######################################################################################################
                FastTripsLogger.info("Step 3. Capacity constraints on transit vehicles.")
                if bump_iter == 0:
                    FastTripsLogger.info("        Bumping one at a time? %s" % ("true" if Assignment.BUMP_ONE_AT_A_TIME else "false"))
                # This needs to run at this point because the arrival times for the passengers are accurate here

                # Who gets bumped?
                # overcap = how many people are problematic
                veh_loaded_df['overcap'] = veh_loaded_df.onboard - veh_loaded_df.capacity
                overcap_df     = veh_loaded_df.loc[veh_loaded_df.overcap > 0]

                FastTripsLogger.debug("%d vehicle trip/stops over capacity: (showing head)\n%s" % \
                                      (len(overcap_df),
                                      overcap_df.head().to_string(formatters=\
                   {Trip.STOPTIMES_COLUMN_ARRIVAL_TIME   :Util.datetime64_formatter,
                    Trip.STOPTIMES_COLUMN_DEPARTURE_TIME :Util.datetime64_formatter})))

                # If none, we're done
                if len(overcap_df) == 0:
                    FastTripsLogger.info("        No overcapacity vehicles")
                    break

                # start by bumping the first ones who board after at capacity - which stops are they?
                bump_stops_df  = overcap_df.groupby([Trip.STOPTIMES_COLUMN_TRIP_ID]).aggregate('first')
                FastTripsLogger.debug("Bump stops (%d rows, showing head):\n%s" %
                                      (len(bump_stops_df),
                                      bump_stops_df.head().to_string(formatters=\
                   {Trip.STOPTIMES_COLUMN_ARRIVAL_TIME   :Util.datetime64_formatter,
                    Trip.STOPTIMES_COLUMN_DEPARTURE_TIME :Util.datetime64_formatter})))

                # One stop at a time -- slower but more accurate
                if Assignment.BUMP_ONE_AT_A_TIME:
                    bump_stops_df.sort([Trip.STOPTIMES_COLUMN_ARRIVAL_TIME], inplace=True)
                    bump_stops_df = bump_stops_df.iloc[:1]

                FastTripsLogger.info("        Need to bump %d passengers from %d stops" % (bump_stops_df.overcap.sum(), len(bump_stops_df)))

                # FastTripsLogger.debug("passenger_trips\n%s" % \
                #                (passenger_trips.to_string(formatters=\
                #                {Assignment.SIM_COL_PAX_BOARD_TIME   :Util.datetime64_min_formatter,
                #                 Assignment.SIM_COL_PAX_ARRIVE_TIME  :Util.datetime64_min_formatter,
                #                 Assignment.SIM_COL_PAX_ALIGHT_TIME  :Util.datetime64_min_formatter,
                #                 'B_time'                            :Util.datetime64_min_formatter,
                #                 Assignment.SIM_COL_PAX_LINK_TIME    :Util.timedelta_formatter})))

                # who boards at those stops?
                bumped_pax_boards = pandas.merge(left    =passenger_trips[[ \
                                                            Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,'A_id',
                                                            Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                            Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,'A_seq',
                                                            Assignment.SIM_COL_PAX_ARRIVE_TIME]],
                                                 left_on =[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,'A_id','A_seq'],
                                                 right   =bump_stops_df.reset_index()[[ \
                                                            Trip.STOPTIMES_COLUMN_TRIP_ID,
                                                            Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                            Trip.STOPTIMES_COLUMN_STOP_ID,
                                                            Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                                            Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                                            Trip.STOPTIMES_COLUMN_ARRIVAL_TIME,
                                                            Trip.STOPTIMES_COLUMN_DEPARTURE_TIME,'overcap']],
                                                 right_on=[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                           Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                                           Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],
                                                 how     ='inner')
                # bump off later arrivals, later trip_list_num
                bumped_pax_boards.sort([Trip.STOPTIMES_COLUMN_ARRIVAL_TIME,
                                        Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                        Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                        Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                        Assignment.SIM_COL_PAX_ARRIVE_TIME,
                                        Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM],
                                       ascending=[True, True, True, True, False, False], inplace=True)
                bumped_pax_boards.reset_index(drop=True, inplace=True)

                # For each trip_id, stop_seq, stop_id, we want the first *overcap* rows
                # group to trip_id, stop_seq, stop_id and count off
                bpb_count = bumped_pax_boards.groupby([Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                       Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                                       Trip.STOPTIMES_COLUMN_STOP_ID_NUM]).cumcount()
                bpb_count.name = 'bump_index'

                # Add the bump index to our passenger-paths/stops
                bumped_pax_boards = pandas.concat([bumped_pax_boards, bpb_count], axis=1)

                FastTripsLogger.debug("bumped_pax_boards (%d rows, showing head):\n%s" % (len(bumped_pax_boards),
                    bumped_pax_boards.head().to_string(formatters=\
                   {Assignment.SIM_COL_PAX_ARRIVE_TIME   :Util.datetime64_formatter,
                    Trip.STOPTIMES_COLUMN_ARRIVAL_TIME   :Util.datetime64_formatter,
                    Trip.STOPTIMES_COLUMN_DEPARTURE_TIME :Util.datetime64_formatter})))

                # use it to filter to those we bump
                bumped_pax_boards = bumped_pax_boards.loc[bumped_pax_boards.bump_index < bumped_pax_boards.overcap]

                FastTripsLogger.debug("filtered bumped_pax_boards (%d rows, showing head):\n%s" % (len(bumped_pax_boards),
                    bumped_pax_boards.head().to_string(formatters=\
                   {Assignment.SIM_COL_PAX_ARRIVE_TIME   :Util.datetime64_formatter,
                    Trip.STOPTIMES_COLUMN_ARRIVAL_TIME   :Util.datetime64_formatter,
                    Trip.STOPTIMES_COLUMN_DEPARTURE_TIME :Util.datetime64_formatter})))

                # filter to unique passengers/paths
                bumped_pax_boards.drop_duplicates(subset=[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                          Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM],inplace=True)
                bumped_pax_boards['bump'] = True

                # keep track of these
                Assignment.bumped_person_ids.update(bumped_pax_boards[Passenger.TRIP_LIST_COLUMN_PERSON_ID].tolist())
                Assignment.bumped_trip_list_nums.update(bumped_pax_boards[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM].tolist())

                FastTripsLogger.debug("bumped_pax_boards without duplicate passengers (%d rows, showing head):\n%s" % \
                    (len(bumped_pax_boards),
                     bumped_pax_boards.head().to_string(formatters=\
                   {Assignment.SIM_COL_PAX_ARRIVE_TIME   :Util.datetime64_formatter,
                    Trip.STOPTIMES_COLUMN_ARRIVAL_TIME   :Util.datetime64_formatter,
                    Trip.STOPTIMES_COLUMN_DEPARTURE_TIME :Util.datetime64_formatter})))

                # Kick out the bumped passengers
                passengers_df = pandas.merge(left     =passengers_df,
                                             right    =bumped_pax_boards[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                          Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
                                                                          'bump']],
                                             on       =[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                        Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM],
                                             how      ='left')
                assert(passengers_df_len == len(passengers_df))
                passengers_df = passengers_df[passengers_df.bump != True]
                FastTripsLogger.info("        Bumped %d passengers; passenger_df length %d -> %d" %
                                     (len(bumped_pax_boards), passengers_df_len, len(passengers_df)))
                passengers_df.drop('bump', axis=1, inplace=True)
                passengers_df_len = len(passengers_df)

                # recreate
                passenger_trips     = Assignment.get_passenger_trips(passengers_df, veh_trips_df)
                passenger_trips_len = len(passenger_trips)

                new_bump_wait = bumped_pax_boards[[Trip.STOPTIMES_COLUMN_TRIP_ID,
                                                   Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                   Trip.STOPTIMES_COLUMN_STOP_ID,
                                                   'A_id','A_seq',
                                                   Assignment.SIM_COL_PAX_ARRIVE_TIME]].groupby([\
                                                     Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,'A_id','A_seq']).first()
                new_bump_wait.reset_index(drop=False, inplace=True)
                new_bump_wait.rename(columns={'A_id' :Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                              'A_seq':Trip.STOPTIMES_COLUMN_STOP_SEQUENCE}, inplace=True)

                FastTripsLogger.debug("new_bump_wait (%d rows, showing head):\n%s" %
                    (len(new_bump_wait), new_bump_wait.head().to_string(formatters=\
                   {Assignment.SIM_COL_PAX_ARRIVE_TIME:Util.datetime64_formatter})))

                # incorporate it into the bump wait df
                if type(Assignment.bump_wait_df) == type(None):
                    Assignment.bump_wait_df = new_bump_wait
                else:
                    Assignment.bump_wait_df = Assignment.bump_wait_df.append(new_bump_wait)
                    Assignment.bump_wait_df.drop_duplicates([Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                             Trip.STOPTIMES_COLUMN_STOP_SEQUENCE], inplace=True)

                # incorporate it into the bump wait
                # This is (trip_id, stop_id) -> Timestamp
                bump_wait_dict  = Assignment.bump_wait_df.set_index([Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                                     Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                                                     Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]).to_dict()[Assignment.SIM_COL_PAX_ARRIVE_TIME]
                bump_wait_dict2 = {k: v.to_datetime() for k, v in bump_wait_dict.iteritems()}
                Assignment.bump_wait.update(bump_wait_dict2)

                # FastTripsLogger.debug("Bump wait ---------")
                # for key,val in Assignment.bump_wait.iteritems():
                #     FastTripsLogger.debug("(%10s, %10s, %10s) -> %s" %
                #                           (str(key[0]), str(key[1]), str(key[2]), val.strftime("%H:%M:%S")))

                bump_iter += 1
                FastTripsLogger.info("        -> complete loop iter %d" % bump_iter)

        if type(Assignment.bump_wait_df) == pandas.DataFrame and len(Assignment.bump_wait_df) > 0:
            Assignment.bump_wait_df[Assignment.SIM_COL_PAX_ARRIVE_TIME_MIN] = \
                Assignment.bump_wait_df[Assignment.SIM_COL_PAX_ARRIVE_TIME].map(lambda x: (60.0*x.hour) + x.minute + (x.second/60.0))

        FastTripsLogger.debug("Bumped passenger ids: %s" % str(Assignment.bumped_person_ids))
        FastTripsLogger.debug("Bumped path ids: %s" % str(Assignment.bumped_trip_list_nums))
        if type(Assignment.bump_wait_df) == pandas.DataFrame and len(Assignment.bump_wait_df) > 0:
            FastTripsLogger.debug("Bump_wait_df:\n%s" % Assignment.bump_wait_df.to_string(formatters=\
                {Assignment.SIM_COL_PAX_ARRIVE_TIME :Util.datetime64_formatter}))

        ######################################################################################################
        FastTripsLogger.info("Step 4. Convert times to strings")

        ######         TODO: this is really catering to output format; an alternative might be more appropriate
        passenger_trips.loc[:,  'board_time_str'] = passenger_trips[Assignment.SIM_COL_PAX_BOARD_TIME ].apply(Util.datetime64_formatter)
        passenger_trips.loc[:,'arrival_time_str'] = passenger_trips[Assignment.SIM_COL_PAX_ARRIVE_TIME].apply(Util.datetime64_formatter)
        passenger_trips.loc[:, 'alight_time_str'] = passenger_trips[Assignment.SIM_COL_PAX_ALIGHT_TIME].apply(Util.datetime64_formatter)
        assert(len(passenger_trips) == passenger_trips_len)

        # Aggregate (by joining) across each passenger + path
        ptrip_group = passenger_trips.groupby([Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                               Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM])
        # these are Series
        board_time_str   = ptrip_group['board_time_str'  ].apply(lambda x:','.join(x))
        arrival_time_str = ptrip_group['arrival_time_str'].apply(lambda x:','.join(x))
        alight_time_str  = ptrip_group['alight_time_str' ].apply(lambda x:','.join(x))

        # Aggregate other fields across each passenger + path
        pax_exp_df = passengers_df.groupby([Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                            Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM]).agg(
            {'pathmode'     :'first',  # path mode
             'A_id'         :'first',  # origin
             'B_id'         :'last',   # destination
             'A_time'       :'first',  # start time
             'B_time'       :'last',   # end time
             'cost'         :'first',  # total travel cost is calculated for the whole path
            })

        # Put them together and return
        assert(len(pax_exp_df) == len(board_time_str))
        pax_exp_df = pandas.concat([pax_exp_df,
                                    board_time_str,
                                    arrival_time_str,
                                    alight_time_str], axis=1)
        # print pax_exp_df.to_string(formatters={'A_time':Assignment.datetime64_min_formatter,
        #                                        'B_time':Assignment.datetime64_min_formatter})

        if len(Assignment.TRACE_PERSON_IDS) > 0:
            simulated_person_ids = passengers_df[Passenger.TRIP_LIST_COLUMN_PERSON_ID].values

        for trace_pax in Assignment.TRACE_PERSON_IDS:
            if trace_pax not in simulated_person_ids:
                FastTripsLogger.debug("Passenger %s not in final simulated list" % trace_pax)
            else:
                FastTripsLogger.debug("Final passengers_df for %s\n%s" % \
                   (str(trace_pax),
                    passengers_df.loc[passengers_df[Passenger.TRIP_LIST_COLUMN_PERSON_ID]==trace_pax].to_string(formatters=\
                   {'A_time'               :Util.datetime64_min_formatter,
                    'B_time'               :Util.datetime64_min_formatter,
                    'linktime'             :Util.timedelta_formatter,
                    'board_time'           :Util.datetime64_min_formatter,
                    'alight_time'          :Util.datetime64_min_formatter,
                    'board_time_prev'      :Util.datetime64_min_formatter,
                    'alight_time_prev'     :Util.datetime64_min_formatter,
                    'B_time_prev'          :Util.datetime64_min_formatter,
                    'A_time_next'          :Util.datetime64_min_formatter,})))

                FastTripsLogger.debug("Passengers experienced times for %s\n%s" % \
                   (str(trace_pax),
                    pax_exp_df.loc[trace_pax].to_string(formatters=\
                   {'A_time'               :Util.datetime64_min_formatter,
                    'B_time'               :Util.datetime64_min_formatter})))

        return (len(pax_exp_df), veh_loaded_df, pax_exp_df)

    @staticmethod
    def print_load_profile(FT, veh_trips_df, output_dir):
        """
        Print the load profile output
        """
        # reset columns
        print_veh_trips_df = veh_trips_df

        Trip.calculate_dwell_times(print_veh_trips_df)
        print_veh_trips_df = FT.trips.calculate_headways(print_veh_trips_df)

        # recode/reformat
        print_veh_trips_df['traveledDist']  = -1
        print_veh_trips_df['departureTime'] = print_veh_trips_df.departure_time.apply(Util.datetime64_min_formatter)
        # reorder
        columns = [Trip.TRIPS_COLUMN_ROUTE_ID,
                   Trip.TRIPS_COLUMN_TRIP_ID,
                   Trip.TRIPS_COLUMN_DIRECTION_ID,
                   Trip.STOPTIMES_COLUMN_STOP_ID,
                   'traveledDist',
                   'departureTime',
                   'headway',
                   'dwell_time',
                   'boards',
                   'alights',
                   'onboard']
        # this one may not be in their; direction_id is optional
        if Trip.TRIPS_COLUMN_DIRECTION_ID not in print_veh_trips_df.columns.values:
            columns.remove(Trip.TRIPS_COLUMN_DIRECTION_ID)
        print_veh_trips_df = print_veh_trips_df[columns]

        load_file = open(os.path.join(output_dir, "ft_output_loadProfile.txt"), 'w')
        print_veh_trips_df.to_csv(load_file,
                              float_format="%.2f",
                              index=False)
        load_file.close()

def find_trip_based_paths_process_worker(iteration, worker_num, input_network_dir, input_demand_dir,
                                         output_dir, todo_path_queue, done_queue, hyperpath, bump_wait_df):
    """
    Process worker function.  Processes all the paths in queue.

    todo_queue has (passenger_id, path object)
    """
    worker_str = "_worker%02d" % worker_num

    # Setup a new FT instance for this worker.
    # This is just for reading input files into the FT structures,
    # but it won't change the FT structures themselves (so it's a read-only instance).
    #
    # You'd think we could have just passed the FT structure to this method but that would involve pickling/unpickling the
    # data and ends up meaning it takes a *really long time* to start the new process ~ 2 minutes per process.
    # Simply reading the input files again is faster.  No need to read the demand tho.
    from .FastTrips import FastTrips
    worker_FT = FastTrips(input_network_dir=input_network_dir, input_demand_dir=input_demand_dir, output_dir=output_dir, 
                          is_child_process=True, logname_append=worker_str, appendLog=True if iteration > 1 else False)

    FastTripsLogger.info("Iteration %d Worker %2d starting" % (iteration, worker_num))

    Assignment.initialize_fasttrips_extension(worker_num, output_dir, worker_FT)
    if iteration > 1:
        Assignment.set_fasttrips_bump_wait(bump_wait_df)

    while True:
        # go through my queue -- check if we're done
        todo = todo_path_queue.get()
        if todo == 'DONE':
            done_queue.put('DONE')
            FastTripsLogger.debug("Received DONE from the todo_path_queue")
            return

        # do the work
        path = todo

        FastTripsLogger.info("Processing person %20s path %d" % (path.person_id, path.trip_list_id_num))

        trace_person = False
        if path.person_id in Assignment.TRACE_PERSON_IDS:
            FastTripsLogger.debug("Tracing assignment of person %s" % path.person_id)
            trace_person = True

        try:
            (cost, return_states) = Assignment.find_trip_based_path(iteration, worker_FT, path, hyperpath, trace=trace_person)
            done_queue.put( (path.trip_list_id_num, cost, return_states) )
        except:
            FastTripsLogger.exception('Exception')
            # call it a day
            done_queue.put('DONE')
            return
