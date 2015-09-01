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
import Queue
import collections,datetime,math,multiprocessing,os,random,sys,traceback
import numpy,pandas
import _fasttrips

from .Logger import FastTripsLogger, setupLogging
from .Passenger import Passenger
from .Path import Path
from .Stop import Stop
from .TAZ import TAZ
from .Trip import Trip

class Assignment:
    """
    Assignment class.  Documentation forthcoming.

    """

    #: Configuration: Maximum number of iterations to remove capacity violations. When
    #: the transit system is not crowded or when capacity constraint is
    #: relaxed the model will terminate after the first iteration
    ITERATION_FLAG                  = 1

    ASSIGNMENT_TYPE_SIM_ONLY        = 'Simulation Only'
    ASSIGNMENT_TYPE_DET_ASGN        = 'Deterministic Assignment'
    ASSIGNMENT_TYPE_STO_ASGN        = 'Stochastic Assignment'
    #: Configuration: Assignment Type
    #: 'Simulation Only' - No Assignment (only simulation, given paths in the input)
    #: 'Deterministic Assignment'
    #: 'Stochastic Assignment'
    ASSIGNMENT_TYPE                 = ASSIGNMENT_TYPE_DET_ASGN

    #: Configuration: Simulation flag. It should be on for iterative assignment. In a one shot
    #: assignment with simulation flag off, the passengers are assigned to
    #: paths but are not loaded to the network.
    SIMULATION_FLAG                 = True

    #: Configuration: Passenger trajectory output flag. Passengers' path and time will be
    #: reported if this flag is on. Note that the simulation flag should be on for
    #: passengers' time.
    OUTPUT_PASSENGER_TRAJECTORIES   = True

    #: Configuration: Path time-window. This is the time in which the paths are generated.
    #: E.g. with a typical 30 min window, any path within 30 min of the
    #: departure time will be checked.
    PATH_TIME_WINDOW                = datetime.timedelta(minutes = 30)

    #: Configuration: Create skims flag. This is specific to the travel demand models
    #: (not working in this version)
    CREATE_SKIMS                    = False

    #: Configuration: Beginning of the time period for which the skim is required.
    #: (in minutes from start of day)
    SKIM_START_TIME                 = 300

    #: Configuration: End of the time period for which the skim is required
    #: (minutes from start of day)
    SKIM_END_TIME                   = 600

    #: Route choice configuration: Dispersion parameter in the logit function.
    #: Higher values result in less stochasticity. Must be nonnegative. 
    #: If unknown use a value between 0.5 and 1
    DISPERSION_PARAMETER            = 1.0

    #: Route choice configuration: Use vehicle capacity constraints
    CAPACITY_CONSTRAINT             = False

    #: Use this as the date
    TODAY                           = datetime.date.today()

    #: Trace these passengers
    TRACE_PASSENGER_IDS             = [22267]

    #: Number of processes to use for path finding (via :py:mod:`multiprocessing`)
    #: Set to 1 to run everything in this process
    #: Set to less than 1 to use the result of :py:func:`multiprocessing.cpu_count`
    #: Set to positive integer greater than 1 to set a fixed number of processes
    NUMBER_OF_PROCESSES             = 14

    #: Extra time so passengers don't get bumped (?)
    BUMP_BUFFER                     = datetime.timedelta(minutes = 5)

    #: This is the only simulation state that exists across iterations
    #: It's a dictionary of (trip_id, stop_id) -> earliest time a bumped passenger started waiting
    bump_wait                       = {}
    bump_wait_df                    = None

    #: This is a :py:class:`set` of bumped passenger IDs.  For multiple-iteration assignment,
    #: this determines which passengers to assign.
    bumped_passenger_ids            = set()
    bumped_path_ids                 = set()

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
    #: loads, and iterate until we have no capacity issues.
    BUMP_ONE_AT_A_TIME              = True

    #: assignment results - Passenger table
    PASSENGERS_CSV                  = r"passengers_df_iter%d.csv"

    @staticmethod
    def datetime64_formatter(x):
        """
        Formatter to convert :py:class:`numpy.datetime64` to string that looks like `HH:MM.SS`
        """
        return pandas.to_datetime(x).strftime('%H:%M.%S')

    @staticmethod
    def datetime64_min_formatter(x):
        """
        Formatter to convert :py:class:`numpy.datetime64` to minutes after minutes
        (with two decimal places)
        """
        return '%.2f' % (pandas.to_datetime(x).hour*60.0 + \
                         pandas.to_datetime(x).minute + \
                         pandas.to_datetime(x).second/60.0)

    @staticmethod
    def timedelta_formatter(x):
        """
        Formatter to convert :py:class:`numpy.timedelta64` to string that looks like `4m 35.6s`
        """
        seconds = x/numpy.timedelta64(1,'s')
        minutes = int(seconds/60)
        seconds -= minutes*60
        return '%4dm %04.1fs' % (minutes,seconds)

    def __init__(self):
        """
        This does nothing.  Assignment methods are static methods for now.
        """
        pass

    @staticmethod
    def read_configuration():
        """
        Read the configuration parameters and override the above
        """
        raise Exception("Not implemented")

    @staticmethod
    def initialize_fasttrips_extension(process_number, output_dir, FT):
        """
        Initialize the C++ fasttrips extension by passing it the network supply.
        """
        FastTripsLogger.debug("Initializing fasttrips extension for process number %d" % process_number)
        # make a copy to convert the 2-column MultiIndex to a 2D array easily
        access_links_df = FT.tazs.access_links_df.reset_index()
        # create access and egress cost
        access_links_df[TAZ.ACCLINKS_COLUMN_ACC_COST] = access_links_df[TAZ.ACCLINKS_COLUMN_TIME_MIN]*Path.WALK_ACCESS_TIME_WEIGHT
        access_links_df[TAZ.ACCLINKS_COLUMN_EGR_COST] = access_links_df[TAZ.ACCLINKS_COLUMN_TIME_MIN]*Path.WALK_EGRESS_TIME_WEIGHT

        FastTripsLogger.debug("\n" + str(access_links_df.head()))
        FastTripsLogger.debug("\n" + str(access_links_df.tail()))

        # make a copy for index flattening
        stop_times_df = FT.trips.stop_times_df.reset_index()

        # transfers copy for index flattening, cost
        transfers_df = FT.stops.transfers_df.reset_index()
        transfers_df[Stop.TRANSFERS_COLUMN_COST] = transfers_df[Stop.TRANSFERS_COLUMN_TIME_MIN]*Path.WALK_TRANSFER_TIME_WEIGHT

        _fasttrips.initialize_supply(output_dir, process_number,
                                     access_links_df[[TAZ.ACCLINKS_COLUMN_TAZ,
                                                      TAZ.ACCLINKS_COLUMN_STOP    ]].as_matrix().astype('int32'),
                                     access_links_df[[TAZ.ACCLINKS_COLUMN_TIME_MIN,
                                                      TAZ.ACCLINKS_COLUMN_ACC_COST,
                                                      TAZ.ACCLINKS_COLUMN_EGR_COST]].as_matrix().astype('float64'),
                                     stop_times_df[[Trip.STOPTIMES_COLUMN_TRIP_ID,
                                                    Trip.STOPTIMES_COLUMN_SEQUENCE,
                                                    Trip.STOPTIMES_COLUMN_STOP_ID]].as_matrix().astype('int32'),
                                     stop_times_df[[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN,
                                                    Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN]].as_matrix().astype('float64'),
                                     transfers_df[[Stop.TRANSFERS_COLUMN_FROM_STOP,
                                                   Stop.TRANSFERS_COLUMN_TO_STOP]].as_matrix().astype('int32'),
                                     transfers_df[[Stop.TRANSFERS_COLUMN_TIME_MIN,
                                                   Stop.TRANSFERS_COLUMN_COST]].as_matrix().astype('float64'))

    @staticmethod
    def set_fasttrips_bump_wait(bump_wait_df):
        """
        Sends the bump wait information to the fasttrips extension
        """
        _fasttrips.set_bump_wait(bump_wait_df[['trip_id','stop_seq','stop_id']].as_matrix().astype('int32'),
                                 bump_wait_df['A_time_min'].values.astype('float64'))


    @staticmethod
    def assign_paths(output_dir, FT):
        """
        Finds the paths for the passengers.
        """

        Assignment.bump_wait = {}
        for iteration in range(1,Assignment.ITERATION_FLAG+1):
            FastTripsLogger.info("***************************** ITERATION %d **************************************" % iteration)

            if Assignment.ASSIGNMENT_TYPE == Assignment.ASSIGNMENT_TYPE_SIM_ONLY or \
               os.path.exists(os.path.join(output_dir, Assignment.PASSENGERS_CSV % iteration)):
                FastTripsLogger.info("Simulation only")
                (num_paths_found, passengers_df) = Assignment.read_assignment_results(output_dir, iteration)

            else:
                num_paths_found = Assignment.generate_paths(FT, output_dir, iteration)
                passengers_df      = Assignment.setup_passengers(FT, output_dir, iteration)

            veh_trips_df       = Assignment.setup_trips(FT)

            if Assignment.OUTPUT_PASSENGER_TRAJECTORIES:
                Assignment.print_passenger_paths(passengers_df, output_dir)

            if Assignment.SIMULATION_FLAG == True:
                FastTripsLogger.info("****************************** SIMULATING *****************************")
                (num_passengers_arrived,veh_trips_df,pax_exp_df) = Assignment.simulate(FT, passengers_df, veh_trips_df)

            if Assignment.OUTPUT_PASSENGER_TRAJECTORIES:
                Assignment.print_passenger_times(pax_exp_df, output_dir)

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
        Assignment.print_load_profile(veh_trips_df, output_dir)

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

        est_paths_to_find   = len(FT.passengers)
        if iteration > 1:
            est_paths_to_find = len(Assignment.bumped_path_ids) + len(Assignment.reassign_nonlast_paths)

        info_freq           = pow(10, int(math.log(est_paths_to_find+1,10)-2))
        if info_freq < 1: info_freq = 1

        num_processes       = Assignment.NUMBER_OF_PROCESSES
        if  Assignment.NUMBER_OF_PROCESSES < 1:
            num_processes   = multiprocessing.cpu_count()

        # this is probalby time consuming... put in a try block
        try:
            # Setup multiprocessing processes
            if num_processes > 1:
                todo_queue      = multiprocessing.Queue()
                done_queue      = multiprocessing.Queue()
                for process_idx in range(1, 1+num_processes):
                    FastTripsLogger.info("Starting worker process %2d" % process_idx)
                    process_list.append(multiprocessing.Process(target=find_trip_based_paths_process_worker,
                                                                args=(iteration, process_idx, FT.input_dir, FT.output_dir,
                                                                      todo_queue, done_queue,
                                                                      Assignment.ASSIGNMENT_TYPE==Assignment.ASSIGNMENT_TYPE_STO_ASGN,
                                                                      Assignment.bump_wait_df)))
                    process_list[-1].start()
            else:
                Assignment.initialize_fasttrips_extension(0, output_dir, FT)

            # process tasks or send tasks to workers for processing
            num_paths_found_prev  = 0
            num_paths_found_now   = 0
            for path_id,passenger in FT.passengers.iteritems():
                passenger_id = passenger.passenger_id

                if not passenger.path.goes_somewhere(): continue

                if iteration > 1 and passenger_id not in Assignment.bumped_passenger_ids and path_id not in Assignment.reassign_nonlast_paths:
                    num_paths_found_prev += 1
                    continue

                if num_processes > 1:
                    todo_queue.put( (passenger, passenger.path) )
                else:
                    trace_passenger = False
                    if passenger_id in Assignment.TRACE_PASSENGER_IDS:
                        FastTripsLogger.debug("Tracing assignment of passenger %s" % str(passenger_id))
                        trace_passenger = True

                    # do the work
                    (asgn_iters, return_states) = Assignment.find_trip_based_path(FT, passenger, passenger.path,
                                                                                  Assignment.ASSIGNMENT_TYPE==Assignment.ASSIGNMENT_TYPE_STO_ASGN,
                                                                                  trace=trace_passenger)
                    passenger.path.states = return_states

                    if passenger.path.path_found():
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
                        passenger_id    = result[0]
                        path_id         = result[1]
                        asgn_iters      = result[2]
                        return_states   = result[3]

                        # find passenger with path_id and set return
                        FT.passengers[path_id].path.states = return_states
                        if FT.passengers[path_id].path.path_found():
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
    def find_trip_based_path(FT, passenger, path, hyperpath, trace):
        """
        Perform trip-based path search.

        Will do so either backwards (destination to origin) if :py:attr:`Path.direction` is :py:attr:`Path.DIR_OUTBOUND`
        or forwards (origin to destination) if :py:attr:`Path.direction` is :py:attr:`Path.DIR_INBOUND`.

        Returns (number of label iterations, return_states).

        :param FT: fasttrips data
        :type FT: a :py:class:`FastTrips` instance
        :param passenger: the passenger whose path we're finding
        :type passenger: a :py:class:`Passenger` instance
        :param path: the path to fill in
        :type path: a :py:class:`Path` instance
        :param hyperpath: pass True to use a stochastic hyperpath-finding algorithm, otherwise a deterministic shortest path
                          search algorithm will be use.
        :type hyperpath: boolean
        :param trace: pass True if this path should be traced to the debug log
        :type trace: boolean

        """
        # FastTripsLogger.debug("C++ extension start")
        # send it to the C++ extension
        (ret_ints, ret_doubles) = _fasttrips.find_path(passenger.passenger_id , path.path_id, hyperpath, path.origin_taz_id, path.destination_taz_id,
                             1 if path.outbound() else 0, float(path.pref_time_min),
                             1 if trace else 0)
        # FastTripsLogger.debug("C++ extension complete")

        # Put the results into an ordered dict statelist
        return_states = collections.OrderedDict()
        midnight = datetime.datetime.combine(Assignment.TODAY, datetime.time())

        for index in range(ret_ints.shape[0]):
            mode = ret_ints[index,1]
            if mode == -100:
                mode = Path.STATE_MODE_ACCESS
            elif mode == -101:
                mode = Path.STATE_MODE_EGRESS
            elif mode == -102:
                mode = Path.STATE_MODE_TRANSFER

            if hyperpath:
                return_states[ret_ints[index, 0]] = [
                              ret_doubles[index,0],                                         # label,
                              midnight + datetime.timedelta(minutes=ret_doubles[index,1]),  # departure/arrival time
                              mode,                                                         # departure/arrival mode
                              ret_ints[index,2],                                            # successor/predecessor
                              ret_ints[index,3],                                            # sequence
                              ret_ints[index,4],                                            # sequence succ/pred
                              datetime.timedelta(minutes=ret_doubles[index,2]),             # link time
                              ret_doubles[index,3],                                         # cost
                              midnight + datetime.timedelta(minutes=ret_doubles[index,4])   # arrival/departure time
                              ]
            else:
                return_states[ret_ints[index, 0]] = [
                              datetime.timedelta(minutes=ret_doubles[index,0]),              # label,
                              midnight + datetime.timedelta(minutes=ret_doubles[index,1]),  # departure/arrival time
                              mode,                                                         # departure/arrival mode
                              ret_ints[index,2],                                            # successor/predecessor
                              ret_ints[index,3],                                            # sequence
                              ret_ints[index,4],                                            # sequence succ/pred
                              datetime.timedelta(minutes=ret_doubles[index,2]),             # link time
                              datetime.timedelta(minutes=ret_doubles[index,3]),             # cost
                              midnight + datetime.timedelta(minutes=ret_doubles[index,4])   # arrival/departure time
                              ]
        return (0, return_states)

    @staticmethod
    def print_passenger_paths(passengers_df, output_dir):
        """
        Print the passenger paths.
        """
        paths_out = open(os.path.join(output_dir, "ft_output_passengerPaths.dat"), 'w')
        Path.write_paths(passengers_df, paths_out)
        paths_out.close()

    @staticmethod
    def print_passenger_times(pax_exp_df, output_dir):
        """
        Print the passenger times.
        """
        # reset columns
        print_pax_exp_df = pax_exp_df.reset_index()

        print_pax_exp_df.reset_index(inplace=True)
        print_pax_exp_df['A_time_str'] = print_pax_exp_df['A_time'].apply(Assignment.datetime64_min_formatter)
        print_pax_exp_df['B_time_str'] = print_pax_exp_df['B_time'].apply(Assignment.datetime64_min_formatter)

        # rename columns
        print_pax_exp_df.rename(columns=
            {'passenger_id'         :'passengerId',
             'pathmode'             :'mode',
             'A_id'                 :'originTaz',
             'B_id'                 :'destinationTaz',
             'A_time_str'           :'startTime',
             'B_time_str'           :'endTime',
             'arrival_time_str'     :'arrivalTimes',
             'board_time_str'       :'boardingTimes',
             'alight_time_str'      :'alightingTimes'
             }, inplace=True)

        # recode/reformat
        print_pax_exp_df[['originTaz','destinationTaz']] = print_pax_exp_df[['originTaz','destinationTaz']].astype(int)

        # reorder
        print_pax_exp_df = print_pax_exp_df[[
            'passengerId',
            'mode',
            'originTaz',
            'destinationTaz',
            'startTime',
            'endTime',
            'arrivalTimes',
            'boardingTimes',
            'alightingTimes',
            'travelCost']]

        times_out = open(os.path.join(output_dir, "ft_output_passengerTimes.dat"), 'w')
        print_pax_exp_df.to_csv(times_out,
                                sep="\t", float_format="%.2f", index=False)
        times_out.close()

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

        uniq_pax = passengers_df[['passenger_id','path_id']].drop_duplicates(subset=['passenger_id','path_id'])
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
        `passenger_id`            int64  the :py:attr:`Passenger.passenger_id`
        `path_id`                 int64  a sequential integer ID unique to each :py:class:`Path` instance
        `pathdir`                 int64  the :py:attr:`Path.direction`
        `pathmode`                int64  the :py:attr:`Path.mode`
        `linkmode`               object  the mode of the link, one of :py:attr:`Path.STATE_MODE_ACCESS`, :py:attr:`Path.STATE_MODE_EGRESS`,
                                         :py:attr:`Path.STATE_MODE_TRANSFER` or :py:attr:`Path.STATE_MODE_TRIP`.  Paths will always start with
                                         access, followed by trips with transfers in between, and ending in an egress following the last trip.
        `trip_id`               float64  the :py:attr:`Trip.trip_id` for trip links.  Set to :py:attr:`numpy.nan` for non-trip links.
        `A_id`                  float64  the :py:attr:`Stop.stop_id` at the start of the link, or a :py:attr:`TAZ.taz_id` for access links
        `B_id`                  float64  the :py:attr:`Stop.stop_id` at the end of the link, or a :py:attr:`TAZ.taz_id` for access links
        `A_seq`,                  int64  the sequence number for the stop at the start of the link, or -1 for access links
        `B_seq`,                  int64  the sequence number for the stop at the start of the link, or -1 for access links
        `A_time`         datetime64[ns]  the time the passenger arrives at `A_id`
        `B_time`         datetime64[ns]  the time the passenger arrives at `B_id`
        `linktime`      timedelta64[ns]  the time spent on the link
        ==============  ===============  =====================================================================================================

        Additionally, this method writes out the dataframe to a csv at :py:attr:`Assignment.PASSENGERS_CSV` in the given `output_dir`
        and labeled with the given `iteration`.
        """
        mylist = []
        for path_id,passenger in FT.passengers.iteritems():
            if not passenger.path.goes_somewhere():   continue
            if not passenger.path.path_found():       continue

            # OUTBOUND passengers have states like this:
            #    stop:          label    departure   dep_mode  successor linktime
            # orig_taz                                 Access    b stop1
            #  b stop1                                  trip1    a stop2
            #  a stop2                               Transfer    b stop3
            #  b stop3                                  trip2    a stop4
            #  a stop4                                 Egress   dest_taz
            #
            # e.g. (preferred arrival = 404 = 06:44:00)
            #    stop:          label    departure   dep_mode  successor linktime
            #      29: 0:24:29.200000     06:19:30     Access    23855.0 0:11:16.200000
            # 23855.0: 0:13:13            06:30:47   21649852    38145.0 0:06:51
            # 38145.0: 0:06:22            06:37:38   Transfer      38650 0:00:42
            #   38650: 0:05:40            06:38:20   25009729    76730.0 0:03:51.400000
            # 76730.0: 0:01:48.600000     06:42:11     Egress         18 0:01:48.600000
            #
            # INBOUND passengers have states like this
            #   stop:          label      arrival   arr_mode predecessor linktime
            # dest_taz                                 Egress    a stop4
            #  a stop4                                  trip2    b stop3
            #  b stop3                               Transfer    a stop2
            #  a stop2                                  trip1    b stop1
            #  b stop1                                 Access   orig_taz
            #
            # e.g. (preferred departure = 447 = 07:27:00)
            #    stop:          label      arrival   arr_mode predecessor linktime
            #    1586: 0:49:06            08:16:06     Egress    73054.0 0:06:27
            # 73054.0: 0:42:39            08:09:39   24201511    69021.0 0:13:11.600000
            # 69021.0: 0:29:27.400000     07:56:27   Transfer      68007 0:00:26.400000
            #   68007: 0:29:01            07:56:01   25539006    64065.0 0:28:11.200000
            # 64065.0: 0:00:49.800000     07:27:49     Access       3793 0:00:49.800000
            prev_linkmode = None
            if len(passenger.path.states) > 1:
                state_list = passenger.path.states.keys()
                if not passenger.path.outbound(): state_list = list(reversed(state_list))

                for state_id in state_list:
                    state           = passenger.path.states[state_id]
                    linkmode        = state[Path.STATE_IDX_DEPARRMODE]
                    trip_id         = None
                    if linkmode not in [Path.STATE_MODE_ACCESS, Path.STATE_MODE_TRANSFER, Path.STATE_MODE_EGRESS]:
                        trip_id     = linkmode
                        linkmode    = Path.STATE_MODE_TRIP

                    a_id            = state_id
                    b_id            = state[Path.STATE_IDX_SUCCPRED]
                    a_seq           = state[Path.STATE_IDX_SEQ]
                    b_seq           = state[Path.STATE_IDX_SEQ_SUCCPRED]
                    a_time          = state[Path.STATE_IDX_DEPARR]
                    b_time          = a_time + state[Path.STATE_IDX_LINKTIME]
                    if not passenger.path.outbound():
                        a_id        = state[Path.STATE_IDX_SUCCPRED]
                        b_id        = state_id
                        a_seq       = state[Path.STATE_IDX_SEQ_SUCCPRED]
                        b_seq       = state[Path.STATE_IDX_SEQ]
                        b_time      = state[Path.STATE_IDX_DEPARR]
                        a_time      = b_time - state[Path.STATE_IDX_LINKTIME]

                    # two trips in a row -- insert zero-walk transfer
                    if linkmode == Path.STATE_MODE_TRIP and prev_linkmode == Path.STATE_MODE_TRIP:
                        row = [passenger.passenger_id,
                               path_id,
                               passenger.path.direction,
                               passenger.path.mode,
                               Path.STATE_MODE_TRANSFER,
                               None,
                               a_id,
                               a_id,
                               a_seq,
                               a_seq,
                               a_time,
                               a_time,
                               datetime.timedelta()
                              ]
                        mylist.append(row)

                    row = [passenger.passenger_id,
                           path_id,
                           passenger.path.direction,
                           passenger.path.mode,
                           linkmode,
                           trip_id,
                           a_id,
                           b_id,
                           a_seq,
                           b_seq,
                           a_time,
                           b_time,
                           state[Path.STATE_IDX_LINKTIME]]
                    mylist.append(row)

                    prev_linkmode = linkmode
        df =  pandas.DataFrame(mylist,
                               columns=['passenger_id', 'path_id',
                                        'pathdir',  # for debugging
                                        'pathmode', # for output
                                        'linkmode', 'trip_id',
                                        'A_id','B_id',
                                        'A_seq','B_seq',
                                        'A_time', 'B_time',
                                        'linktime'])
        FastTripsLogger.debug("Setup passengers dataframe:\n%s" % str(df.dtypes))
        df.to_csv(os.path.join(output_dir, Assignment.PASSENGERS_CSV % iteration), index=False)
        FastTripsLogger.info("Wrote passengers dataframe to %s" % os.path.join(output_dir, Assignment.PASSENGERS_CSV % iteration))
        return df

    @staticmethod
    def setup_trips(FT):
        """
        Sets up and returns a :py:class:`pandas.DataFrame` where each row contains a leg of a transit vehicle trip.
        # 2015-06-22 15:42:34 DEBUG Setup vehicle trips dataframe:
        # routeId                  int64
        # shapeId                  int64
        # direction                int64
        # trip_id                  int64
        # stop_seq                 int64
        # stop_id                  int64
        # capacity                 int64
        # arrive_time     datetime64[ns]
        # depart_time     datetime64[ns]
        # service_type             int64
        """
        # join with trips to get additional fields
        df = pandas.merge(left= FT.trips.stop_times_df.reset_index(),
                          right= FT.trips.trips_df.reset_index(),
                          how='left')
        # print df.head()
        assert(len(FT.trips.stop_times_df) == len(df))
        df.rename(columns=
           {Trip.TRIPS_COLUMN_DIRECTION_ID      :'direction',
            Trip.STOPTIMES_COLUMN_TRIP_ID       :'trip_id',
            Trip.STOPTIMES_COLUMN_SEQUENCE      :'stop_seq',
            Trip.STOPTIMES_COLUMN_STOP_ID       :'stop_id',
            Trip.TRIPS_COLUMN_CAPACITY          :'capacity',
            Trip.STOPTIMES_COLUMN_ARRIVAL_TIME  :'arrive_time',
            Trip.STOPTIMES_COLUMN_DEPARTURE_TIME:'depart_time',
            Trip.TRIPS_COLUMN_SERVICE_TYPE      :'service_type'
            }, inplace=True)
        return df

    @staticmethod
    def simulate(FT, passengers_df, veh_trips_df):
        """
        Actually assign the passengers trips to the vehicles.

        .. todo:: Remove step zero.  Duplicate passenger IDs should be ok because we can generate unique path IDs.

        """
        passengers_df_len       = len(passengers_df)
        veh_trips_df_len        = len(veh_trips_df)

        ######################################################################################################
        FastTripsLogger.info("Step 0. Drop passengers with duplicate passenger IDs to match old FAST-TrIPs behavior")
        # TODO: Remove this.
        #  Old FAST-TrIPs handles multiple trips for a single passenger ID by dropping the first
        # ones.  Replicate that here.
        passengers_dedupe       = passengers_df[['passenger_id','path_id']].copy()
        passengers_dedupe.drop_duplicates(subset='passenger_id',take_last=True, inplace=True)
        passengers_dedupe['keep'] = True

        # these are the passengers we dropped from simulation
        paths_not_kept = passengers_df[['passenger_id','path_id']].copy()
        paths_not_kept.drop_duplicates(subset=['passenger_id','path_id'], inplace=True)
        paths_not_kept = pandas.merge(left   =paths_not_kept,             right   =passengers_dedupe,
                                      left_on=['passenger_id','path_id'], right_on=['passenger_id','path_id'],
                                      how    ='left')
        paths_not_kept = paths_not_kept[paths_not_kept.keep!=True]
        Assignment.reassign_nonlast_paths = paths_not_kept.path_id.tolist()

        passengers_df = pandas.merge(left   =passengers_df,              right   =passengers_dedupe,
                                     left_on=['passenger_id','path_id'], right_on=['passenger_id','path_id'],
                                     how    ='left')
        passengers_df = passengers_df[passengers_df.keep==True]
        passengers_df.drop('keep', axis=1, inplace=True)
        FastTripsLogger.debug(" -> Went from %d passengers to %d passengers" % (passengers_df_len, len(passengers_df)))
        passengers_df_len = len(passengers_df)



        # veh_trips_df.set_index(['trip_id','stop_seq','stop_id'],verify_integrity=True,inplace=True)
        # FastTripsLogger.debug("veh_trips_df types = \n%s" % str(veh_trips_df.dtypes))
        FastTripsLogger.debug("veh_trips_df: \n%s" % veh_trips_df.head(20).to_string(formatters=
            {'arrive_time'          :Assignment.datetime64_formatter,
             'depart_time'          :Assignment.datetime64_formatter,
             'waitqueue_start_time' :Assignment.datetime64_formatter}))

        for trace_pax in Assignment.TRACE_PASSENGER_IDS:
            FastTripsLogger.debug("Initial passengers_df for %s\n%s" % \
               (str(trace_pax),
                passengers_df.loc[passengers_df.passenger_id==trace_pax].to_string(formatters=\
               {'A_time'               :Assignment.datetime64_min_formatter,
                'B_time'               :Assignment.datetime64_min_formatter,
                'linktime'             :Assignment.timedelta_formatter})))

        ######################################################################################################
        FastTripsLogger.info("Step 1. Find out board/alight times for passengers from vehicle times")

        passenger_trips = passengers_df.loc[passengers_df.linkmode=='Trip'].copy()
        passenger_trips_len = len(passenger_trips)
        FastTripsLogger.debug("       Have %d pasenger trips" % passenger_trips_len)

        passenger_trips = pandas.merge(left   =passenger_trips,              right   =veh_trips_df[['trip_id','stop_seq','stop_id','depart_time']],
                                       left_on=['trip_id','A_id','A_seq'],   right_on=['trip_id','stop_id','stop_seq'],
                                       how='left')
        passenger_trips = pandas.merge(left   =passenger_trips,              right   =veh_trips_df[['trip_id','stop_seq','stop_id','arrive_time']],
                                       left_on=['trip_id','B_id','B_seq'],   right_on=['trip_id','stop_id','stop_seq'],
                                       how='left')
        passenger_trips.rename(columns=
           {'depart_time'   :'board_time',      # transit vehicle depart time (at A) = board time for pax
            'A_time'        :'arrival_time',    # passenger arrival at the stop
            'arrive_time'   :'alight_time',     # transit vehicle arrive time (at B) = alight time for pax
            }, inplace=True)
        # redundant with A_id, B_id, A_seq, B_seq
        passenger_trips.drop(['stop_id_x','stop_id_y','stop_seq_x','stop_seq_y'], axis=1, inplace=True)
        FastTripsLogger.debug("       Have %d pasenger trips" % len(passenger_trips))

        ######################################################################################################
        FastTripsLogger.info("Step 2. Some trips (outbound) were found by searching backwards, so they wait *after arriving*.")
        FastTripsLogger.info("        -> They should just move on and wait at the next stop (if there is one)")

        # Get trip board/alight time back to the passengers table
        passengers_df = pandas.merge(left=passengers_df, right=passenger_trips[['passenger_id','path_id','trip_id','board_time','alight_time']],
                                     on=['passenger_id','path_id','trip_id'], how='left')
        passengers_df = pandas.merge(left      =passengers_df, right      =passengers_df[['board_time','alight_time']].shift(1),
                                     left_index=True,          right_index=True,
                                     how       ='left',        suffixes   =('','_prev'))
        # For trips: if B > alight_time, don't wait at B! Just leave!
        passengers_df.loc[(passengers_df.linkmode==Path.STATE_MODE_TRIP     )& \
                          (passengers_df.B_time   >passengers_df.alight_time), 'linktime'] -= passengers_df.B_time-passengers_df.alight_time
        passengers_df.loc[(passengers_df.linkmode==Path.STATE_MODE_TRIP     )& \
                          (passengers_df.B_time   >passengers_df.alight_time), 'B_time'  ]  = passengers_df.alight_time
        # For transfer links and egress links, move up in time since the passenger arrived earlier
        passengers_df.loc[((passengers_df.linkmode==Path.STATE_MODE_TRANSFER      )| \
                           (passengers_df.linkmode==Path.STATE_MODE_EGRESS        ))& \
                           (passengers_df.A_time   >passengers_df.alight_time_prev), 'B_time'] -= passengers_df.A_time-passengers_df.alight_time_prev
        passengers_df.loc[((passengers_df.linkmode==Path.STATE_MODE_TRANSFER      )| \
                           (passengers_df.linkmode==Path.STATE_MODE_EGRESS        ))& \
                           (passengers_df.A_time > passengers_df.alight_time_prev ), 'A_time'] -= passengers_df.A_time-passengers_df.alight_time_prev

        # Sometimes stochastic assignment results in transfers that are too early -- fix
        if Assignment.ASSIGNMENT_TYPE == Assignment.ASSIGNMENT_TYPE_STO_ASGN:
            passengers_df.loc[(passengers_df.linkmode==Path.STATE_MODE_TRANSFER      )& \
                              (passengers_df.A_time   <passengers_df.alight_time_prev), 'B_time'] = passengers_df.alight_time_prev+passengers_df.linktime
            passengers_df.loc[(passengers_df.linkmode==Path.STATE_MODE_TRANSFER      )& \
                              (passengers_df.A_time   <passengers_df.alight_time_prev), 'A_time'] = passengers_df.alight_time_prev

        # Now subsequent trip arrival times can move up also
        passengers_df = pandas.merge(left      =passengers_df, right      =passengers_df[['B_time','linkmode']].shift(1),
                                     left_index=True,          right_index=True,
                                     how       ='left',        suffixes   =('','_prev'))
        passengers_df.loc[(passengers_df.linkmode==Path.STATE_MODE_TRIP     )& \
                          (passengers_df.A_time   >passengers_df.B_time_prev), 'linktime'] += passengers_df.A_time-passengers_df.B_time_prev
        passengers_df.loc[(passengers_df.linkmode==Path.STATE_MODE_TRIP     )& \
                          (passengers_df.A_time   >passengers_df.B_time_prev), 'A_time'  ]  = passengers_df.B_time_prev

        # Sometimes stochastic assignment results in trips arrivals that are too early -- fix
        if Assignment.ASSIGNMENT_TYPE == Assignment.ASSIGNMENT_TYPE_STO_ASGN:
            passengers_df.loc[(passengers_df.linkmode==Path.STATE_MODE_TRIP     )& \
                              (passengers_df.A_time   <passengers_df.B_time_prev), 'linktime'] = passengers_df.B_time-passengers_df.B_time_prev
            passengers_df.loc[(passengers_df.linkmode==Path.STATE_MODE_TRIP     )& \
                              (passengers_df.A_time   <passengers_df.B_time_prev), 'A_time'  ] = passengers_df.B_time_prev

        ######################################################################################################
        FastTripsLogger.info("Step 3. Some trips leave too early and wait at the *first* stop.")
        FastTripsLogger.info("        -> Assume they have perfect prediction and wait to leave.")

        # For trips: If first trip (right after access), if A < board time, get there later!
        passengers_df.loc[(passengers_df.linkmode     ==Path.STATE_MODE_TRIP  ) & \
                          (passengers_df.linkmode_prev==Path.STATE_MODE_ACCESS) & \
                          (passengers_df.A_time        <passengers_df.board_time), 'linktime'] -= passengers_df.board_time-passengers_df.A_time
        passengers_df.loc[(passengers_df.linkmode     ==Path.STATE_MODE_TRIP  ) & \
                          (passengers_df.linkmode_prev==Path.STATE_MODE_ACCESS) & \
                          (passengers_df.A_time        <passengers_df.board_time), 'A_time'  ]  = passengers_df.board_time
        passengers_df = pandas.merge(left      =passengers_df, right      =passengers_df[['A_time']].shift(-1),
                                     left_index=True,          right_index=True,
                                     how       ='left',        suffixes   =('','_next'))
        # For access links: Scoot my times later
        passengers_df.loc[(passengers_df.linkmode    ==Path.STATE_MODE_ACCESS)& \
                          (passengers_df.B_time       <passengers_df.A_time_next), 'A_time'] += passengers_df.A_time_next-passengers_df.B_time
        passengers_df.loc[(passengers_df.linkmode    ==Path.STATE_MODE_ACCESS)& \
                          (passengers_df.B_time       <passengers_df.A_time_next), 'B_time']  = passengers_df.A_time_next


        # passenger_trips is all wrong now -- redo
        passenger_trips = passengers_df.loc[passengers_df.linkmode=='Trip'].copy()
        # FastTripsLogger.debug("Passenger Trips: \n%s" % str(passenger_trips.head()))

        ######################################################################################################
        bump_iter = 0
        Assignment.bumped_passenger_ids.clear()
        Assignment.bumped_path_ids.clear()
        while True: # loop for capacity constraint
            FastTripsLogger.info("Step 4. Put passenger paths on transit vehicles to get vehicle boards/alights/load")

            # Group to boards by counting path_ids for a (trip_id, A_id as stop_id)
            passenger_trips_boards = passenger_trips[['path_id','trip_id','A_id','A_seq']].groupby(['trip_id','A_id','A_seq']).count()
            passenger_trips_boards.index.names = ['trip_id','stop_id','stop_seq']

            # And alights by counting path_ids for a (trip_id, B_id as stop_id)
            passenger_trips_alights = passenger_trips[['path_id','trip_id','B_id','B_seq']].groupby(['trip_id','B_id','B_seq']).count()
            passenger_trips_alights.index.names = ['trip_id','stop_id','stop_seq']

            # Join them to the transit vehicle trips so we can put people on vehicles
            veh_loaded_df = pandas.merge(left   =veh_trips_df,                     right      =passenger_trips_boards,
                                         left_on=['trip_id','stop_id','stop_seq'], right_index=True,
                                         how    ='left')
            veh_loaded_df.rename(columns={'path_id':'boards'}, inplace=True)

            veh_loaded_df = pandas.merge(left   =veh_loaded_df,                   right      =passenger_trips_alights,
                                        left_on=['trip_id','stop_id','stop_seq'], right_index=True,
                                        how    ='left')
            veh_loaded_df.rename(columns={'path_id':'alights'}, inplace=True)
            veh_loaded_df.fillna(value=0, inplace=True)
            assert(len(veh_loaded_df)==veh_trips_df_len)

            # these are ints, not floats
            veh_loaded_df[['boards','alights']] = veh_loaded_df[['boards','alights']].astype(int)

            veh_loaded_df.set_index(['trip_id','stop_seq'],inplace=True)
            veh_loaded_df['onboard'] = veh_loaded_df.boards - veh_loaded_df.alights
            # print veh_trips_df.loc[5123368]

            # on board is the cumulative sum of boards - alights
            trips_cumsum = veh_loaded_df[['onboard']].groupby(level=[0]).cumsum()
            veh_loaded_df.drop('onboard', axis=1, inplace=True) # replace with cumsum
            veh_loaded_df = pandas.merge(left      =veh_loaded_df,  right      =trips_cumsum,
                                         left_index=True,          right_index=True,
                                         how='left')
            assert(len(veh_loaded_df)==veh_trips_df_len)
            # print veh_trips_df.loc[5123368]
            veh_loaded_df.reset_index(inplace=True)

            if not Assignment.CAPACITY_CONSTRAINT:
                # No need to loop
                break

            else:
                ######################################################################################################
                FastTripsLogger.info("Step 5. Capacity constraints on transit vehicles.")
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
                   {'arrive_time'  :Assignment.datetime64_formatter,
                    'depart_time'  :Assignment.datetime64_formatter})))

                # If none, we're done
                if len(overcap_df) == 0:
                    FastTripsLogger.info("        No overcapacity vehicles")
                    break

                # start by bumping the first ones who board after at capacity - which stops are they?
                bump_stops_df  = overcap_df.groupby(['trip_id']).aggregate('first')
                FastTripsLogger.debug("Bump stops (%d rows, showing head):\n%s" %
                                      (len(bump_stops_df),
                                      bump_stops_df.head().to_string(formatters=\
                   {'arrive_time'  :Assignment.datetime64_formatter,
                    'depart_time'  :Assignment.datetime64_formatter})))

                # One stop at a time -- slower but more accurate
                if Assignment.BUMP_ONE_AT_A_TIME:
                    bump_stops_df.sort(['arrive_time'], inplace=True)
                    bump_stops_df = bump_stops_df.iloc[:1]

                FastTripsLogger.info("        Need to bump %d passengers from %d stops" % (bump_stops_df.overcap.sum(), len(bump_stops_df)))

                # who boards at those stops?
                bumped_pax_boards = pandas.merge(left    =passenger_trips[['trip_id','A_id','passenger_id','path_id','A_seq','A_time']],
                                                 left_on =['trip_id','A_id','A_seq'],
                                                 right   =bump_stops_df.reset_index()[['trip_id','stop_id','stop_seq','arrive_time','depart_time','overcap']],
                                                 right_on=['trip_id','stop_id','stop_seq'],
                                                 how     ='inner')
                # bump off later arrivals, later path_id
                bumped_pax_boards.sort(['arrive_time','trip_id','stop_seq','stop_id','A_time','path_id'],
                                       ascending=[True, True, True, True, False, False], inplace=True)
                bumped_pax_boards.reset_index(drop=True, inplace=True)

                # For each trip_id, stop_seq, stop_id, we want the first *overcap* rows
                # group to trip_id, stop_seq, stop_id and count off
                bpb_count = bumped_pax_boards.groupby(['trip_id','stop_seq','stop_id']).cumcount()
                bpb_count.name = 'bump_index'

                # Add the bump index to our passenger-paths/stops
                bumped_pax_boards = pandas.concat([bumped_pax_boards, bpb_count], axis=1)

                FastTripsLogger.debug("bumped_pax_boards (%d rows, showing head):\n%s" % (len(bumped_pax_boards),
                    bumped_pax_boards.head().to_string(formatters=\
                   {'A_time'       :Assignment.datetime64_formatter,
                    'arrive_time'  :Assignment.datetime64_formatter,
                    'depart_time'  :Assignment.datetime64_formatter})))

                # use it to filter to those we bump
                bumped_pax_boards = bumped_pax_boards.loc[bumped_pax_boards.bump_index < bumped_pax_boards.overcap]

                FastTripsLogger.debug("filtered bumped_pax_boards (%d rows, showing head):\n%s" % (len(bumped_pax_boards),
                    bumped_pax_boards.head().to_string(formatters=\
                   {'A_time'       :Assignment.datetime64_formatter,
                    'arrive_time'  :Assignment.datetime64_formatter,
                    'depart_time'  :Assignment.datetime64_formatter})))

                # filter to unique passengers/paths
                bumped_pax_boards.drop_duplicates(subset=['passenger_id','path_id'],inplace=True)
                bumped_pax_boards['bump'] = True

                # keep track of these
                Assignment.bumped_passenger_ids.update(bumped_pax_boards.passenger_id.tolist())
                Assignment.bumped_path_ids.update(bumped_pax_boards.passenger_id.tolist())

                FastTripsLogger.debug("bumped_pax_boards without duplicate passengers (%d rows, showing head):\n%s" % \
                    (len(bumped_pax_boards),
                     bumped_pax_boards.head().to_string(formatters=\
                   {'A_time'       :Assignment.datetime64_formatter,
                    'arrive_time'  :Assignment.datetime64_formatter,
                    'depart_time'  :Assignment.datetime64_formatter})))

                # Kick out the bumped passengers
                passengers_df = pandas.merge(left     =passengers_df,
                                             right    =bumped_pax_boards[['passenger_id','path_id','bump']],
                                             left_on  =['passenger_id','path_id'],
                                             right_on =['passenger_id','path_id'],
                                             how      ='left')
                assert(passengers_df_len == len(passengers_df))
                passengers_df = passengers_df[passengers_df.bump != True]
                FastTripsLogger.info("        Bumped %d passengers; passenger_df length %d -> %d" %
                                     (len(bumped_pax_boards), passengers_df_len, len(passengers_df)))
                passengers_df.drop('bump', axis=1, inplace=True)
                passengers_df_len = len(passengers_df)

                # recreate
                passenger_trips = passengers_df.loc[passengers_df.linkmode=='Trip'].copy()
                passenger_trips_len = len(passenger_trips)

                new_bump_wait = bumped_pax_boards[['trip_id','A_id','A_seq','A_time']].groupby(['trip_id','A_id','A_seq']).first()
                new_bump_wait.reset_index(drop=False, inplace=True)
                new_bump_wait.rename(columns={'A_id':'stop_id','A_seq':'stop_seq'}, inplace=True)

                FastTripsLogger.debug("new_bump_wait (%d rows, showing head):\n%s" %
                    (len(new_bump_wait), new_bump_wait.head().to_string(formatters=\
                   {'A_time'       :Assignment.datetime64_formatter})))

                # incorporate it into the bump wait df
                if type(Assignment.bump_wait_df) == type(None):
                    Assignment.bump_wait_df = new_bump_wait
                else:
                    Assignment.bump_wait_df = Assignment.bump_wait_df.append(new_bump_wait)
                    Assignment.bump_wait_df.drop_duplicates(['trip_id','stop_seq'], inplace=True)

                # incorporate it into the bump wait
                # This is (trip_id, stop_id) -> Timestamp
                bump_wait_dict  = Assignment.bump_wait_df.set_index(['trip_id','stop_id','stop_seq']).to_dict()['A_time']
                bump_wait_dict2 = {k: v.to_datetime() for k, v in bump_wait_dict.iteritems()}
                Assignment.bump_wait.update(bump_wait_dict2)

                # FastTripsLogger.debug("Bump wait ---------")
                # for key,val in Assignment.bump_wait.iteritems():
                #     FastTripsLogger.debug("(%10s, %10s, %10s) -> %s" %
                #                           (str(key[0]), str(key[1]), str(key[2]), val.strftime("%H:%M:%S")))

                bump_iter += 1
                FastTripsLogger.info("        -> complete loop iter %d" % bump_iter)

        if type(Assignment.bump_wait_df) == pandas.DataFrame and len(Assignment.bump_wait_df) > 0:
            Assignment.bump_wait_df['A_time_min'] = Assignment.bump_wait_df['A_time'].map(lambda x: (60.0*x.hour) + x.minute + (x.second/60.0))

        FastTripsLogger.debug("Bumped passenger ids: %s" % str(Assignment.bumped_passenger_ids))
        FastTripsLogger.debug("Bumped path ids: %s" % str(Assignment.bumped_path_ids))
        if type(Assignment.bump_wait_df) == pandas.DataFrame and len(Assignment.bump_wait_df) > 0:
            FastTripsLogger.debug("Bump_wait_df:\n%s" % Assignment.bump_wait_df.to_string(formatters=\
                {'A_time'       :Assignment.datetime64_formatter}))

        ######################################################################################################
        FastTripsLogger.info("Step 6. Add up travel costs")
        Path.calculate_tripcost(passengers_df)

        ######################################################################################################
        FastTripsLogger.info("Step 7. Convert times to strings (minutes past midnight) for joining")

        ######         TODO: this is really catering to output format; an alternative might be more appropriate
        passenger_trips.loc[:,  'board_time_str'] = passenger_trips.board_time.apply(Assignment.datetime64_min_formatter)
        passenger_trips.loc[:,'arrival_time_str'] = passenger_trips.A_time.apply(Assignment.datetime64_min_formatter)
        passenger_trips.loc[:, 'alight_time_str'] = passenger_trips.alight_time.apply(Assignment.datetime64_min_formatter)
        assert(len(passenger_trips) == passenger_trips_len)

        # Aggregate (by joining) across each passenger + path
        ptrip_group = passenger_trips.groupby(['passenger_id','path_id'])
        # these are Series
        board_time_str   = ptrip_group['board_time_str'  ].apply(lambda x:','.join(x))
        arrival_time_str = ptrip_group['arrival_time_str'].apply(lambda x:','.join(x))
        alight_time_str  = ptrip_group['alight_time_str' ].apply(lambda x:','.join(x))

        # Aggregate other fields across each passenger + path
        pax_exp_df = passengers_df.groupby(['passenger_id','path_id']).agg(
            {'pathmode'     :'first',  # path mode
             'A_id'         :'first',  # origin
             'B_id'         :'last',   # destination
             'A_time'       :'first',  # start time
             'B_time'       :'last',   # end time
             'travelCost'   :'sum',    # total travel cost
            })

        # Put them together and return
        assert(len(pax_exp_df) == len(board_time_str))
        pax_exp_df = pandas.concat([pax_exp_df,
                                    board_time_str,
                                    arrival_time_str,
                                    alight_time_str], axis=1)
        # print pax_exp_df.to_string(formatters={'A_time':Assignment.datetime64_min_formatter,
        #                                        'B_time':Assignment.datetime64_min_formatter})

        if len(Assignment.TRACE_PASSENGER_IDS) > 0:
            simulated_passenger_ids = passengers_df.passenger_id.values

        for trace_pax in Assignment.TRACE_PASSENGER_IDS:
            if trace_pax not in simulated_passenger_ids:
                FastTripsLogger.debug("Passenger %d not in final simulated list" % trace_pax)
            else:
                FastTripsLogger.debug("Final passengers_df for %s\n%s" % \
                   (str(trace_pax),
                    passengers_df.loc[passengers_df.passenger_id==trace_pax].to_string(formatters=\
                   {'A_time'               :Assignment.datetime64_min_formatter,
                    'B_time'               :Assignment.datetime64_min_formatter,
                    'linktime'             :Assignment.timedelta_formatter,
                    'board_time'           :Assignment.datetime64_min_formatter,
                    'alight_time'          :Assignment.datetime64_min_formatter,
                    'board_time_prev'      :Assignment.datetime64_min_formatter,
                    'alight_time_prev'     :Assignment.datetime64_min_formatter,
                    'B_time_prev'          :Assignment.datetime64_min_formatter,
                    'A_time_next'          :Assignment.datetime64_min_formatter,})))

                FastTripsLogger.debug("Passengers experienced times for %s\n%s" % \
                   (str(trace_pax),
                    pax_exp_df.loc[trace_pax].to_string(formatters=\
                   {'A_time'               :Assignment.datetime64_min_formatter,
                    'B_time'               :Assignment.datetime64_min_formatter})))

        return (len(pax_exp_df), veh_loaded_df, pax_exp_df)

    @staticmethod
    def print_load_profile(veh_trips_df, output_dir):
        """
        Print the load profile output
        """
        # reset columns
        print_veh_trips_df = veh_trips_df

        Trip.calculate_dwell_times(print_veh_trips_df)
        print_veh_trips_df = Trip.calculate_headways(print_veh_trips_df)

        # rename columns
        print_veh_trips_df.rename(columns=
           {'trip_id'        :'tripId',
            'stop_id'        :'stopId',
            'dwell_time'     :'dwellTime',
            'boards'         :'boardings',
            'alights'        :'alightings',
            'onboard'        :'load'
            }, inplace=True)

        # recode/reformat
        print_veh_trips_df['traveledDist']  = -1
        print_veh_trips_df['departureTime'] = print_veh_trips_df.depart_time.apply(Assignment.datetime64_min_formatter)
        # reorder
        print_veh_trips_df = print_veh_trips_df[['routeId','shapeId','tripId','direction','stopId',
                                         'traveledDist','departureTime','headway','dwellTime',
                                         'boardings','alightings','load']]

        load_file = open(os.path.join(output_dir, "ft_output_loadProfile.dat"), 'w')
        print_veh_trips_df.to_csv(load_file,
                              sep="\t",
                              float_format="%.2f",
                              index=False)
        load_file.close()

def find_trip_based_paths_process_worker(iteration, worker_num, input_dir, output_dir, todo_path_queue, done_queue, hyperpath, bump_wait_df):
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
    worker_FT = FastTrips(input_dir=input_dir, output_dir=output_dir, read_demand=False,
                          log_to_console=False, logname_append=worker_str, appendLog=True if iteration > 1 else False)

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
        passenger       = todo[0]
        path            = todo[1]

        FastTripsLogger.info("Processing passenger %s path %s" % (str(passenger.passenger_id), str(path.path_id)))

        trace_passenger = False
        if passenger.passenger_id in Assignment.TRACE_PASSENGER_IDS:
            FastTripsLogger.debug("Tracing assignment of passenger %s" % str(passenger.passenger_id))
            trace_passenger = True

        try:
            (asgn_iters, return_states) = Assignment.find_trip_based_path(worker_FT, passenger, path, hyperpath, trace=trace_passenger)
            done_queue.put( (passenger.passenger_id, path.path_id, asgn_iters, return_states) )
        except:
            FastTripsLogger.exception('Exception')
            # call it a day
            done_queue.put('DONE')
            return
