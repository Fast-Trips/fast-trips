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
import collections,datetime,math,os,random,sys
import numpy,pandas

from .Logger import FastTripsLogger
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
    TRACE_PASSENGER_IDS             = []

    #: Maximum number of times to call :py:meth:`choose_path_from_hyperpath_states`
    MAX_HYPERPATH_ASSIGN_ATTEMPTS   = 1001   # that's a lot

    #: Temporary rand.txt location for :py:meth:`Assignment.test_rand`.
    RAND_FILENAME                   = r"../FAST-TrIPs-1/rand/rand.txt"
    #: The file handle for :py:attr:`Assignment.RAND_FILENAME`
    RAND_FILE                       = None
    #: Maximum random number returned by :py:meth:`Assignment.test_rand`
    RAND_MAX                        = 0

    #: Extra time so passengers don't get bumped (?)
    BUMP_BUFFER                     = datetime.timedelta(minutes = 5)

    #: This is the only simulation state that exists across iterations
    #: It's a dictionary of (trip_id, stop_id) -> earliest time a bumped passenger started waiting
    bump_wait                       = {}

    #: assignment results - Passenger table
    PASSENGERS_CSV                  = r"passengers_df.csv"

    #: Vehicle trips table
    VEH_TRIPS_CSV                   = r"veh_trips_df.csv"

    #: formatter to convert :py:class:`numpy.datetime64` to string that looks like `HH:MM.SS`
    @staticmethod
    def datetime64_formatter(x):
        return pandas.to_datetime(x).strftime('%H:%M.%S')

    #: formatter to convert :py:class:`numpy.datetime64` to minutes after minutes
    #: (with two decimal places)
    @staticmethod
    def datetime64_min_formatter(x):
        return '%.2f' % (pandas.to_datetime(x).hour*60.0 + \
                         pandas.to_datetime(x).minute + \
                         pandas.to_datetime(x).second/60.0)
    #: formatter to convert :py:class:`numpy.timedelta64` to string that looks like `4m 35.6s`
    @staticmethod
    def timedelta_formatter(x):
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
    def assign_paths(output_dir, FT):
        """
        Finds the paths for the passengers.
        """

        Assignment.bump_wait = {}
        for iteration in range(1,Assignment.ITERATION_FLAG+1):
            FastTripsLogger.info("***************************** ITERATION %d **************************************" % iteration)

            if Assignment.ASSIGNMENT_TYPE == Assignment.ASSIGNMENT_TYPE_SIM_ONLY or \
               (os.path.exists(os.path.join(output_dir, Assignment.PASSENGERS_CSV)) and
                os.path.exists(os.path.join(output_dir, Assignment.VEH_TRIPS_CSV))):
                FastTripsLogger.info("Simulation only")
                (num_paths_assigned, passengers_df, veh_trips_df) = Assignment.read_assignment_results(output_dir)

            else:
                num_paths_assigned = Assignment.assign_passengers(FT, iteration)
                passengers_df      = Assignment.setup_passengers(FT, output_dir)
                veh_trips_df       = Assignment.setup_trips(FT, output_dir)

            if Assignment.OUTPUT_PASSENGER_TRAJECTORIES:
                Assignment.print_passenger_paths(passengers_df, output_dir)

            if Assignment.SIMULATION_FLAG == True:
                FastTripsLogger.info("****************************** SIMULATING *****************************")
                (num_passengers_arrived,veh_trips_df,pax_exp_df) = Assignment.simulate(FT, passengers_df, veh_trips_df)

            if Assignment.OUTPUT_PASSENGER_TRAJECTORIES:
                Assignment.print_passenger_times(pax_exp_df, output_dir)

            # capacity gap stuff
            num_bumped_passengers = num_paths_assigned - num_passengers_arrived
            capacity_gap = 100.0*num_bumped_passengers/num_paths_assigned

            FastTripsLogger.info("")
            FastTripsLogger.info("  TOTAL ASSIGNED PASSENGERS: %10d" % num_paths_assigned)
            FastTripsLogger.info("  ARRIVED PASSENGERS:        %10d" % num_passengers_arrived)
            FastTripsLogger.info("  MISSED PASSENGERS:         %10d" % num_bumped_passengers)
            FastTripsLogger.info("  CAPACITY GAP:              %10.5f" % capacity_gap)

            if capacity_gap < 0.001 or Assignment.ASSIGNMENT_TYPE == Assignment.ASSIGNMENT_TYPE_STO_ASGN:
                break

        # end for loop
        FastTripsLogger.info("**************************** WRITING OUTPUTS ****************************")
        Assignment.print_load_profile(veh_trips_df, output_dir)

    @staticmethod
    def assign_passengers(FT, iteration):
        """
        Assigns paths to passengers using deterministic trip-based shortest path (TBSP) or
        stochastic trip-based hyperpath (TBHP).

        Returns the number of paths assigned.
        """
        FastTripsLogger.info("**************************** GENERATING PATHS ****************************")
        start_time          = datetime.datetime.now()
        num_paths_assigned  = 0
        for passenger in FT.passengers:
            passenger_id = passenger.passenger_id

            if not passenger.path.goes_somewhere(): continue

            if iteration > 1 and passenger.simulation_status == Passenger.STATUS_ARRIVED:
                num_paths_assigned += 1
                continue

            trace_passenger = False
            if passenger_id in Assignment.TRACE_PASSENGER_IDS:
                FastTripsLogger.debug("Tracing assignment of passenger %s" % str(passenger_id))
                trace_passenger = True

            if Assignment.ASSIGNMENT_TYPE == Assignment.ASSIGNMENT_TYPE_DET_ASGN:
                asgn_iters = Assignment.find_trip_based_shortest_path(FT, passenger.path, trace_passenger)
            else:
                asgn_iters = Assignment.find_trip_based_hyperpath(FT, passenger.path, trace_passenger)

            if passenger.path.path_found():
                num_paths_assigned += 1

            if True or num_paths_assigned % 1000 == 0:
                time_elapsed = datetime.datetime.now() - start_time
                FastTripsLogger.info(" %6d / %6d passenger paths assigned.  Time elapsed: %2dh:%2dm:%2ds" % (
                                     num_paths_assigned, len(FT.passengers),
                                     int( time_elapsed.total_seconds() / 3600),
                                     int( (time_elapsed.total_seconds() % 3600) / 60),
                                     time_elapsed.total_seconds() % 60))
            elif num_paths_assigned % 50 == 0:
                FastTripsLogger.debug("%6d / %6d passenger paths assigned" % (num_paths_assigned, len(FT.passengers)))
        return num_paths_assigned

    @staticmethod
    def find_trip_based_shortest_path(FT, path, trace):
        """
        Perform trip-based shortest path search.
        Will do so either backwards (destination to origin) if :py:attr:`Path.direction` is :py:attr:`Path.DIR_OUTBOUND`
        or forwards (origin to destination) if :py:attr:`Path.direction` is :py:attr:`Path.DIR_INBOUND`.

        Updates the path information in the given path and returns the number of label iterations required.

        :param FT: fasttrips data
        :type FT: a :py:class:`FastTrips` instance
        :param path: the path to fill in
        :type path: a :py:class:`Path` instance
        :param trace: pass True if this path should be traced to the debug log
        :type trace: boolean
        """
        if path.outbound():
            start_taz_id    = path.destination_taz_id
            dir_factor      = 1  # a lot of attributes are just the negative -- this facilitates that
        else:
            start_taz_id    = path.origin_taz_id
            dir_factor      = -1

        stop_states = {}                    # stop_id -> [label, departure, departure_mode, successor]
        stop_queue  = Queue.PriorityQueue() # (label, stop_id)
        MAX_TIME    = datetime.timedelta(minutes = 999.999)
        stop_done   = set() # stop ids
        trips_used  = set() # trip ids

        # possible egress links
        for stop_id, access_link in FT.tazs[start_taz_id].access_links.iteritems():
            # outbound: departure time = destination - access
            # inbound:  arrival time   = origin      + access
            deparr_time          = datetime.datetime.combine(Assignment.TODAY, path.preferred_time) - \
                                   (access_link[TAZ.ACCESS_LINK_IDX_TIME]*dir_factor)
            stop_states[stop_id] = [ \
                access_link[TAZ.ACCESS_LINK_IDX_TIME],                                  # label
                deparr_time,                                                            # departure/arrival
                Path.STATE_MODE_EGRESS if path.outbound() else Path.STATE_MODE_ACCESS,  # departure/arrival mode
                start_taz_id,                                                           # successor/predecessor
                access_link[TAZ.ACCESS_LINK_IDX_TIME]]                                  # link time
            stop_queue.put( (access_link[TAZ.ACCESS_LINK_IDX_TIME], stop_id ))
            if trace: FastTripsLogger.debug(" %s   %s" % ("+egress" if path.outbound() else "+access",
                                                          Path.state_str(stop_id, stop_states[stop_id])))

        # labeling loop
        label_iterations = 0
        while not stop_queue.empty():
            (current_label, current_stop_id) = stop_queue.get()

            if current_stop_id in stop_done: continue                   # stop is already processed
            stop_done.add(current_stop_id)                              # process this stop now - just once

            if trace:
                FastTripsLogger.debug("Pulling from stop_queue (iteration %d, label %.4f, stop %s) :======" % \
                                      (label_iterations, current_label.total_seconds()/60.0, str(current_stop_id)))
                FastTripsLogger.debug("           " + Path.state_str(current_stop_id, stop_states[current_stop_id]))
                FastTripsLogger.debug("==============================")

            current_stop_state = stop_states[current_stop_id]

            # Update by transfer
            # (We don't want to transfer to egress or transfer to a transfer)
            if current_stop_state[Path.STATE_IDX_DEPARRMODE] not in [Path.STATE_MODE_EGRESS,
                                                                     Path.STATE_MODE_TRANSFER,
                                                                     Path.STATE_MODE_ACCESS]:

                for xfer_stop_id,xfer_attr in FT.stops[current_stop_id].transfers.iteritems():

                    # new_label = length of trip so far if the passenger transfers from this stop
                    new_label       = current_label + xfer_attr[Stop.TRANSFERS_IDX_TIME]
                    # outbound: departure time = departure - transfer
                    # inbound:  arrival time   = arrival + transfer
                    deparr_time = current_stop_state[Path.STATE_IDX_DEPARR] - (xfer_attr[Stop.TRANSFERS_IDX_TIME]*dir_factor)

                    # check (departure mode, stop) if someone's waiting already
                    # curious... this only applies to OUTBOUND
                    if path.outbound() and \
                       (current_stop_state[Path.STATE_IDX_DEPARRMODE], current_stop_id) in Assignment.bump_wait:
                        # time a bumped passenger started waiting
                        latest_time = Assignment.bump_wait[(current_stop_state[Path.STATE_IDX_DEPARRMODE], current_stop_id)]
                        # we can't come in time
                        if deparr_time - Assignment.PATH_TIME_WINDOW > latest_time: continue
                        # leave earlier -- to get in line 5 minutes before bump wait time
                        # (confused... We don't resimulate previous bumping passenger so why does this make sense?)
                        new_label       = new_label + (current_stop_state[Path.STATE_IDX_DEPARR] - latest_time) + Assignment.BUMP_BUFFER
                        deparr_time     = latest_time - xfer_attr[Stop.TRANSFERS_IDX_TIME] - Assignment.BUMP_BUFFER

                    old_label       = MAX_TIME
                    if xfer_stop_id in stop_states:
                        old_label   = stop_states[xfer_stop_id][Path.STATE_IDX_LABEL]

                    if new_label < old_label:
                        stop_states[xfer_stop_id] = [new_label,                 # label,
                                                     deparr_time,               # departure/arrival time
                                                     Path.STATE_MODE_TRANSFER,  # departure/arrival mode
                                                     current_stop_id,           # successor/predecessor
                                                     xfer_attr[Stop.TRANSFERS_IDX_TIME]] # link time
                        stop_queue.put( (new_label, xfer_stop_id) )
                        if trace: FastTripsLogger.debug(" +transfer " + Path.state_str(xfer_stop_id, stop_states[xfer_stop_id]))

            # Update by trips
            if path.outbound():
                # These are the trips that arrive at the stop in time to depart on time
                valid_trips = FT.stops[current_stop_id].get_trips_arriving_within_time(Assignment.TODAY,
                                                                                       current_stop_state[Path.STATE_IDX_DEPARR],
                                                                                       Assignment.PATH_TIME_WINDOW)
            else:
                # These are the trips that depart from the stop in time for passenger
                valid_trips = FT.stops[current_stop_id].get_trips_departing_within_time(Assignment.TODAY,
                                                                                        current_stop_state[Path.STATE_IDX_DEPARR],
                                                                                        Assignment.PATH_TIME_WINDOW)

            for (trip_id, seq, arrdep_time) in valid_trips:
                if trace: FastTripsLogger.debug("valid trips: %s  %d  %s" % (str(trip_id), seq, arrdep_time.strftime("%H:%M:%S")))

                if trip_id in trips_used: continue

                # trip arrival time (outbound) / trip departure time (inbound)
                arrdep_datetime = datetime.datetime.combine(Assignment.TODAY, arrdep_time)
                wait_time       = (current_stop_state[Path.STATE_IDX_DEPARR] - arrdep_datetime)*dir_factor

                # check (departure, stop) to see if there's a queue
                if path.outbound():
                    # if outbound, this trip loop is possible trips *before* the current trip
                    # checking that we get here in time for the current trip
                    check_for_bump_wait = (current_stop_state[Path.STATE_IDX_DEPARRMODE], current_stop_id)
                    # arrive from the loop trip
                    arrive_datetime     = arrdep_datetime
                else:
                    # if inbound, the trip is the next trip
                    # checking that we can get here in time for that trip
                    check_for_bump_wait = (trip_id, current_stop_id)
                    # arrive for this trip
                    arrive_datetime     = current_stop_state[Path.STATE_IDX_DEPARR]

                if check_for_bump_wait in Assignment.bump_wait:
                    # time a bumped passenger started waiting
                    latest_time = Assignment.bump_wait[check_for_bump_wait]
                    if trace: FastTripsLogger.debug("checking latest_time %s vs arrive_datetime %s for potential trip %s" % \
                                                    (latest_time.strftime("%H:%M:%S"), arrive_datetime.strftime("%H:%M:%S"),
                                                     str(trip_id)))
                    if arrive_datetime + datetime.timedelta(minutes = 0.01) >= latest_time and \
                       current_stop_state[Path.STATE_IDX_DEPARRMODE] != trip_id:
                        if trace: FastTripsLogger.debug("Continuing")
                        continue

                if path.outbound():  # outbound: iterate through the stops before this one
                    trip_seq_list = range(seq-1, 0, -1)
                else:                # inbound: iterate through the stops after this one
                    trip_seq_list = range(seq+1, FT.trips[trip_id].number_of_stops()+1)

                trips_used.add(trip_id)
                for seq_num in trip_seq_list:
                    # board for outbound / alight for inbound
                    possible_board_alight  = FT.trips[trip_id].stops[seq_num-1]

                    # new_label = length of trip so far if the passenger boards/alights at this stop
                    board_alight_stop   = possible_board_alight[Trip.STOPS_IDX_STOP_ID]
                    deparr_time         = datetime.datetime.combine(Assignment.TODAY,
                                          possible_board_alight[Trip.STOPS_IDX_DEPARTURE_TIME] if path.outbound() else \
                                          possible_board_alight[Trip.STOPS_IDX_ARRIVAL_TIME])
                    in_vehicle_time     = (arrdep_datetime - deparr_time)*dir_factor
                    new_label           = current_label + in_vehicle_time + wait_time

                    old_label           = MAX_TIME
                    if board_alight_stop in stop_states:
                        old_label       = stop_states[board_alight_stop][Path.STATE_IDX_LABEL]

                    if new_label < old_label:
                        stop_states[board_alight_stop] = [ \
                            new_label,                  # label,
                            deparr_time,                # departure/arrival time
                            trip_id,                    # departure/arrival mode
                            current_stop_id,            # successor/predessor
                            in_vehicle_time+wait_time ] # link time
                        stop_queue.put( (new_label, board_alight_stop) )
                        if trace: FastTripsLogger.debug(" +trip     " + Path.state_str(board_alight_stop, stop_states[board_alight_stop]))

            # Done with this label iteration!
            label_iterations += 1

        # all stops are labeled: let's look at the end TAZ
        if path.outbound():
            end_taz_id = path.origin_taz_id
        else:
            end_taz_id = path.destination_taz_id
        taz_state  = (MAX_TIME, 0, "", 0)
        for stop_id, access_link in FT.tazs[end_taz_id].access_links.iteritems():

            if stop_id not in stop_states: continue

            stop_state      = stop_states[stop_id]

            # first leg has to be a trip
            if stop_state[Path.STATE_IDX_DEPARRMODE] == Path.STATE_MODE_TRANSFER: continue
            if stop_state[Path.STATE_IDX_DEPARRMODE] == Path.STATE_MODE_EGRESS:   continue
            if stop_state[Path.STATE_IDX_DEPARRMODE] == Path.STATE_MODE_ACCESS:   continue

            # outbound: depart time = depart time - access time
            # inbound:  arrive time = arrive time + access time
            new_label       = stop_state[Path.STATE_IDX_LABEL]  + access_link[TAZ.ACCESS_LINK_IDX_TIME]
            deparr_time     = stop_state[Path.STATE_IDX_DEPARR] - (access_link[TAZ.ACCESS_LINK_IDX_TIME]*dir_factor)

            if path.outbound() and \
               (stop_state[Path.STATE_IDX_DEPARRMODE], stop_id) in Assignment.bump_wait:
                # time a bumped passenger started waiting
                latest_time = Assignment.bump_wait[(stop_state[Path.STATE_IDX_DEPARRMODE], stop_id)]
                # we can't come in time
                if deparr_time - Assignment.PATH_TIME_WINDOW > latest_time: continue
                # leave earlier -- to get in line 5 minutes before bump wait time
                new_label       = new_label + (stop_state[Path.STATE_IDX_DEPARR] - latest_time) + Assignment.BUMP_BUFFER
                deparr_time  = latest_time - access_link[TAZ.ACCESS_LINK_IDX_TIME] - Assignment.BUMP_BUFFER

            new_taz_state   = ( \
                new_label,                                                                                  # label,
                deparr_time,                                                                                # departure/arrival time
                Path.STATE_MODE_ACCESS if path.direction==Path.DIR_OUTBOUND else Path.STATE_MODE_EGRESS,    # departure/arrival mode
                stop_id,                                                                                    # successor/predessor
                access_link[TAZ.ACCESS_LINK_IDX_TIME])                                                      # link time

            debug_str = ""
            if new_taz_state[Path.STATE_IDX_LABEL] < taz_state[Path.STATE_IDX_LABEL]:
                taz_state = new_taz_state
                if trace: debug_str = " !"

            if trace: FastTripsLogger.debug(" %s   %s %s" % ("+access" if path.outbound() else "+egress",
                                                            Path.state_str(end_taz_id, new_taz_state), debug_str))
        # Put results into path
        path.reset_states()
        if taz_state[Path.STATE_IDX_LABEL] != MAX_TIME:

            path.states[end_taz_id] = taz_state
            stop_state              = taz_state
            if path.outbound():  # outbound: egress to access and back
                final_state_type = Path.STATE_MODE_EGRESS
            else:                # inbound: access to egress and back
                final_state_type = Path.STATE_MODE_ACCESS
            while stop_state[Path.STATE_IDX_DEPARRMODE] != final_state_type:

                stop_id    = stop_state[Path.STATE_IDX_SUCCPRED]
                stop_state = stop_states[stop_id]
                path.states[stop_id] = stop_state

        if trace: FastTripsLogger.debug("Final path:\n%s" % str(path))
        return label_iterations

    @staticmethod
    def calculate_nonwalk_label(state_list, not_found_value):
        """
        Quick method to calculate nonwalk-label on a state list for :py:meth:`find_backwards_trip_based_hyperpath`
        """
        nonwalk_label = 0.0
        for state in state_list:
            if state[Path.STATE_IDX_DEPARRMODE] not in [Path.STATE_MODE_EGRESS, Path.STATE_MODE_TRANSFER, Path.STATE_MODE_ACCESS]:
                nonwalk_label += math.exp(-1.0*Assignment.DISPERSION_PARAMETER*state[Path.STATE_IDX_COST])
        if nonwalk_label == 0.0:
            nonwalk_label = not_found_value
        else:
            nonwalk_label = -1.0/Assignment.DISPERSION_PARAMETER*math.log(nonwalk_label)
        return nonwalk_label

    @staticmethod
    def test_rand():
        """
        A temporary random() call that reads :py:attr:`Assignment.RAND_FILENAME`
        and returns a random integer.
        """
        if Assignment.RAND_FILE == None:
            Assignment.RAND_FILE = open(Assignment.RAND_FILENAME, 'r')
            Assignment.RAND_MAX  = int(Assignment.RAND_FILE.readline().strip())
            FastTripsLogger.info("Opened rand file [%s], read MAX_RAND=%d" % \
                                 (Assignment.RAND_FILENAME, Assignment.RAND_MAX))
        return int(Assignment.RAND_FILE.readline().strip())

    @staticmethod
    def choose_state(prob_state_list):
        """
        prob_state_list = list of (cumulative probability, state)
        Returns the chosen state.

        .. todo:: For now, this uses :py:meth:`Assignment.test_rand`, but this should be removed.
        .. todo:: For now, cumulative probabilities are multiplied by 1000 so they're integers.

        """
        # Use test_rand() in a way consistent with the C++ implementation for now
        random_num = Assignment.test_rand()
        # FastTripsLogger.debug("random_num = %d -> %d" % (random_num, random_num % int(prob_state_list[-1][0])))
        random_num = random_num % prob_state_list[-1][0]
        for (cum_prob,state) in prob_state_list:
            if random_num < cum_prob:
                return state
        raise Exception("This shouldn't happen")

        # This is how I would prefer to do this
        random_num = random.random()*prob_state_list[-1][0]    #  [0.0, max cum prob)
        for (cum_prob,state) in prob_state_list:
            if random_num < cum_prob:
                return state
        raise

    @staticmethod
    def find_trip_based_hyperpath(FT, path, trace):
        """
        Perform backwards (destination to origin) trip-based hyperpath search.
        Will do so either backwards (destination to origin) if :py:attr:`Path.direction` is :py:attr:`Path.DIR_OUTBOUND`
        or forwards (origin to destination) if :py:attr:`Path.direction` is :py:attr:`Path.DIR_INBOUND`.

        Updates the path information in the given path and returns the number of label iterations required.

        :param FT: fasttrips data
        :type FT: a :py:class:`FastTrips` instance
        :param path: the path to fill in
        :type path: a :py:class:`Path` instance
        :param trace: pass True if this path should be traced to the debug log
        :type trace: boolean

        """
        if path.outbound():
            start_taz_id    = path.destination_taz_id
            dir_factor      = 1  # a lot of attributes are just the negative -- this facilitates that
        else:
            start_taz_id    = path.origin_taz_id
            dir_factor      = -1

        stop_states     = collections.defaultdict(list) # stop_id -> [label, departure, departure_mode, successor, cost, arrival]
        stop_queue      = Queue.PriorityQueue()         # (label, stop_id)
        MAX_TIME        = datetime.timedelta(minutes = 999.999)
        MAX_DATETIME    = datetime.datetime.combine(Assignment.TODAY, datetime.time()) + datetime.timedelta(hours=48)
        MAX_COST        = 999999
        stop_done       = set() # stop ids
        trips_used      = set() # trip ids

        # possible egress/access links
        for stop_id, access_link in FT.tazs[start_taz_id].access_links.iteritems():
            # outbound: departure time = destination - access
            # inbound:  arrival time   = origin      + access
            deparr_time          = datetime.datetime.combine(Assignment.TODAY, path.preferred_time) - \
                                   (access_link[TAZ.ACCESS_LINK_IDX_TIME]*dir_factor)
            # todo: why the 1+ ?
            cost                 = 1 + ((Path.WALK_EGRESS_TIME_WEIGHT if path.outbound() else Path.WALK_ACCESS_TIME_WEIGHT)* \
                                        access_link[TAZ.ACCESS_LINK_IDX_TIME].total_seconds()/60.0)
            stop_states[stop_id].append( [
                cost,                                                                   # label
                deparr_time,                                                            # departure/arrival
                Path.STATE_MODE_EGRESS if path.outbound() else Path.STATE_MODE_ACCESS,  # departure/arrival mode
                start_taz_id,                                                           # successor/prececessor
                access_link[TAZ.ACCESS_LINK_IDX_TIME],                                  # link time
                cost,                                                                   # cost
                MAX_DATETIME] )                                                         # arrival/departure
            stop_queue.put( (cost, stop_id) )
            if trace: FastTripsLogger.debug(" %s   %s" % ("+egress" if path.outbound() else "+access",
                                                          Path.state_str(stop_id, stop_states[stop_id][0])))

        # labeling loop
        label_iterations = 0
        while not stop_queue.empty():
            (current_label, current_stop_id) = stop_queue.get()

            if current_stop_id in stop_done: continue                   # stop is already processed
            if not FT.stops[current_stop_id].is_transfer(): continue    # no transfers to the stop
            stop_done.add(current_stop_id)                              # process this stop now - just once

            if trace:
                FastTripsLogger.debug("Pulling from stop_queue (iteration %d, label %.6f, stop %s) :======" % \
                                      (label_iterations, current_label, str(current_stop_id)))
                FastTripsLogger.debug("           " + Path.state_str_header(stop_states[current_stop_id][0], path.direction))
                for stop_state in stop_states[current_stop_id]:
                    FastTripsLogger.debug("           " + Path.state_str(current_stop_id, stop_state))
                FastTripsLogger.debug("==============================")

            current_stop_state      = stop_states[current_stop_id]                      # this is a list
            current_mode            = current_stop_state[0][Path.STATE_IDX_DEPARRMODE]  # why index 0?
            # latest departure for outbound, earliest arrival for inbound
            latest_dep_earliest_arr = current_stop_state[0][Path.STATE_IDX_DEPARR]
            for state in current_stop_state[1:]:
                if path.outbound():
                    latest_dep_earliest_arr = max( latest_dep_earliest_arr, state[Path.STATE_IDX_DEPARR])
                else:
                    latest_dep_earliest_arr = min( latest_dep_earliest_arr, state[Path.STATE_IDX_DEPARR])

            if trace:
                FastTripsLogger.debug("  current mode:     " + str(current_mode))
                FastTripsLogger.debug("  %s: %s" % ("latest departure" if path.outbound() else "earliest arrival",
                                                    latest_dep_earliest_arr.strftime("%H:%M:%S")))

            # Update by transfer
            # (We don't want to transfer to egress or transfer to a transfer)
            if current_mode not in [Path.STATE_MODE_EGRESS, Path.STATE_MODE_ACCESS]:

                # calculate the nonwalk label from nonwalk links
                nonwalk_label = Assignment.calculate_nonwalk_label(current_stop_state, MAX_COST)
                if trace: FastTripsLogger.debug("  nonwalk label:    %.4f" % nonwalk_label)

                for xfer_stop_id,xfer_attr in FT.stops[current_stop_id].transfers.iteritems():

                    transfer_time   = xfer_attr[Stop.TRANSFERS_IDX_TIME]
                    # outbound: departure time = latest departure - transfer
                    # inbound:  arrival time   = earliest arrival + transfer
                    deparr_time     = latest_dep_earliest_arr - (transfer_time*dir_factor)
                    cost            = nonwalk_label + (Path.WALK_TRANSFER_TIME_WEIGHT*transfer_time.total_seconds()/60.0)

                    old_label       = MAX_COST
                    new_label       = cost
                    if xfer_stop_id in stop_states:
                        old_label   = stop_states[xfer_stop_id][-1][Path.STATE_IDX_LABEL]
                        new_label   = math.exp(-1.0*Assignment.DISPERSION_PARAMETER*old_label) + \
                                      math.exp(-1.0*Assignment.DISPERSION_PARAMETER*cost)
                        new_label   = max(0.01, -1.0/Assignment.DISPERSION_PARAMETER*math.log(new_label))

                    if new_label < MAX_COST and new_label > 0:
                        stop_states[xfer_stop_id].append( [
                            new_label,                 # label,
                            deparr_time,               # departure/arrival time
                            Path.STATE_MODE_TRANSFER,  # departure/arrival mode
                            current_stop_id,           # successor/predecessor
                            transfer_time,             # link time
                            cost,                      # cost
                            MAX_DATETIME] )            # arrival/departure
                        stop_queue.put( (new_label, xfer_stop_id) )
                        if trace: FastTripsLogger.debug(" +transfer " + Path.state_str(xfer_stop_id, stop_states[xfer_stop_id][-1]))

            # Update by trips
            if path.outbound():
                # These are the trips that arrive at the stop in time to depart on time
                valid_trips = FT.stops[current_stop_id].get_trips_arriving_within_time(Assignment.TODAY,
                                                                                       latest_dep_earliest_arr,
                                                                                       Assignment.PATH_TIME_WINDOW)
            else:
                # These are the trips that depart from the stop in time for passenger
                valid_trips = FT.stops[current_stop_id].get_trips_departing_within_time(Assignment.TODAY,
                                                                                        latest_dep_earliest_arr,
                                                                                        Assignment.PATH_TIME_WINDOW)

            for (trip_id, seq, arrdep_time) in valid_trips:
                if trace: FastTripsLogger.debug("valid trips: %s  %d  %s" % (str(trip_id), seq, arrdep_time.strftime("%H:%M:%S")))

                if trip_id in trips_used: continue
                trips_used.add(trip_id)

                # trip arrival time (outbound) / trip departure time (inbound)
                arrdep_datetime = datetime.datetime.combine(Assignment.TODAY, arrdep_time)
                wait_time       = (latest_dep_earliest_arr - arrdep_datetime)*dir_factor

                if path.outbound():  # outbound: iterate through the stops before this one
                    trip_seq_list = range(1,seq)
                else:                # inbound: iterate through the stops after this one
                    trip_seq_list = range(seq+1, FT.trips[trip_id].number_of_stops()+1)

                for seq_num in trip_seq_list:
                    # board for outbound / alight for inbound
                    possible_board_alight   = FT.trips[trip_id].stops[seq_num-1]

                    # new_label = length of trip so far if the passenger boards at this stop
                    board_alight_stop   = possible_board_alight[Trip.STOPS_IDX_STOP_ID]
                    new_mode            = stop_states[board_alight_stop][0][Path.STATE_IDX_DEPARRMODE] \
                                          if board_alight_stop in stop_states else None  # why 0 index?
                    if new_mode in [Path.STATE_MODE_EGRESS, Path.STATE_MODE_ACCESS]: continue

                    deparr_time         = datetime.datetime.combine(Assignment.TODAY,
                                          possible_board_alight[Trip.STOPS_IDX_DEPARTURE_TIME] if path.outbound() else \
                                          possible_board_alight[Trip.STOPS_IDX_ARRIVAL_TIME])
                    in_vehicle_time     = (arrdep_datetime - deparr_time)*dir_factor

                    if current_mode in [Path.STATE_MODE_EGRESS, Path.STATE_MODE_ACCESS]:
                        cost = current_label + in_vehicle_time.total_seconds()/60.0                        + \
                                               (Path.SCHEDULE_DELAY_WEIGHT*wait_time.total_seconds()/60.0) + \
                                               (Path.FARE_PER_BOARDING*60.0/Path.VALUE_OF_TIME)
                    else:
                        cost = current_label + in_vehicle_time.total_seconds()/60.0                   + \
                                               (Path.WAIT_TIME_WEIGHT*wait_time.total_seconds()/60.0) + \
                                               (Path.FARE_PER_BOARDING*60.0/Path.VALUE_OF_TIME)       + \
                                               Path.TRANSFER_PENALTY

                    old_label       = MAX_COST
                    new_label       = cost
                    if board_alight_stop in stop_states:
                        old_label   = stop_states[board_alight_stop][-1][Path.STATE_IDX_LABEL]
                        new_label   = math.exp(-1.0*Assignment.DISPERSION_PARAMETER*old_label) + \
                                      math.exp(-1.0*Assignment.DISPERSION_PARAMETER*cost)
                        new_label   = max(0.01, -1.0/Assignment.DISPERSION_PARAMETER*math.log(new_label))

                    if new_label < MAX_COST and new_label > 0:
                        stop_states[board_alight_stop].append([
                            new_label,                 # label,
                            deparr_time,               # departure/arrival time
                            trip_id,                   # departure/arrival mode
                            current_stop_id,           # successor/predecessor
                            in_vehicle_time+wait_time, # link time
                            cost,                      # cost
                            arrdep_datetime] )         # arrival/departure
                        stop_queue.put( (new_label, board_alight_stop) )
                        if trace: FastTripsLogger.debug(" +trip     " + Path.state_str(board_alight_stop, stop_states[board_alight_stop][-1]))

            # Done with this label iteration!
            label_iterations += 1

        # all stops are labeled: let's look at the end TAZ
        if path.outbound():
            end_taz_id = path.origin_taz_id
        else:
            end_taz_id = path.destination_taz_id
        taz_state  = []
        for stop_id, access_link in FT.tazs[end_taz_id].access_links.iteritems():

            access_time             = access_link[TAZ.ACCESS_LINK_IDX_TIME]

            if stop_id not in stop_states:
                latest_dep_earliest_arr = MAX_DATETIME
                nonwalk_label           = MAX_COST
            else:
                stop_state              = stop_states[stop_id] # this is a list

                # if trace:
                #     FastTripsLogger.debug("stop state for stop that has %s to TAZ ====" % "access" if path.outbound() else "egress")
                #     FastTripsLogger.debug("           " + Path.state_str_header(stop_state[0], path.direction))
                #     for ss in stop_state:
                #         FastTripsLogger.debug("           " + Path.state_str(stop_id, ss))
                #     FastTripsLogger.debug("==============================")

                # earliest departure for outbound, latest arrival for inbound
                earliest_dep_latest_arr  = stop_state[0][Path.STATE_IDX_DEPARR]
                for state in stop_state[1:]:
                    if path.outbound():
                        earliest_dep_latest_arr = min( earliest_dep_latest_arr, state[Path.STATE_IDX_DEPARR] )
                    else:
                        earliest_dep_latest_arr = max( earliest_dep_latest_arr, state[Path.STATE_IDX_DEPARR] )

                nonwalk_label           = Assignment.calculate_nonwalk_label(stop_state, MAX_COST)

                # outbound: departure time = earlist departure - access
                # inbound:  arrival time   = latest arrival    - egress --??  Why wouldn't we add?
                deparr_time             = earliest_dep_latest_arr - access_time

            new_cost        = nonwalk_label + ((Path.WALK_ACCESS_TIME_WEIGHT if path.outbound() else Path.WALK_EGRESS_TIME_WEIGHT)* \
                                               access_time.total_seconds()/60.0)

            # if trace:
            #     FastTripsLogger.debug(" %s: %s" % ("earliest departure" if path.outbound() else "latest arrival",
            #                                         latest_dep_earliest_arr.strftime("%H:%M:%S")))
            #     FastTripsLogger.debug("  nonwalk label:    %.4f" % nonwalk_label)
            #     FastTripsLogger.debug("       new cost:    %.4f" % new_cost)

            old_label       = MAX_COST
            new_label       = new_cost
            if len(taz_state) > 0:
                old_label   = taz_state[-1][Path.STATE_IDX_LABEL]
                new_label   = math.exp(-1.0*Assignment.DISPERSION_PARAMETER*old_label) + \
                              math.exp(-1.0*Assignment.DISPERSION_PARAMETER*new_cost)
                new_label   = max(0.01, -1.0/Assignment.DISPERSION_PARAMETER*math.log(new_label))

            if new_label < MAX_COST and new_label > 0:
                taz_state.append( [
                    new_label,                                                              # label,
                    deparr_time,                                                            # departure/arrival time
                    Path.STATE_MODE_ACCESS if path.outbound() else Path.STATE_MODE_EGRESS,  # departure/arrival mode
                    stop_id,                                                                # successor/predecessor
                    access_time,                                                            # link time
                    new_cost,                                                               # cost
                    MAX_DATETIME] )                                                         # arrival/departure time
                if trace: FastTripsLogger.debug(" %s   %s" % ("+access" if path.outbound() else "+egress",
                                                              Path.state_str(end_taz_id, taz_state[-1])))

        # Nothing found
        if len(taz_state) == 0:  return label_iterations

        # Choose path and save those results
        path_found = False
        attempts   = 0
        while not path_found and attempts < Assignment.MAX_HYPERPATH_ASSIGN_ATTEMPTS:
            path_found = Assignment.choose_path_from_hyperpath_states(FT, path, trace, taz_state, stop_states)
            attempts += 1

            if not path_found:
                path.reset_states()
                continue

        return label_iterations

    @staticmethod
    def choose_path_from_hyperpath_states(FT, path, trace, taz_state, stop_states):
        """
        Choose a path from the hyperpath states.
        Returns True if path is set, False if we failed.
        """
        taz_label   = taz_state[-1][Path.STATE_IDX_LABEL]
        cost_cutoff = 1 #  taz_label - math.log(0.001)
        access_cum_prob = [] # (cum_prob, state)
        # Setup access probabilities
        for state in taz_state:
            prob = int(1000.0*math.exp(-1.0*Assignment.DISPERSION_PARAMETER*state[Path.STATE_IDX_COST])/ \
                              math.exp(-1.0*Assignment.DISPERSION_PARAMETER*taz_label))
            # these are too small to consider
            if prob < cost_cutoff: continue
            if len(access_cum_prob) == 0:
                access_cum_prob.append( (prob, state) )
            else:
                access_cum_prob.append( (prob + access_cum_prob[-1][0], state) )
            if trace: FastTripsLogger.debug("%10s: prob %4d  cum_prob %4d" % \
                                            (state[Path.STATE_IDX_SUCCPRED], prob, access_cum_prob[-1][0]))

        if path.outbound():
            start_state_id = path.origin_taz_id
            dir_factor     = 1
        else:
            start_state_id = path.destination_taz_id
            dir_factor     = -1

        path.states[start_state_id] = Assignment.choose_state(access_cum_prob)
        if trace: FastTripsLogger.debug(" -> Chose  %s" % Path.state_str(start_state_id, path.states[start_state_id]))

        current_stop = path.states[start_state_id][Path.STATE_IDX_SUCCPRED]
        # outbound: arrival time
        # inbound:  departure time
        arrdep_time  = path.states[start_state_id][Path.STATE_IDX_DEPARR] + \
                       (path.states[start_state_id][Path.STATE_IDX_LINKTIME]*dir_factor)
        last_trip    = path.states[start_state_id][Path.STATE_IDX_DEPARRMODE]
        while True:
            # setup probabilities
            if trace: FastTripsLogger.debug("current_stop=%8s; %s_time=%s; last_trip=%s" % \
                                            (str(current_stop), "arrival" if path.outbound() else "departure",
                                            arrdep_time.strftime("%H:%M:%S"), str(last_trip)))
            stop_cum_prob = [] # (cum_prob, state)
            sum_exp       = 0
            for state in stop_states[current_stop]:
                # no double walk
                if path.outbound() and \
                  (state[Path.STATE_IDX_DEPARRMODE] in [Path.STATE_MODE_EGRESS, Path.STATE_MODE_TRANSFER] and \
                                          last_trip in [Path.STATE_MODE_ACCESS, Path.STATE_MODE_TRANSFER]): continue
                if not path.outbound() and \
                  (state[Path.STATE_IDX_DEPARRMODE] in [Path.STATE_MODE_ACCESS, Path.STATE_MODE_TRANSFER] and \
                                          last_trip in [Path.STATE_MODE_EGRESS, Path.STATE_MODE_TRANSFER]): continue

                # outbound: we cannot depart before we arrive
                if     path.outbound() and state[Path.STATE_IDX_DEPARR] < arrdep_time: continue
                # inbound: we cannot arrive after we depart
                if not path.outbound() and state[Path.STATE_IDX_DEPARR] > arrdep_time: continue

                # calculating denominator
                sum_exp += math.exp(-1.0*Assignment.DISPERSION_PARAMETER*state[Path.STATE_IDX_COST])
                stop_cum_prob.append( [0, state] ) # need to finish finding denom to fill in cum prob
                if trace: FastTripsLogger.debug("           " + Path.state_str(current_stop, state))

            # Nope, dead end
            if len(stop_cum_prob) == 0:
                # Try assignment again...
                return False

            # denom found - cum prob time
            for idx in range(len(stop_cum_prob)):
                prob = int(1000*math.exp(-1.0*Assignment.DISPERSION_PARAMETER*stop_cum_prob[idx][1][Path.STATE_IDX_COST]) / sum_exp)
                if idx == 0:
                    stop_cum_prob[idx][0] = prob
                else:
                    stop_cum_prob[idx][0] = stop_cum_prob[idx-1][0] + prob
                if trace: FastTripsLogger.debug("%8s: prob %4d  cum_prob %4d" % \
                                                (stop_cum_prob[idx][1][Path.STATE_IDX_SUCCPRED], prob, stop_cum_prob[idx][0]))
            # choose!
            next_state   = Assignment.choose_state(stop_cum_prob)
            if trace: FastTripsLogger.debug(" -> Chose  %s" % Path.state_str(current_stop, next_state))

            # revise first link possibly -- let's not waste time
            if path.outbound() and len(path.states) == 1:
                dep_time = datetime.datetime.combine(Assignment.TODAY,
                                                     FT.trips[next_state[Path.STATE_IDX_DEPARRMODE]].get_scheduled_departure(current_stop))
                # effective trip start time
                path.states[path.origin_taz_id][Path.STATE_IDX_DEPARR] = dep_time - path.states[path.origin_taz_id][Path.STATE_IDX_LINKTIME]

            path.states[current_stop] = next_state
            current_stop = next_state[Path.STATE_IDX_SUCCPRED]
            last_trip    = next_state[Path.STATE_IDX_DEPARRMODE]
            if next_state[Path.STATE_IDX_DEPARRMODE] == Path.STATE_MODE_TRANSFER:
                # outbound: arrival time   = arrival time + link time
                # inbound:  departure time = departure time - link time
                arrdep_time = arrdep_time + (next_state[Path.STATE_IDX_LINKTIME]*dir_factor)
            else:
                arrdep_time = next_state[Path.STATE_IDX_ARRIVAL]
            # if we get to egress, we're done!
            if (    path.outbound() and next_state[Path.STATE_IDX_DEPARRMODE] == Path.STATE_MODE_EGRESS) or \
               (not path.outbound() and next_state[Path.STATE_IDX_DEPARRMODE] == Path.STATE_MODE_ACCESS):
                if trace: FastTripsLogger.debug("Final path: %s" % str(path))
                break
        return True

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
    def read_assignment_results(output_dir):
        """
        Reads assignment results from :py:attr:`Assignment.PASSENGERS_CSV` and :py:attr:Assignment.VEH_TRIPS_CSV`

        Returns 3-tuple with:

        * num_paths_assigned
        * passengers_df
        * veh_trips_df)
        """
        # 2015-06-22 15:42:31 DEBUG Setup passengers dataframe:
        # passenger_id              int64
        # path_id                   int64
        # pathdir                   int64
        # pathmode                  int64
        # linkmode                 object
        # trip_id                 float64
        # A_id                    float64
        # B_id                    float64
        # A_time           datetime64[ns]
        # B_time           datetime64[ns]
        # linktime        timedelta64[ns]

        # read existing paths
        passengers_df = pandas.read_csv(os.path.join(output_dir, Assignment.PASSENGERS_CSV),
                                        parse_dates=['A_time','B_time'])
        passengers_df['linktime'] = pandas.to_timedelta(passengers_df['linktime'])

        FastTripsLogger.info("Read %s" % os.path.join(output_dir, Assignment.PASSENGERS_CSV))
        FastTripsLogger.debug("passengers_df.dtypes=\n%s" % str(passengers_df.dtypes))

        # 2015-06-22 15:42:34 DEBUG Setup vehicle trips dataframe:
        # route_id                 int64
        # shape_id                 int64
        # direction                int64
        # trip_id                  int64
        # stop_seq                 int64
        # stop_id                  int64
        # capacity                 int64
        # arrive_time     datetime64[ns]
        # depart_time     datetime64[ns]
        # service_type             int64
        veh_trips_df  = pandas.read_csv(os.path.join(output_dir, Assignment.VEH_TRIPS_CSV),
                                        parse_dates=['arrive_time','depart_time'])

        FastTripsLogger.info("Read %s" % os.path.join(output_dir, Assignment.VEH_TRIPS_CSV))
        FastTripsLogger.debug("veh_trips_df.dtypes=\n%s" % str(veh_trips_df.dtypes))

        uniq_pax = passengers_df[['passenger_id']].drop_duplicates(subset='passenger_id')
        num_paths_assigned = len(uniq_pax)

        return (num_paths_assigned, passengers_df, veh_trips_df)

    @staticmethod
    def setup_passengers(FT, output_dir):
        """
        Create pandas.DataFrame with passenger states

        passenger id
        path id
        mode (access, transfer, trip, egress, new: wait?)
        trip id (NaN if mode != trip)
        board id (stop id or taz id if access)
        alight id (stop id or taz id if egress)
        board time
        alight time
        link timedelta
        processed bool

        """
        mylist = []
        path_id = 0
        for passenger in FT.passengers:
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
                    a_time          = state[Path.STATE_IDX_DEPARR]
                    b_time          = a_time + state[Path.STATE_IDX_LINKTIME]
                    if not passenger.path.outbound():
                        a_id        = state[Path.STATE_IDX_SUCCPRED]
                        b_id        = state_id
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
                           a_time,
                           b_time,
                           state[Path.STATE_IDX_LINKTIME]]
                    mylist.append(row)

                    prev_linkmode = linkmode
            path_id += 1
        df =  pandas.DataFrame(mylist,
                               columns=['passenger_id', 'path_id',
                                        'pathdir',  # for debugging
                                        'pathmode', # for output
                                        'linkmode', 'trip_id',
                                        'A_id','B_id',
                                        'A_time', 'B_time',
                                        'linktime'])
        FastTripsLogger.debug("Setup passengers dataframe:\n%s" % str(df.dtypes))
        df.to_csv(os.path.join(output_dir, Assignment.PASSENGERS_CSV), index=False)
        FastTripsLogger.info("Wrote passengers dataframe to %s" % os.path.join(output_dir, Assignment.PASSENGERS_CSV))
        return df

    @staticmethod
    def setup_trips(FT, output_dir):
        """
        Sets up and returns a :py:class:`pandas.DataFrame` where each row contains a leg of a transit vehicle trip.
        """
        mylist = []
        for trip_id, trip in FT.trips.iteritems():
            stop_seq = 0
            for stop_tuple in trip.stops:
                row = [trip.route_id,
                       trip.shape_id,
                       trip.direction_id,
                       trip_id,
                       stop_seq,
                       stop_tuple[Trip.STOPS_IDX_STOP_ID],
                       datetime.datetime.combine(Assignment.TODAY,
                                                 stop_tuple[Trip.STOPS_IDX_ARRIVAL_TIME]),
                       datetime.datetime.combine(Assignment.TODAY,
                                                 stop_tuple[Trip.STOPS_IDX_DEPARTURE_TIME]),
                       trip.service_type
                       ]
                mylist.append(row)
                stop_seq += 1
        df = pandas.DataFrame(mylist,
                              columns=['route_id','shape_id','direction', # these go into output -- otherwise they're useless
                                       'trip_id','stop_seq','stop_id',
                                       'arrive_time','depart_time',
                                       'service_type', # for dwell time
                                      ])
        FastTripsLogger.debug("Setup vehicle trips dataframe:\n%s" % str(df.dtypes))
        df.to_csv(os.path.join(output_dir, Assignment.VEH_TRIPS_CSV), index=False)
        FastTripsLogger.info("Wrote vehicle trips dataframe to %s" % os.path.join(output_dir, Assignment.VEH_TRIPS_CSV))

        return df

    @staticmethod
    def simulate(FT, passengers_df, veh_trips_df):
        """
        Actually assign the passengers trips to the vehicles.

        .. todo:: Remove step zero.  Duplicate passenger IDs should be ok because we can generate unique path IDs.

        """
        start_time              = datetime.datetime.now()
        passengers_arrived      = 0   #: arrived at destination TAZ
        passengers_bumped       = 0

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

        passengers_df = pandas.merge(left   =passengers_df,              right   =passengers_dedupe,
                                     left_on=['passenger_id','path_id'], right_on=['passenger_id','path_id'],
                                     how    ='left')
        passengers_df = passengers_df[passengers_df.keep==True]
        passengers_df.drop('keep', axis=1, inplace=True)

        # veh_trips_df.set_index(['trip_id','stop_seq','stop_id'],verify_integrity=True,inplace=True)
        FastTripsLogger.debug("veh_trips_df types = \n%s" % str(veh_trips_df.dtypes))
        FastTripsLogger.debug("veh_trips_df: \n%s" % veh_trips_df.head(50).to_string(formatters=
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
        FastTripsLogger.info("Step 1. Put passenger paths on transit vehicles to get vehicle boards/alights/load")

        # Just look at trips
        passenger_trips = passengers_df.loc[passengers_df['linkmode']=='Trip']
        passenger_trips_len = len(passenger_trips)
        FastTripsLogger.debug("Passenger Trips: \n%s" % str(passenger_trips.head()))

        # Group to boards by counting path_ids for a (trip_id, A_id as stop_id)
        passenger_trips_boards = passenger_trips[['path_id','trip_id','A_id']].groupby(['trip_id','A_id']).count()
        passenger_trips_boards.index.names = ['trip_id','stop_id']

        # And alights by counting path_ids for a (trip_id, B_id as stop_id)
        passenger_trips_alights = passenger_trips[['path_id','trip_id','B_id']].groupby(['trip_id','B_id']).count()
        passenger_trips_alights.index.names = ['trip_id','stop_id']

        # Join them to the transit vehicle trips so we can put people on vehicles
        # TODO: This will be wrong when stop_id is not unique for a trip
        veh_trips_df = pandas.merge(left   =veh_trips_df,          right      =passenger_trips_boards,
                                    left_on=['trip_id','stop_id'], right_index=True,
                                    how    ='left')
        veh_trips_df.rename(columns={'path_id':'boards'}, inplace=True)

        veh_trips_df = pandas.merge(left   =veh_trips_df,          right      =passenger_trips_alights,
                                    left_on=['trip_id','stop_id'], right_index=True,
                                    how    ='left')
        veh_trips_df.rename(columns={'path_id':'alights'}, inplace=True)
        veh_trips_df.fillna(value=0, inplace=True)

        FastTripsLogger.debug("%d == %d ?" % (len(veh_trips_df), veh_trips_df_len))
        # assert(len(veh_trips_df)==veh_trips_df_len)

        veh_trips_df.set_index(['trip_id','stop_seq'],inplace=True)
        veh_trips_df['onboard'] = veh_trips_df.boards - veh_trips_df.alights
        # print veh_trips_df.loc[5123368]

        # on board is the cumulative sum of boards - alights
        trips_cumsum = veh_trips_df[['onboard']].groupby(level=[0]).cumsum()
        veh_trips_df.drop('onboard', axis=1, inplace=True) # replace with cumsum
        veh_trips_df = pandas.merge(left      =veh_trips_df,  right      =trips_cumsum,
                                    left_index=True,          right_index=True,
                                    how='left')
        FastTripsLogger.debug("%d == %d ?" % (len(veh_trips_df), veh_trips_df_len))
        # print veh_trips_df.loc[5123368]

        # Who gets bumped?
        if Assignment.CAPACITY_CONSTRAINT:
            # TODO
            pass

        ######################################################################################################
        FastTripsLogger.info("Step 2. Find out board/alight times for passengers from vehicle times")

        passenger_trips = pandas.merge(left   =passenger_trips,      right   =veh_trips_df[['stop_id','depart_time']].reset_index(),
                                       left_on=['trip_id','A_id'],   right_on=['trip_id','stop_id'],
                                       how='left')
        passenger_trips = pandas.merge(left   =passenger_trips,      right   =veh_trips_df[['stop_id','arrive_time']].reset_index(),
                                       left_on=['trip_id','B_id'],   right_on=['trip_id','stop_id'],
                                       how='left')
        passenger_trips.rename(columns=
           {'depart_time'   :'board_time',      # transit vehicle depart time (at A) = board time for pax
            'A_time'        :'arrival_time',    # passenger arrival at the stop
            'arrive_time'   :'alight_time',     # transit vehicle arrive time (at B) = alight time for pax
            'stop_seq_x'    :'A_seq',
            'stop_seq_y'    :'B_seq'
            }, inplace=True)
        passenger_trips.drop(['stop_id_x','stop_id_y'], axis=1, inplace=True) # redundant with A_id, B_id

        ######################################################################################################
        FastTripsLogger.info("Step 3. Some trips (outbound) were found by searching backwards, so they wait *after arriving*.")
        FastTripsLogger.info("        -> They should just move on and wait at the next stop (if there is one)")

        # Get trip board/alight time back to the passengers table
        passengers_df = pandas.merge(left=passengers_df, right=passenger_trips[['passenger_id','path_id','trip_id','board_time','alight_time','A_seq','B_seq']],
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
        FastTripsLogger.info("Step 4. Some trips leave too early and wait at the *first* stop.")
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

        ######################################################################################################
        FastTripsLogger.info("Step 5. Add up travel costs")
        Path.calculate_tripcost(passengers_df)

        # passenger_trips is all wrong now -- redo
        passenger_trips = passengers_df.loc[passengers_df.linkmode=='Trip'].copy()
        passenger_trips.rename(columns={'A_time' :'arrival_time'}, inplace=True)

        ######################################################################################################
        FastTripsLogger.info("Step 6. Convert times to strings (minutes past midnight) for joining")

        ######         TODO: this is really catering to output format; an alternative might be more appropriate
        passenger_trips.loc[:,  'board_time_str'] = passenger_trips.board_time.apply(Assignment.datetime64_min_formatter)
        passenger_trips.loc[:,'arrival_time_str'] = passenger_trips.arrival_time.apply(Assignment.datetime64_min_formatter)
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

        for trace_pax in Assignment.TRACE_PASSENGER_IDS:
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

        return (len(pax_exp_df), veh_trips_df, pax_exp_df)

    @staticmethod
    def print_load_profile(veh_trips_df, output_dir):
        """
        Print the load profile output
        """
        # reset columns
        print_veh_trips_df = veh_trips_df.reset_index()

        Trip.calculate_dwell_times(print_veh_trips_df)
        print_veh_trips_df = Trip.calculate_headways(print_veh_trips_df)

        # rename columns
        print_veh_trips_df.rename(columns=
           {'route_id'       :'routeId',
            'shape_id'       :'shapeId',
            'trip_id'        :'tripId',
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