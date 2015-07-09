# cython: profile=True
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
import heapq
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
    TRACE_PASSENGER_IDS             = [48]

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

    #: This is a :py:class:`set` of bumped passenger IDs.  For multiple-iteration assignment,
    #: this determines which passengers to assign.
    bumped_ids                      = set()

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
                (num_paths_assigned, passengers_df) = Assignment.read_assignment_results(output_dir, iteration)

            else:
                num_paths_assigned = Assignment.assign_passengers(FT, iteration)
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

            if iteration > 1 and passenger_id not in Assignment.bumped_ids:
                num_paths_assigned += 1
                continue

            trace_passenger = False
            if passenger_id in Assignment.TRACE_PASSENGER_IDS:
                FastTripsLogger.debug("Tracing assignment of passenger %s" % str(passenger_id))
                trace_passenger = True

            asgn_iters = Assignment.find_trip_based_path(FT, passenger.path,
                                                         hyperpath=Assignment.ASSIGNMENT_TYPE==Assignment.ASSIGNMENT_TYPE_STO_ASGN,
                                                         trace=trace_passenger)

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
    def choose_state(prob_state_list, trace):
        """
        prob_state_list = list of (cumulative probability, state)
        Returns the chosen state.

        .. todo:: For now, this uses :py:meth:`Assignment.test_rand`, but this should be removed.
        .. todo:: For now, cumulative probabilities are multiplied by 1000 so they're integers.

        """
        # Use test_rand() in a way consistent with the C++ implementation for now
        random_num = Assignment.test_rand()
        if trace: FastTripsLogger.debug("random_num = %d -> %d" % (random_num, random_num % int(prob_state_list[-1][0])))
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
    def find_trip_based_path(FT, path, hyperpath, trace):
        """
        Perform trip-based path search.

        Will do so either backwards (destination to origin) if :py:attr:`Path.direction` is :py:attr:`Path.DIR_OUTBOUND`
        or forwards (origin to destination) if :py:attr:`Path.direction` is :py:attr:`Path.DIR_INBOUND`.

        Updates the path information in the given path and returns the number of label iterations required.

        :param FT: fasttrips data
        :type FT: a :py:class:`FastTrips` instance
        :param path: the path to fill in
        :type path: a :py:class:`Path` instance
        :param hyperpath: pass True to use a stochastic hyperpath-finding algorithm, otherwise a deterministic shortest path
                          search algorithm will be use.
        :type hyperpath: boolean
        :param trace: pass True if this path should be traced to the debug log
        :type trace: boolean

        """
        cdef:
            int start_taz_id, dir_factor
            int stop_id, trip_id, seq
            int label_iterations
            object access_link


        if path.outbound():
            start_taz_id    = path.destination_taz_id
            dir_factor      = 1  # a lot of attributes are just the negative -- this facilitates that
        else:
            start_taz_id    = path.origin_taz_id
            dir_factor      = -1

        stop_states     = collections.defaultdict(list)
        stop_queue      = [] # Queue.PriorityQueue()         # (label, stop_id)
        MAX_TIME        = datetime.timedelta(minutes = 999.999)
        MAX_DATETIME    = datetime.datetime.combine(Assignment.TODAY, datetime.time()) + datetime.timedelta(hours=48)
        MAX_COST        = 999999
        stop_done       = set() # stop ids
        trips_used      = set() # trip ids

        # possible egress/access links
        for stop_id, access_link in FT.tazs[start_taz_id].access_links.iteritems():
            # outbound: departure time = destination - access
            # inbound:  arrival time   = origin      + access
            deparr_time = datetime.datetime.combine(Assignment.TODAY, path.preferred_time) - \
                          (access_link[TAZ.ACCESS_LINK_IDX_TIME]*dir_factor)

            if hyperpath:
                # todo: why the 1+ ?
                cost    = 1 + ((Path.WALK_EGRESS_TIME_WEIGHT if path.outbound() else Path.WALK_ACCESS_TIME_WEIGHT)* \
                          access_link[TAZ.ACCESS_LINK_IDX_TIME].total_seconds()/60.0)
            else:
                cost    = access_link[TAZ.ACCESS_LINK_IDX_TIME]
            stop_states[stop_id].append( [
                cost,                                                                   # label
                deparr_time,                                                            # departure/arrival
                Path.STATE_MODE_EGRESS if path.outbound() else Path.STATE_MODE_ACCESS,  # departure/arrival mode
                start_taz_id,                                                           # successor/prececessor
                access_link[TAZ.ACCESS_LINK_IDX_TIME],                                  # link time
                cost,                                                                   # cost
                MAX_DATETIME] )                                                         # arrival/departure
            heapq.heappush(stop_queue, (cost, stop_id) )
            if trace: FastTripsLogger.debug(" %s   %s" % ("+egress" if path.outbound() else "+access",
                                                          Path.state_str(stop_id, stop_states[stop_id][0])))

        # stop_states: stop_id -> [label, departure/arrival, departure/arrival mode, successor/precessor, cost, arrival]
        #  *means*
        # for outbound: we can depart from *stop_id*
        #                      via *departure mode*
        #                      at *departure time*
        #                      and get to stop *successor*
        #                      and the total cost from *stop_id* to the destination TAZ is *label*
        # for inbound: we can arrive at *stop_id*
        #                     via *arrival mode*
        #                     at *arrival time*
        #                     from stop *predecessor*
        #                     and the total cost from the origin TAZ to the *stop_id* is *label*

        # labeling loop
        label_iterations = 0
        while stop_queue:  # continues until queue is empty
            (current_label, current_stop_id) = heapq.heappop(stop_queue)

            if current_stop_id in stop_done: continue                   # stop is already processed
            if not FT.stops[current_stop_id].is_transfer(): continue    # no transfers to the stop
            stop_done.add(current_stop_id)                              # process this stop now - just once

            if trace:
                FastTripsLogger.debug("Pulling from stop_queue (iteration %d, label %-12s, stop %s) :======" % \
                                      (label_iterations,
                                       str(current_label) if type(current_label)==datetime.timedelta else "%.4f" % current_label,
                                       str(current_stop_id)))
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
            if current_mode not in [Path.STATE_MODE_EGRESS, Path.STATE_MODE_ACCESS,\
                                    Path.STATE_MODE_TRANSFER if not hyperpath else "TransferOK"]:
                nonwalk_label = 0
                if hyperpath:
                    # calculate the nonwalk label from nonwalk links
                    nonwalk_label = Assignment.calculate_nonwalk_label(current_stop_state, MAX_COST)
                    if trace: FastTripsLogger.debug("  nonwalk label:    %.4f" % nonwalk_label)

                for xfer_stop_id,xfer_attr in FT.stops[current_stop_id].transfers.iteritems():

                    transfer_time       = xfer_attr[Stop.TRANSFERS_IDX_TIME]
                    # outbound: departure time = latest departure - transfer
                    #  inbound: arrival time   = earliest arrival + transfer
                    deparr_time         = latest_dep_earliest_arr - (transfer_time*dir_factor)
                    use_new_state       = False

                    # stochastic/hyperpath: cost update
                    if hyperpath:
                        cost            = nonwalk_label + (Path.WALK_TRANSFER_TIME_WEIGHT*transfer_time.total_seconds()/60.0)

                        old_label       = MAX_COST
                        new_label       = cost
                        if xfer_stop_id in stop_states:
                            old_label   = stop_states[xfer_stop_id][-1][Path.STATE_IDX_LABEL]
                            new_label   = math.exp(-1.0*Assignment.DISPERSION_PARAMETER*old_label) + \
                                          math.exp(-1.0*Assignment.DISPERSION_PARAMETER*cost)
                            new_label   = max(0.01, -1.0/Assignment.DISPERSION_PARAMETER*math.log(new_label))

                        if new_label < MAX_COST and new_label > 0: use_new_state = True

                    # deterministic: cost is just additive
                    else:
                        cost        = transfer_time
                        new_label   = current_stop_state[0][Path.STATE_IDX_LABEL] + transfer_time

                        # check (departure mode, stop) if someone's waiting already
                        # curious... this only applies to OUTBOUND
                        if path.outbound() and (current_mode, current_stop_id) in Assignment.bump_wait:
                            # time a bumped passenger started waiting
                            latest_time = Assignment.bump_wait[(current_mode, current_stop_id)]
                            # we can't come in time
                            if deparr_time - Assignment.PATH_TIME_WINDOW > latest_time: continue
                            # leave earlier -- to get in line 5 minutes before bump wait time
                            # (confused... We don't resimulate previous bumping passenger so why does this make sense?)
                            new_label       = new_label + (current_stop_state[0][Path.STATE_IDX_DEPARR] - latest_time) + Assignment.BUMP_BUFFER
                            deparr_time     = latest_time - xfer_attr[Stop.TRANSFERS_IDX_TIME] - Assignment.BUMP_BUFFER

                        old_label       = MAX_TIME
                        if xfer_stop_id in stop_states:
                            old_label   = stop_states[xfer_stop_id][0][Path.STATE_IDX_LABEL]

                            if new_label < old_label:
                                use_new_state = True
                                # clear it - we only have one
                                del stop_states[xfer_stop_id][0]
                        else:
                            use_new_state = True

                    if use_new_state:
                        stop_states[xfer_stop_id].append( [
                            new_label,                 # label,
                            deparr_time,               # departure/arrival time
                            Path.STATE_MODE_TRANSFER,  # departure/arrival mode
                            current_stop_id,           # successor/predecessor
                            transfer_time,             # link time
                            cost,                      # cost
                            MAX_DATETIME] )            # arrival/departure
                        heapq.heappush(stop_queue, (new_label, xfer_stop_id) )
                        if trace: FastTripsLogger.debug(" +transfer " + Path.state_str(xfer_stop_id, stop_states[xfer_stop_id][-1]))

            # Update by trips
            if path.outbound():
                # These are the trips that arrive at the stop in time to depart on time
                valid_trips = Stop.get_trips_arriving_within_time(FT.stops[current_stop_id].trips, Assignment.TODAY,
                                                                                       latest_dep_earliest_arr,
                                                                                       Assignment.PATH_TIME_WINDOW)
            else:
                # These are the trips that depart from the stop in time for passenger
                valid_trips = Stop.get_trips_departing_within_time(FT.stops[current_stop_id].trips, Assignment.TODAY,
                                                                                        latest_dep_earliest_arr,
                                                                                        Assignment.PATH_TIME_WINDOW)

            for (trip_id, seq, arrdep_time) in valid_trips:
                if trace: FastTripsLogger.debug("valid trips: %s  %d  %s" % (str(trip_id), seq, arrdep_time.strftime("%H:%M:%S")))

                if trip_id in trips_used: continue

                # trip arrival time (outbound) / trip departure time (inbound)
                arrdep_datetime = datetime.datetime.combine(Assignment.TODAY, arrdep_time)
                wait_time       = (latest_dep_earliest_arr - arrdep_datetime)*dir_factor

                # deterministic assignment: check capacities
                if not hyperpath:
                    if path.outbound():
                        # if outbound, this trip loop is possible trips *before* the current trip
                        # checking that we get here in time for the current trip
                        check_for_bump_wait = (current_stop_state[0][Path.STATE_IDX_DEPARRMODE], current_stop_id)
                        # arrive from the loop trip
                        arrive_datetime     = arrdep_datetime
                    else:
                        # if inbound, the trip is the next trip
                        # checking that we can get here in time for that trip
                        check_for_bump_wait = (trip_id, current_stop_id)
                        # arrive for this trip
                        arrive_datetime     = current_stop_state[0][Path.STATE_IDX_DEPARR]

                    if check_for_bump_wait in Assignment.bump_wait:
                        # time a bumped passenger started waiting
                        latest_time = Assignment.bump_wait[check_for_bump_wait]
                        if trace: FastTripsLogger.debug("checking latest_time %s vs arrive_datetime %s for potential trip %s" % \
                                                        (latest_time.strftime("%H:%M:%S"), arrive_datetime.strftime("%H:%M:%S"),
                                                         str(trip_id)))
                        if arrive_datetime + datetime.timedelta(minutes = 0.01) >= latest_time and \
                           current_stop_state[0][Path.STATE_IDX_DEPARRMODE] != trip_id:
                            if trace: FastTripsLogger.debug("Continuing")
                            continue
                trips_used.add(trip_id)

                if path.outbound():  # outbound: iterate through the stops before this one
                    trip_seq_list = range(1,seq)
                else:                # inbound: iterate through the stops after this one
                    trip_seq_list = range(seq+1, FT.trips[trip_id].number_of_stops()+1)

                for seq_num in trip_seq_list:
                    # board for outbound / alight for inbound
                    possible_board_alight   = FT.trips[trip_id].stops[seq_num-1]

                    # new_label = length of trip so far if the passenger boards/alights at this stop
                    board_alight_stop   = possible_board_alight[Trip.STOPS_IDX_STOP_ID]

                    if hyperpath:
                        new_mode        = stop_states[board_alight_stop][0][Path.STATE_IDX_DEPARRMODE] \
                                          if board_alight_stop in stop_states else None  # why 0 index?
                        if new_mode in [Path.STATE_MODE_EGRESS, Path.STATE_MODE_ACCESS]: continue

                    deparr_time         = datetime.datetime.combine(Assignment.TODAY,
                                          possible_board_alight[Trip.STOPS_IDX_DEPARTURE_TIME] if path.outbound() else \
                                          possible_board_alight[Trip.STOPS_IDX_ARRIVAL_TIME])
                    in_vehicle_time     = (arrdep_datetime - deparr_time)*dir_factor
                    use_new_state       = False

                    # stochastic/hyperpath: cost update
                    if hyperpath:
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

                        if new_label < MAX_COST and new_label > 0: use_new_state = True
                    # deterministic: cost is just additive
                    else:
                        cost            = in_vehicle_time + wait_time
                        new_label       = current_label + in_vehicle_time + wait_time

                        old_label       = MAX_TIME
                        if board_alight_stop in stop_states:
                            old_label   = stop_states[board_alight_stop][0][Path.STATE_IDX_LABEL]

                            if new_label < old_label:
                                use_new_state = True
                                # clear it - we only have one
                                del stop_states[board_alight_stop][0]
                            elif trace:
                                rej_state = [
                                    new_label,                 # label,
                                    deparr_time,               # departure/arrival time
                                    trip_id,                   # departure/arrival mode
                                    current_stop_id,           # successor/predecessor
                                    in_vehicle_time+wait_time, # link time
                                    cost,                      # cost
                                    arrdep_datetime]           # arrival/departure
                                FastTripsLogger.debug(" -trip     " + Path.state_str(board_alight_stop, rej_state) + " - old_label " + str(old_label))
                        else:
                            use_new_state = True

                    if use_new_state:
                        stop_states[board_alight_stop].append([
                            new_label,                 # label,
                            deparr_time,               # departure/arrival time
                            trip_id,                   # departure/arrival mode
                            current_stop_id,           # successor/predecessor
                            in_vehicle_time+wait_time, # link time
                            cost,                      # cost
                            arrdep_datetime] )         # arrival/departure
                        heapq.heappush(stop_queue, (new_label, board_alight_stop) )
                        if trace: FastTripsLogger.debug(" +trip     " + Path.state_str(board_alight_stop, stop_states[board_alight_stop][-1]))

            # Done with this label iteration!
            label_iterations += 1

        # all stops are labeled: let's look at the end TAZ
        if path.outbound():
            end_taz_id = path.origin_taz_id
        else:
            end_taz_id = path.destination_taz_id

        taz_state         = []
        for stop_id, access_link in FT.tazs[end_taz_id].access_links.iteritems():

            access_time   = access_link[TAZ.ACCESS_LINK_IDX_TIME]
            use_new_state = False
            if stop_id not in stop_states:
                # for deterministic - we can't get to this stop so move on
                if not hyperpath: continue

                # for stochastic/hyperpath - this factors in to the cost...
                # todo: why?
                earliest_dep_latest_arr = MAX_DATETIME
                nonwalk_label           = MAX_COST
                stop_state              = None
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
                if hyperpath:
                    for state in stop_state[1:]:
                        if path.outbound():
                            earliest_dep_latest_arr = min( earliest_dep_latest_arr, state[Path.STATE_IDX_DEPARR] )
                        else:
                            earliest_dep_latest_arr = max( earliest_dep_latest_arr, state[Path.STATE_IDX_DEPARR] )

                    nonwalk_label = Assignment.calculate_nonwalk_label(stop_state, MAX_COST)

            # outbound: origin TAZ departure time = earliest departure from the first stop - access
            # inbound:  dest   TAZ arrival time   = latest arrival at the last stop        - egress --??  Why wouldn't we add?
            #: .. todo:: hyperpath seems wrong
            if hyperpath:
                deparr_time     = earliest_dep_latest_arr - access_time
            else:
                deparr_time     = earliest_dep_latest_arr - (access_time*dir_factor)

            # if trace:
            #     FastTripsLogger.debug(" %s: %s" % ("earliest departure" if path.outbound() else "latest arrival",
            #                                         latest_dep_earliest_arr.strftime("%H:%M:%S")))
            #     FastTripsLogger.debug("  nonwalk label:    %.4f" % nonwalk_label)
            #     FastTripsLogger.debug("       new cost:    %.4f" % new_cost)

            # stochastic/hyperpath: cost update
            if hyperpath:
                new_cost    = nonwalk_label + ((Path.WALK_ACCESS_TIME_WEIGHT if path.outbound() else Path.WALK_EGRESS_TIME_WEIGHT)* \
                              access_time.total_seconds()/60.0)

                old_label       = MAX_COST
                new_label       = new_cost
                if len(taz_state) > 0:
                    old_label   = taz_state[-1][Path.STATE_IDX_LABEL]
                    new_label   = math.exp(-1.0*Assignment.DISPERSION_PARAMETER*old_label) + \
                                  math.exp(-1.0*Assignment.DISPERSION_PARAMETER*new_cost)
                    new_label   = max(0.01, -1.0/Assignment.DISPERSION_PARAMETER*math.log(new_label))

                if new_label < MAX_COST and new_label > 0:
                    use_new_state = True

            # deterministic: cost is just additive
            else:
                # first leg has to be a trip
                if stop_state[0][Path.STATE_IDX_DEPARRMODE] == Path.STATE_MODE_TRANSFER: continue
                if stop_state[0][Path.STATE_IDX_DEPARRMODE] == Path.STATE_MODE_EGRESS:   continue
                if stop_state[0][Path.STATE_IDX_DEPARRMODE] == Path.STATE_MODE_ACCESS:   continue
                new_cost        = access_link[TAZ.ACCESS_LINK_IDX_TIME]
                new_label       = stop_state[0][Path.STATE_IDX_LABEL] + access_link[TAZ.ACCESS_LINK_IDX_TIME]

                # capacity check
                if path.outbound() and (stop_state[0][Path.STATE_IDX_DEPARRMODE], stop_id) in Assignment.bump_wait:
                   # time a bumped passenger started waiting
                    latest_time = Assignment.bump_wait[(stop_state[0][Path.STATE_IDX_DEPARRMODE], stop_id)]
                    # we can't come in time
                    if deparr_time - Assignment.PATH_TIME_WINDOW > latest_time: continue
                    # leave earlier -- to get in line 5 minutes before bump wait time
                    new_label   = new_label + (stop_state[Path.STATE_IDX_DEPARR] - latest_time) + Assignment.BUMP_BUFFER
                    deparr_time = latest_time - access_link[TAZ.ACCESS_LINK_IDX_TIME] - Assignment.BUMP_BUFFER

                old_label = MAX_TIME
                if len(taz_state) > 0:
                    old_label = taz_state[0][Path.STATE_IDX_LABEL]
                    if new_label < old_label:
                        use_new_state = True
                        # clear it - we only have one
                        del taz_state[0]
                else:
                    use_new_state = True

            if use_new_state:
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

        # Put results into path
        path.reset_states()

        # Nothing found
        if len(taz_state) == 0:  return label_iterations

        if hyperpath:
            # Choose path and save those results
            path_found = False
            attempts   = 0
            while not path_found and attempts < Assignment.MAX_HYPERPATH_ASSIGN_ATTEMPTS:
                path_found = Assignment.choose_path_from_hyperpath_states(FT, path, trace, taz_state, stop_states)
                attempts += 1

                if not path_found:
                    path.reset_states()
                    continue
        else:
            path.states[end_taz_id] = taz_state[0]
            stop_state              = taz_state[0]
            if path.outbound():  # outbound: egress to access and back
                final_state_type = Path.STATE_MODE_EGRESS
            else:                # inbound: access to egress and back
                final_state_type = Path.STATE_MODE_ACCESS
            while stop_state[Path.STATE_IDX_DEPARRMODE] != final_state_type:

                stop_id    = stop_state[Path.STATE_IDX_SUCCPRED]
                stop_state = stop_states[stop_id][0]
                path.states[stop_id] = stop_state

        if trace: FastTripsLogger.debug("Final path:\n%s" % str(path))
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
            if trace: FastTripsLogger.debug("%10s %10s: prob %4d  cum_prob %4d" % \
                                            (state[Path.STATE_IDX_SUCCPRED],
                                            str(state[Path.STATE_IDX_DEPARRMODE]),
                                            prob, access_cum_prob[-1][0]))

        if path.outbound():
            start_state_id = path.origin_taz_id
            dir_factor     = 1
        else:
            start_state_id = path.destination_taz_id
            dir_factor     = -1

        path.states[start_state_id] = Assignment.choose_state(access_cum_prob, trace)
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
                if trace: FastTripsLogger.debug("%8s %8s: prob %4d  cum_prob %4d" % \
                                                (stop_cum_prob[idx][1][Path.STATE_IDX_SUCCPRED],
                                                 str(stop_cum_prob[idx][1][Path.STATE_IDX_DEPARRMODE]),
                                                 prob, stop_cum_prob[idx][0]))
            # choose!
            next_state   = Assignment.choose_state(stop_cum_prob, trace)
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
        num_paths_assigned = len(uniq_pax)

        return (num_paths_assigned, passengers_df)

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
        `A_time`         datetime64[ns]  the time the passenger arrives at `A_id`
        `B_time`         datetime64[ns]  the time the passenger arrives at `B_id`
        `linktime`      timedelta64[ns]  the time spent on the link
        ==============  ===============  =====================================================================================================

        Additionally, this method writes out the dataframe to a csv at :py:attr:`Assignment.PASSENGERS_CSV` in the given `output_dir`
        and labeled with the given `iteration`.
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
        df.to_csv(os.path.join(output_dir, Assignment.PASSENGERS_CSV % iteration), index=False)
        FastTripsLogger.info("Wrote passengers dataframe to %s" % os.path.join(output_dir, Assignment.PASSENGERS_CSV % iteration))
        return df

    @staticmethod
    def setup_trips(FT):
        """
        Sets up and returns a :py:class:`pandas.DataFrame` where each row contains a leg of a transit vehicle trip.
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
                       trip.capacity,
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
                                       'trip_id','stop_seq','stop_id','capacity',
                                       'arrive_time','depart_time',
                                       'service_type', # for dwell time
                                      ])
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

        passengers_df = pandas.merge(left   =passengers_df,              right   =passengers_dedupe,
                                     left_on=['passenger_id','path_id'], right_on=['passenger_id','path_id'],
                                     how    ='left')
        passengers_df = passengers_df[passengers_df.keep==True]
        passengers_df.drop('keep', axis=1, inplace=True)
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

        passenger_trips = pandas.merge(left   =passenger_trips,      right   =veh_trips_df[['trip_id','stop_seq','stop_id','depart_time']],
                                       left_on=['trip_id','A_id'],   right_on=['trip_id','stop_id'],
                                       how='left')
        passenger_trips = pandas.merge(left   =passenger_trips,      right   =veh_trips_df[['trip_id','stop_seq','stop_id','arrive_time']],
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
        FastTripsLogger.info("Step 2. Some trips (outbound) were found by searching backwards, so they wait *after arriving*.")
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
        Assignment.bumped_ids.clear()
        while True: # loop for capacity constraint
            FastTripsLogger.info("Step 4. Put passenger paths on transit vehicles to get vehicle boards/alights/load")

            # Group to boards by counting path_ids for a (trip_id, A_id as stop_id)
            passenger_trips_boards = passenger_trips[['path_id','trip_id','A_id']].groupby(['trip_id','A_id']).count()
            passenger_trips_boards.index.names = ['trip_id','stop_id']

            # And alights by counting path_ids for a (trip_id, B_id as stop_id)
            passenger_trips_alights = passenger_trips[['path_id','trip_id','B_id']].groupby(['trip_id','B_id']).count()
            passenger_trips_alights.index.names = ['trip_id','stop_id']

            # Join them to the transit vehicle trips so we can put people on vehicles
            # TODO: This will be wrong when stop_id is not unique for a trip
            veh_loaded_df = pandas.merge(left   =veh_trips_df,          right      =passenger_trips_boards,
                                         left_on=['trip_id','stop_id'], right_index=True,
                                         how    ='left')
            veh_loaded_df.rename(columns={'path_id':'boards'}, inplace=True)

            veh_loaded_df = pandas.merge(left   =veh_loaded_df,          right      =passenger_trips_alights,
                                        left_on=['trip_id','stop_id'], right_index=True,
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

                FastTripsLogger.debug("%d vehicle trip/stops over capacity:\n%s" % \
                                      (len(overcap_df),
                                      overcap_df.to_string(formatters=\
                   {'arrive_time'  :Assignment.datetime64_formatter,
                    'depart_time'  :Assignment.datetime64_formatter})))

                # If none, we're done
                if len(overcap_df) == 0:
                    FastTripsLogger.info("        No overcapacity vehicles")
                    break

                # start by bumping the first ones who board after at capacity - which stops are they?
                bump_stops_df  = overcap_df.groupby(['trip_id']).aggregate('first')
                FastTripsLogger.debug("Bump stops:\n%s" %
                                      bump_stops_df.to_string(formatters=\
                   {'arrive_time'  :Assignment.datetime64_formatter,
                    'depart_time'  :Assignment.datetime64_formatter}))

                # One stop at a time -- slower but more accurate
                if Assignment.BUMP_ONE_AT_A_TIME:
                    bump_stops_df.sort(['arrive_time'], inplace=True)
                    bump_stops_df = bump_stops_df.iloc[:1]

                FastTripsLogger.info("        Need to bump %d passengers from %d stops" % (bump_stops_df.overcap.sum(), len(bump_stops_df)))

                # who boards at those stops?
                bumped_pax_boards = pandas.merge(left    =passenger_trips[['trip_id','A_id','passenger_id','path_id','A_seq','A_time']],
                                                 left_on =['trip_id','A_id'],
                                                 right   =bump_stops_df.reset_index()[['trip_id','stop_id','stop_seq','arrive_time','depart_time','overcap']],
                                                 right_on=['trip_id','stop_id'],
                                                 how     ='inner')
                # bump off later arrivals, later path_id
                bumped_pax_boards.sort(['arrive_time','trip_id','stop_id','A_time','path_id'],
                                       ascending=[True, True, True, False, False], inplace=True)
                bumped_pax_boards.reset_index(drop=True, inplace=True)

                # For each trip_id, stop_id, we want the first *overcap* rows
                # group to trip_id, stop_id and count off
                bpb_count = bumped_pax_boards.groupby(['trip_id','stop_id']).cumcount()
                bpb_count.name = 'bump_index'

                # Add the bump index to our passenger-paths/stops
                bumped_pax_boards = pandas.concat([bumped_pax_boards, bpb_count], axis=1)

                FastTripsLogger.debug("bumped_pax_boards:\n%s" % bumped_pax_boards.to_string(formatters=\
                   {'A_time'       :Assignment.datetime64_formatter,
                    'arrive_time'  :Assignment.datetime64_formatter,
                    'depart_time'  :Assignment.datetime64_formatter}))

                # use it to filter to those we bump
                bumped_pax_boards = bumped_pax_boards.loc[bumped_pax_boards.bump_index < bumped_pax_boards.overcap]

                FastTripsLogger.debug("filtered bumped_pax_boards:\n%s" % bumped_pax_boards.to_string(formatters=\
                   {'A_time'       :Assignment.datetime64_formatter,
                    'arrive_time'  :Assignment.datetime64_formatter,
                    'depart_time'  :Assignment.datetime64_formatter}))

                # filter to unique passengers/paths
                bumped_pax_boards.drop_duplicates(subset=['passenger_id','path_id'],inplace=True)
                bumped_pax_boards['bump'] = True

                # keep track of these
                Assignment.bumped_ids.update(bumped_pax_boards.passenger_id.tolist())

                FastTripsLogger.debug("bumped_pax_boards without duplicate passengers:\n%s" % bumped_pax_boards.to_string(formatters=\
                   {'A_time'       :Assignment.datetime64_formatter,
                    'arrive_time'  :Assignment.datetime64_formatter,
                    'depart_time'  :Assignment.datetime64_formatter}))

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

                bump_wait = bumped_pax_boards[['trip_id','A_id','A_time']].groupby(['trip_id','A_id']).first()
                FastTripsLogger.debug("bump_wait:\n%s" % bump_wait.to_string(formatters=\
                   {'A_time'       :Assignment.datetime64_formatter}))

                # This is (trip_id, stop_id) -> Timestamp
                bump_wait_dict = bump_wait.to_dict()['A_time']
                new_bump_wait  = {k: v.to_datetime() for k, v in bump_wait_dict.iteritems()}
                Assignment.bump_wait.update(new_bump_wait)

                FastTripsLogger.debug("Bump wait ---------")
                for key,val in Assignment.bump_wait.iteritems():
                    FastTripsLogger.debug("(%s, %s) -> %s" % (str(key[0]), str(key[1]), val.strftime("%H:%M:%S")))

                bump_iter += 1
                FastTripsLogger.info("        -> complete loop iter %d" % bump_iter)

        FastTripsLogger.debug("Bumped ids: %s" % str(Assignment.bumped_ids))

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