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
import collections,datetime,math,os,sys

from .Event import Event
from .Logger import FastTripsLogger, DEBUG_NOISY
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
    #: TODO: change to time?
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

    #: Trace this passenger
    TRACE_PASSENGER_ID              = 1

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

        for iteration in range(1,Assignment.ITERATION_FLAG+1):
            FastTripsLogger.info("***************************** ITERATION %d **************************************" % iteration)

            if Assignment.ASSIGNMENT_TYPE == Assignment.ASSIGNMENT_TYPE_SIM_ONLY:
                # read existing paths
                raise Exception("Simulation only not implemented yet")
            else:
                num_paths_assigned = Assignment.assign_passengers(FT, iteration)

            if Assignment.SIMULATION_FLAG == True:
                FastTripsLogger.info("****************************** SIMULATING *****************************")
                num_passengers_arrived = Assignment.simulate(FT)

            if Assignment.OUTPUT_PASSENGER_TRAJECTORIES:
                Assignment.print_passenger_paths(FT, output_dir)
                Assignment.print_passenger_times(FT, output_dir)

            # capacity gap stuff
            num_bumped_passengers = num_paths_assigned - num_passengers_arrived
            capacity_gap = 100.0*num_bumped_passengers/num_paths_assigned

            FastTripsLogger.info("  TOTAL ASSIGNED PASSENGERS: %10d" % num_paths_assigned)
            FastTripsLogger.info("  ARRIVED PASSENGERS:        %10d" % num_passengers_arrived)
            FastTripsLogger.info("  MISSED PASSENGERS:         %10d" % num_bumped_passengers)
            FastTripsLogger.info("  CAPACITY GAP:              %10.4f" % capacity_gap)

            if capacity_gap < 0.001 or Assignment.ASSIGNMENT_TYPE == Assignment.ASSIGNMENT_TYPE_STO_ASGN:
                break

        # end for loop
        FastTripsLogger.info("**************************** WRITING OUTPUTS ****************************")
        Assignment.print_load_profile(FT, output_dir)

    @staticmethod
    def assign_passengers(FT, iteration):
        """
        Assigns paths to passengers using deterministic trip-based shortest path (TBSP).

        Returns the number of paths assigned.
        """
        FastTripsLogger.info("**************************** GENERATING PATHS ****************************")
        start_time          = datetime.datetime.now()
        num_paths_assigned  = 0
        for passenger_id, passenger in FT.passengers.iteritems():

            if not passenger.path.goes_somewhere(): continue

            if iteration > 1 and passenger.simulation_status == Passenger.STATUS_ARRIVED:
                num_paths_assigned += 1
                continue

            if passenger_id == Assignment.TRACE_PASSENGER_ID:
                FastTripsLogger.debug("Tracing assignment of passenger %s" % str(passenger_id))

            if passenger.path.direction == Path.DIR_OUTBOUND:

                if Assignment.ASSIGNMENT_TYPE == Assignment.ASSIGNMENT_TYPE_DET_ASGN:
                    asgn_iters = Assignment.find_backwards_trip_based_shortest_path(FT, passenger.path,
                                                                                    passenger_id==Assignment.TRACE_PASSENGER_ID)
                else:
                    asgn_iters = Assignment.find_backwards_trip_based_hyperpath(FT, passenger.path,
                                                                                passenger_id==Assignment.TRACE_PASSENGER_ID)

            elif passenger.direction == Passenger.DIR_INBOUND:

                if Assignment.ASSIGNMENT_TYPE == Assignment.ASSIGNMENT_TYPE_DET_ASGN:
                    asgn_iters = Assignment.find_forwards_trip_based_shortest_path(FT, passenger.path,
                                                                                   passenger_id==Assignment.TRACE_PASSENGER_ID)
                else:
                    asgn_iters = Assignment.find_forwards_trip_based_hyperpath(FT, passenger.path,
                                                                               passenger_id==Assignment.TRACE_PASSENGER_ID)

            if passenger.path.path_found():
                num_paths_assigned += 1

            if num_paths_assigned % 1000 == 0:
                time_elapsed = datetime.datetime.now() - start_time
                FastTripsLogger.info(" %6d / %6d passenger paths assigned.  Time elapsed: %2dh:%2dm:%2ds" % (
                                     num_paths_assigned, len(FT.passengers),
                                     int( time_elapsed.total_seconds() / 3600),
                                     int( (time_elapsed.total_seconds() % 3600) / 60),
                                     time_elapsed.total_seconds() % 60))
                return
            elif num_paths_assigned % 50 == 0:
                FastTripsLogger.debug("%6d / %6d passenger paths assigned" % (num_paths_assigned, len(FT.passengers)))
        return num_paths_assigned

    @staticmethod
    def find_backwards_trip_based_shortest_path(FT, path, trace):
        """
        Perform backwards (destination to origin) trip-based shortest path search.
        Updates the path information in the given path and returns the number of label iterations required.

        :param FT: fasttrips data
        :type FT: a :py:class:`FastTrips` instance
        :param path: the path to fill in
        :type path: a :py:class:`Path` instance
        :param trace: pass True if this path should be traced to the debug log
        :type trace: boolean

        """
        # reset stuff?
        dest_taz = FT.tazs[path.destination_taz_id]

        stop_states = {}                    # stop_id -> (label, departure, departure_mode, successor)
        stop_queue  = Queue.PriorityQueue() # (label, stop_id)
        MAX_TIME    = datetime.timedelta(minutes = 999.999)
        stop_done   = set() # stop ids
        trips_used  = set() # trip ids

        # possible egress links
        for stop_id, access_link in dest_taz.access_links.iteritems():
            departure_time       = datetime.datetime.combine(Assignment.TODAY, path.preferred_time) - \
                                   access_link[TAZ.ACCESS_LINK_IDX_TIME]
            stop_states[stop_id] = (access_link[TAZ.ACCESS_LINK_IDX_TIME], # label
                                    departure_time,                        # departure
                                    Path.STATE_MODE_EGRESS,                # departure mode
                                    path.destination_taz_id,               # successor
                                    access_link[TAZ.ACCESS_LINK_IDX_TIME]) # link time
            stop_queue.put( (access_link[TAZ.ACCESS_LINK_IDX_TIME], stop_id ))
            if trace: FastTripsLogger.debug("  " + Path.state_str(stop_id, stop_states[stop_id]))

        # labeling loop
        label_iterations = 0
        while not stop_queue.empty():
            (current_label, current_stop_id) = stop_queue.get()
            if trace:
                FastTripsLogger.debug("Pulling from stop_queue (iteration %d) :======" % label_iterations)
                FastTripsLogger.debug("  " + Path.state_str(current_stop_id, stop_states[current_stop_id]))
                FastTripsLogger.debug("==============================")

            if current_stop_id in stop_done: continue                   # stop is already processed
            stop_done.add(current_stop_id)                              # process this stop now - just once

            current_stop_state = stop_states[current_stop_id]

            # Update by transfer
            # (We don't want to transfer to egress or transfer to a transfer)
            if current_stop_state[Path.STATE_IDX_DEPMODE] not in [Path.STATE_MODE_EGRESS,Path.STATE_MODE_TRANSFER]:

                for xfer_stop_id,xfer_attr in FT.stops[current_stop_id].transfers.iteritems():

                    # new_label = length of trip so far if the passenger transfers from this stop
                    new_label       = current_label + xfer_attr[Stop.TRANSFERS_IDX_TIME]
                    departure_time  = current_stop_state[Path.STATE_IDX_DEPARTURE] - xfer_attr[Stop.TRANSFERS_IDX_TIME]

                    # todo check (departure mode, stop) in available capacity
                    old_label       = MAX_TIME
                    if xfer_stop_id in stop_states:
                        old_label   = stop_states[xfer_stop_id][Path.STATE_IDX_LABEL]
                    # print "transfer from %d:  new_label=%s departure_time=%s old_label=%s" % (xfer_stop_id, new_label, departure_time, old_label)

                    if new_label < old_label:
                        stop_states[xfer_stop_id] = (new_label,                 # label,
                                                     departure_time,            # departure time
                                                     Path.STATE_MODE_TRANSFER,  # departure mode
                                                     current_stop_id,           # successor
                                                     xfer_attr[Stop.TRANSFERS_IDX_TIME]) # link time
                        stop_queue.put( (new_label, xfer_stop_id) )
                        if trace: FastTripsLogger.debug("  " + Path.state_str(xfer_stop_id, stop_states[xfer_stop_id]))

            # Update by trips
            # These are the trips that arrive at the stop in time to depart on time
            valid_trips = FT.stops[current_stop_id].get_trips_arriving_within_time(Assignment.TODAY,
                                                                                   current_stop_state[Path.STATE_IDX_DEPARTURE],
                                                                                   Assignment.PATH_TIME_WINDOW)
            for (trip_id, seq, arrival_time) in valid_trips:

                if trip_id in trips_used: continue
                trips_used.add(trip_id)

                # todo check (departure mode, stop) in available capacity

                wait_time = current_stop_state[Path.STATE_IDX_DEPARTURE] - datetime.datetime.combine(Assignment.TODAY, arrival_time)

                # iterate through the stops before this one
                for seq_num in range(seq-1, 0, -1):
                    possible_board = FT.trips[trip_id].stops[seq_num-1]

                    # new_label = length of trip so far if the passenger boards at this stop
                    board_stop      = possible_board[Trip.STOPS_IDX_STOP_ID]
                    departure_time  = datetime.datetime.combine(Assignment.TODAY, possible_board[Trip.STOPS_IDX_DEPARTURE_TIME])
                    in_vehicle_time = datetime.datetime.combine(Assignment.TODAY, arrival_time) - departure_time
                    new_label       = current_label + in_vehicle_time + wait_time

                    old_label       = MAX_TIME
                    if board_stop in stop_states:
                        old_label   = stop_states[board_stop][Path.STATE_IDX_LABEL]

                    if new_label < old_label:
                        stop_states[board_stop] = (new_label,       # label,
                                                   departure_time,  # departure time
                                                   trip_id,         # departure mode
                                                   current_stop_id, # successor
                                                   in_vehicle_time+wait_time) # link time
                        stop_queue.put( (new_label, board_stop) )
                        if trace: FastTripsLogger.debug("  " + Path.state_str(board_stop, stop_states[board_stop]))

            # Done with this label iteration!
            label_iterations += 1

        # all stops are labeled: let's look at the origin TAZ
        origin_taz = FT.tazs[path.origin_taz_id]
        taz_state  = (MAX_TIME, 0, "", 0)
        for stop_id, access_link in origin_taz.access_links.iteritems():

            if stop_id not in stop_states: continue

            stop_state      = stop_states[stop_id]

            # first leg has to be a trip
            if stop_state[Path.STATE_IDX_DEPMODE] == Path.STATE_MODE_TRANSFER: continue
            if stop_state[Path.STATE_IDX_DEPMODE] == Path.STATE_MODE_EGRESS:   continue

            new_label       = stop_state[Path.STATE_IDX_LABEL] + access_link[TAZ.ACCESS_LINK_IDX_TIME]
            departure_time  = stop_state[Path.STATE_IDX_DEPARTURE] - access_link[TAZ.ACCESS_LINK_IDX_TIME]
            # todo check (departure mode, stop) in available capacity

            new_taz_state   = (new_label,                 # label,
                               departure_time,            # departure time
                               Path.STATE_MODE_ACCESS,    # departure mode
                               stop_id,                   # successor
                               access_link[TAZ.ACCESS_LINK_IDX_TIME]) # link time
            debug_str = ""
            if new_taz_state[Path.STATE_IDX_LABEL] < taz_state[Path.STATE_IDX_LABEL]:
                taz_state = new_taz_state
                if trace: debug_str = " !"
            if trace: FastTripsLogger.debug("  " + Path.state_str(path.origin_taz_id, new_taz_state) + debug_str)

        # Put results into path
        path.states[path.origin_taz_id] = taz_state
        if taz_state[Path.STATE_IDX_LABEL] != MAX_TIME:

            stop_state = taz_state
            while stop_state[Path.STATE_IDX_DEPMODE] != Path.STATE_MODE_EGRESS:

                stop_id    = stop_state[Path.STATE_IDX_SUCCESSOR]
                stop_state = stop_states[stop_id]
                path.states[stop_id] = stop_state

        return label_iterations

    @staticmethod
    def find_backwards_trip_based_hyperpath(FT, path, trace):
        """
        Perform backwards (destination to origin) trip-based hyperpath search.
        Updates the path information in the given path and returns the number of label iterations required.

        :param FT: fasttrips data
        :type FT: a :py:class:`FastTrips` instance
        :param path: the path to fill in
        :type path: a :py:class:`Path` instance
        :param trace: pass True if this path should be traced to the debug log
        :type trace: boolean

        """
        # reset stuff?
        dest_taz = FT.tazs[path.destination_taz_id]

        stop_states     = collections.defaultdict(list) # stop_id -> [(label, departure, departure_mode, successor, cost, arrival)]
        stop_queue      = Queue.PriorityQueue()         # (label, stop_id)
        MAX_TIME        = datetime.timedelta(minutes = 999.999)
        MAX_DATETIME    = datetime.datetime.combine(Assignment.TODAY, datetime.time()) + datetime.timedelta(hours=48)
        MAX_COST        = 999999
        stop_done       = set() # stop ids
        trips_used      = set() # trip ids

        # possible egress links
        for stop_id, access_link in dest_taz.access_links.iteritems():
            departure_time       = datetime.datetime.combine(Assignment.TODAY, path.preferred_time) - \
                                   access_link[TAZ.ACCESS_LINK_IDX_TIME]
            cost                 = 1 + (Path.WALK_EGRESS_TIME_WEIGHT*access_link[TAZ.ACCESS_LINK_IDX_TIME].total_seconds()/60.0) # todo: why 1+
            stop_states[stop_id].append( (cost,                                  # label
                                          departure_time,                        # departure
                                          Path.STATE_MODE_EGRESS,                # departure mode
                                          path.destination_taz_id,               # successor
                                          access_link[TAZ.ACCESS_LINK_IDX_TIME], # link time
                                          cost,                                  # cost
                                          MAX_DATETIME) )                        # arrival
            stop_queue.put( (cost, stop_id) )
            if trace: FastTripsLogger.debug(" +egress   " + Path.state_str(stop_id, stop_states[stop_id][0]))

        # labeling loop
        label_iterations = 0
        while not stop_queue.empty():
            (current_label, current_stop_id) = stop_queue.get()

            if current_stop_id in stop_done: continue                   # stop is already processed
            if not FT.stops[current_stop_id].is_transfer(): continue    # no transfers to the stop
            stop_done.add(current_stop_id)                              # process this stop now - just once

            if trace:
                FastTripsLogger.debug("Pulling from stop_queue (iteration %d) :======" % label_iterations)
                for stop_state in stop_states[current_stop_id]:
                    FastTripsLogger.debug("  " + Path.state_str(current_stop_id, stop_state))
                FastTripsLogger.debug("==============================")

            current_stop_state = stop_states[current_stop_id] # this is a list
            current_mode       = current_stop_state[0][Path.STATE_IDX_DEPMODE]  # why index 0?
            latest_departure   = current_stop_state[0][Path.STATE_IDX_DEPARTURE]
            for state in current_stop_state[1:]:
                latest_departure = max( latest_departure, state[Path.STATE_IDX_DEPARTURE])

            if trace:
                FastTripsLogger.debug("  current mode:     " + str(current_mode))
                FastTripsLogger.debug("  latest departure: " + latest_departure.strftime("%H:%M:%S"))

            # Update by transfer
            # (We don't want to transfer to egress or transfer to a transfer)
            if current_mode not in [Path.STATE_MODE_EGRESS]:

                # calculate the nonwalk label from nonwalk links
                nonwalk_label = 0.0
                for state in current_stop_state:
                    if state[Path.STATE_IDX_DEPMODE] not in [Path.STATE_MODE_EGRESS, Path.STATE_MODE_TRANSFER]:
                        nonwalk_label += math.exp(-1.0*Assignment.DISPERSION_PARAMETER*state[Path.STATE_IDX_COST])
                if nonwalk_label == 0.0:
                    nonwalk_label = MAX_COST
                else:
                    nonwalk_label = -1.0/Assignment.DISPERSION_PARAMETER*math.log(nonwalk_label)
                if trace: FastTripsLogger.debug("  nonwalk label:    %.4f" % nonwalk_label)

                for xfer_stop_id,xfer_attr in FT.stops[current_stop_id].transfers.iteritems():

                    transfer_time   = xfer_attr[Stop.TRANSFERS_IDX_TIME]
                    departure_time  = latest_departure - transfer_time
                    cost            = nonwalk_label + (Path.WALK_TRANSFER_TIME_WEIGHT*transfer_time.total_seconds()/60.0)

                    # todo check (departure mode, stop) in available capacity
                    old_label       = MAX_COST
                    new_label       = cost
                    if xfer_stop_id in stop_states:
                        old_label   = stop_states[xfer_stop_id][-1][Path.STATE_IDX_LABEL]
                        new_label   = math.exp(-1.0*Assignment.DISPERSION_PARAMETER*old_label) + \
                                      math.exp(-1.0*Assignment.DISPERSION_PARAMETER*cost)
                        new_label   = max(0.01, -1.0/Assignment.DISPERSION_PARAMETER*math.log(new_label))

                    if new_label < MAX_COST and new_label > 0:
                        stop_states[xfer_stop_id].append( (new_label,                 # label,
                                                           departure_time,            # departure time
                                                           Path.STATE_MODE_TRANSFER,  # departure mode
                                                           current_stop_id,           # successor
                                                           transfer_time,             # link time
                                                           cost,                      # cost
                                                           MAX_DATETIME) )            # arrival
                        stop_queue.put( (new_label, xfer_stop_id) )
                        if trace: FastTripsLogger.debug(" +transfer " + Path.state_str(xfer_stop_id, stop_states[xfer_stop_id][-1]))

            # Update by trips
            # These are the trips that arrive at the stop in time to depart on time
            valid_trips = FT.stops[current_stop_id].get_trips_arriving_within_time(Assignment.TODAY,
                                                                                   latest_departure,
                                                                                   Assignment.PATH_TIME_WINDOW)
            for (trip_id, seq, arrival_time) in valid_trips:

                if trip_id in trips_used: continue
                trips_used.add(trip_id)

                # todo check (departure mode, stop) in available capacity

                wait_time = latest_departure - datetime.datetime.combine(Assignment.TODAY, arrival_time)

                # iterate through the stops before this one
                for seq_num in range(1, seq):
                    possible_board = FT.trips[trip_id].stops[seq_num-1]

                    # new_label = length of trip so far if the passenger boards at this stop
                    board_stop      = possible_board[Trip.STOPS_IDX_STOP_ID]
                    new_mode        = stop_states[board_stop][0][Path.STATE_IDX_DEPMODE] if board_stop in stop_states else None  # why 0 index?
                    if new_mode == Path.STATE_MODE_EGRESS: continue

                    departure_time  = datetime.datetime.combine(Assignment.TODAY, possible_board[Trip.STOPS_IDX_DEPARTURE_TIME])
                    in_vehicle_time = datetime.datetime.combine(Assignment.TODAY, arrival_time) - departure_time

                    if current_mode == Path.STATE_MODE_EGRESS:
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
                    if board_stop in stop_states:
                        old_label   = stop_states[board_stop][-1][Path.STATE_IDX_LABEL]
                        new_label   = math.exp(-1.0*Assignment.DISPERSION_PARAMETER*old_label) + \
                                      math.exp(-1.0*Assignment.DISPERSION_PARAMETER*cost)
                        new_label   = max(0.01, -1.0/Assignment.DISPERSION_PARAMETER*math.log(new_label))

                    # if trace:
                    #     FastTripsLogger.debug("seq_num=%d new_mode=%s newlabel=%.3f departure=%s trip=%s stop=%s cost=%.3f arrival=%s" %
                    #                           (seq_num, str(new_mode), new_label, departure_time.strftime("%H:%M:%S"),
                    #                            str(trip_id), str(board_stop), cost, arrival_time.strftime("%H:%M:%S")))
                    if new_label < MAX_COST and new_label > 0:
                        stop_states[board_stop].append( (new_label,                 # label,
                                                         departure_time,            # departure time
                                                         trip_id,                   # departure mode
                                                         current_stop_id,           # successor
                                                         in_vehicle_time+wait_time, # link time
                                                         cost,                      # cost
                                                         arrival_time) )            # arrival
                        stop_queue.put( (new_label, board_stop) )
                        if trace: FastTripsLogger.debug(" +trip     " + Path.state_str(board_stop, stop_states[board_stop][-1]))

            # Done with this label iteration!
            label_iterations += 1

        sys.exit(2)

        # all stops are labeled: let's look at the origin TAZ
        origin_taz = FT.tazs[path.origin_taz_id]
        taz_state  = (MAX_TIME, 0, "", 0)
        for stop_id, access_link in origin_taz.access_links.iteritems():

            if stop_id not in stop_states: continue

            stop_state      = stop_states[stop_id]

            # first leg has to be a trip
            if stop_state[Path.STATE_IDX_DEPMODE] == Path.STATE_MODE_TRANSFER: continue
            if stop_state[Path.STATE_IDX_DEPMODE] == Path.STATE_MODE_EGRESS:   continue

            new_label       = stop_state[Path.STATE_IDX_LABEL] + access_link[TAZ.ACCESS_LINK_IDX_TIME]
            departure_time  = stop_state[Path.STATE_IDX_DEPARTURE] - access_link[TAZ.ACCESS_LINK_IDX_TIME]
            # todo check (departure mode, stop) in available capacity

            new_taz_state   = (new_label,                 # label,
                               departure_time,            # departure time
                               Path.STATE_MODE_ACCESS,    # departure mode
                               stop_id,                   # successor
                               access_link[TAZ.ACCESS_LINK_IDX_TIME]) # link time
            debug_str = ""
            if new_taz_state[Path.STATE_IDX_LABEL] < taz_state[Path.STATE_IDX_LABEL]:
                taz_state = new_taz_state
                if trace: debug_str = " !"
            if trace: FastTripsLogger.debug("  " + Path.state_str(path.origin_taz_id, new_taz_state) + debug_str)

        # Put results into path
        path.states[path.origin_taz_id] = taz_state
        if taz_state[Path.STATE_IDX_LABEL] != MAX_TIME:

            stop_state = taz_state
            while stop_state[Path.STATE_IDX_DEPMODE] != Path.STATE_MODE_EGRESS:

                stop_id    = stop_state[Path.STATE_IDX_SUCCESSOR]
                stop_state = stop_states[stop_id]
                path.states[stop_id] = stop_state

        return label_iterations

    @staticmethod
    def print_passenger_paths(FT, output_dir):
        """
        Print the passenger paths.
        """
        paths_out = open(os.path.join(output_dir, "ft_output_passengerPaths.dat"), 'w')
        paths_out.write("passengerId\t%s\n" % Path.path_str_header())
        for passenger_id, passenger in FT.passengers.iteritems():
            if passenger.path.path_found():
                paths_out.write("%s\t%s\n" % (str(passenger_id), passenger.path.path_str()))
        paths_out.close()

    @staticmethod
    def print_passenger_times(FT, output_dir):
        """
        Print the passenger times.
        """
        times_out = open(os.path.join(output_dir, "ft_output_passengerTimes.dat"), 'w')
        times_out.write("passengerId\t%s\n" % Path.time_str_header())
        for passenger_id, passenger in FT.passengers.iteritems():
            if passenger.path.path_found():
                times_out.write("%s\t%s\n" % (str(passenger_id), passenger.path.time_str()))
        times_out.close()

    @staticmethod
    def simulate(FT):
        """
        Actually assign the passengers trips to the vehicles.
        """
        start_time              = datetime.datetime.now()
        events_simulated        = 0
        passengers_arrived      = 0   #: arrived at destination TAZ
        passengers_bumped       = 0

        trip_pax                = collections.defaultdict(list)  # trip_id to current [passenger_ids] on board
        stop_pax                = collections.defaultdict(list)  # stop_id to current [passenger_ids] waiting to board
        passenger_id_to_state   = {}  # state: (current passenger status, current path idx)
        transfer_passengers     = []  # passenger ids for passengers who are currently walking or transfering
        bump_wait               = {}  # (trip_id, stop_id) -> earliest time a bumped passenger started waiting

        passenger_arrivals      = collections.defaultdict(list)  # passenger id to [arrival time] at stops
        passenger_boards        = collections.defaultdict(list)  # passenger id to [board time] at stops
        passenger_alights       = collections.defaultdict(list)  # passenger id to [alight time] at stops
        passenger_dest_arrivals = {}                             # passenger id to arrival time at destination TAZ

        trip_boards             = collections.defaultdict(list)  # trip_id to [number boards per stop]
        trip_alights            = collections.defaultdict(list)  # trip_id to [number alights per stop]
        trip_dwells             = collections.defaultdict(list)  # trip_id to [dwell times per stop]

        # reset
        for passenger_id in FT.passengers.keys():
            if not FT.passengers[passenger_id].path.goes_somewhere():   continue
            if not FT.passengers[passenger_id].path.path_found():       continue

            passenger_id_to_state[passenger_id] = (Passenger.STATUS_WALKING, 0)
            transfer_passengers.append(passenger_id)

        for event in FT.events:
            event_datetime = datetime.datetime.combine(Assignment.TODAY, event.event_time)

            if event.event_type == Event.EVENT_TYPE_ARRIVAL:

                # are there alight passengers at this stop?
                num_alights = 0
                for passenger_id in list(trip_pax[event.trip_id]):

                    path_idx    = passenger_id_to_state[passenger_id][1]
                    path_state  = FT.passengers[passenger_id].path.states.items()[path_idx][1]
                    alight_stop = path_state[Path.STATE_IDX_SUCCESSOR]

                    if alight_stop != event.stop_id: continue

                    FastTripsLogger.log(DEBUG_NOISY, "Event (trip %10d, stop %8d, type %s, time %s)" % 
                                          (int(event.trip_id), int(event.stop_id), event.event_type, 
                                           event.event_time.strftime("%H:%M:%S")))
                    FastTripsLogger.log(DEBUG_NOISY, "Alighting: --------\n%s" % str(FT.passengers[passenger_id].path))

                    passenger_id_to_state[passenger_id] = (Passenger.STATUS_WALKING, path_idx+1)
                    passenger_alights[passenger_id].append(event_datetime)
                    transfer_passengers.append(passenger_id)
                    trip_pax[event.trip_id].remove(passenger_id)
                    num_alights += 1

                    FastTripsLogger.log(DEBUG_NOISY, "Alight @ %s -> Passenger %s: state: %s" % (event_datetime.strftime("%H:%M:%S"),
                                          str(passenger_id), str(passenger_id_to_state[passenger_id])))

                trip_alights[event.trip_id].append(num_alights)

            elif event.event_type == Event.EVENT_TYPE_DEPARTURE:

                # transfer passengers: iterate on a copy so we can remove from the list
                for passenger_id in list(transfer_passengers):

                    path_idx    = passenger_id_to_state[passenger_id][1]
                    path_state  = FT.passengers[passenger_id].path.states.items()[path_idx][1]
                    if path_idx == 0:
                        alight_time = path_state[Path.STATE_IDX_DEPARTURE]
                    else:
                        alight_time = passenger_alights[passenger_id][-1]

                    # transfer to a different stop
                    if path_state[Path.STATE_IDX_DEPMODE] in [Path.STATE_MODE_TRANSFER, Path.STATE_MODE_EGRESS, Path.STATE_MODE_ACCESS]:
                        walk_time   = path_state[Path.STATE_IDX_LINKTIME]
                        board_stop  = path_state[Path.STATE_IDX_SUCCESSOR]
                        new_path_idx= path_idx + 1
                    else:  # passenger is already here - the board stop is the one path_idx points to
                        walk_time   = datetime.timedelta()
                        board_stop  = FT.passengers[passenger_id].path.states.items()[path_idx][0]
                        new_path_idx= path_idx
                    arrive_time = alight_time + walk_time

                    # passenger arrived at boarding stop
                    if event_datetime >= arrive_time:
                        FastTripsLogger.log(DEBUG_NOISY, "Event (trip %10d, stop %8d, type %s, time %s)" % 
                                              (int(event.trip_id), int(event.stop_id), event.event_type, 
                                               event.event_time.strftime("%H:%M:%S")))
                        FastTripsLogger.log(DEBUG_NOISY, "Transfer: --------\n%s" % str(FT.passengers[passenger_id].path))

                        # arrived at destination
                        if path_state[Path.STATE_IDX_DEPMODE] == Path.STATE_MODE_EGRESS:
                            passenger_id_to_state[passenger_id]     = (Passenger.STATUS_ARRIVED, new_path_idx)
                            passenger_dest_arrivals[passenger_id]   = arrive_time
                            passengers_arrived                      += 1

                        else:
                            passenger_id_to_state[passenger_id] = (Passenger.STATUS_WAITING, new_path_idx)
                            stop_pax[board_stop].append(passenger_id)
                            passenger_arrivals[passenger_id].append(arrive_time)

                        transfer_passengers.remove(passenger_id)
                        FastTripsLogger.log(DEBUG_NOISY, "Arrive @ %s -> Passenger %s: state: %s" % (arrive_time.strftime("%H:%M:%S"),
                                              str(passenger_id), str(passenger_id_to_state[passenger_id])))

                # board passengers at this stop
                num_boards = 0
                for passenger_id in list(stop_pax[event.stop_id]):

                    path_idx    = passenger_id_to_state[passenger_id][1]
                    path_state  = FT.passengers[passenger_id].path.states.items()[path_idx][1]
                    if path_state[Path.STATE_IDX_DEPMODE] != event.trip_id: continue

                    available_capacity = FT.trips[event.trip_id].capacity - len(trip_pax[event.trip_id])

                    if Assignment.CAPACITY_CONSTRAINT and available_capacity == 0:

                        FastTripsLogger.log(DEBUG_NOISY, "Event (trip %10d, stop %8d, type %s, time %s)" % 
                                              (int(event.trip_id), int(event.stop_id), event.event_type, 
                                               event.event_time.strftime("%H:%M:%S")))
                        FastTripsLogger.log(DEBUG_NOISY, "Bumping: --------\n%s" % str(FT.passengers[passenger_id].path))

                        passenger_id_to_state[passenger_id] = (Passenger.STATUS_BUMPED, -1)

                        # update bump_wait
                        if (((event.trip_id, event.stop_id) not in bump_wait) or \
                             (bump_wait[(event.trip_id, event.stop_id)] > passenger_arrivals[passenger_id][-1])):
                            bump_wait[(event.trip_id, event.stop_id)] = passenger_arrivals[passenger_id][-1]

                        passengers_bumped += 1

                        FastTripsLogger.log(DEBUG_NOISY, "-> Passenger %s: state: %s" % (str(passenger_id), str(passenger_id_to_state[passenger_id])))
                    else:

                        FastTripsLogger.log(DEBUG_NOISY, "Event (trip %10d, stop %8d, type %s, time %s)" % 
                                              (int(event.trip_id), int(event.stop_id), event.event_type, 
                                               event.event_time.strftime("%H:%M:%S")))
                        FastTripsLogger.log(DEBUG_NOISY, "Boarding: --------\n%s" % str(FT.passengers[passenger_id].path))

                        trip_pax[event.trip_id].append(passenger_id)
                        passenger_id_to_state[passenger_id] = (Passenger.STATUS_ON_BOARD, passenger_id_to_state[passenger_id][1])
                        passenger_boards[passenger_id].append(event_datetime)
                        num_boards += 1

                        FastTripsLogger.log(DEBUG_NOISY, "Board @ %s -> Passenger %s: state: %s" % (event_datetime.strftime("%H:%M:%S"),
                                              str(passenger_id), str(passenger_id_to_state[passenger_id])))
                    # remove passenger from waiting list
                    stop_pax[event.stop_id].remove(passenger_id)

                trip_boards[event.trip_id].append(num_boards)

                # calculateDwellTime
                trip_dwells[event.trip_id].append(FT.trips[event.trip_id].calculate_dwell_time(trip_boards[event.trip_id][-1],
                                                                                               trip_alights[event.trip_id][-1]))

            events_simulated += 1
            if events_simulated % 10000 == 0:
                time_elapsed = datetime.datetime.now() - start_time
                FastTripsLogger.info(" %6d / %6d events simulated.  Time elapsed: %2dh:%2dm:%2ds" % (
                                     events_simulated, len(FT.events),
                                     int( time_elapsed.total_seconds() / 3600),
                                     int( (time_elapsed.total_seconds() % 3600) / 60),
                                     time_elapsed.total_seconds() % 60))

        # Put results into path
        for passenger_id in FT.passengers.keys():
            FT.passengers[passenger_id].set_experienced_status_and_times( \
                passenger_id_to_state[passenger_id][0] if passenger_id in passenger_id_to_state else Passenger.STATUS_INITIAL,
                passenger_arrivals[passenger_id],
                passenger_boards[passenger_id],
                passenger_alights[passenger_id],
                passenger_dest_arrivals[passenger_id] if passenger_id in passenger_dest_arrivals else None)

        # and trip
        for trip_id, trip in FT.trips.iteritems():
            trip.set_simulation_results(boards  = trip_boards[trip_id],
                                        alights = trip_alights[trip_id],
                                        dwells  = trip_dwells[trip_id])

        return passengers_arrived

    @staticmethod
    def print_load_profile(FT, output_dir):
        """
        Print the load profile output
        """
        load_file = open(os.path.join(output_dir, "ft_output_loadProfile.dat"), 'w')
        Trip.write_load_header_to_file(load_file)
        for trip_id,trip in FT.trips.iteritems():
            trip.write_load_to_file(load_file)
        load_file.close()