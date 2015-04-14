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
import datetime,sys

from .Logger import FastTripsLogger
from .Passenger import Passenger
from .TAZ import TAZ

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

    #: Route choice configuration: Weight of in-vehicle time
    IN_VEHICLE_TIME_WEIGHT          = 1.0

    #: Route choice configuration: Weight of waiting time
    WAIT_TIME_WEIGHT                = 1.77

    #: Route choice configuration: Weight of access walk time
    WALK_ACCESS_TIME_WEIGHT         = 3.93

    #: Route choice configuration: Weight of egress walk time
    WALK_EGRESS_TIME_WEIGHT         = 3.93

    #: Route choice configuration: Weight of transfer walking time
    WALK_TRANSFER_TIME_WEIGHT       = 3.93

    #: Route choice configuration: Weight transfer penalty (minutes)
    TRANSFER_PENALTY                = 47.73

    #: Route choice configuration: Weight of schedule delay (0 - no penalty)
    SCHEDULE_DELAY_WEIGHT           = 0.0

    #: Route choice configuration: Fare in dollars per boarding (with no transfer credit)
    FARE_PER_BOARDING               = 0.0

    #: Route choice configuration: Value of time (dollars per hour)
    VALUE_OF_TIME                   = 999

    #: Route choice configuration: Dispersion parameter in the logit function.
    #: Higher values result in less stochasticity. Must be nonnegative. 
    #: If unknown use a value between 0.5 and 1
    DISPERSION_PARAMETER            = 1.0

    #: Route choice configuration: Use vehicle capacity constraints
    CAPACITY_CONSTRAINT             = False

    #: Use this as the date
    TODAY                           = datetime.date.today()

    @staticmethod
    def read_configuration():
        """
        Read the configuration parameters and override the above
        """
        raise Exception("Not implemented")

    @staticmethod
    def assign_passengers(output_dir, FT):
        """
        Assigns the passengers.
        """

        for iteration in range(1,Assignment.ITERATION_FLAG+1):
            FastTripsLogger.info("***************************** ITERATION %d **************************************" % iteration)

            if Assignment.ASSIGNMENT_TYPE == Assignment.ASSIGNMENT_TYPE_SIM_ONLY:
                # read existing paths
                raise Exception("Simulation only not implemented yet")
            elif Assignment.ASSIGNMENT_TYPE == Assignment.ASSIGNMENT_TYPE_DET_ASGN:

                # deterministic assignment
                num_assign_passengers = Assignment.deterministically_assign_passengers(output_dir, FT, iteration)

            elif Assignment.ASSIGNMENT_TYPE == Assignment.ASSIGNMENT_TYPE_STO_ASGN:
                raise Exception("Stochastic assignment not implemented yet")

            if Assignment.SIMULATION_FLAG == True:
                FastTripsLogger.info("****************************** SIMULATING *****************************")
                raise Exception("Simulation not implemented yet")

            if Assignment.OUTPUT_PASSENGER_TRAJECTORIES:
                raise Exception("Output passenger trajectories not implemented yet")

            # capacity gap stuff

        # end for loop

    @staticmethod
    def deterministically_assign_passengers(output_dir, FT, iteration):
        """
        Assigns passengers using deterministic trip-based shortest path (TBSP).
        """
        for passenger_id, passenger in FT.passengers.iteritems():

            if passenger.origin_taz_id == passenger.destination_taz_id:
                continue

            if passenger.direction == Passenger.DIR_OUTBOUND:

                Assignment.find_backwards_trip_based_shortest_path(output_dir, FT,
                                                                   passenger.origin_taz_id,
                                                                   passenger.destination_taz_id,
                                                                   passenger.preferred_time)

            elif passenger.direction == Passenger.DIR_INBOUND:

                Assignment.find_forwards_trip_based_shortest_path(output_dir, FT,
                                                                  passenger.origin_taz_id,
                                                                  passenger.destination_taz_id,
                                                                  passenger.preferred_time)

    @staticmethod
    def find_backwards_trip_based_shortest_path(output_dir, FT,
                                                origin_taz_id,
                                                destination_taz_id,
                                                preferred_time):
        """
        Perform backwards (destination to origin) trip-based shortest path search.
        """
        # reset stuff?
        dest_taz = FT.tazs[destination_taz_id]

        # stop_id -> (label, departure, departure_mode, successor)
        stop_states = {}
        stop_queue  = Queue.PriorityQueue()
        MAX_TIME    = 999.999
        stop_done   = set() # stop ids
        trips_used  = set() # trip ids

        # egress
        for stop_id, access_link in dest_taz.access_links.iteritems():
            departure_time       = datetime.datetime.combine(Assignment.TODAY, preferred_time) - access_link[TAZ.ACCESS_LINK_IDX_TIME]
            stop_states[stop_id] = (access_link[TAZ.ACCESS_LINK_IDX_TIME],                      # label
                                    departure_time,                                             # departure
                                    "Egress",                                                   # departure mode
                                    destination_taz_id)                                         # successor
            print "Putting (%s, %d)" % (str(access_link[TAZ.ACCESS_LINK_IDX_TIME]), stop_id)
            stop_queue.put( (access_link[TAZ.ACCESS_LINK_IDX_TIME], stop_id ))

        # labeling loop
        while not stop_queue.empty():
            (label, stop_id) = stop_queue.get()

            # done conditions for this stop
            if stop_id in stop_done: continue
            stop_done.add(stop_id)

            stop_state = stop_states[stop_id]
            print "Got (%s, %s, %s)" % (str(label), str(stop_id), str(stop_state))

            # Update by transfers
            # TODO

            # Update by trips
            # These are the trips that arrive at the stop in time to depart on time
            valid_trips = FT.stops[stop_id].get_trips_arriving_within_time(stop_state[1], Assignment.PATH_TIME_WINDOW)
            print valid_trips

            for (trip_id, seq, arrival_time) in valid_trips:

                #??
                if trips_id in trips_used: continue


            sys.exit(0)