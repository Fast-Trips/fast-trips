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
from .Logger import FastTripsLogger
from .Route import Route
from .Stop import Stop
from .Trip import Trip

class FastTrips:
    """
    This is the model itself.  Should be simple and run pieces and store the big data structures.
    """

    @staticmethod
    def run_model(input_dir, output_dir):
        # read stops into stop_id -> Stop instance
        stop_id_to_stop = Stop.read_stops(input_dir)
        # incorporate transfers into those stops
        Stop.read_transfers(input_dir, stop_id_to_stop)

        # read routes into route_id -> Route instance
        route_id_to_route = Route.read_routes(input_dir)
        # read trips into those routes
        trip_id_to_trip = Trip.read_trips(input_dir, route_id_to_route)

        # read the stops and their times into the trips
        Trip.read_stop_times(input_dir, trip_id_to_trip, stop_id_to_stop)

        transfer_stops = 0
        for stop_id,stop in stop_id_to_stop.iteritems():
            if stop.is_transfer(): transfer_stops += 1

        FastTripsLogger.info("Found %6d transfer stops" % transfer_stops)

        # fasttrips.readTAZs()
        # fasttrips.readAccessLinks()
        # fasttrips.readPassengers()
        # fasttrips.passengerAssignment();