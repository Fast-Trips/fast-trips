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
import os,sys
import pandas

from .Logger import FastTripsLogger

class Trip:
    """
    Trip class.  Documentation forthcoming.
    """

    #: File with trips.
    #: TODO document format
    INPUT_TRIPS_FILE        = "ft_input_trips.dat"

    #: File with stop times
    #: TODO document format
    INPUT_STOPTIMES_FILE    = "ft_input_stopTimes.dat"

    def __init__(self, trip_record, route_id_to_route):
        """
        Constructor from dictionary mapping attribute to string value.
        """
        self.trip_id        = trip_record['tripId']
        self.route_id       = trip_record['routeId']

        #: Service type:
        #: 0 - Tram, streetcar, light rail
        #: 1 - Subway, metro
        #: 2 - Rail
        #: 3 - Bus
        #: 4 - Ferry
        #: 5 - Cable car
        #: 6 - Gondola, suspended cable car
        self.service_type   = trip_record['type']
        assert self.service_type in [0,1,2,3,4,5,6]

        #: The vehicle capacity for the trip
        self.capacity       = trip_record['capacity']

        #: ID that defines a shape for the trip
        self.shape_id       = trip_record['shapeId']

        #: binary varlue (0 or 1) indicating the direction of the trip
        self.direction_id   = trip_record['directionId']

        # Tell the route about me!
        route_id_to_route[self.route_id].add_trip(self)

        # list of (stop_id, arrival time, departure time)
        self.stops          = []

    def add_stop(self, stop_time_record, stop_id_to_stop):
        """
        Add the stop time information to this trip.
        """
        # verify they're in sequence
        assert(stop_time_record['sequence']) == len(self.stops)+1
        self.stops.append( (stop_time_record['stopId'],
                            stop_time_record['arrivalTime'],
                            stop_time_record['departureTime'])
                         )

        # tell the stop to update accordingly
        stop_id_to_stop[stop_time_record['stopId']].add_to_trip(self)

    @staticmethod
    def read_trips(input_dir, route_id_to_route):
        """
        Read the trips from the input file in *input_dir*.
        """
        trips_df = pandas.read_csv(os.path.join(input_dir, Trip.INPUT_TRIPS_FILE), sep="\t")
        FastTripsLogger.debug("=========== TRIPS ===========\n" + str(trips_df.head()))
        FastTripsLogger.debug("\n"+str(trips_df.dtypes))

        trip_id_to_trip = {}
        trip_records = trips_df.to_dict(orient='records')
        for trip_record in trip_records:
            trip = Trip(trip_record, route_id_to_route)
            trip_id_to_trip[trip.trip_id] = trip

        FastTripsLogger.info("Read %7d trips" % len(trip_id_to_trip))
        return trip_id_to_trip


    @staticmethod
    def read_stop_times(input_dir, trip_id_to_trip, stop_id_to_stop):
        """
        Read the stop times from the input file in *input_dir*.
        """
        stop_times_df = pandas.read_csv(os.path.join(input_dir, Trip.INPUT_STOPTIMES_FILE), sep="\t")
        FastTripsLogger.debug("=========== STOP TIMES ===========\n" + str(stop_times_df.head()))
        FastTripsLogger.debug("\n"+str(stop_times_df.dtypes))

        stop_time_records = stop_times_df.to_dict(orient='records')
        for stop_time_record in stop_time_records:
            trip = trip_id_to_trip[stop_time_record['tripId']]
            trip.add_stop(stop_time_record, stop_id_to_stop)

        FastTripsLogger.info("Read %7d stop times" % len(stop_times_df))


