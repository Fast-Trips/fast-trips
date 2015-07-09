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

from operator import attrgetter

from .Assignment import Assignment
from .Logger import FastTripsLogger
from .Passenger import Passenger
from .Route import Route
from .Stop import Stop
from .TAZ import TAZ
from .Trip import Trip

class FastTrips:
    """
    This is the model itself.  Should be simple and run pieces and store the big data structures.
    """

    def __init__(self, input_dir):
        """
        Constructor.  Reads input files from *input_dir*.
        """
        #: :py:class:`list` of :py:class:`fasttrips.Passenger` instances
        self.passengers      = None

        #: :py:class:`dict` with :py:attr:`fasttrips.Route.route_id` key and :py:class:`fasttrips.Route` value
        self.routes          = None

        #: :py:class:`dict` with :py:attr:`fasttrips.Stop.stop_id` key and :py:class:`fasttrips.Stop` value
        self.stops           = None

        #: :py:class:`dict` with :py:attr:`fasttrips.TAZ.taz_id` key and :py:class:`fasttrips.TAZ` value
        self.tazs            = None

        #: :py:class:`dict` with :py:attr:`fasttrips.Trip.trip_id` key and :py:class:`fasttrips.Trip` value
        self.trips           = None

        self.read_input_files(input_dir)

    def read_input_files(self, input_dir):
        """
        Reads in the input files files from *input_dir* and initializes the relevant data structures.
        """
        # read stops into stop_id -> Stop instance
        self.stops = Stop.read_stops(input_dir)
        # incorporate transfers into those stops
        Stop.read_transfers(input_dir, self.stops)

        # read routes into route_id -> Route instance
        self.routes = Route.read_routes(input_dir)
        # read trips into those routes
        self.trips = Trip.read_trips(input_dir, self.routes)

        # read the stops and their times into the trips
        Trip.read_stop_times(input_dir, self.trips, self.stops)

        transfer_stops = 0
        for stop_id,stop in self.stops.iteritems():
            if stop.is_transfer(): transfer_stops += 1

        FastTripsLogger.info("Found %6d transfer stops" % transfer_stops)

        # read the TAZs into taz_id -> TAZ instance
        self.tazs = TAZ.read_TAZs(input_dir)

        # read the access links into both the TAZs and the stops involved
        TAZ.read_access_links(input_dir, self.tazs, self.stops)

        # Read the demand int passenger_id -> passenger instance
        self.passengers = Passenger.read_demand(input_dir)

    def run_assignment(self, output_dir):
        # Do it!
        Assignment.assign_paths(output_dir, self);