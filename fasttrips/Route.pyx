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
import os
import pandas

from .Logger import FastTripsLogger

class Route(object):
    """
    Route class.  Documentation forthcoming.
    """

    #: File with routes.
    #: TODO document format
    INPUT_ROUTES_FILE = "ft_input_routes.dat"

    def __init__(self, route_record):
        """
        Constructor from dictionary mapping attribute to value.
        """
        #: ID that uniquely identifies a route
        self.route_id           = route_record['routeId'        ]
        #: Short name of the route
        self.short_name         = route_record['routeShortName' ]
        #: Full name of the route
        self.long_name          = route_record['routeLongName'  ]

        #: Service type:
        #: 0 - Tram, streetcar, light rail
        #: 1 - Subway, metro
        #: 2 - Rail
        #: 3 - Bus
        #: 4 - Ferry
        #: 5 - Cable car
        #: 6 - Gondola, suspended cable car
        self.service_type       = route_record['routeType']
        assert self.service_type in [0,1,2,3,4,5,6]

        #: These are the :py:class:`fasttrips.Trip` instances that run on this route.
        #: This a :py:class:`dict` mapping the *trip_id* to a :py:class:`fasttrips.Trip` instance
        self.trips              = {}

    def add_trip(self, trip):
        """
        Add the given trip to my trip list
        """
        self.trips[trip.trip_id] = trip

    @staticmethod
    def read_routes(input_dir):
        """
        Read the stops from the input file in *input_dir*.
        """
        routes_df = pandas.read_csv(os.path.join(input_dir, Route.INPUT_ROUTES_FILE), sep="\t")
        FastTripsLogger.debug("=========== ROUTES ===========\n" + str(routes_df.head()))
        FastTripsLogger.debug("\n"+str(routes_df.dtypes))

        route_id_to_route = {}
        route_records = routes_df.to_dict(orient='records')
        for route_record in route_records:
            route = Route(route_record)
            route_id_to_route[route.route_id] = route

        FastTripsLogger.info("Read %7d routes" % len(route_id_to_route))
        return route_id_to_route
