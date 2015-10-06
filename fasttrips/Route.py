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
    Route class.

    One instance represents all of the Routes.

    Stores route information in :py:attr:`Route.routes_df` and agency information in
    :py:attr:`Route.agencies_df`. Each are instances of :py:class:`pandas.DataFrame`.
    """

    #: File with fasttrips routes information (this extends the
    #: `gtfs routes <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/routes.md>`_ file).
    #: See `routes_ft specification <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/routes_ft.md>`_.
    INPUT_ROUTES_FILE                       = "routes_ft.txt"
    #: gtfs Routes column name: Unique identifier
    ROUTES_COLUMN_ID                        = "route_id"
    #: fasttrips Routes column name: Mode
    ROUTES_COLUMN_MODE                      = "mode"

    #: fasttrips Routes column name: Fare Class
    ROUTES_COLUMN_FARE_CLASS                = "fare_class"
    #: fasttrips Routes column name: Proof of Payment
    ROUTES_COLUMN_PROOF_OF_PAYMENT          = "proof_of_payment"

    def __init__(self, input_dir, gtfs_schedule):
        """
        Constructor.  Reads the gtfs data from the transitfeed schedule, and the additional
        fast-trips routes data from the input file in *input_dir*.
        """
        pandas.set_option('display.width', 1000)

        # Combine all gtfs Route objects to a single pandas DataFrame
        route_dicts = []
        for gtfs_route in gtfs_schedule.GetRouteList():
            route_dict = {}
            for fieldname in gtfs_route._FIELD_NAMES:
                if fieldname in gtfs_route.__dict__:
                    route_dict[fieldname] = gtfs_route.__dict__[fieldname]
            route_dicts.append(route_dict)
        self.routes_df = pandas.DataFrame(data=route_dicts)

        # Read the fast-trips supplemental routes data file
        routes_ft_df = pandas.read_csv(os.path.join(input_dir, "..", Route.INPUT_ROUTES_FILE))
        # verify required columns are present
        routes_ft_cols = list(routes_ft_df.columns.values)
        assert(Route.ROUTES_COLUMN_ID           in routes_ft_cols)
        assert(Route.ROUTES_COLUMN_MODE         in routes_ft_cols)

        # Join to the routes dataframe
        self.routes_df = pandas.merge(left=self.routes_df, right=routes_ft_df,
                                      how='left',
                                      on=Route.ROUTES_COLUMN_ID)

        self.routes_df.set_index(Route.ROUTES_COLUMN_ID, inplace=True, verify_integrity=True)

        FastTripsLogger.debug("=========== ROUTES ===========\n" + str(self.routes_df.head()))
        FastTripsLogger.debug("\n"+str(self.routes_df.dtypes))
        FastTripsLogger.info("Read %7d routes" % len(self.routes_df))


        agency_dicts = []
        for gtfs_agency in gtfs_schedule.GetAgencyList():
            agency_dict = {}
            for fieldname in gtfs_agency._FIELD_NAMES:
                if fieldname in gtfs_agency.__dict__:
                    agency_dict[fieldname] = gtfs_agency.__dict__[fieldname]
            agency_dicts.append(agency_dict)
        self.agencies_df = pandas.DataFrame(data=agency_dicts)

        FastTripsLogger.debug("=========== AGENCIES ===========\n" + str(self.agencies_df.head()))
        FastTripsLogger.debug("\n"+str(self.agencies_df.dtypes))
        FastTripsLogger.info("Read %7d agencies" % len(self.agencies_df))
