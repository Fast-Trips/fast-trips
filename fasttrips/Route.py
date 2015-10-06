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
import datetime, os
import pandas

from .Logger import FastTripsLogger

class Route(object):
    """
    Route class.

    One instance represents all of the Routes.

    Stores route information in :py:attr:`Route.routes_df` and agency information in
    :py:attr:`Route.agencies_df`. Each are instances of :py:class:`pandas.DataFrame`.

    Fare information is in :py:attr:`Route.fare_attrs_df`, :py:attr:`Route.fare_rules_df` and
    :py:attr:`Route.fare_transfer_rules_df`.
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

    #: File with fasttrips fare attributes information (this *subsitutes rather than extends* the
    #: `gtfs fare_attributes <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_attributes_ft.md>`_ file).
    #: See `fare_attributes_ft specification <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_attributes_ft.md>`_.
    INPUT_FARE_ATTRIBUTES_FILE              = "fare_attributes_ft.txt"
    # fasttrips Fare attributes column name: Fare Class
    FARE_ATTR_COLUMN_FARE_CLASS             = "fare_class"
    # fasttrips Fare attributes column name: Price
    FARE_ATTR_COLUMN_PRICE                  = "price"
    # fasttrips Fare attributes column name: Currency Type
    FARE_ATTR_COLUMN_CURRENCY_TYPE          = "currency_type"
    # fasttrips Fare attributes column name: Payment Method
    FARE_ATTR_COLUMN_PAYMENT_METHOD         = "payment_method"
    # fasttrips Fare attributes column name: Transfers (number permitted on this fare)
    FARE_ATTR_COLUMN_TRANSFERS              = "transfers"

    #: File with fasttrips fare rules information (this extends the
    #: `gtfs fare_rules <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_rules.md>`_ file).
    #: See `fare_rules_ft specification <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_rules_ft.md>`_.
    INPUT_FARE_RULES_FILE                   = "fare_rules_ft.txt"
    #: fasttrips Fare rules column name: Fare ID
    FARE_RULES_COLUMN_FARE_ID               = "fare_id"
    #: fasttrips Fare rules column name: Fare ID
    FARE_RULES_COLUMN_FARE_CLASS            = "fare_class"
    #: fasttrips Fare rules column name: Start time for the fare. 'HH:MM:SS' string
    FARE_RULES_COLUMN_START_TIME_STR        = "start_time_str"
    #: fasttrips Fare rules column name: Start time for the fare. Minutes after midnight
    FARE_RULES_COLUMN_START_TIME_MIN        = "start_time_min"
    #: fasttrips Fare rules column name: Start time for the fare. A DateTime
    FARE_RULES_COLUMN_START_TIME            = "start_time"
    #: fasttrips Fare rules column name: End time for the fare rule. 'HH:MM:SS' string
    FARE_RULES_COLUMN_END_TIME_STR          = "end_time_str"
    #: fasttrips Fare rules column name: End time for the fare rule. Minutes after midnight
    FARE_RULES_COLUMN_END_TIME_MIN          = "end_time_min"
    #: fasttrips Fare rules column name: End time for the fare rule. A DateTime.
    FARE_RULES_COLUMN_END_TIME              = "end_time"

    #: File with fasttrips fare transfer rules information.
    #: See `fare_transfer_rules specification <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_transfer_rules.md>`_.
    INPUT_FARE_TRANSFER_RULES_FILE              = "fare_transfer_rules.txt"
    #: fasttrips Fare transfer rules column name: From Fare Class
    FARE_TRANSFER_RULES_COLUMN_FROM_FARE_CLASS  = "from_fare_class"
    #: fasttrips Fare transfer rules column name: To Fare Class
    FARE_TRANSFER_RULES_COLUMN_TO_FARE_CLASS    = "to_fare_class"
    #: fasttrips Fare transfer rules column name: Is flat fee?
    FARE_TRANSFER_RULES_COLUMN_IS_FLAT_FEE      = "is_flat_fee"
    #: fasttrips Fare transfer rules column name: Transfer Rule
    FARE_TRANSFER_RULES_COLUMN_TRANSFER_RULE    = "transfer_rule"

    def __init__(self, input_dir, gtfs_schedule, today):
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
        routes_ft_df = pandas.read_csv(os.path.join(input_dir, Route.INPUT_ROUTES_FILE))
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
        FastTripsLogger.info("Read %7d %15s from %25s, %25s" %
                             (len(self.routes_df), "routes", "routes.txt", Route.INPUT_ROUTES_FILE))


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
        FastTripsLogger.info("Read %7d %15s from %25s" %
                             (len(self.agencies_df), "agencies", "agency.txt"))

        fare_attr_dicts = []
        fare_rule_dicts = []
        for gtfs_fare_attr in gtfs_schedule.GetFareAttributeList():
            fare_attr_dict = {}
            for fieldname in gtfs_fare_attr._FIELD_NAMES:
                if fieldname in gtfs_fare_attr.__dict__:
                    fare_attr_dict[fieldname] = gtfs_fare_attr.__dict__[fieldname]
            fare_attr_dicts.append(fare_attr_dict)

            for gtfs_fare_rule in gtfs_fare_attr.GetFareRuleList():
                fare_rule_dict = {}
                for fieldname in gtfs_fare_rule._FIELD_NAMES:
                    if fieldname in gtfs_fare_rule.__dict__:
                        fare_rule_dict[fieldname] = gtfs_fare_rule.__dict__[fieldname]
                fare_rule_dicts.append(fare_rule_dict)

        self.fare_attrs_df = pandas.DataFrame(data=fare_attr_dicts)

        FastTripsLogger.debug("=========== FARE ATTRIBUTES ===========\n" + str(self.fare_attrs_df.head()))
        FastTripsLogger.debug("\n"+str(self.fare_attrs_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s" %
                             (len(self.fare_attrs_df), "fare attributes", "fare_attributes.txt"))

        # subsitute fasttrips fare attributes
        if os.path.exists(os.path.join(input_dir, Route.INPUT_FARE_ATTRIBUTES_FILE)):
            self.fare_attrs_df = pandas.read_csv(os.path.join(input_dir, Route.INPUT_FARE_ATTRIBUTES_FILE))
            # verify required columns are present
            fare_attrs_cols = list(self.fare_attrs_df.columns.values)
            assert(Route.FARE_ATTR_COLUMN_FARE_CLASS        in fare_attrs_cols)
            assert(Route.FARE_ATTR_COLUMN_PRICE             in fare_attrs_cols)
            assert(Route.FARE_ATTR_COLUMN_CURRENCY_TYPE     in fare_attrs_cols)
            assert(Route.FARE_ATTR_COLUMN_PAYMENT_METHOD    in fare_attrs_cols)
            assert(Route.FARE_ATTR_COLUMN_TRANSFERS         in fare_attrs_cols)

            FastTripsLogger.debug("===> REPLACED BY FARE ATTRIBUTES FT\n" + str(self.fare_attrs_df.head()))
            FastTripsLogger.debug("\n"+str(self.fare_attrs_df.dtypes))
            FastTripsLogger.info("Read %7d %15s from %25s" %
                                 (len(self.fare_attrs_df), "fare attributes", Route.INPUT_FARE_ATTRIBUTES_FILE))

            #: fares are by fare_class rather than by fare_id
            self.fare_by_class = True
        else:
            self.fare_by_class = False

        # Fare rules
        self.fare_rules_df = pandas.DataFrame(data=fare_rule_dicts)

        if os.path.exists(os.path.join(input_dir, Route.INPUT_FARE_RULES_FILE)):
            fare_rules_ft_df = pandas.read_csv(os.path.join(input_dir, Route.INPUT_FARE_RULES_FILE))
            # verify required columns are present
            fare_rules_ft_cols = list(fare_rules_ft_df.columns.values)
            assert(Route.FARE_RULES_COLUMN_FARE_ID      in fare_rules_ft_cols)
            assert(Route.FARE_RULES_COLUMN_FARE_CLASS   in fare_rules_ft_cols)
            assert(Route.FARE_RULES_COLUMN_START_TIME   in fare_rules_ft_cols)
            assert(Route.FARE_RULES_COLUMN_END_TIME     in fare_rules_ft_cols)

            # string version - we already have
            fare_rules_ft_df[Route.FARE_RULES_COLUMN_START_TIME_STR] = fare_rules_ft_df[Route.FARE_RULES_COLUMN_START_TIME]
            fare_rules_ft_df[Route.FARE_RULES_COLUMN_END_TIME_STR  ] = fare_rules_ft_df[Route.FARE_RULES_COLUMN_END_TIME]

            # datetime version
            fare_rules_ft_df[Route.FARE_RULES_COLUMN_START_TIME] = \
                fare_rules_ft_df[Route.FARE_RULES_COLUMN_START_TIME_STR].map(lambda x: \
                    datetime.datetime.combine(today, datetime.datetime.strptime(x, '%H:%M:%S').time()))
            fare_rules_ft_df[Route.FARE_RULES_COLUMN_END_TIME] = \
                fare_rules_ft_df[Route.FARE_RULES_COLUMN_END_TIME_STR].map(lambda x: \
                    datetime.datetime.combine(today, datetime.datetime.strptime(x, '%H:%M:%S').time()))

            # float version
            fare_rules_ft_df[Route.FARE_RULES_COLUMN_START_TIME_MIN] = \
                fare_rules_ft_df[Route.FARE_RULES_COLUMN_START_TIME].map(lambda x: \
                    60*x.time().hour + x.time().minute + x.time().second/60.0 )
            fare_rules_ft_df[Route.FARE_RULES_COLUMN_END_TIME_MIN] = \
                fare_rules_ft_df[Route.FARE_RULES_COLUMN_END_TIME].map(lambda x: \
                    60*x.time().hour + x.time().minute + x.time().second/60.0 )

            # join to fare rules dataframe
            self.fare_rules_df = pandas.merge(left=self.fare_rules_df, right=fare_rules_ft_df,
                                              how='left',
                                              on=Route.FARE_RULES_COLUMN_FARE_ID)


        FastTripsLogger.debug("=========== FARE RULES ===========\n" + str(self.fare_rules_df.head()))
        FastTripsLogger.debug("\n"+str(self.fare_rules_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s, %25s" %
                             (len(self.fare_rules_df), "fare rules", "fare_rules.txt", self.INPUT_FARE_RULES_FILE))

        if os.path.exists(os.path.join(input_dir, Route.INPUT_FARE_TRANSFER_RULES_FILE)):
            self.fare_transfer_rules_df = pandas.read_csv(os.path.join(input_dir, Route.INPUT_FARE_TRANSFER_RULES_FILE))
            # verify required columns are present
            fare_transfer_rules_cols = list(self.fare_transfer_rules_df.columns.values)
            assert(Route.FARE_TRANSFER_RULES_COLUMN_FROM_FARE_CLASS in fare_transfer_rules_cols)
            assert(Route.FARE_TRANSFER_RULES_COLUMN_TO_FARE_CLASS   in fare_transfer_rules_cols)
            assert(Route.FARE_TRANSFER_RULES_COLUMN_IS_FLAT_FEE     in fare_transfer_rules_cols)
            assert(Route.FARE_TRANSFER_RULES_COLUMN_TRANSFER_RULE   in fare_transfer_rules_cols)

            FastTripsLogger.debug("=========== FARE TRANSFER RULES ===========\n" + str(self.fare_transfer_rules_df.head()))
            FastTripsLogger.debug("\n"+str(self.fare_transfer_rules_df.dtypes))
            FastTripsLogger.info("Read %7d %15s from %25s" %
                                 (len(self.fare_transfer_rules_df), "fare xfer rules", Route.INPUT_FARE_TRANSFER_RULES_FILE))
        else:
            self.fare_transfer_rules_df = None
