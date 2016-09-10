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
import numpy,pandas

from .Error  import NetworkInputError
from .Logger import FastTripsLogger
from .Util   import Util

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
    ROUTES_COLUMN_ROUTE_ID                  = "route_id"
    #: gtfs Routes column name: Short name
    ROUTES_COLUMN_ROUTE_SHORT_NAME          = "route_short_name"
    #: gtfs Routes column name: Long name
    ROUTES_COLUMN_ROUTE_LONG_NAME           = "route_long_name"
    #: gtfs Routes column name: Route type
    ROUTES_COLUMN_ROUTE_TYPE                = "route_type"
    #: gtfs Routes column name: Agency ID
    ROUTES_COLUMN_AGENCY_ID                 = "agency_id"
    #: fasttrips Routes column name: Mode
    ROUTES_COLUMN_MODE                      = "mode"
    #: fasttrips Routes column name: Proof of Payment
    ROUTES_COLUMN_PROOF_OF_PAYMENT          = "proof_of_payment"

    # ========== Added by fasttrips =======================================================
    #: fasttrips Routes column name: Mode number
    ROUTES_COLUMN_ROUTE_ID_NUM              = "route_id_num"
    #: fasttrips Routes column name: Mode number
    ROUTES_COLUMN_MODE_NUM                  = "mode_num"
    #: fasttrips Routes column name: Mode type
    ROUTES_COLUMN_MODE_TYPE                 = "mode_type"
    #: Value for :py:attr:`Route.ROUTES_COLUMN_MODE_TYPE` column: access
    MODE_TYPE_ACCESS                        = "access"
    #: Value for :py:attr:`Route.ROUTES_COLUMN_MODE_TYPE` column: egress
    MODE_TYPE_EGRESS                        = "egress"
    #: Value for :py:attr:`Route.ROUTES_COLUMN_MODE_TYPE` column: transit
    MODE_TYPE_TRANSIT                       = "transit"
    #: Value for :py:attr:`Route.ROUTES_COLUMN_MODE_TYPE` column: transfer
    MODE_TYPE_TRANSFER                      = "transfer"
    #: Access mode numbers start from here
    MODE_NUM_START_ACCESS                   = 101
    #: Egress mode numbers start from here
    MODE_NUM_START_EGRESS                   = 201
    #: Route mode numbers start from here
    MODE_NUM_START_ROUTE                    = 301

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
    #: fasttrips Fare rules column name: Fare class
    FARE_RULES_COLUMN_FARE_CLASS            = FARE_ATTR_COLUMN_FARE_CLASS
    #: fasttrips Fare rules column name: Start time for the fare. A DateTime
    FARE_RULES_COLUMN_START_TIME            = "start_time"
    #: fasttrips Fare rules column name: End time for the fare rule. A DateTime.
    FARE_RULES_COLUMN_END_TIME              = "end_time"
    #: fasttrips Fare rules column name: Fare ID num
    FARE_RULES_COLUMN_FARE_ID_NUM           = "fare_id_num"

    #: File with fasttrips fare transfer rules information.
    #: See `fare_transfer_rules specification <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_transfer_rules_ft.md>`_.
    INPUT_FARE_TRANSFER_RULES_FILE              = "fare_transfer_rules_ft.txt"
    #: fasttrips Fare transfer rules column name: From Fare Class
    FARE_TRANSFER_RULES_COLUMN_FROM_FARE_CLASS  = "from_fare_class"
    #: fasttrips Fare transfer rules column name: To Fare Class
    FARE_TRANSFER_RULES_COLUMN_TO_FARE_CLASS    = "to_fare_class"
    #: fasttrips Fare transfer rules column name: Is flat fee?
    FARE_TRANSFER_RULES_COLUMN_IS_FLAT_FEE      = "is_flat_fee"
    #: fasttrips Fare transfer rules column name: Transfer Rule
    FARE_TRANSFER_RULES_COLUMN_TRANSFER_RULE    = "transfer_rule"

    #: File with route ID, route ID number correspondence (and fare id num)
    OUTPUT_ROUTE_ID_NUM_FILE                    = "ft_intermediate_route_id.txt"
    #: File with fare id num, fare id, fare class, price, xfers
    OUTPUT_FARE_ID_FILE                         = "ft_intermediate_fare.txt"
    #: File with mode, mode number correspondence
    OUTPUT_MODE_NUM_FILE                        = "ft_intermediate_supply_mode_id.txt"

    def __init__(self, input_dir, output_dir, gtfs_schedule, today):
        """
        Constructor.  Reads the gtfs data from the transitfeed schedule, and the additional
        fast-trips routes data from the input file in *input_dir*.
        """
        self.output_dir         = output_dir

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
        routes_ft_df = pandas.read_csv(os.path.join(input_dir, Route.INPUT_ROUTES_FILE),
                                       dtype={Route.ROUTES_COLUMN_ROUTE_ID:object,
                                              Route.ROUTES_COLUMN_MODE    :object})
        # verify required columns are present
        routes_ft_cols = list(routes_ft_df.columns.values)
        assert(Route.ROUTES_COLUMN_ROUTE_ID     in routes_ft_cols)
        assert(Route.ROUTES_COLUMN_MODE         in routes_ft_cols)

        # verify no routes_ids are duplicated
        if routes_ft_df.duplicated(subset=[Route.ROUTES_COLUMN_ROUTE_ID]).sum()>0:
            error_msg = "Found %d duplicate %s in %s" % (routes_ft_df.duplicated(subset=[Route.ROUTES_COLUMN_ROUTE_ID]).sum(),
                                                         Route.ROUTES_COLUMN_ROUTE_ID, Route.INPUT_ROUTES_FILE)
            FastTripsLogger.fatal(error_msg)
            FastTripsLogger.fatal("\nDuplicates:\n%s" % \
                                  str(routes_ft_df.loc[routes_ft_df.duplicated(subset=[Route.ROUTES_COLUMN_ROUTE_ID])]))
            raise NetworkInputError(Route.INPUT_ROUTES_FILE, error_msg)

        # Join to the routes dataframe
        self.routes_df = pandas.merge(left=self.routes_df, right=routes_ft_df,
                                      how='left',
                                      on=Route.ROUTES_COLUMN_ROUTE_ID)
        # Get the mode list
        self.modes_df = self.routes_df[[Route.ROUTES_COLUMN_MODE]].drop_duplicates().reset_index(drop=True)
        self.modes_df[Route.ROUTES_COLUMN_MODE_NUM] = self.modes_df.index + Route.MODE_NUM_START_ROUTE
        self.modes_df[Route.ROUTES_COLUMN_MODE_TYPE] = Route.MODE_TYPE_TRANSIT

        # Join to mode numbering
        self.routes_df = Util.add_new_id(self.routes_df, Route.ROUTES_COLUMN_MODE, Route.ROUTES_COLUMN_MODE_NUM,
                                         self.modes_df,  Route.ROUTES_COLUMN_MODE, Route.ROUTES_COLUMN_MODE_NUM)

        # Route IDs are strings.  Create a unique numeric route ID.
        self.route_id_df = Util.add_numeric_column(self.routes_df[[Route.ROUTES_COLUMN_ROUTE_ID]],
                                                   id_colname=Route.ROUTES_COLUMN_ROUTE_ID,
                                                   numeric_newcolname=Route.ROUTES_COLUMN_ROUTE_ID_NUM)
        FastTripsLogger.debug("Route ID to number correspondence\n" + str(self.route_id_df.head()))
        FastTripsLogger.debug(str(self.route_id_df.dtypes))

        self.routes_df = self.add_numeric_route_id(self.routes_df,
                                                   id_colname=Route.ROUTES_COLUMN_ROUTE_ID,
                                                   numeric_newcolname=Route.ROUTES_COLUMN_ROUTE_ID_NUM)

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
            self.fare_attrs_df = pandas.read_csv(os.path.join(input_dir, Route.INPUT_FARE_ATTRIBUTES_FILE),
                                                 dtype={Route.FARE_ATTR_COLUMN_PRICE:numpy.float64})
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

        # Fare rules (map routes to fare_id)
        self.fare_rules_df = pandas.DataFrame(data=fare_rule_dicts)
        if len(self.fare_rules_df) > 0:
            self.fare_ids_df = Util.add_numeric_column(self.fare_rules_df[[Route.FARE_RULES_COLUMN_FARE_ID]],
                                                       id_colname=Route.FARE_RULES_COLUMN_FARE_ID,
                                                       numeric_newcolname=Route.FARE_RULES_COLUMN_FARE_ID_NUM)
            self.fare_rules_df = pandas.merge(left  =self.fare_rules_df,
                                              right =self.fare_ids_df,
                                              how   ="left")
        else:
            self.fare_ids_df = pandas.DataFrame()

        if os.path.exists(os.path.join(input_dir, Route.INPUT_FARE_RULES_FILE)):
            fare_rules_ft_df = pandas.read_csv(os.path.join(input_dir, Route.INPUT_FARE_RULES_FILE),
                                               dtype={Route.FARE_RULES_COLUMN_START_TIME:str, Route.FARE_RULES_COLUMN_END_TIME:str})
            # verify required columns are present
            fare_rules_ft_cols = list(fare_rules_ft_df.columns.values)
            assert(Route.FARE_RULES_COLUMN_FARE_ID      in fare_rules_ft_cols)
            assert(Route.FARE_RULES_COLUMN_FARE_CLASS   in fare_rules_ft_cols)
            assert(Route.FARE_RULES_COLUMN_START_TIME   in fare_rules_ft_cols)
            assert(Route.FARE_RULES_COLUMN_END_TIME     in fare_rules_ft_cols)

            # convert these to datetimes
            fare_rules_ft_df[Route.FARE_RULES_COLUMN_START_TIME] = \
                fare_rules_ft_df[Route.FARE_RULES_COLUMN_START_TIME].map(lambda x: Util.read_time(x))
            fare_rules_ft_df[Route.FARE_RULES_COLUMN_END_TIME] = \
                fare_rules_ft_df[Route.FARE_RULES_COLUMN_END_TIME].map(lambda x: Util.read_time(x, True))

            # Split fare classes so they don't overlap
            fare_rules_ft_df = self.remove_fare_class_overlap(fare_rules_ft_df)

            # join to fare rules dataframe
            self.fare_rules_df = pandas.merge(left=self.fare_rules_df, right=fare_rules_ft_df,
                                              how='left',
                                              on=Route.FARE_RULES_COLUMN_FARE_ID)

            # join to fare_attributes on fare_class if we have it, or fare_id if we don't

            #: Fare ID/class (fare period)/attribute mapping.
            #:
            #: ===================  =====================================================================================================================================
            #:  Column name         Column Description
            #: ===================  =====================================================================================================================================
            #: `fare_id`            GTFS fare_id (See `fare_rules`_)
            #: `fare_id_num`        Numbered fare_id
            #: `route_id`           (optional) Route(s) associated with this fare ID. (See `fare_rules`_)
            #: `origin_id`          (optional) Origin fare zone ID(s) for fare ID. (See `fare_rules`_)
            #: `destination_id`     (optional) Destination fare zone ID(s) for fare ID. (See `fare_rules`_)
            #: `contains_id`        (optional) Contains fare zone ID(s) for fare ID. (See `fare_rules`_)
            #: `fare_class`         GTFS-plus fare_class (See `fare_rules_ft`_)
            #: `start_time`         Fare class start time (See `fare_rules_ft`_)
            #: `end_time`           Fare class end time (See `fare_rules_ft`_)
            #: `currency_type`      Currency of fare class or id (See `fare_attributes`_ or `fare_attributes_ft`_)
            #: `price`              Price of fare class or id (See `fare_attributes`_ or `fare_attributes_ft`_)
            #: `payment_method`     When the fare must be paid (See `fare_attributes`_ or `fare_attributes_ft`_)
            #: `transfers`          Number of transfers permiited on this fare (See `fare_attributes`_ or `fare_attributes_ft`_)
            #: `transfer_duration`  (optional) Integer length of time in seconds before transfer expires (See `fare_attributes`_ or `fare_attributes_ft`_)
            #: ===================  =====================================================================================================================================
            #:
            #: .. _fare_rules: https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_rules.md
            #: .. _fare_rules_ft: https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_rules_ft.md
            #: .. _fare_attributes: https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_attributes.md
            #: .. _fare_attributes_ft: https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_attributes_ft.md
            self.fare_rules_df = pandas.merge(left =self.fare_rules_df,
                                              right=self.fare_attrs_df,
                                              how  ='left',
                                              on   = Route.FARE_RULES_COLUMN_FARE_CLASS if self.fare_by_class else Route.FARE_RULES_COLUMN_FARE_ID)


        FastTripsLogger.debug("=========== FARE RULES ===========\n" + str(self.fare_rules_df.head().to_string(formatters=\
                              {Route.FARE_RULES_COLUMN_START_TIME:Util.datetime64_formatter,
                               Route.FARE_RULES_COLUMN_END_TIME  :Util.datetime64_formatter})))
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

        self.write_routes_for_extension()

    def add_numeric_route_id(self, input_df, id_colname, numeric_newcolname):
        """
        Passing a :py:class:`pandas.DataFrame` with a route ID column called *id_colname*,
        adds the numeric route id as a column named *numeric_newcolname* and returns it.
        """
        return Util.add_new_id(input_df, id_colname, numeric_newcolname,
                               mapping_df=self.route_id_df,
                               mapping_id_colname=Route.ROUTES_COLUMN_ROUTE_ID,
                               mapping_newid_colname=Route.ROUTES_COLUMN_ROUTE_ID_NUM)

    def add_access_egress_modes(self, access_modes_df, egress_modes_df):
        """
        Adds access and egress modes to the mode list
        Writes out mapping to disk
        """
        access_modes_df[Route.ROUTES_COLUMN_MODE_TYPE] = Route.MODE_TYPE_ACCESS
        egress_modes_df[Route.ROUTES_COLUMN_MODE_TYPE] = Route.MODE_TYPE_EGRESS
        implicit_modes_df = pandas.DataFrame({Route.ROUTES_COLUMN_MODE_TYPE: [Route.MODE_TYPE_TRANSFER],
                                              Route.ROUTES_COLUMN_MODE:      [Route.MODE_TYPE_TRANSFER],
                                              Route.ROUTES_COLUMN_MODE_NUM:  [                       1]})

        self.modes_df = pandas.concat([implicit_modes_df,
                                      self.modes_df,
                                      access_modes_df,
                                      egress_modes_df], axis=0)
        self.modes_df.reset_index(inplace=True)

        # write intermediate files
        self.modes_df.to_csv(os.path.join(self.output_dir, Route.OUTPUT_MODE_NUM_FILE),
                             columns=[Route.ROUTES_COLUMN_MODE_NUM, Route.ROUTES_COLUMN_MODE],
                             sep=" ", index=False)
        FastTripsLogger.debug("Wrote %s" % os.path.join(self.output_dir, Route.OUTPUT_MODE_NUM_FILE))

    def add_numeric_mode_id(self, input_df, id_colname, numeric_newcolname, warn=False):
        """
        Passing a :py:class:`pandas.DataFrame` with a mode ID column called *id_colname*,
        adds the numeric mode id as a column named *numeric_newcolname* and returns it.
        """
        return Util.add_new_id(input_df, id_colname, numeric_newcolname,
                               mapping_df=self.modes_df[[Route.ROUTES_COLUMN_MODE_NUM, Route.ROUTES_COLUMN_MODE]],
                               mapping_id_colname=Route.ROUTES_COLUMN_MODE,
                               mapping_newid_colname=Route.ROUTES_COLUMN_MODE_NUM,
                               warn=warn)

    def remove_fare_class_overlap(self, fare_rules_ft_df):
        """
        Split fare classes so they don't overlap
        """
        fare_rules_ft_df["fare_period_id"] = fare_rules_ft_df.index+1
        # FastTripsLogger.debug("remove_fare_class_overlap: initial\n%s" % fare_rules_ft_df)
        max_fare_period_id = fare_rules_ft_df["fare_period_id"].max()

        while True:
            # join with itself to see if any are contained
            df = pandas.merge(left =fare_rules_ft_df,
                              right=fare_rules_ft_df,
                              on   =Route.FARE_RULES_COLUMN_FARE_ID,
                              how  ="outer")

            # if there's one fare class per fare id, nothing to do
            if len(df)==len(fare_rules_ft_df):
                FastTripsLogger.debug("One fare class per fare id, no need to split")
                return fare_rules_ft_df

            # remove dupes
            df = df.loc[ df["fare_period_id_x"] != df["fare_period_id_y"] ]

            FastTripsLogger.debug("remove_fare_class_overlap:\n%s" % df)

            # this is an overlap -- error
            #  ____y_______                     x starts after y starts
            #          ______x______            x starts before y ends
            #                                   x ends after y ends
            intersecting_fare_periods = df.loc[ (df["start_time_x"]>df["start_time_y"])& \
                                                (df["start_time_x"]<df["end_time_y"])& \
                                                (df["end_time_x"  ]>df["end_time_y"]) ]
            if len(intersecting_fare_periods) > 0:
                error_msg = "Partially overlapping fare periods are ambiguous. \n%s" % str(intersecting_fare_periods)
                FastTripsLogger.error(error_msg)
                raise NetworkInputError(Route.INPUT_FARE_RULES_FILE, error_msg)

            # is x a subset of y?
            #    ___x___            x starts after y starts
            # ______y_______        x ends before y ends
            subset_fare_periods = df.loc[ (df["start_time_x"]>=df["start_time_y"])& \
                                          (df["end_time_x"  ]<=df["end_time_y"]) ]
            # if no subsets, done -- return
            if len(subset_fare_periods) == 0:
                FastTripsLogger.debug("remove_fare_class_overlap returning\n%s" % fare_rules_ft_df)
                return fare_rules_ft_df

            # do one at a time -- split first into three rows
            FastTripsLogger.debug("splitting\n%s" % str(subset_fare_periods.head(1)))
            row_dict = subset_fare_periods.head(1).to_dict(orient="records")[0]
            FastTripsLogger.debug(row_dict)
            y_1 = {'fare_id'          :row_dict['fare_id'],
                   'fare_class'       :row_dict['fare_class_y'],
                   'start_time'       :row_dict['start_time_y'],
                   'end_time'         :row_dict['start_time_x'],
                   'fare_period_id'   :row_dict['fare_period_id_y']}
            x   = {'fare_id'          :row_dict['fare_id'],
                   'fare_class'       :row_dict['fare_class_x'],
                   'start_time'       :row_dict['start_time_x'],
                   'end_time'         :row_dict['end_time_x'],
                   'fare_period_id'   :row_dict['fare_period_id_x']}
            y_2 = {'fare_id'          :row_dict['fare_id'],
                   'fare_class'       :row_dict['fare_class_y'],
                   'start_time'       :row_dict['end_time_x'],
                   'end_time'         :row_dict['end_time_y'],
                   'fare_period_id'   :max_fare_period_id+1} # new
            max_fare_period_id += 1

            new_df = pandas.DataFrame([y_1,x,y_2])
            FastTripsLogger.debug("\n%s" % str(new_df))

            # put it together with the unaffected fare_classes we already had
            prev_df = fare_rules_ft_df.loc[ (fare_rules_ft_df["fare_period_id"]!=row_dict["fare_period_id_x"])&
                                            (fare_rules_ft_df["fare_period_id"]!=row_dict["fare_period_id_y"]) ]
            fare_rules_ft_df = prev_df.append(new_df)

            # sort by fare_id, start_time
            fare_rules_ft_df.sort_values([Route.FARE_RULES_COLUMN_FARE_ID,
                                          Route.FARE_RULES_COLUMN_START_TIME], inplace=True)
            # reorder columns
            fare_rules_ft_df = fare_rules_ft_df[[Route.FARE_RULES_COLUMN_FARE_ID,
                                                 Route.FARE_RULES_COLUMN_FARE_CLASS,
                                                 "fare_period_id",
                                                 Route.FARE_RULES_COLUMN_START_TIME,
                                                 Route.FARE_RULES_COLUMN_END_TIME]]
            FastTripsLogger.debug("\n%s" % str(fare_rules_ft_df))

        # this shouldn't happen
        FastTripsLogger.warn("This shouldn't happen")

    def write_routes_for_extension(self):
        """
        Write to an intermediate formatted file for the C++ extension.
        Since there are strings involved, it's easier than passing it to the extension.
        """
        # add fare_id columns
        route_id_df = pandas.merge(left =self.route_id_df,
                                   right=self.fare_rules_df,
                                   how  ="left",
                                   on   =Route.ROUTES_COLUMN_ROUTE_ID)
        route_id_df.fillna(value={Route.FARE_RULES_COLUMN_FARE_ID_NUM:-1}, inplace=True)
        # convert it back to int
        route_id_df[Route.FARE_RULES_COLUMN_FARE_ID_NUM] = route_id_df[Route.FARE_RULES_COLUMN_FARE_ID_NUM].astype(int)

        # write intermediate file -- route id num, route id, and fare_id_num ('None' if not applicable)
        route_id_df.to_csv(os.path.join(self.output_dir, Route.OUTPUT_ROUTE_ID_NUM_FILE),
                           columns=[Route.ROUTES_COLUMN_ROUTE_ID_NUM, Route.ROUTES_COLUMN_ROUTE_ID, Route.FARE_RULES_COLUMN_FARE_ID_NUM],
                           sep=" ", index=False)
        FastTripsLogger.debug("Wrote %s" % os.path.join(self.output_dir, Route.OUTPUT_ROUTE_ID_NUM_FILE))


        # write fare file
        if len(self.fare_rules_df) > 0:
            # copy for writing
            fare_rules_df = self.fare_rules_df.copy()

            # replace with float versions
            fare_rules_df[Route.FARE_RULES_COLUMN_START_TIME] = (fare_rules_df[Route.FARE_RULES_COLUMN_START_TIME] - Util.SIMULATION_DAY_START)/numpy.timedelta64(1,'m')
            fare_rules_df[Route.FARE_RULES_COLUMN_END_TIME  ] = (fare_rules_df[Route.FARE_RULES_COLUMN_END_TIME  ] - Util.SIMULATION_DAY_START)/numpy.timedelta64(1,'m')

            # temp column: duraton; sort by this so the smallest duration is found first
            fare_rules_df["duration"] = fare_rules_df[Route.FARE_RULES_COLUMN_END_TIME  ] - fare_rules_df[Route.FARE_RULES_COLUMN_START_TIME]
            fare_rules_df.sort_values(by=[Route.FARE_RULES_COLUMN_FARE_ID_NUM,"duration"], ascending=True, inplace=True)

            # File with fare id num, fare id, fare class, price, xfers
            fare_rules_df.to_csv(os.path.join(self.output_dir, Route.OUTPUT_FARE_ID_FILE),
                                columns=[Route.FARE_RULES_COLUMN_FARE_ID_NUM,
                                         Route.FARE_RULES_COLUMN_FARE_ID,
                                         Route.FARE_ATTR_COLUMN_FARE_CLASS,
                                         Route.FARE_RULES_COLUMN_START_TIME,
                                         Route.FARE_RULES_COLUMN_END_TIME,
                                         Route.FARE_ATTR_COLUMN_PRICE,
                                         Route.FARE_ATTR_COLUMN_TRANSFERS],
                                sep=" ", index=False)
            FastTripsLogger.debug("Wrote %s" % os.path.join(self.output_dir, Route.OUTPUT_FARE_ID_FILE))
        else:
            FastTripsLogger.debug("No fare rules so no file %s" % os.path.join(self.output_dir, Route.OUTPUT_FARE_ID_FILE))
