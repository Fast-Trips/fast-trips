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

import numpy as np
import pandas as pd

from .Error  import NetworkInputError, NotImplementedError, UnexpectedError
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
    # fasttrips Fare attributes column name: Fare Period
    FARE_ATTR_COLUMN_FARE_PERIOD            = "fare_period"
    # fasttrips Fare attributes column name: Price
    FARE_ATTR_COLUMN_PRICE                  = "price"
    # fasttrips Fare attributes column name: Currency Type
    FARE_ATTR_COLUMN_CURRENCY_TYPE          = "currency_type"
    # fasttrips Fare attributes column name: Payment Method
    FARE_ATTR_COLUMN_PAYMENT_METHOD         = "payment_method"
    # fasttrips Fare attributes column name: Transfers (number permitted on this fare)
    FARE_ATTR_COLUMN_TRANSFERS              = "transfers"
    # fasttrips Fare attributes column name: Transfer duration (Integer length of time in seconds before transfer expires. Omit or leave empty if they do not.)
    FARE_ATTR_COLUMN_TRANSFER_DURATION      = "transfer_duration"

    #: File with fasttrips fare periods information
    #: See `fare_rules_ft specification <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_rules_ft.md>`_.
    INPUT_FARE_PERIODS_FILE                 = "fare_periods_ft.txt"
    #: fasttrips Fare rules column name: Fare ID
    FARE_RULES_COLUMN_FARE_ID               = "fare_id"
    #: GTFS fare rules column name: Route ID
    FARE_RULES_COLUMN_ROUTE_ID              = ROUTES_COLUMN_ROUTE_ID
    #: GTFS fare rules column name: Origin Zone ID
    FARE_RULES_COLUMN_ORIGIN_ID             = "origin_id"
    #: GTFS fare rules column name: Destination Zone ID
    FARE_RULES_COLUMN_DESTINATION_ID        = "destination_id"
    #: GTFS fare rules column name: Contains ID
    FARE_RULES_COLUMN_CONTAINS_ID           = "contains_id"
    #: fasttrips Fare rules column name: Fare class
    FARE_RULES_COLUMN_FARE_PERIOD           = FARE_ATTR_COLUMN_FARE_PERIOD
    #: fasttrips Fare rules column name: Start time for the fare. A DateTime
    FARE_RULES_COLUMN_START_TIME            = "start_time"
    #: fasttrips Fare rules column name: End time for the fare rule. A DateTime.
    FARE_RULES_COLUMN_END_TIME              = "end_time"

    # ========== Added by fasttrips =======================================================
    #: fasttrips Fare rules column name: Fare ID num
    FARE_RULES_COLUMN_FARE_ID_NUM           = "fare_id_num"
    #: fasttrips Fare rules column name: Route ID num
    FARE_RULES_COLUMN_ROUTE_ID_NUM           = ROUTES_COLUMN_ROUTE_ID_NUM
    #: fasttrips fare rules column name: Origin Zone ID number
    FARE_RULES_COLUMN_ORIGIN_ID_NUM         = "origin_id_num"
    #: fasttrips fare rules column name: Destination ID number
    FARE_RULES_COLUMN_DESTINATION_ID_NUM    = "destination_id_num"

    #: File with fasttrips fare transfer rules information.
    #: See `fare_transfer_rules specification <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_transfer_rules_ft.md>`_.
    INPUT_FARE_TRANSFER_RULES_FILE              = "fare_transfer_rules_ft.txt"
    #: fasttrips Fare transfer rules column name: From Fare Class
    FARE_TRANSFER_RULES_COLUMN_FROM_FARE_PERIOD = "from_fare_period"
    #: fasttrips Fare transfer rules column name: To Fare Class
    FARE_TRANSFER_RULES_COLUMN_TO_FARE_PERIOD   = "to_fare_period"
    #: fasttrips Fare transfer rules column name: Transfer type?
    FARE_TRANSFER_RULES_COLUMN_TYPE             = "transfer_fare_type"
    #: fasttrips Fare transfer rules column name: Transfer amount (discount or fare)
    FARE_TRANSFER_RULES_COLUMN_AMOUNT           = "transfer_fare"

    #: Value for :py:attr:`Route.FARE_TRANSFER_RULES_COLUMN_TYPE`: transfer discount
    TRANSFER_TYPE_TRANSFER_DISCOUNT = "transfer_discount"
    #: Value for :py:attr:`Route.FARE_TRANSFER_RULES_COLUMN_TYPE`: free transfer
    TRANSFER_TYPE_TRANSFER_FREE     = "transfer_free"
    #: Value for :py:attr:`Route.FARE_TRANSFER_RULES_COLUMN_TYPE`: transfer fare cost
    TRANSFER_TYPE_TRANSFER_COST     = "transfer_cost"

    #: Valid options for :py:attr:`Route.FARE_TRANSFER_RULES_COLUMN_TYPE`
    TRANSFER_TYPE_OPTIONS = [TRANSFER_TYPE_TRANSFER_DISCOUNT,
                             TRANSFER_TYPE_TRANSFER_FREE,
                             TRANSFER_TYPE_TRANSFER_COST]

    #: File with route ID, route ID number correspondence (and fare id num)
    OUTPUT_ROUTE_ID_NUM_FILE                    = "ft_intermediate_route_id.txt"
    #: File with fare id num, fare id, fare class, price, xfers
    OUTPUT_FARE_ID_FILE                         = "ft_intermediate_fare.txt"
    #: File with fare transfer rules
    OUTPUT_FARE_TRANSFER_FILE                   = "ft_intermediate_fare_transfers.txt"
    #: File with mode, mode number correspondence
    OUTPUT_MODE_NUM_FILE                        = "ft_intermediate_supply_mode_id.txt"

    def __init__(self, input_archive, output_dir, gtfs, today, stops):
        """
        Constructor.  Reads the gtfs data from the transitfeed schedule, and the additional
        fast-trips routes data from the input file in *input_archive*.
        """
        self.output_dir         = output_dir

        self.routes_df = gtfs.routes

        FastTripsLogger.info("Read %7d %15s from %25d %25s" %
                             (len(self.routes_df), 'date valid route', len(gtfs.routes), 'total routes'))

        # Read the fast-trips supplemental routes data file
        routes_ft_df = gtfs.get(Route.INPUT_ROUTES_FILE)

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
        self.routes_df = pd.merge(left=self.routes_df, right=routes_ft_df,
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

        self.agencies_df = gtfs.agency

        FastTripsLogger.debug("=========== AGENCIES ===========\n" + str(self.agencies_df.head()))
        FastTripsLogger.debug("\n"+str(self.agencies_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s" %
                             (len(self.agencies_df), "agencies", "agency.txt"))

        self.fare_attrs_df = gtfs.fare_attributes

        FastTripsLogger.debug("=========== FARE ATTRIBUTES ===========\n" + str(self.fare_attrs_df.head()))
        FastTripsLogger.debug("\n"+str(self.fare_attrs_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s" %
                             (len(self.fare_attrs_df), "fare attributes", "fare_attributes.txt"))

        # subsitute fasttrips fare attributes
        self.fare_attrs_df = gtfs.get(Route.INPUT_FARE_ATTRIBUTES_FILE)
        if not self.fare_attrs_df.empty:
            # verify required columns are present
            fare_attrs_cols = list(self.fare_attrs_df.columns.values)
            assert(Route.FARE_ATTR_COLUMN_FARE_PERIOD       in fare_attrs_cols)
            assert(Route.FARE_ATTR_COLUMN_PRICE             in fare_attrs_cols)
            assert(Route.FARE_ATTR_COLUMN_CURRENCY_TYPE     in fare_attrs_cols)
            assert(Route.FARE_ATTR_COLUMN_PAYMENT_METHOD    in fare_attrs_cols)
            assert(Route.FARE_ATTR_COLUMN_TRANSFERS         in fare_attrs_cols)

            if Route.FARE_ATTR_COLUMN_TRANSFER_DURATION not in fare_attrs_cols:
                self.fare_attrs_df[Route.FARE_ATTR_COLUMN_TRANSFER_DURATION] = np.nan

            FastTripsLogger.debug("===> REPLACED BY FARE ATTRIBUTES FT\n" + str(self.fare_attrs_df.head()))
            FastTripsLogger.debug("\n"+str(self.fare_attrs_df.dtypes))
            FastTripsLogger.info("Read %7d %15s from %25s" %
                                 (len(self.fare_attrs_df), "fare attributes", Route.INPUT_FARE_ATTRIBUTES_FILE))

            #: fares are by fare_period rather than by fare_id
            self.fare_by_class = True
        else:
            self.fare_by_class = False

        # Fare rules (map routes to fare_id)
        self.fare_rules_df = gtfs.fare_rules

        if len(self.fare_rules_df) > 0:
            self.fare_ids_df = Util.add_numeric_column(self.fare_rules_df[[Route.FARE_RULES_COLUMN_FARE_ID]],
                                                       id_colname=Route.FARE_RULES_COLUMN_FARE_ID,
                                                       numeric_newcolname=Route.FARE_RULES_COLUMN_FARE_ID_NUM)
            self.fare_rules_df = pd.merge(left  =self.fare_rules_df,
                                              right =self.fare_ids_df,
                                              how   ="left")
        else:
            self.fare_ids_df = pd.DataFrame()


        # optionally reverse those with origin/destinations if configured
        from .Assignment import Assignment
        if Assignment.FARE_ZONE_SYMMETRY:
            FastTripsLogger.debug("applying FARE_ZONE_SYMMETRY to %d fare rules" % len(self.fare_rules_df))
            # select only those with an origin and destination
            reverse_fare_rules = self.fare_rules_df.loc[ pd.notnull(self.fare_rules_df[Route.FARE_RULES_COLUMN_ORIGIN_ID])&
                                                         pd.notnull(self.fare_rules_df[Route.FARE_RULES_COLUMN_DESTINATION_ID]) ].copy()
            # FastTripsLogger.debug("reverse_fare_rules 1 head()=\n%s" % str(reverse_fare_rules.head()))

            # reverse them
            reverse_fare_rules.rename(columns={Route.FARE_RULES_COLUMN_ORIGIN_ID          : Route.FARE_RULES_COLUMN_DESTINATION_ID,
                                               Route.FARE_RULES_COLUMN_DESTINATION_ID     : Route.FARE_RULES_COLUMN_ORIGIN_ID},
                                      inplace=True)
            # FastTripsLogger.debug("reverse_fare_rules 2 head()=\n%s" % str(reverse_fare_rules.head()))

            # join them to eliminate dupes
            reverse_fare_rules = pd.merge(left     =reverse_fare_rules,
                                              right    =self.fare_rules_df,
                                              how      ="left",
                                              on       =[Route.FARE_RULES_COLUMN_FARE_ID,
                                                         Route.FARE_RULES_COLUMN_FARE_ID_NUM,
                                                         Route.FARE_RULES_COLUMN_ROUTE_ID,
                                                         Route.FARE_RULES_COLUMN_ORIGIN_ID,
                                                         Route.FARE_RULES_COLUMN_DESTINATION_ID,
                                                         Route.FARE_RULES_COLUMN_CONTAINS_ID],
                                              indicator=True)
            # dupes exist in both -- drop those
            reverse_fare_rules = reverse_fare_rules.loc[ reverse_fare_rules["_merge"]=="left_only"]
            reverse_fare_rules.drop(["_merge"], axis=1, inplace=True)

            # add them to fare rules
            self.fare_rules_df = pd.concat([self.fare_rules_df, reverse_fare_rules])
            FastTripsLogger.debug("fare rules with symmetry %d head()=\n%s" % (len(self.fare_rules_df), str(self.fare_rules_df.head())))

        # sort by fare ID num so zone-to-zone and their reverse are together
        if len(self.fare_rules_df) > 0:
            self.fare_rules_df.sort_values(by=[Route.FARE_RULES_COLUMN_FARE_ID_NUM], inplace=True)

        fare_rules_ft_df = gtfs.get(Route.INPUT_FARE_PERIODS_FILE)
        if not fare_rules_ft_df.empty:
            # verify required columns are present
            fare_rules_ft_cols = list(fare_rules_ft_df.columns.values)
            assert(Route.FARE_RULES_COLUMN_FARE_ID      in fare_rules_ft_cols)
            assert(Route.FARE_RULES_COLUMN_FARE_PERIOD  in fare_rules_ft_cols)
            assert(Route.FARE_RULES_COLUMN_START_TIME   in fare_rules_ft_cols)
            assert(Route.FARE_RULES_COLUMN_END_TIME     in fare_rules_ft_cols)

            # Split fare classes so they don't overlap
            fare_rules_ft_df = self.remove_fare_period_overlap(fare_rules_ft_df)

            # join to fare rules dataframe
            self.fare_rules_df = pd.merge(left=self.fare_rules_df, right=fare_rules_ft_df,
                                              how='left',
                                              on=Route.FARE_RULES_COLUMN_FARE_ID)

            # add route id numbering if applicable
            if Route.FARE_RULES_COLUMN_ROUTE_ID in list(self.fare_rules_df.columns.values):
                self.fare_rules_df = self.add_numeric_route_id(self.fare_rules_df,
                                                               Route.FARE_RULES_COLUMN_ROUTE_ID,
                                                               Route.FARE_RULES_COLUMN_ROUTE_ID_NUM)
            # add origin zone numbering if applicable
            if (Route.FARE_RULES_COLUMN_ORIGIN_ID in list(self.fare_rules_df.columns.values)) and \
               (pd.notnull(self.fare_rules_df[Route.FARE_RULES_COLUMN_ORIGIN_ID]).sum() > 0):
                self.fare_rules_df = stops.add_numeric_stop_zone_id(self.fare_rules_df,
                                                                    Route.FARE_RULES_COLUMN_ORIGIN_ID,
                                                                    Route.FARE_RULES_COLUMN_ORIGIN_ID_NUM)
            # add destination zone numbering if applicable
            if (Route.FARE_RULES_COLUMN_DESTINATION_ID in list(self.fare_rules_df.columns.values)) and \
                (pd.notnull(self.fare_rules_df[Route.FARE_RULES_COLUMN_DESTINATION_ID]).sum() > 0):
                self.fare_rules_df = stops.add_numeric_stop_zone_id(self.fare_rules_df,
                                                                    Route.FARE_RULES_COLUMN_DESTINATION_ID,
                                                                    Route.FARE_RULES_COLUMN_DESTINATION_ID_NUM)
                # They should both be present
                # This is unlikely
                if Route.FARE_RULES_COLUMN_ORIGIN_ID not in list(self.fare_rules_df.columns.values):
                    error_str = "Fast-trips only supports both origin_id and destination_id or neither in fare rules"
                    FastTripsLogger.fatal(error_str)
                    raise NotImplementedError(error_str)

                # check for each row, either both are present or neither -- use xor, or ^
                xor_id = self.fare_rules_df.loc[ pd.isnull(self.fare_rules_df[Route.FARE_RULES_COLUMN_ORIGIN_ID])^
                                                 pd.isnull(self.fare_rules_df[Route.FARE_RULES_COLUMN_DESTINATION_ID]) ]
                if len(xor_id) > 0:
                    error_str = "Fast-trips supports fare rules with both origin id and destination id specified, or neither ONLY.\n%s" % str(xor_id)
                    FastTripsLogger.fatal(error_str)
                    raise NotImplementedError(error_str)

            # We don't support contains_id
            if Route.FARE_RULES_COLUMN_CONTAINS_ID in list(self.fare_rules_df.columns.values):
                non_null_contains_id = self.fare_rules_df.loc[pd.notnull(self.fare_rules_df[Route.FARE_RULES_COLUMN_CONTAINS_ID])]
                if len(non_null_contains_id) > 0:
                    error_str = "Fast-trips does not support contains_id in fare rules:\n%s" % str(non_null_contains_id)
                    FastTripsLogger.fatal(error_str)
                    raise NotImplementedError(error_str)

            # We don't support rows with only one of origin_id or destination_id specified

        elif len(self.fare_rules_df) > 0:
            # we have fare rules but no fare periods -- make the fare periods the same
            self.fare_rules_df[Route.FARE_RULES_COLUMN_FARE_PERIOD] = self.fare_rules_df[Route.FARE_RULES_COLUMN_FARE_ID]
            self.fare_rules_df[Route.FARE_RULES_COLUMN_START_TIME] = Util.read_time("00:00:00")
            self.fare_rules_df[Route.FARE_RULES_COLUMN_END_TIME  ] = Util.read_time("24:00:00")

        # join to fare_attributes on fare_period if we have it, or fare_id if we don't
        if len(self.fare_rules_df) > 0:

            #: Fare ID/class (fare period)/attribute mapping.
            #:
            #: ===================  =====================================================================================================================================
            #:  Column name         Column Description
            #: ===================  =====================================================================================================================================
            #: `fare_id`            GTFS fare_id (See `fare_rules`_)
            #: `fare_id_num`        Numbered fare_id
            #: `route_id`           (optional) Route(s) associated with this fare ID. (See `fare_rules`_)
            #: `origin_id`          (optional) Origin fare zone ID(s) for fare ID. (See `fare_rules`_)
            #: `origin_id_num`      (optional) Origin fare zone number for fare ID.
            #: `destination_id`     (optional) Destination fare zone ID(s) for fare ID. (See `fare_rules`_)
            #: `destination_id_num` (optional) Destination fare zone number for fare ID.
            #: `contains_id`        (optional) Contains fare zone ID(s) for fare ID. (See `fare_rules`_)
            #: `fare_period`        GTFS-plus fare_period (See `fare_periods_ft`_)
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
            self.fare_rules_df = pd.merge(left =self.fare_rules_df,
                                              right=self.fare_attrs_df,
                                              how  ='left',
                                              on   = Route.FARE_RULES_COLUMN_FARE_PERIOD if self.fare_by_class else Route.FARE_RULES_COLUMN_FARE_ID)


        FastTripsLogger.debug("=========== FARE RULES ===========\n" + str(self.fare_rules_df.head(10).to_string(formatters=\
                              {Route.FARE_RULES_COLUMN_START_TIME:Util.datetime64_formatter,
                               Route.FARE_RULES_COLUMN_END_TIME  :Util.datetime64_formatter})))
        FastTripsLogger.debug("\n"+str(self.fare_rules_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s, %25s" %
                             (len(self.fare_rules_df), "fare rules", "fare_rules.txt", self.INPUT_FARE_PERIODS_FILE))
        self.fare_transfer_rules_df = gtfs.get(Route.INPUT_FARE_TRANSFER_RULES_FILE)
        if not self.fare_transfer_rules_df.empty:
            # verify required columns are present
            fare_transfer_rules_cols = list(self.fare_transfer_rules_df.columns.values)
            assert(Route.FARE_TRANSFER_RULES_COLUMN_FROM_FARE_PERIOD in fare_transfer_rules_cols)
            assert(Route.FARE_TRANSFER_RULES_COLUMN_TO_FARE_PERIOD   in fare_transfer_rules_cols)
            assert(Route.FARE_TRANSFER_RULES_COLUMN_TYPE             in fare_transfer_rules_cols)
            assert(Route.FARE_TRANSFER_RULES_COLUMN_AMOUNT           in fare_transfer_rules_cols)

            # verify valid values for transfer type
            invalid_type = self.fare_transfer_rules_df.loc[ self.fare_transfer_rules_df[Route.FARE_TRANSFER_RULES_COLUMN_TYPE].isin(Route.TRANSFER_TYPE_OPTIONS)==False ]
            if len(invalid_type) > 0:
                error_msg = "Invalid value for %s:\n%s" % (Route.FARE_TRANSFER_RULES_COLUMN_TYPE, str(invalid_type))
                FastTripsLogger.fatal(error_msg)
                raise NetworkInputError(Route.INPUT_FARE_TRANSFER_RULES_FILE, error_msg)

            # verify the amount is positive
            negative_amount = self.fare_transfer_rules_df.loc[ self.fare_transfer_rules_df[Route.FARE_TRANSFER_RULES_COLUMN_AMOUNT] < 0]
            if len(negative_amount) > 0:
                error_msg = "Negative transfer amounts are invalid:\n%s" % str(negative_amount)
                FastTripsLogger.fatal(error_msg)
                raise NetworkInputError(Route.INPUT_FARE_TRANSFER_RULES_FILE, error_msg)

            FastTripsLogger.debug("=========== FARE TRANSFER RULES ===========\n" + str(self.fare_transfer_rules_df.head()))
            FastTripsLogger.debug("\n"+str(self.fare_transfer_rules_df.dtypes))
            FastTripsLogger.info("Read %7d %15s from %25s" %
                                 (len(self.fare_transfer_rules_df), "fare xfer rules", Route.INPUT_FARE_TRANSFER_RULES_FILE))
        else:
            self.fare_transfer_rules_df = pd.DataFrame()

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
        implicit_modes_df = pd.DataFrame({Route.ROUTES_COLUMN_MODE_TYPE: [Route.MODE_TYPE_TRANSFER],
                                              Route.ROUTES_COLUMN_MODE:      [Route.MODE_TYPE_TRANSFER],
                                              Route.ROUTES_COLUMN_MODE_NUM:  [                       1]})

        self.modes_df = pd.concat([implicit_modes_df,
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

    def remove_fare_period_overlap(self, fare_rules_ft_df):
        """
        Split fare classes so they don't overlap
        """
        fare_rules_ft_df["fare_period_id"] = fare_rules_ft_df.index+1
        # FastTripsLogger.debug("remove_fare_period_overlap: initial\n%s" % fare_rules_ft_df)
        max_fare_period_id = fare_rules_ft_df["fare_period_id"].max()

        loop_iters = 0
        while True:
            # join with itself to see if any are contained
            df = pd.merge(left =fare_rules_ft_df,
                              right=fare_rules_ft_df,
                              on   =Route.FARE_RULES_COLUMN_FARE_ID,
                              how  ="outer")

            # if there's one fare period per fare id, nothing to do
            if len(df)==len(fare_rules_ft_df):
                FastTripsLogger.debug("One fare period per fare id, no need to split")
                return fare_rules_ft_df

            # remove dupes
            df = df.loc[ df["fare_period_id_x"] != df["fare_period_id_y"] ]

            FastTripsLogger.debug("remove_fare_period_overlap:\n%s" % df)

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
                raise NetworkInputError(Route.INPUT_FARE_PERIODS_FILE, error_msg)

            # is x a subset of y?
            #    ___x___            x starts after y starts
            # ______y_______        x ends before y ends
            subset_fare_periods = df.loc[ (df["start_time_x"]>=df["start_time_y"])& \
                                          (df["end_time_x"  ]<=df["end_time_y"]) ]
            # if no subsets, done -- return
            if len(subset_fare_periods) == 0:
                FastTripsLogger.debug("remove_fare_period_overlap returning\n%s" % fare_rules_ft_df)
                return fare_rules_ft_df

            # do one at a time -- split first into three rows
            FastTripsLogger.debug("splitting\n%s" % str(subset_fare_periods))
            row_dict = subset_fare_periods.head(1).to_dict(orient="records")[0]
            FastTripsLogger.debug(row_dict)
            y_1 = {'fare_id'          :row_dict['fare_id'],
                   'fare_period'      :row_dict['fare_period_y'],
                   'start_time'       :row_dict['start_time_y'],
                   'end_time'         :row_dict['start_time_x'],
                   'fare_period_id'   :row_dict['fare_period_id_y']}
            x   = {'fare_id'          :row_dict['fare_id'],
                   'fare_period'      :row_dict['fare_period_x'],
                   'start_time'       :row_dict['start_time_x'],
                   'end_time'         :row_dict['end_time_x'],
                   'fare_period_id'   :row_dict['fare_period_id_x']}
            y_2 = {'fare_id'          :row_dict['fare_id'],
                   'fare_period'      :row_dict['fare_period_y'],
                   'start_time'       :row_dict['end_time_x'],
                   'end_time'         :row_dict['end_time_y'],
                   'fare_period_id'   :max_fare_period_id+1} # new
            max_fare_period_id += 1

            new_df = pd.DataFrame([y_1,x,y_2])
            FastTripsLogger.debug("\n%s" % str(new_df))

            # put it together with the unaffected fare_periodes we already had
            prev_df = fare_rules_ft_df.loc[ (fare_rules_ft_df["fare_period_id"]!=row_dict["fare_period_id_x"])&
                                            (fare_rules_ft_df["fare_period_id"]!=row_dict["fare_period_id_y"]) ]
            fare_rules_ft_df = prev_df.append(new_df)

            # sort by fare_id, start_time
            fare_rules_ft_df.sort_values([Route.FARE_RULES_COLUMN_FARE_ID,
                                          Route.FARE_RULES_COLUMN_START_TIME], inplace=True)
            # reorder columns
            fare_rules_ft_df = fare_rules_ft_df[[Route.FARE_RULES_COLUMN_FARE_ID,
                                                 Route.FARE_RULES_COLUMN_FARE_PERIOD,
                                                 "fare_period_id",
                                                 Route.FARE_RULES_COLUMN_START_TIME,
                                                 Route.FARE_RULES_COLUMN_END_TIME]]
            FastTripsLogger.debug("\n%s" % str(fare_rules_ft_df))

            loop_iters += 1
            # don't loop forever -- there's a problem
            if loop_iters > 5:
                error_str = "Route.remove_fare_period_overlap looping too much!  Something is wrong."
                FastTripsLogger.critical(error_str)
                raise UnexpectedError(error_str)

        # this shouldn't happen
        FastTripsLogger.warn("This shouldn't happen")

    def write_routes_for_extension(self):
        """
        Write to an intermediate formatted file for the C++ extension.
        Since there are strings involved, it's easier than passing it to the extension.
        """
        from .Assignment import Assignment
        # write intermediate file -- route id num, route id
        self.route_id_df[[Route.ROUTES_COLUMN_ROUTE_ID_NUM, Route.ROUTES_COLUMN_ROUTE_ID]].to_csv(
            os.path.join(self.output_dir, Route.OUTPUT_ROUTE_ID_NUM_FILE), sep=" ", index=False)
        FastTripsLogger.debug("Wrote %s" % os.path.join(self.output_dir, Route.OUTPUT_ROUTE_ID_NUM_FILE))


        # write fare file
        if len(self.fare_rules_df) > 0:
            # copy for writing
            fare_rules_df = self.fare_rules_df.copy()

            # replace with float versions
            fare_rules_df[Route.FARE_RULES_COLUMN_START_TIME] = (fare_rules_df[Route.FARE_RULES_COLUMN_START_TIME] - Assignment.NETWORK_BUILD_DATE_START_TIME)/np.timedelta64(1,'m')
            fare_rules_df[Route.FARE_RULES_COLUMN_END_TIME  ] = (fare_rules_df[Route.FARE_RULES_COLUMN_END_TIME  ] - Assignment.NETWORK_BUILD_DATE_START_TIME)/np.timedelta64(1,'m')

            # fillna with -1
            for num_col in [Route.FARE_RULES_COLUMN_ROUTE_ID_NUM, Route.FARE_RULES_COLUMN_ORIGIN_ID_NUM, Route.FARE_RULES_COLUMN_DESTINATION_ID_NUM, Route.FARE_ATTR_COLUMN_TRANSFERS]:
                if num_col in list(fare_rules_df.columns.values):
                    fare_rules_df.loc[ pd.isnull(fare_rules_df[num_col]), num_col] = -1
                    fare_rules_df[num_col] = fare_rules_df[num_col].astype(int)
                else:
                    fare_rules_df[num_col] = -1

            # temp column: duraton; sort by this so the smallest duration is found first
            fare_rules_df["duration"] = fare_rules_df[Route.FARE_RULES_COLUMN_END_TIME  ] - fare_rules_df[Route.FARE_RULES_COLUMN_START_TIME]
            fare_rules_df.sort_values(by=[Route.FARE_RULES_COLUMN_FARE_ID_NUM,"duration"], ascending=True, inplace=True)

            # transfer_duration fillna with -1
            fare_rules_df.fillna({Route.FARE_ATTR_COLUMN_TRANSFER_DURATION:-1}, inplace=True)

            # File with fare id num, fare id, fare class, price, xfers
            fare_rules_df.to_csv(os.path.join(self.output_dir, Route.OUTPUT_FARE_ID_FILE),
                                columns=[Route.FARE_RULES_COLUMN_FARE_ID_NUM,
                                         Route.FARE_RULES_COLUMN_FARE_ID,
                                         Route.FARE_ATTR_COLUMN_FARE_PERIOD,
                                         Route.FARE_RULES_COLUMN_ROUTE_ID_NUM,
                                         Route.FARE_RULES_COLUMN_ORIGIN_ID_NUM,
                                         Route.FARE_RULES_COLUMN_DESTINATION_ID_NUM,
                                         Route.FARE_RULES_COLUMN_START_TIME,
                                         Route.FARE_RULES_COLUMN_END_TIME,
                                         Route.FARE_ATTR_COLUMN_PRICE,
                                         Route.FARE_ATTR_COLUMN_TRANSFERS,
                                         Route.FARE_ATTR_COLUMN_TRANSFER_DURATION],
                                sep=" ", index=False)
            FastTripsLogger.debug("Wrote %s" % os.path.join(self.output_dir, Route.OUTPUT_FARE_ID_FILE))

            if len(self.fare_transfer_rules_df) > 0:
                # File with fare transfer rules
                self.fare_transfer_rules_df.to_csv(os.path.join(self.output_dir, Route.OUTPUT_FARE_TRANSFER_FILE),
                                                   sep=" ", index=False)
                FastTripsLogger.debug("Wrote %s" % os.path.join(self.output_dir, Route.OUTPUT_FARE_TRANSFER_FILE))
        else:
            FastTripsLogger.debug("No fare rules so no file %s" % os.path.join(self.output_dir, Route.OUTPUT_FARE_ID_FILE))

    def add_fares(self, trip_links_df):
        """
        Adds (or replaces) fare columns to the given :py:class:`pandas.DataFrame`.

        New columns are

        * :py:attr:`Assignment.SIM_COL_PAX_FARE`
        * :py:attr:`Assignment.SIM_COL_PAX_FARE_PERIOD`
        * :py:attr:`Route.FARE_TRANSFER_RULES_COLUMN_FROM_FARE_PERIOD`
        * :py:attr:`Route.FARE_TRANSFER_RULES_COLUMN_TYPE`
        * :py:attr:`Route.FARE_TRANSFER_RULES_COLUMN_AMOUNT`
        * :py:attr:`Assignment.SIM_COL_PAX_FREE_TRANSFER`

        """
        FastTripsLogger.info("          Adding fares to pathset")

        from .Assignment import Assignment
        if Assignment.SIM_COL_PAX_FARE in list(trip_links_df.columns.values):
            trip_links_df.drop([Assignment.SIM_COL_PAX_FARE,
                                Assignment.SIM_COL_PAX_FARE_PERIOD,
                                Route.FARE_TRANSFER_RULES_COLUMN_FROM_FARE_PERIOD,
                                Route.FARE_TRANSFER_RULES_COLUMN_TYPE,
                                Route.FARE_TRANSFER_RULES_COLUMN_AMOUNT,
                                Assignment.SIM_COL_PAX_FREE_TRANSFER], axis=1, inplace=True)

        # no fares configured
        if len(self.fare_rules_df) == 0:
            trip_links_df[Assignment.SIM_COL_PAX_FARE                      ] = 0
            trip_links_df[Assignment.SIM_COL_PAX_FARE_PERIOD               ] = None
            trip_links_df[Route.FARE_TRANSFER_RULES_COLUMN_FROM_FARE_PERIOD] = None
            trip_links_df[Route.FARE_TRANSFER_RULES_COLUMN_TYPE            ] = None
            trip_links_df[Route.FARE_TRANSFER_RULES_COLUMN_AMOUNT          ] = None
            trip_links_df[Assignment.SIM_COL_PAX_FREE_TRANSFER             ] = None
            return trip_links_df

        orig_columns     = list(trip_links_df.columns.values)
        fare_columns     = [Assignment.SIM_COL_PAX_FARE,
                            Assignment.SIM_COL_PAX_FARE_PERIOD]
        transfer_columns = [Route.FARE_TRANSFER_RULES_COLUMN_FROM_FARE_PERIOD,
                            Route.FARE_TRANSFER_RULES_COLUMN_TYPE,
                            Route.FARE_TRANSFER_RULES_COLUMN_AMOUNT,
                            Assignment.SIM_COL_PAX_FREE_TRANSFER]

        # give them a unique index and store it for later
        trip_links_df.reset_index(drop=True, inplace=True)
        trip_links_df["trip_links_df index"] = trip_links_df.index

        num_trip_links = len(trip_links_df)
        FastTripsLogger.debug("add_fares initial trips (%d):\n%s" % (num_trip_links, str(trip_links_df.head(20))))
        FastTripsLogger.debug("add_fares initial fare_rules (%d):\n%s" % (len(self.fare_rules_df), str(self.fare_rules_df.head(20))))

        # initialize
        trip_links_unmatched = trip_links_df
        trip_links_matched   = pd.DataFrame()
        del trip_links_df

        from .Passenger import Passenger
        # level 0: match on all three
        fare_rules0 = self.fare_rules_df.loc[pd.notnull(self.fare_rules_df[Route.FARE_RULES_COLUMN_ROUTE_ID      ])&
                                             pd.notnull(self.fare_rules_df[Route.FARE_RULES_COLUMN_ORIGIN_ID     ])&
                                             pd.notnull(self.fare_rules_df[Route.FARE_RULES_COLUMN_DESTINATION_ID])]

        if len(fare_rules0) > 0:
            trip_links_match0 = pd.merge(left     =trip_links_unmatched,
                                             right    =fare_rules0,
                                             how      ="inner",
                                             left_on  =[Route.FARE_RULES_COLUMN_ROUTE_ID,"A_zone_id","B_zone_id"],
                                             right_on =[Route.FARE_RULES_COLUMN_ROUTE_ID,Route.FARE_RULES_COLUMN_ORIGIN_ID,Route.FARE_RULES_COLUMN_DESTINATION_ID],
                                             suffixes =["","_fare_rules"])

            # delete rows where the board time is not within the fare period
            trip_links_match0 = trip_links_match0.loc[ pd.isnull(trip_links_match0[Route.FARE_ATTR_COLUMN_PRICE])|
                                                      ((trip_links_match0[Assignment.SIM_COL_PAX_BOARD_TIME] >= trip_links_match0[Route.FARE_RULES_COLUMN_START_TIME])&
                                                       (trip_links_match0[Assignment.SIM_COL_PAX_BOARD_TIME] <  trip_links_match0[Route.FARE_RULES_COLUMN_END_TIME])) ]
            FastTripsLogger.debug("add_fares level 0 (%d):\n%s" % (len(trip_links_match0), str(trip_links_match0.head(20))))

            if len(trip_links_match0) > 0:

                # update matched and unmatched == they should be disjoint with union = whole
                trip_links_unmatched = pd.merge(left =trip_links_unmatched,
                                                    right=trip_links_match0[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                             Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                                             Passenger.PF_COL_PATH_NUM,
                                                                             Passenger.PF_COL_LINK_NUM]],
                                                    how  ="left",
                                                    on   =[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                           Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                           Passenger.PF_COL_PATH_NUM,
                                                           Passenger.PF_COL_LINK_NUM],
                                                    indicator=True)
                trip_links_unmatched = trip_links_unmatched.loc[ trip_links_unmatched["_merge"] == "left_only" ]
                trip_links_unmatched.drop(["_merge"], axis=1, inplace=True)

                trip_links_matched = pd.concat([trip_links_matched, trip_links_match0], axis=0, copy=False)
                FastTripsLogger.debug("matched: %d  unmatched: %d   total: %d" % (len(trip_links_matched), len(trip_links_unmatched), len(trip_links_matched)+len(trip_links_unmatched)))
                del trip_links_match0

                # TODO - Addding stop gap solution - if there are duplicates, drop them
                # but there's probably a better way to handle this, like flagging in input
                # See https://app.asana.com/0/15582794263969/319659099709517

                # trip_links_matched["dupe"] = trip_links_matched.duplicated(subset="trip_links_df index")
                # FastTripsLogger.debug("dupes: \n%s" % trip_links_matched.loc[trip_links_matched["dupe"]==True].sort_values(by="trip_links_df index"))
                trip_links_matched.drop_duplicates(subset="trip_links_df index", keep="first", inplace=True)

        # level 1: match on route only
        fare_rules1 = self.fare_rules_df.loc[pd.notnull(self.fare_rules_df[Route.FARE_RULES_COLUMN_ROUTE_ID      ])&
                                             pd.isnull (self.fare_rules_df[Route.FARE_RULES_COLUMN_ORIGIN_ID     ])&
                                             pd.isnull (self.fare_rules_df[Route.FARE_RULES_COLUMN_DESTINATION_ID])]
        if len(fare_rules1) > 0:
            trip_links_match1 = pd.merge(left     =trip_links_unmatched,
                                             right    =fare_rules1,
                                             how      ="inner",
                                             on       =Route.FARE_RULES_COLUMN_ROUTE_ID,
                                             suffixes =["","_fare_rules"])

            # delete rows where the board time is not within the fare period
            trip_links_match1 = trip_links_match1.loc[ pd.isnull(trip_links_match1[Route.FARE_ATTR_COLUMN_PRICE])|
                                                      ((trip_links_match1[Assignment.SIM_COL_PAX_BOARD_TIME] >= trip_links_match1[Route.FARE_RULES_COLUMN_START_TIME])&
                                                       (trip_links_match1[Assignment.SIM_COL_PAX_BOARD_TIME] <  trip_links_match1[Route.FARE_RULES_COLUMN_END_TIME])) ]
            FastTripsLogger.debug("add_fares level 1 (%d):\n%s" % (len(trip_links_match1), str(trip_links_match1.head())))

            if len(trip_links_match1) > 0:
                # update matched and unmatched == they should be disjoint with union = whole
                trip_links_unmatched = pd.merge(left =trip_links_unmatched,
                                                    right=trip_links_match1[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                             Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                                             Passenger.PF_COL_PATH_NUM,
                                                                             Passenger.PF_COL_LINK_NUM]],
                                                    how  ="left",
                                                    on   =[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                           Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                           Passenger.PF_COL_PATH_NUM,
                                                           Passenger.PF_COL_LINK_NUM],
                                                    indicator=True)
                trip_links_unmatched = trip_links_unmatched.loc[ trip_links_unmatched["_merge"] == "left_only" ]
                trip_links_unmatched.drop(["_merge"], axis=1, inplace=True)

                trip_links_matched = pd.concat([trip_links_matched, trip_links_match1], axis=0, copy=False)
                FastTripsLogger.debug("matched: %d  unmatched: %d   total: %d" % (len(trip_links_matched), len(trip_links_unmatched), len(trip_links_matched)+len(trip_links_unmatched)))
                del trip_links_match1

        # level 2: match on origin and destination zones only
        fare_rules2 = self.fare_rules_df.loc[pd.isnull (self.fare_rules_df[Route.FARE_RULES_COLUMN_ROUTE_ID      ])&
                                             pd.notnull(self.fare_rules_df[Route.FARE_RULES_COLUMN_ORIGIN_ID     ])&
                                             pd.notnull(self.fare_rules_df[Route.FARE_RULES_COLUMN_DESTINATION_ID])]
        if len(fare_rules2) > 0:
            trip_links_match2 = pd.merge(left     =trip_links_unmatched,
                                             right    =fare_rules2,
                                             how      ="inner",
                                             left_on  =["A_zone_id","B_zone_id"],
                                             right_on =[Route.FARE_RULES_COLUMN_ORIGIN_ID,Route.FARE_RULES_COLUMN_DESTINATION_ID],
                                             suffixes =["","_fare_rules"])

            # delete rows where the board time is not within the fare period
            trip_links_match2 = trip_links_match2.loc[ pd.isnull(trip_links_match2[Route.FARE_ATTR_COLUMN_PRICE])|
                                                      ((trip_links_match2[Assignment.SIM_COL_PAX_BOARD_TIME] >= trip_links_match2[Route.FARE_RULES_COLUMN_START_TIME])&
                                                       (trip_links_match2[Assignment.SIM_COL_PAX_BOARD_TIME] <  trip_links_match2[Route.FARE_RULES_COLUMN_END_TIME])) ]
            FastTripsLogger.debug("add_fares level 2 (%d):\n%s" % (len(trip_links_match2), str(trip_links_match2.head())))

            if len(trip_links_match2) > 0:
                # update matched and unmatched == they should be disjoint with union = whole
                trip_links_unmatched = pd.merge(left =trip_links_unmatched,
                                                    right=trip_links_match2[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                             Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                                             Passenger.PF_COL_PATH_NUM,
                                                                             Passenger.PF_COL_LINK_NUM]],
                                                    how  ="left",
                                                    on   =[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                           Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                           Passenger.PF_COL_PATH_NUM,
                                                           Passenger.PF_COL_LINK_NUM],
                                                    indicator=True)
                trip_links_unmatched = trip_links_unmatched.loc[ trip_links_unmatched["_merge"] == "left_only" ]
                trip_links_unmatched.drop(["_merge"], axis=1, inplace=True)

                trip_links_matched = pd.concat([trip_links_matched, trip_links_match2], axis=0, copy=False)
                FastTripsLogger.debug("matched: %d  unmatched: %d   total: %d" % (len(trip_links_matched), len(trip_links_unmatched), len(trip_links_matched)+len(trip_links_unmatched)))
                del trip_links_match2

        # level 3: no route, origin or destination specified
        fare_rules3 = self.fare_rules_df.loc[pd.isnull(self.fare_rules_df[Route.FARE_RULES_COLUMN_ROUTE_ID      ])&
                                             pd.isnull(self.fare_rules_df[Route.FARE_RULES_COLUMN_ORIGIN_ID     ])&
                                             pd.isnull(self.fare_rules_df[Route.FARE_RULES_COLUMN_DESTINATION_ID])].copy()
        if len(fare_rules3) > 0:
            # need a column to merge on
            merge_column = "fare level 3 merge col"
            fare_rules3[merge_column] = 1
            trip_links_unmatched[merge_column] = 1

            FastTripsLogger.debug("fare_rules3 (%d):\n%s" % (len(fare_rules3), str(fare_rules3.head())))

            trip_links_match3 = pd.merge(left     =trip_links_unmatched,
                                             right    =fare_rules3,
                                             how      ="inner",
                                             on       =merge_column,
                                             suffixes =["","_fare_rules"])

            trip_links_match3.drop([merge_column], axis=1, inplace=True)
            trip_links_unmatched.drop([merge_column], axis=1, inplace=True)

            # delete rows where the board time is not within the fare period
            trip_links_match3 = trip_links_match3.loc[ pd.isnull(trip_links_match3[Route.FARE_ATTR_COLUMN_PRICE])|
                                                      ((trip_links_match3[Assignment.SIM_COL_PAX_BOARD_TIME] >= trip_links_match3[Route.FARE_RULES_COLUMN_START_TIME])&
                                                       (trip_links_match3[Assignment.SIM_COL_PAX_BOARD_TIME] <  trip_links_match3[Route.FARE_RULES_COLUMN_END_TIME])) ]
            FastTripsLogger.debug("add_fares level 3 (%d):\n%s" % (len(trip_links_match3), str(trip_links_match3.head())))

            if len(trip_links_match3) > 0:
                # update matched and unmatched == they should be disjoint with union = whole
                trip_links_unmatched = pd.merge(left =trip_links_unmatched,
                                                    right=trip_links_match3[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                             Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                                             Passenger.PF_COL_PATH_NUM,
                                                                             Passenger.PF_COL_LINK_NUM]],
                                                    how  ="left",
                                                    on   =[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                           Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                           Passenger.PF_COL_PATH_NUM,
                                                           Passenger.PF_COL_LINK_NUM],
                                                    indicator=True)
                trip_links_unmatched = trip_links_unmatched.loc[ trip_links_unmatched["_merge"] == "left_only" ]
                trip_links_unmatched.drop(["_merge"], axis=1, inplace=True)

                trip_links_matched = pd.concat([trip_links_matched, trip_links_match3], axis=0, copy=False)
                FastTripsLogger.debug("matched: %d  unmatched: %d   total: %d" % (len(trip_links_matched), len(trip_links_unmatched), len(trip_links_matched)+len(trip_links_unmatched)))
                del trip_links_match3

        # put them together
        trip_links_df = pd.concat([trip_links_matched, trip_links_unmatched], axis=0, copy=False)
        trip_links_df.sort_values(by=[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
                                      Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                      Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                      Passenger.PF_COL_PATH_NUM,
                                      Passenger.PF_COL_LINK_NUM],
                                      inplace=True)
        del trip_links_matched
        del trip_links_unmatched

        # rename price to fare
        trip_links_df.rename(columns={Route.FARE_ATTR_COLUMN_PRICE:Assignment.SIM_COL_PAX_FARE}, inplace=True)

        # join fails mean 0
        trip_links_df.fillna(value={Assignment.SIM_COL_PAX_FARE:0.0}, inplace=True)

        # reorder columns
        trip_links_df = trip_links_df[orig_columns + fare_columns + [Route.FARE_ATTR_COLUMN_TRANSFERS, Route.FARE_ATTR_COLUMN_TRANSFER_DURATION]]

        FastTripsLogger.debug("trip_links_df (%d):\n%s" % (len(trip_links_df), str(trip_links_df.head())))

        # make sure we didn't lose or add any
        assert len(trip_links_df) == num_trip_links

        # apply fare transfers
        trip_links_df = self.apply_fare_transfer_rules(trip_links_df)

        trip_links_df = self.apply_free_transfers(trip_links_df)

        # drop other columns
        trip_links_df = trip_links_df[orig_columns + fare_columns + transfer_columns]

        return trip_links_df

    def apply_fare_transfer_rules(self, trip_links_df):
        """
        Applies fare transfers by attaching previous fare period.

        Adds (or replaces) columns :py:attr:`Route.FARE_TRANSFER_RULES_COLUMN_FROM_FARE_PERIOD`, :py:attr:`Route.FARE_TRANSFER_RULES_COLUMN_TYPE`
        and :py:attr:`Route.FARE_TRANSFER_RULES_COLUMN_AMOUNT` and adjusts the values in
        :py:attr:`Assignment.SIM_COL_PAX_FARE`.

        """
        from .Passenger import Passenger
        from .Assignment import Assignment

        if Route.FARE_TRANSFER_RULES_COLUMN_FROM_FARE_PERIOD in list(trip_links_df.columns.values):
            trip_links_df.drop([Route.FARE_TRANSFER_RULES_COLUMN_FROM_FARE_PERIOD,
                                Route.FARE_TRANSFER_RULES_COLUMN_TYPE,
                                Route.FARE_TRANSFER_RULES_COLUMN_AMOUNT], axis=1, inplace=True)

        # no transfer rules => nothing to do
        if len(self.fare_transfer_rules_df) == 0:
            trip_links_df[Route.FARE_TRANSFER_RULES_COLUMN_FROM_FARE_PERIOD] = None
            trip_links_df[Route.FARE_TRANSFER_RULES_COLUMN_TYPE]             = None
            trip_links_df[Route.FARE_TRANSFER_RULES_COLUMN_AMOUNT]           = None
            return trip_links_df

        # FastTripsLogger.debug("apply_fare_transfers (%d):\n%s" % (len(trip_links_df), str(trip_links_df.head(20))))

        # previous trip link
        trip_links_df["%s prev" % Passenger.PF_COL_LINK_NUM] = trip_links_df[Passenger.PF_COL_LINK_NUM] - 2
        trip_links_df = pd.merge(left    =trip_links_df,
                                     right   =trip_links_df[[Passenger.PERSONS_COLUMN_PERSON_ID,
                                                             Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                             Passenger.PF_COL_PATH_NUM,
                                                             Passenger.PF_COL_LINK_NUM,
                                                             Assignment.SIM_COL_PAX_FARE_PERIOD]],
                                     left_on =[Passenger.PERSONS_COLUMN_PERSON_ID,
                                               Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                               Passenger.PF_COL_PATH_NUM,
                                               "%s prev" % Passenger.PF_COL_LINK_NUM],
                                     right_on=[Passenger.PERSONS_COLUMN_PERSON_ID,
                                               Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                               Passenger.PF_COL_PATH_NUM,
                                               Passenger.PF_COL_LINK_NUM],
                                     suffixes=["","_prev"],
                                     how     ="left")
        # extra columns are linknum prev, linknum_prev, fare_prev, fare_period_prev,

        # join with transfers table
        trip_links_df = pd.merge(left    =trip_links_df,
                                     right   =self.fare_transfer_rules_df,
                                     left_on =["fare_period_prev","fare_period"],
                                     right_on=[Route.FARE_TRANSFER_RULES_COLUMN_FROM_FARE_PERIOD,
                                               Route.FARE_TRANSFER_RULES_COLUMN_TO_FARE_PERIOD],
                                     how     ="left")
        # FastTripsLogger.debug("apply_fare_transfers (%d):\n%s" % (len(trip_links_df), str(trip_links_df.head(20))))

        # keep Route.FARE_TRANSFER_RULES_COLUMN_FROM_FARE_PERIOD, Route.FARE_TRANSFER_RULES_COLUMN_TYPE, Route.FARE_TRANSFER_RULES_COLUMN_AMOUNT
        # so lose the rest
        trip_links_df.drop(["%s prev" % Passenger.PF_COL_LINK_NUM,
                            "%s_prev" % Passenger.PF_COL_LINK_NUM,
                            "%s_prev" % Assignment.SIM_COL_PAX_FARE_PERIOD,
                            Route.FARE_TRANSFER_RULES_COLUMN_TO_FARE_PERIOD], axis=1, inplace=True)
        # FastTripsLogger.debug("apply_fare_transfers (%d):\n%s" % (len(trip_links_df), str(trip_links_df.head(20))))

        # apply transfer discount
        trip_links_df.loc[ pd.notnull(trip_links_df[Route.FARE_TRANSFER_RULES_COLUMN_TYPE])&
                           (trip_links_df[Route.FARE_TRANSFER_RULES_COLUMN_TYPE]==Route.TRANSFER_TYPE_TRANSFER_DISCOUNT),
                           Assignment.SIM_COL_PAX_FARE ] = trip_links_df[Assignment.SIM_COL_PAX_FARE] - trip_links_df[Route.FARE_TRANSFER_RULES_COLUMN_AMOUNT]

        # apply transfer free
        trip_links_df.loc[ pd.notnull(trip_links_df[Route.FARE_TRANSFER_RULES_COLUMN_TYPE])&
                           (trip_links_df[Route.FARE_TRANSFER_RULES_COLUMN_TYPE]==Route.TRANSFER_TYPE_TRANSFER_FREE),
                           Assignment.SIM_COL_PAX_FARE ] = 0.0

        # apply transfer fare
        trip_links_df.loc[ pd.notnull(trip_links_df[Route.FARE_TRANSFER_RULES_COLUMN_TYPE])&
                           (trip_links_df[Route.FARE_TRANSFER_RULES_COLUMN_TYPE]==Route.TRANSFER_TYPE_TRANSFER_COST),
                           Assignment.SIM_COL_PAX_FARE ] = trip_links_df[Route.FARE_TRANSFER_RULES_COLUMN_AMOUNT]

        # make sure it's not negative
        trip_links_df.loc[ trip_links_df[Assignment.SIM_COL_PAX_FARE] < 0, Assignment.SIM_COL_PAX_FARE] = 0.0
        FastTripsLogger.debug("apply_fare_transfers (%d):\n%s" % (len(trip_links_df), str(trip_links_df.head(20))))

        return trip_links_df


    def apply_free_transfers(self, trip_links_df):
        """
        Apply the free transfers allowed in  to trip_links_df fare_attributes_ft.txt (configured by columns transfers, transfer_duration).
        Sets columns Assignment.SIM_COL_PAX_FREE_TRANSFER to None, 0.0 or 1.0
        """
        # free transfers within a fare id
        from .Assignment import Assignment
        from .Passenger  import Passenger
        from .PathSet    import PathSet

        # create a fare_index that counts up for a unique person-trip id, pathnum, and fare_period
        trip_links_df["fare_index"] = trip_links_df.groupby([Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                             Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                             Passenger.PF_COL_PATH_NUM,
                                                             Assignment.SIM_COL_PAX_FARE_PERIOD]).cumcount()
        trip_links_df.loc[ trip_links_df[Passenger.PF_COL_LINK_MODE]!=PathSet.STATE_MODE_TRIP, "fare_index"] = -1


        # transfer_time in seconds (to compare with transfer_duration) get the first fare board
        first_fare_board = trip_links_df[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                          Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                          Passenger.PF_COL_PATH_NUM,
                                          Passenger.PF_COL_LINK_NUM,
                                          Assignment.SIM_COL_PAX_FARE_PERIOD,
                                          "fare_index",
                                          Assignment.SIM_COL_PAX_BOARD_TIME]].loc[trip_links_df["fare_index"]==0]
        FastTripsLogger.debug("apply_free_transfers: first_fare_board=\n%s" % str(first_fare_board.head(10)))

        trip_links_df = pd.merge(left    =trip_links_df,
                                     right   =first_fare_board,
                                     on      =[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                               Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                               Passenger.PF_COL_PATH_NUM,
                                               Assignment.SIM_COL_PAX_FARE_PERIOD],
                                     how     ="left",
                                     suffixes=["","_ffb"])
        # calculate time from first board (for this fare period id) in seconds
        trip_links_df["transfer_time_sec"] = (trip_links_df[Assignment.SIM_COL_PAX_BOARD_TIME]-trip_links_df["%s_ffb" % Assignment.SIM_COL_PAX_BOARD_TIME])/np.timedelta64(1,'s')

        # FastTripsLogger.debug("apply_free_transfers: trip_links_df=\n%s" % str(trip_links_df.loc[ trip_links_df["transfer_time_sec"] >0 ]))

        trip_links_df[Assignment.SIM_COL_PAX_FREE_TRANSFER] = 0.0
        # free transfer if transfers > 0 and 0 < fare_index <= transfers
        trip_links_df.loc[ (trip_links_df[Route.FARE_ATTR_COLUMN_TRANSFERS] > 0)&                          # transfers > 0
                           (trip_links_df["fare_index"]>0)&                                                # is a transfer
                           (trip_links_df["fare_index"]<=trip_links_df[Route.FARE_ATTR_COLUMN_TRANSFERS]), # is within the number of free transfers allowed
                                    Assignment.SIM_COL_PAX_FREE_TRANSFER] = 1.0
        # only applicable to transit links
        trip_links_df.loc[ trip_links_df[Passenger.PF_COL_LINK_MODE]!=PathSet.STATE_MODE_TRIP,
                                    Assignment.SIM_COL_PAX_FREE_TRANSFER ] = None

        # only applicable if transfer is within transfer_duration -- revoke if transfer_time_sec > transfer duration
        trip_links_df.loc[ (trip_links_df[Assignment.SIM_COL_PAX_FREE_TRANSFER]==1.0) &
                           (trip_links_df["transfer_time_sec"] > trip_links_df[Route.FARE_ATTR_COLUMN_TRANSFER_DURATION]),
                                    Assignment.SIM_COL_PAX_FREE_TRANSFER] = 0.0

        # make the transfer free
        trip_links_df.loc[ trip_links_df[Assignment.SIM_COL_PAX_FREE_TRANSFER]==1.0, Assignment.SIM_COL_PAX_FARE] = 0.0

        # debug: show transfers within fare period
        FastTripsLogger.debug("apply_free_transfers: fare_index>0\n%s" % str(trip_links_df[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                                              Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                                                              Passenger.PF_COL_PATH_NUM,
                                                                                              Passenger.PF_COL_LINK_NUM,
                                                                                              Passenger.PF_COL_LINK_MODE,
                                                                                              Assignment.SIM_COL_PAX_BOARD_TIME,
                                                                                              Assignment.SIM_COL_PAX_FARE_PERIOD,
                                                                                              Route.FARE_ATTR_COLUMN_TRANSFERS,
                                                                                              Route.FARE_ATTR_COLUMN_TRANSFER_DURATION,"transfer_time_sec",
                                                                                              "fare_index",Assignment.SIM_COL_PAX_FREE_TRANSFER]].loc[trip_links_df["fare_index"] > 0].head(10)))
        # debug: show transfers within fare period
        FastTripsLogger.debug("apply_free_transfers: free_transfer=1.0\n%s" % str(trip_links_df[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                                              Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                                                              Passenger.PF_COL_PATH_NUM,
                                                                                              Passenger.PF_COL_LINK_NUM,
                                                                                              Passenger.PF_COL_LINK_MODE,
                                                                                              Assignment.SIM_COL_PAX_BOARD_TIME,
                                                                                              Assignment.SIM_COL_PAX_FARE_PERIOD,
                                                                                              Route.FARE_ATTR_COLUMN_TRANSFERS,
                                                                                              Route.FARE_ATTR_COLUMN_TRANSFER_DURATION,"transfer_time_sec",
                                                                                              "fare_index",Assignment.SIM_COL_PAX_FREE_TRANSFER]].loc[trip_links_df[Assignment.SIM_COL_PAX_FREE_TRANSFER] > 0].head(10)))



        # drop new columns
        trip_links_df.drop(["fare_index", "fare_index_ffb", "transfer_time_sec",
                            "%s_ffb" % Passenger.PF_COL_LINK_NUM,
                            "%s_ffb" % Assignment.SIM_COL_PAX_BOARD_TIME], axis=1, inplace=True)
        return trip_links_df
