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
import datetime
import os

import pandas as pd

from .Error  import NetworkInputError
from .Logger import FastTripsLogger


class Transfer:
    """
    Transfer class.

    One instance represents all of the Transfer links.

    Stores transfer link information in :py:attr:`Transfer.transfers_df`, an
    instance of :py:class:`pandas.DataFrame`.
    """
    #: File with fasttrips transfer information (this extends the
    #: `gtfs transfers <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/transfers.md>`_ file).
    #: See `transfers_ft specification <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/transfers_ft.md>`_.
    INPUT_TRANSFERS_FILE                    = "transfers_ft.txt"
    #: gtfs Transfers column name: Origin stop identifier
    TRANSFERS_COLUMN_FROM_STOP              = 'from_stop_id'
    #: gtfs Transfers column name: Destination stop identifier
    TRANSFERS_COLUMN_TO_STOP                = 'to_stop_id'
    #: gtfs Transfers column name: Transfer Type
    TRANSFERS_COLUMN_TRANSFER_TYPE          = 'transfer_type'
    #: gtfs Transfers column name: Minimum transfer time for transfer_type=2.  Float, seconds.
    TRANSFERS_COLUMN_MIN_TRANSFER_TIME      = 'min_transfer_time'
    #: fasttrips Transfers column name: Link walk distance, in miles. This is a float.
    TRANSFERS_COLUMN_DISTANCE               = 'dist'
    #: fasttrips Transfers column name: Origin route identifier
    TRANSFERS_COLUMN_FROM_ROUTE             = 'from_route_id'
    #: fasttrips Transfers column name: Destination route identifier
    TRANSFERS_COLUMN_TO_ROUTE               = 'to_route_id'
    #: fasttrips Transfers column name: Schedule precedence
    TRANSFERS_COLUMN_SCHEDULE_PRECEDENCE    = 'schedule_precedence'

     #: fasttrips Transfers column name: Elevation Gain, feet gained along link.  Integer.
    TRANSFERS_COLUMN_ELEVATION_GAIN         = 'elevation_gain'
     #: fasttrips Transfers column name: Population Density, people per square mile.  Float.
    TRANSFERS_COLUMN_POPULATION_DENSITY     = 'population_density'
     #: fasttrips Transfers column name: Retail Density, employees per square mile. Float.
    TRANSFERS_COLUMN_RETAIL_DENSITY         = 'retail_density'
     #: fasttrips Transfers column name: Auto Capacity, vehicles per hour per mile. Float.
    TRANSFERS_COLUMN_AUTO_CAPACITY          = 'auto_capacity'
     #: fasttrips Transfers column name: Indirectness, ratio of Manhattan distance to crow-fly distance. Float.
    TRANSFERS_COLUMN_INDIRECTNESS           = 'indirectness'

    # ========== Added by fasttrips =======================================================
    #: fasttrips Transfers column name: Is this a stop-to-stop transfer?  (e.g. from transfers.txt, and not involving a lot)
    TRANSFERS_COLUMN_STOP_TO_STOP           = "stop2stop"
    #: fasttrips Transfers column name: Origin Stop Numerical Identifier. Int.
    TRANSFERS_COLUMN_FROM_STOP_NUM          = 'from_stop_id_num'
    #: fasttrips Transfers column name: Destination Stop Numerical Identifier. Int.
    TRANSFERS_COLUMN_TO_STOP_NUM            = 'to_stop_id_num'
    #: gtfs Transfers column name: Minimum transfer time for transfer_type=2.  Float, min.
    TRANSFERS_COLUMN_MIN_TRANSFER_TIME_MIN  = 'min_transfer_time_min'

    #: Transfer walk speed, in miles per hour
    #:
    #: .. todo:: Make this configurable?
    #:
    WALK_SPEED_MILES_PER_HOUR  = 2.7

    #: Transfers column name: Link walk time.  This is a TimeDelta.
    #:
    #: .. todo:: Remove these?  Maybe weights should be distance based?  Walk speed is configured how?
    #:
    TRANSFERS_COLUMN_TIME       = 'time'
    #: Transfers column name: Link walk time in minutes.  This is a float.
    TRANSFERS_COLUMN_TIME_MIN   = 'time_min'
    #: Transfers column name: Link generic cost.  Float.
    TRANSFERS_COLUMN_PENALTY    = 'transfer_penalty'

    #: File with transfer links for C++ extension
    #: It's easier to pass it via file rather than through the
    #: initialize_fasttrips_extension() because of the strings involved
    OUTPUT_TRANSFERS_FILE       = "ft_intermediate_transfers.txt"

    def __init__(self, input_archive, output_dir, gtfs_feed):
        """
        Constructor.  Reads the gtfs data from the transitfeed schedule, and the additional
        fast-trips transfers data from the input files in *input_archive*.
        """
        self.output_dir       = output_dir

        # Combine all gtfs Transfer objects to a single pandas DataFrame
        self.transfers_df = gtfs_feed.transfers

        # make it zero if transfer_type != 2, since that's the only time it applies
        self.transfers_df.loc[self.transfers_df[Transfer.TRANSFERS_COLUMN_TRANSFER_TYPE] != 2, \
                              Transfer.TRANSFERS_COLUMN_MIN_TRANSFER_TIME] = 0

        # these are from transfers.txt so they don't involve lots
        self.transfers_df[Transfer.TRANSFERS_COLUMN_STOP_TO_STOP] = True

        # Read the fast-trips supplemental transfers data file
        transfers_ft_df = gtfs_feed.get(Transfer.INPUT_TRANSFERS_FILE)

        # verify required columns are present
        transfer_ft_cols = list(transfers_ft_df.columns.values)
        assert(Transfer.TRANSFERS_COLUMN_FROM_STOP           in transfer_ft_cols)
        assert(Transfer.TRANSFERS_COLUMN_TO_STOP             in transfer_ft_cols)
        assert(Transfer.TRANSFERS_COLUMN_DISTANCE            in transfer_ft_cols)

        # join to the transfers dataframe -- need to use the transfers_ft as the primary because
        # it may have PNR lot id to/from stop transfers (while gtfs transfers does not),
        # and we don't want to drop them
        if len(transfers_ft_df) > 0:
            self.transfers_df = pd.merge(left=self.transfers_df, right=transfers_ft_df,
                                             how='right',
                                             on=[Transfer.TRANSFERS_COLUMN_FROM_STOP,
                                                 Transfer.TRANSFERS_COLUMN_TO_STOP])

            # fill in NAN
            self.transfers_df.fillna(value={Transfer.TRANSFERS_COLUMN_MIN_TRANSFER_TIME:0,
                                            Transfer.TRANSFERS_COLUMN_TRANSFER_TYPE:0,
                                            Transfer.TRANSFERS_COLUMN_STOP_TO_STOP:False},
                                     inplace=True)

            if Transfer.TRANSFERS_COLUMN_FROM_ROUTE not in self.transfers_df.columns.values:
                self.transfers_df[Transfer.TRANSFERS_COLUMN_FROM_ROUTE] = None
            if Transfer.TRANSFERS_COLUMN_TO_ROUTE not in self.transfers_df.columns.values:
                self.transfers_df[Transfer.TRANSFERS_COLUMN_TO_ROUTE] = None

            # support BOTH TRANSFERS_COLUMN_FROM_ROUTE and TRANSFERS_COLUMN_TO_ROUTE but not one
            one_route_specified_df = self.transfers_df.loc[ self.transfers_df[Transfer.TRANSFERS_COLUMN_FROM_ROUTE].notnull()^
                                                            self.transfers_df[Transfer.TRANSFERS_COLUMN_TO_ROUTE].notnull() ]
            if len(one_route_specified_df):
                error_msg = "Only one of %s or %s specified for transfer: need both or neither:\n%s" % \
                    (Transfer.TRANSFERS_COLUMN_FROM_ROUTE, Transfer.TRANSFERS_COLUMN_TO_ROUTE,
                     str(one_route_specified_df))
                FastTripsLogger.fatal(error_msg)
                raise NetworkInputError(Transfer.INPUT_TRANSFERS_FILE, error_msg)

        # SPECIAL -- we rely on this in the extension
        self.transfers_df[Transfer.TRANSFERS_COLUMN_PENALTY] = 1.0

        FastTripsLogger.debug("=========== TRANSFERS ===========\n" + str(self.transfers_df.head()))
        FastTripsLogger.debug("\n"+str(self.transfers_df.dtypes))

        # TODO: this is to be consistent with original implementation. Remove?
        if len(self.transfers_df) > 0:

            self.transfers_df[Transfer.TRANSFERS_COLUMN_MIN_TRANSFER_TIME_MIN] = \
                self.transfers_df[Transfer.TRANSFERS_COLUMN_MIN_TRANSFER_TIME]/60.0

            # fill in null dist
            null_dist = self.transfers_df.loc[ self.transfers_df[Transfer.TRANSFERS_COLUMN_DISTANCE].isnull() ]
            if len(null_dist) > 0:
                FastTripsLogger.warn("Filling in %d transfers with null dist" % len(null_dist))
                self.transfers_df.loc[ self.transfers_df[Transfer.TRANSFERS_COLUMN_DISTANCE].isnull(),
                                        Transfer.TRANSFERS_COLUMN_DISTANCE ] = 0.0

            # transfer time is based on distance
            self.transfers_df[Transfer.TRANSFERS_COLUMN_TIME_MIN] = \
                self.transfers_df[Transfer.TRANSFERS_COLUMN_DISTANCE]*60.0/Transfer.WALK_SPEED_MILES_PER_HOUR

            # Sanity check transfer times.  A 13 hour-long walk transfer is suspicious.
            # TODO: make this less arbitrary?  It's based on the max SFCTA xfer link but it is too high
            too_long_transfers = self.transfers_df.loc[self.transfers_df[Transfer.TRANSFERS_COLUMN_TIME_MIN] > 780]
            if len(too_long_transfers) > 0:
                error_msg = "Found %d excessively long transfer links out of %d total transfer links. Expected distances are in miles. Unit problem?" % \
                            (len(too_long_transfers), len(self.transfers_df))
                FastTripsLogger.fatal(error_msg)
                FastTripsLogger.fatal("\n%s\n" % str(too_long_transfers.head()))
                raise NetworkInputError(Transfer.INPUT_TRANSFERS_FILE, error_msg)

            self.transfers_df.loc[\
                self.transfers_df[Transfer.TRANSFERS_COLUMN_TIME_MIN] < self.transfers_df[Transfer.TRANSFERS_COLUMN_MIN_TRANSFER_TIME_MIN], \
                Transfer.TRANSFERS_COLUMN_TIME_MIN] = self.transfers_df[Transfer.TRANSFERS_COLUMN_MIN_TRANSFER_TIME_MIN]

            # convert time column from float to timedelta
            self.transfers_df[Transfer.TRANSFERS_COLUMN_TIME] = \
                self.transfers_df[Transfer.TRANSFERS_COLUMN_TIME_MIN].map(lambda x: datetime.timedelta(minutes=x))

        FastTripsLogger.debug("Final\n"+str(self.transfers_df))
        FastTripsLogger.debug("\n"+str(self.transfers_df.dtypes))

        FastTripsLogger.info("Read %7d %15s from %25s, %25s" %
                             (len(self.transfers_df), "transfers", "transfers.txt", Transfer.INPUT_TRANSFERS_FILE))

    def add_numeric_stop_id(self, stops):
        """
        Stops are now equipped to add numeric ID (DAPs are in) so grab them
        """

        # Add the numeric stop ids to transfers
        if len(self.transfers_df) > 0:
            self.transfers_df = stops.add_numeric_stop_id(self.transfers_df,
                                                         id_colname=Transfer.TRANSFERS_COLUMN_FROM_STOP,
                                                         numeric_newcolname=Transfer.TRANSFERS_COLUMN_FROM_STOP_NUM,
                                                         warn=True,
                                                         warn_msg="Numeric stop id not found for transfer from_stop_id")
            self.transfers_df = stops.add_numeric_stop_id(self.transfers_df,
                                                         id_colname=Transfer.TRANSFERS_COLUMN_TO_STOP,
                                                         numeric_newcolname=Transfer.TRANSFERS_COLUMN_TO_STOP_NUM,
                                                         warn=True,
                                                         warn_msg="Numeric stop id not found for transfer to_stop_id")
            # We're ready to write it
            self.write_transfers_for_extension()

    def add_transfer_attributes(self, transfer_links_df, all_links_df):
        """
        Adds transfer attributes for transfer links and returns those transfer links with the additional columns.

        Pass all_links_df in order to get the from_route_id and to_route_id for the transfers.
        """
        from .Passenger import Passenger

        len_transfer_links_df = len(transfer_links_df)
        transfer_links_cols   = list(transfer_links_df.columns.values)
        FastTripsLogger.debug("add_transfer_attributes: transfer_links_df(%d) head(20)=\n%s\ntransfers_df head(20)=\n%s" % \
                              (len_transfer_links_df, transfer_links_df.head(20).to_string(), self.transfers_df.head(20).to_string()))

        # nothing to do
        if len_transfer_links_df == 0:
            return transfer_links_df
        if len(self.transfers_df) == 0:
            return transfer_links_df

        # these will be filled for route matches
        transfer_links_done = pd.DataFrame()

        # match on both from route and to route
        if Transfer.TRANSFERS_COLUMN_FROM_ROUTE not in self.transfers_df.columns.values:
            transfers_with_routes_df = pd.DataFrame()
            transfers_wo_routes_df   = self.transfers_df
        else:
            transfers_with_routes_df = self.transfers_df.loc[ self.transfers_df[Transfer.TRANSFERS_COLUMN_FROM_ROUTE].notnull() ]
            transfers_wo_routes_df   = self.transfers_df.loc[ self.transfers_df[Transfer.TRANSFERS_COLUMN_FROM_ROUTE].isnull()  ]

        FastTripsLogger.debug("add_transfer_attributes: have %d transfers with routes and %d transfers without routes" % \
                              (len(transfers_with_routes_df), len(transfers_wo_routes_df)))

        if len(transfers_with_routes_df) > 0:
            # this is what we need of the trips
            trip_links_df = all_links_df.loc[ all_links_df[Passenger.PF_COL_ROUTE_ID].notnull(),
                                               [Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                Passenger.PF_COL_PATH_NUM,
                                                Passenger.PF_COL_LINK_NUM,
                                                Passenger.PF_COL_ROUTE_ID]
                                            ]
            # FastTripsLogger.debug("trip_links_df head(20)=\n%s" % trip_links_df.head().to_string())

            # match transfer with trip's next link to get from route_id
            trip_links_df["next_link_num"] = trip_links_df[Passenger.PF_COL_LINK_NUM] + 1
            transfer_links_df = pd.merge(left      =transfer_links_df,
                                             left_on   =[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                         Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                         Passenger.PF_COL_PATH_NUM,
                                                         Passenger.PF_COL_LINK_NUM],
                                             right     =trip_links_df[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                       Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                                       Passenger.PF_COL_PATH_NUM,
                                                                       "next_link_num",
                                                                       Passenger.PF_COL_ROUTE_ID]],
                                             right_on  =[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                         Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                         Passenger.PF_COL_PATH_NUM,
                                                         "next_link_num"],
                                             suffixes=[""," from"],
                                             how="left")
            transfer_links_df.rename(columns={"%s from" % Passenger.PF_COL_ROUTE_ID:Transfer.TRANSFERS_COLUMN_FROM_ROUTE}, inplace=True)

            # match transfer with trip's prev link to get to route_id
            trip_links_df["prev_link_num"] = trip_links_df[Passenger.PF_COL_LINK_NUM] - 1
            transfer_links_df = pd.merge(left      =transfer_links_df,
                                             left_on   =[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                         Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                         Passenger.PF_COL_PATH_NUM,
                                                         Passenger.PF_COL_LINK_NUM],
                                             right     =trip_links_df[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                       Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                                       Passenger.PF_COL_PATH_NUM,
                                                                       "prev_link_num",
                                                                       Passenger.PF_COL_ROUTE_ID]],
                                             right_on  =[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                         Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                         Passenger.PF_COL_PATH_NUM,
                                                         "prev_link_num"],
                                             suffixes=[""," to"],
                                             how="left")
            transfer_links_df.rename(columns={"%s to" % Passenger.PF_COL_ROUTE_ID:Transfer.TRANSFERS_COLUMN_TO_ROUTE}, inplace=True)
            transfer_links_df.drop(["prev_link_num","next_link_num"], axis=1, inplace=True)

            # FastTripsLogger.debug("transfer_links_df after adding route info:\n%s" % transfer_links_df.head(20).to_string())

            # match on transfer attributes
            transfer_links_df = pd.merge(left     =transfer_links_df,
                                             left_on  =["A_id_num","B_id_num",
                                                        Transfer.TRANSFERS_COLUMN_FROM_ROUTE,
                                                        Transfer.TRANSFERS_COLUMN_TO_ROUTE],
                                             right    =transfers_with_routes_df,
                                             right_on =[Transfer.TRANSFERS_COLUMN_FROM_STOP_NUM,
                                                        Transfer.TRANSFERS_COLUMN_TO_STOP_NUM,
                                                        Transfer.TRANSFERS_COLUMN_FROM_ROUTE,
                                                        Transfer.TRANSFERS_COLUMN_TO_ROUTE],
                                             how      ="left",
                                             indicator=True)
            # FastTripsLogger.debug("transfer_links_df _merge: \n%s" % str(transfer_links_df["_merge"].value_counts()))
            transfer_links_df.drop([Transfer.TRANSFERS_COLUMN_FROM_STOP_NUM,Transfer.TRANSFERS_COLUMN_TO_STOP_NUM], axis=1, inplace=True)

            # now some of these have attributes, some still need
            transfer_links_done = transfer_links_df.loc[transfer_links_df["_merge"]=="both"].copy()
            transfer_links_done.drop(["_merge"], axis=1, inplace=True)

            # select remainder and reset to original columns
            transfer_links_df = transfer_links_df.loc[ transfer_links_df["_merge"]=="left_only", transfer_links_cols ]

            # FastTripsLogger.debug("transfer_links_df split into %d done:\n%s" % (len(transfer_links_done), transfer_links_done.head(20).to_string()))
            # FastTripsLogger.debug("transfer_links_df split into %d not done:\n%s" % (len(transfer_links_df), transfer_links_df.head(20).to_string()))

        # match on both from stops ONLY
        if len(transfers_wo_routes_df) > 0:
            transfer_links_df = pd.merge(left     =transfer_links_df,
                                             left_on  =["A_id_num","B_id_num"],
                                             right    =transfers_wo_routes_df,
                                             right_on =[Transfer.TRANSFERS_COLUMN_FROM_STOP_NUM,
                                                        Transfer.TRANSFERS_COLUMN_TO_STOP_NUM],
                                             how      ="left")
            transfer_links_df.drop([Transfer.TRANSFERS_COLUMN_FROM_STOP_NUM,Transfer.TRANSFERS_COLUMN_TO_STOP_NUM], axis=1, inplace=True)

        # put the two parts back together
        if len(transfer_links_done) > 0:
            transfer_links_df = pd.concat([transfer_links_df, transfer_links_done], axis=0, ignore_index=True)

        # make sure we didn't lose anything
        assert(len_transfer_links_df == len(transfer_links_df))

        return transfer_links_df

    def write_transfers_for_extension(self):
        """
        This writes to an intermediate file a formatted file for the C++ extension.
        Since there are strings involved, it's easier than passing it to the extension.

        Only write the stop/stop transfers since lot/stop transfers are only used for creating drive access links.
        """
        transfers_df = self.transfers_df.loc[self.transfers_df[Transfer.TRANSFERS_COLUMN_STOP_TO_STOP]==True].copy()

        # drop transfer_type==3 => that means no transfer possible
        # https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/transfers.md
        transfers_df = transfers_df.loc[transfers_df[Transfer.TRANSFERS_COLUMN_TRANSFER_TYPE] != 3]

        # drop some of the attributes
        drop_attrs = [Transfer.TRANSFERS_COLUMN_TIME,                # use numerical version
                      Transfer.TRANSFERS_COLUMN_FROM_STOP,           # use numerical version
                      Transfer.TRANSFERS_COLUMN_TO_STOP,             # use numerical version
                      Transfer.TRANSFERS_COLUMN_MIN_TRANSFER_TIME,   # minute version is sufficient
                      Transfer.TRANSFERS_COLUMN_SCHEDULE_PRECEDENCE, # don't know what to do with this
                      Transfer.TRANSFERS_COLUMN_STOP_TO_STOP,        # not needed
                      Transfer.TRANSFERS_COLUMN_FROM_ROUTE,          # TODO?
                      Transfer.TRANSFERS_COLUMN_TO_ROUTE             # TODO?
                     ]
        keep_attrs = set(list(transfers_df.columns.values)) - set(drop_attrs)
        transfers_df = transfers_df[list(keep_attrs)]

        # transfers time_min is really walk_time_min
        transfers_df["walk_time_min"] = transfers_df[Transfer.TRANSFERS_COLUMN_TIME_MIN]

        # the index is from stop id num, to stop id num
        transfers_df.set_index([Transfer.TRANSFERS_COLUMN_FROM_STOP_NUM,
                                Transfer.TRANSFERS_COLUMN_TO_STOP_NUM], inplace=True)
        # this will make it so beyond from stop num and to stop num,
        # the remaining columns collapse to variable name, variable value
        transfers_df = transfers_df.stack().reset_index()
        transfers_df.rename(columns={"level_2":"attr_name", 0:"attr_value"}, inplace=True)

        transfers_df.to_csv(os.path.join(self.output_dir, Transfer.OUTPUT_TRANSFERS_FILE),
                            sep=" ", index=False)
        FastTripsLogger.debug("Wrote %s" % os.path.join(self.output_dir, Transfer.OUTPUT_TRANSFERS_FILE))
