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
import datetime,os,sys
import pandas

from .Logger import FastTripsLogger
from .Stop   import Stop

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
    #: fasttrips Stops column name: Origin Stop Numerical Identifier. Int.
    TRANSFERS_COLUMN_FROM_STOP_NUM          = 'from_stop_id_num'
    #: fasttrips Stops column name: Destination Stop Numerical Identifier. Int.
    TRANSFERS_COLUMN_TO_STOP_NUM            = 'to_stop_id_num'
    #: gtfs Transfers column name: Minimum transfer time for transfer_type=2.  Float, min.
    TRANSFERS_COLUMN_MIN_TRANSFER_TIME_MIN  = 'min_transfer_time_min'

    #: Transfers column name: Link walk time.  This is a TimeDelta.
    #:
    #: .. todo:: Remove these?  Maybe weights should be distance based?  Walk speed is configured how?
    #:
    TRANSFERS_COLUMN_TIME       = 'time'
    #: Transfers column name: Link walk time in minutes.  This is a float.
    TRANSFERS_COLUMN_TIME_MIN   = 'time_min'
    #: Transfers column name: Link generic cost.  Float.

    #: File with transfer links for C++ extension
    #: It's easier to pass it via file rather than through the
    #: initialize_fasttrips_extension() because of the strings involved
    OUTPUT_TRANSFERS_FILE       = "ft_intermediate_transfers.txt"

    def __init__(self, input_dir, output_dir, gtfs_schedule, is_child_process):
        """
        Constructor.  Reads the gtfs data from the transitfeed schedule, and the additional
        fast-trips transfers data from the input files in *input_dir*.
        """
        self.output_dir       = output_dir
        self.is_child_process = is_child_process

        # Combine all gtfs Transfer objects to a single pandas DataFrame
        transfer_dicts = []
        for gtfs_transfer in gtfs_schedule.GetTransferList():
            transfer_dict = {}
            for fieldname in gtfs_transfer._FIELD_NAMES:
                if fieldname in gtfs_transfer.__dict__:
                    transfer_dict[fieldname] = gtfs_transfer.__dict__[fieldname]
            transfer_dicts.append(transfer_dict)
        if len(transfer_dicts) > 0:
            self.transfers_df = pandas.DataFrame(data=transfer_dicts)

            # these are strings - empty string should mean 0 min transfer time
            self.transfers_df.replace(to_replace={Transfer.TRANSFERS_COLUMN_MIN_TRANSFER_TIME:{"":"0"}},
                                      inplace=True)
            # make it numerical
            self.transfers_df[Transfer.TRANSFERS_COLUMN_MIN_TRANSFER_TIME] = \
                self.transfers_df[Transfer.TRANSFERS_COLUMN_MIN_TRANSFER_TIME].astype(float)

            # make it zero if transfer_type != 2, since that's the only time it applies
            self.transfers_df.loc[self.transfers_df[Transfer.TRANSFERS_COLUMN_TRANSFER_TYPE] != 2, \
                                  Transfer.TRANSFERS_COLUMN_MIN_TRANSFER_TIME] = 0

        else:
            self.transfers_df = pandas.DataFrame(columns=[Transfer.TRANSFERS_COLUMN_FROM_STOP,
                                                          Transfer.TRANSFERS_COLUMN_FROM_STOP_NUM,
                                                          Transfer.TRANSFERS_COLUMN_TO_STOP,
                                                          Transfer.TRANSFERS_COLUMN_TO_STOP_NUM,
                                                          Transfer.TRANSFERS_COLUMN_TIME,
                                                          Transfer.TRANSFERS_COLUMN_TIME_MIN])

        # Read the fast-trips supplemental transfers data file
        transfers_ft_df = pandas.read_csv(os.path.join(input_dir, Transfer.INPUT_TRANSFERS_FILE), 
                                          dtype={Transfer.TRANSFERS_COLUMN_FROM_STOP:object, Transfer.TRANSFERS_COLUMN_TO_STOP:object})
        # verify required columns are present
        transfer_ft_cols = list(transfers_ft_df.columns.values)
        assert(Transfer.TRANSFERS_COLUMN_FROM_STOP           in transfer_ft_cols)
        assert(Transfer.TRANSFERS_COLUMN_TO_STOP             in transfer_ft_cols)
        assert(Transfer.TRANSFERS_COLUMN_DISTANCE            in transfer_ft_cols)
        assert(Transfer.TRANSFERS_COLUMN_FROM_ROUTE          in transfer_ft_cols)
        assert(Transfer.TRANSFERS_COLUMN_TO_ROUTE            in transfer_ft_cols)
        assert(Transfer.TRANSFERS_COLUMN_SCHEDULE_PRECEDENCE in transfer_ft_cols)

        # join to the transfers dataframe -- need to use the transfers_ft as the primary because
        # it may have PNR lot id to/from stop transfers (while gtfs transfers does not),
        # and we don't want to drop them
        if len(transfers_ft_df) > 0:
            self.transfers_df = pandas.merge(left=self.transfers_df, right=transfers_ft_df,
                                             how='right',
                                             on=[Transfer.TRANSFERS_COLUMN_FROM_STOP,
                                                 Transfer.TRANSFERS_COLUMN_TO_STOP])

            # fill in NAN
            self.transfers_df.fillna(value={Transfer.TRANSFERS_COLUMN_MIN_TRANSFER_TIME:0,
                                            Transfer.TRANSFERS_COLUMN_TRANSFER_TYPE:0},
                                     inplace=True)

        FastTripsLogger.debug("=========== TRANSFERS ===========\n" + str(self.transfers_df.head()))
        FastTripsLogger.debug("\n"+str(self.transfers_df.dtypes))

        # TODO: this is to be consistent with original implementation. Remove?
        if len(self.transfers_df) > 0:
            self.transfers_df[Transfer.TRANSFERS_COLUMN_MIN_TRANSFER_TIME_MIN] = \
                self.transfers_df[Transfer.TRANSFERS_COLUMN_MIN_TRANSFER_TIME]/60.0

            # transfer time is based on distance
            self.transfers_df[Transfer.TRANSFERS_COLUMN_TIME_MIN] = \
                self.transfers_df[Transfer.TRANSFERS_COLUMN_DISTANCE]*60.0/3.0

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
                                                         numeric_newcolname=Transfer.TRANSFERS_COLUMN_FROM_STOP_NUM)
            self.transfers_df = stops.add_numeric_stop_id(self.transfers_df,
                                                         id_colname=Transfer.TRANSFERS_COLUMN_TO_STOP,
                                                         numeric_newcolname=Transfer.TRANSFERS_COLUMN_TO_STOP_NUM)
            # We're ready to write it
            if not self.is_child_process:
                self.write_transfers_for_extension()

    def write_transfers_for_extension(self):
        """
        This writes to an intermediate file a formatted file for the C++ extension.
        Since there are strings involved, it's easier than passing it to the extension.
        """
        transfers_df = self.transfers_df.copy()

        # drop transfer_type==3 => that means no transfer possible
        # https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/transfers.md
        transfers_df = transfers_df.loc[transfers_df[Transfer.TRANSFERS_COLUMN_TRANSFER_TYPE] != 3]

        # drop some of the attributes
        transfers_df.drop([Transfer.TRANSFERS_COLUMN_TIME,                # use numerical version
                           Transfer.TRANSFERS_COLUMN_FROM_STOP,           # use numerical version
                           Transfer.TRANSFERS_COLUMN_TO_STOP,             # use numerical version
                           Transfer.TRANSFERS_COLUMN_MIN_TRANSFER_TIME,   # minute version is sufficient
                           Transfer.TRANSFERS_COLUMN_SCHEDULE_PRECEDENCE, # don't know what to do with this
                           Transfer.TRANSFERS_COLUMN_FROM_ROUTE,          # TODO?
                           Transfer.TRANSFERS_COLUMN_TO_ROUTE             # TODO?
                          ], axis=1, inplace=True)

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