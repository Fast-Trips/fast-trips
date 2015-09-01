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
import collections,datetime,os,sys
import pandas

from .Logger import FastTripsLogger
from .Trip import Trip

class Stop:
    """
    Stop class.
    
    One instance represents all of the Stops as well as their transfer links.
    
    Stores stop information in :py:attr:`Stop.stops_df`, an instance of :py:class:`pandas.DataFrame`
    and transfer link information in :py:attr:`Stop.transfers_df`, another instance of
    :py:class:`pandas.DataFrame`.
    """

    #: File with stops.
    #: This is a tab-delimited file with required columns specified by
    #: :py:attr:`Stop.STOPS_COLUMN_ID`, :py:attr:`Stop.STOPS_COLUMN_NAME`
    #: :py:attr:`Stop.STOPS_COLUMN_DESCRIPTION`,
    #: :py:attr:`Stop.STOPS_COLUMN_LATITUDE`, :py:attr:`Stop.STOPS_COLUMN_LONGITUDE`
    #: :py:attr:`Stop.STOPS_COLUMN_CAPACITY`
    INPUT_STOPS_FILE            = "ft_input_stops.dat"
    #: Stops column name: Unique identifier
    STOPS_COLUMN_ID             = 'stopId'
    #: Stops column name: Stop name (string)
    STOPS_COLUMN_NAME           = 'stopName'
    #: Stops column name: Stop description (string)
    STOPS_COLUMN_DESCRIPTION    = 'stopDescription'
    #: Stops column name: Latitude
    STOPS_COLUMN_LATITUDE       = 'Latitude'
    #: Stops column name: Longitude
    STOPS_COLUMN_LONGITUDE      = 'Longitude'
    #: Stops column name: Capacity
    STOPS_COLUMN_CAPACITY       = 'capacity'
    
    #: File with transfers.
    INPUT_TRANSFERS_FILE        = "ft_input_transfers.dat"
    #: Transfers column name: Origin stop identifier
    TRANSFERS_COLUMN_FROM_STOP  = 'fromStop'
    #: Transfers column name: Destination stop identifier
    TRANSFERS_COLUMN_TO_STOP    = 'toStop'
    #: Transfers column name: Link walk distance
    TRANSFERS_COLUMN_DISTANCE   = 'dist'
    #: Transfers column name: Link walk time.  This is a TimeDelta.
    TRANSFERS_COLUMN_TIME       = 'time'
    #: Transfers column name: Link walk time in minutes.  This is a float.
    TRANSFERS_COLUMN_TIME_MIN   = 'time_min'
    #: Transfers column name: Link generic cost.  Float.
    TRANSFERS_COLUMN_COST       = 'cost'

    def __init__(self, input_dir):
        """
        Constructor.  Reads the Stops data from the input files in *input_dir*.
        """
        pandas.set_option('display.width', 1000)
        self.stops_df = pandas.read_csv(os.path.join(input_dir, Stop.INPUT_STOPS_FILE), sep="\t")
        # verify required columns are present
        stops_cols = list(self.stops_df.columns.values)
        assert(Stop.STOPS_COLUMN_ID             in stops_cols)
        assert(Stop.STOPS_COLUMN_NAME           in stops_cols)
        assert(Stop.STOPS_COLUMN_DESCRIPTION    in stops_cols)
        assert(Stop.STOPS_COLUMN_LATITUDE       in stops_cols)
        assert(Stop.STOPS_COLUMN_LONGITUDE      in stops_cols)
        assert(Stop.STOPS_COLUMN_CAPACITY       in stops_cols)
        self.stops_df.set_index(Stop.STOPS_COLUMN_ID, inplace=True, verify_integrity=True)

        FastTripsLogger.debug("=========== STOPS ===========\n" + str(self.stops_df.head()))
        FastTripsLogger.debug("\n"+str(self.stops_df.index.dtype)+"\n"+str(self.stops_df.dtypes))
        FastTripsLogger.info("Read %7d stops" % len(self.stops_df))

        self.transfers_df = pandas.read_csv(os.path.join(input_dir, Stop.INPUT_TRANSFERS_FILE), sep="\t")
        # verify required columns are present
        transfer_cols = list(self.transfers_df.columns.values)
        assert(Stop.TRANSFERS_COLUMN_FROM_STOP  in transfer_cols)
        assert(Stop.TRANSFERS_COLUMN_TO_STOP    in transfer_cols)
        assert(Stop.TRANSFERS_COLUMN_DISTANCE   in transfer_cols)
        assert(Stop.TRANSFERS_COLUMN_TIME       in transfer_cols)


        FastTripsLogger.debug("=========== TRANSFERS ===========\n" + str(self.transfers_df.head()))
        FastTripsLogger.debug("\n"+str(self.transfers_df.dtypes))

        # ignore time column and calculate it from distance
        # TODO: this is to be consistent with original implementation. Remove?
        self.transfers_df[Stop.TRANSFERS_COLUMN_TIME_MIN] = self.transfers_df[Stop.TRANSFERS_COLUMN_DISTANCE]*60.0/3.0;
        # convert time column from float to timedelta
        self.transfers_df[Stop.TRANSFERS_COLUMN_TIME] = \
            self.transfers_df[Stop.TRANSFERS_COLUMN_TIME_MIN].map(lambda x: datetime.timedelta(minutes=x))

        FastTripsLogger.debug("Final\n"+str(self.transfers_df.dtypes))
        FastTripsLogger.info("Read %7d transfers" % len(self.transfers_df))

        #: Trips table.
        self.trip_times_df      = None

    def add_trips(self, stop_times_df):
        """
        Add myself to the given trip.

        :param stop_times_df: The :py:attr:`Trip.stop_times_df` table
        :type stop_times_df: a :py:class:`pandas.DataFrame` instance

        """ 
        self.trip_times_df = stop_times_df.copy()
        self.trip_times_df.reset_index(inplace=True)
        self.trip_times_df.set_index([Trip.STOPTIMES_COLUMN_STOP_ID, Trip.STOPTIMES_COLUMN_TRIP_ID, Trip.STOPTIMES_COLUMN_SEQUENCE], inplace=True, verify_integrity=True)
        FastTripsLogger.debug("Stop trip_times_df\n" + str(self.trip_times_df.head()))

    def get_transfers(self, stop_id, xfer_from):
        if xfer_from:
            return self.transfers_df.loc[self.transfers_df[Stop.TRANSFERS_COLUMN_FROM_STOP]==stop_id]
        else:
            return self.transfers_df.loc[self.transfers_df[Stop.TRANSFERS_COLUMN_TO_STOP]==stop_id]


    def get_trips_arriving_within_time(self, stop_id, latest_arrival, time_window):
        """
        Return list of [(trip_id, sequence, arrival_time)] where the arrival time is before *latest_arrival* but within *time_window*.

        :param latest_arrival: The latest time the transit vehicle can arrive.
        :type latest_arrival: a :py:class:`datetime.time` instance
        :param time_window: The time window extending before *latest_arrival* within which an arrival is valid.
        :type time_window: a :py:class:`datetime.timedelta` instance

        """
        latest_arrival_min = 60.0*latest_arrival.hour + latest_arrival.minute + latest_arrival.second/60.0
        # filter to stop
        df = self.trip_times_df.loc[stop_id]
        # arrive before latest arrival and arrive within time window
        df = df.loc[(df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN] < latest_arrival_min)&
                    (df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN] > (latest_arrival_min - time_window.total_seconds()/60.0))]

        to_return = []
        df = df[[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME]]
        for index, row in df.iterrows():
            to_return.append( (index[0],                                  # trip id
                               index[1],                                  # sequence,
                               row[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME]    # arrival time
                            ) )
        return to_return

    def get_trips_departing_within_time(self, stop_id, earliest_departure, time_window):
        """
        Return list of [(trip_id, sequence, departure_time)] where the departure time is after *earliest_departure* but within *time_window*.

        :param earliest_departure: The earliest time the transit vehicle can depart.
        :type earliest_departure: a :py:class:`datetime.time` instance
        :param time_window: The time window extending after *earliest_departure* within which a departure is valid.
        :type time_window: a :py:class:`datetime.timedelta` instance

        """
        earliest_departure_min = 60.0*earliest_departure.hour + earliest_departure.minute + earliest_departure.second/60.0
        # filter to stop
        df = self.trip_times_df.loc[stop_id]
        # depart after the earliest departure
        df = df.loc[(df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN] > earliest_departure_min)&
                    (df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN] < (earliest_departure_min + time_window.total_seconds()/60.0))]

        to_return = []
        df = df[[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME]]
        for index, row in df.iterrows():
            to_return.append( (index[0],                                  # trip id
                               index[1],                                  # sequence,
                               row[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME]    # arrival time
                            ) )
        return to_return


    def is_transfer(self, stop_id, xfer_from):
        """
        Returns true iff this is a transfer stop; e.g. if it's served by multiple routes or has a transfer link.
        """
        if xfer_from and len(self.transfers_df.loc[self.transfers_df[Stop.TRANSFERS_COLUMN_FROM_STOP]==stop_id]) > 0:
            return True
        if not xfer_from and len(self.transfers_df.loc[self.transfers_df[Stop.TRANSFERS_COLUMN_TO_STOP]==stop_id]) > 0:
            return True
        return False

