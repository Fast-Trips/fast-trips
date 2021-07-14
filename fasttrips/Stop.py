from __future__ import division
from builtins import str
from builtins import object

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

from .Logger import FastTripsLogger
from .Trip import Trip
from .Util import Util


class Stop(object):
    """
    Stop class.

    One instance represents all of the Stops as well as their transfer links.

    Stores stop information in :py:attr:`Stop.stops_df`, an instance of :py:class:`pandas.DataFrame`,
    """

    #: File with fasttrips stop information (this extends the
    #: `gtfs stops <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/stops.md>`_ file).
    #: See `stops_ft specification <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/stops_ft.md>`_.
    INPUT_STOPS_FILE                        = "stops_ft.txt"
    #: gtfs Stops column name: Unique identifier (object)
    STOPS_COLUMN_STOP_ID                    = 'stop_id'
    #: gtfs Stops column name: Stop name (string)
    STOPS_COLUMN_STOP_NAME                  = 'stop_name'
    #: gtfs Stops column name: Latitude
    STOPS_COLUMN_STOP_LATITUDE              = 'stop_lat'
    #: gtfs Stops column name: Longitude
    STOPS_COLUMN_STOP_LONGITUDE             = 'stop_lon'
    #: gtfs Stops column name: Zone ID
    STOPS_COLUMN_ZONE_ID                    = 'zone_id'

    #: fasttrips Stops column name: Shelter
    STOPS_COLUMN_SHELTER                    = 'shelter'
    #: fasttrips Stops column name: Lighting
    STOPS_COLUMN_LIGHTING                   = 'lighting'
    #: fasttrips Stops column name: Bike Parking
    STOPS_COLUMN_BIKE_PARKING               = 'bike_parking'
    #: fasttrips Stops column name: Bike Share Station
    STOPS_COLUMN_BIKE_SHARE_STATION         = 'bike_share_station'
    #: fasttrips Stops column name: Seating
    STOPS_COLUMN_SEATING                    = 'seating'
    #: fasttrips Stops column name: Platform Height
    STOPS_COLUMN_PLATFORM_HEIGHT            = 'platform_height'
    #: fasttrips Stops column name: Level
    STOPS_COLUMN_LEVEL                      = 'level'
    #: fasttrips Stops column name: Off-Board Payment
    STOPS_COLUMN_OFF_BOARD_PAYMENT          = 'off_board_payment'

    # ========== Added by fasttrips =======================================================
    #: fasttrips Stops column name: Stop Numerical Identifier. Int.
    STOPS_COLUMN_STOP_ID_NUM                = 'stop_id_num'
    #: fasttrips Stops column name: Zone Numerical Identifier. Int.
    STOPS_COLUMN_ZONE_ID_NUM                = 'zone_id_num'


    #: File with stop ID, stop ID number correspondence
    OUTPUT_STOP_ID_NUM_FILE                   = 'ft_intermediate_stop_id.txt'

    def __init__(self, input_archive, output_dir, gtfs, today):
        """
        Constructor.  Reads the gtfs data from the transitfeed schedule, and the additional
        fast-trips stops data from the input files in *input_archive*.
        """
        # keep this for later
        self.output_dir       = output_dir

        self.stops_df = gtfs.stops

        FastTripsLogger.info("Read %7d %15s from %25d %25s" %
                             (len(self.stops_df), 'date valid stop', len(gtfs.stops), 'total stops'))

        # Read the fast-trips supplemental stops data file. Make sure stop ID is read as a string.
        stops_ft_df = gtfs.get(Stop.INPUT_STOPS_FILE)
        assert(len(stops_ft_df) > 0)

        # verify required columns are present
        stops_ft_cols = list(stops_ft_df.columns.values)
        assert(Stop.STOPS_COLUMN_STOP_ID             in stops_ft_cols)

        # if more than one column, join to the stops dataframe
        if len(stops_ft_cols) > 1:
            self.stops_df = pd.merge(left=self.stops_df, right=stops_ft_df,
                                         how='left', on=Stop.STOPS_COLUMN_STOP_ID)

        # Stop IDs are strings. Create a unique numeric stop ID.
        self.stop_id_df = Util.add_numeric_column(self.stops_df[[Stop.STOPS_COLUMN_STOP_ID]],
                                                  id_colname=Stop.STOPS_COLUMN_STOP_ID,
                                                  numeric_newcolname=Stop.STOPS_COLUMN_STOP_ID_NUM)
        FastTripsLogger.debug("Stop ID to number correspondence\n" + str(self.stop_id_df.head()))
        FastTripsLogger.debug(str(self.stop_id_df.dtypes))

        #: Note the max stop ID num in :py:attr:`Stop.max_stop_id_num`.
        self.max_stop_id_num = self.stop_id_df[Stop.STOPS_COLUMN_STOP_ID_NUM].max()
        FastTripsLogger.debug("max stop ID number: %d" % self.max_stop_id_num)

        self.stops_df = self.add_numeric_stop_id(self.stops_df,
                                                 id_colname=Stop.STOPS_COLUMN_STOP_ID,
                                                 numeric_newcolname=Stop.STOPS_COLUMN_STOP_ID_NUM)

        # Zone IDs are strings.  Add numeric zone ID.
        self.zone_id_df = pd.DataFrame()
        if Stop.STOPS_COLUMN_ZONE_ID in list(self.stops_df.columns.values):
            # Blank zone IDs should be null
            self.stops_df.loc[ self.stops_df[Stop.STOPS_COLUMN_ZONE_ID]=="", Stop.STOPS_COLUMN_ZONE_ID] = None
            zones_df = self.stops_df.loc[ pd.notnull(self.stops_df[Stop.STOPS_COLUMN_ZONE_ID]) ]
            if len(zones_df) > 0:
                self.zone_id_df = Util.add_numeric_column(zones_df[[Stop.STOPS_COLUMN_ZONE_ID]],
                                                          id_colname=Stop.STOPS_COLUMN_ZONE_ID,
                                                          numeric_newcolname=Stop.STOPS_COLUMN_ZONE_ID_NUM)
                # add it to the stops
                self.stops_df = pd.merge(left=self.stops_df, right=self.zone_id_df, how="left", on=Stop.STOPS_COLUMN_ZONE_ID)

                # and the stop_id_df
                self.stop_id_df = pd.merge(left =self.stops_df,
                                               right=self.stops_df[[Stop.STOPS_COLUMN_STOP_ID_NUM,
                                                                    Stop.STOPS_COLUMN_ZONE_ID,
                                                                    Stop.STOPS_COLUMN_ZONE_ID_NUM]].drop_duplicates(),
                                               how="left")

        FastTripsLogger.debug("Zone ID to number correspondence\n" + str(self.zone_id_df.head()))

        FastTripsLogger.debug("=========== STOPS ===========\n" + str(self.stops_df.head()))
        FastTripsLogger.debug("\n"+str(self.stops_df.index.dtype)+"\n"+str(self.stops_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s, %25s" %
                             (len(self.stops_df), "stops", "stops.txt", Stop.INPUT_STOPS_FILE))

        #: Trips table.
        self.trip_times_df      = None

    def add_daps_tazs_to_stops(self, dap_df, dap_id_colname, taz_df, taz_id_colname):
        """
        Drive access points (PNR lots, KNR lots, etc) and TAZs are like stops.
        Add the DAPs and TAZs to our stop list and their numbering in the
        :py:attr:`Stop.stop_id_df`.

        Pass in dataframes with JUST an ID column.

        This method will also update the :py:attr:`Stop.transfers_df` with Stop IDs since this is
        now possible since DAPs needed to be numbered for this to work.
        """
        assert(len(dap_df.columns) == 1)

        # make sure the DAP IDs are unique from Stop IDs
        daps_unique_df  = dap_df.drop_duplicates().reset_index(drop=True)
        # join_daps_stops = pd.merge(left=daps_unique_df, right=self.stop_id_df,
        #                                how="left",
        #                                left_on=dap_id_colname,  right_on=Stop.STOPS_COLUMN_STOP_ID)
        # there should be only NaNs since DAP lot IDs need to be unique from Stop IDs
        # non_unique_lots = join_daps_stops.loc[ pd.notnull(join_daps_stops[Stop.STOPS_COLUMN_STOP_ID]) ]
        # if len(non_unique_lots) > 0:
        #     error_str = "These drive access lot IDs are also stop IDs: \n%s" % str(non_unique_lots)
        #     FastTripsLogger.warn(error_str)
        #     raise NetworkInputError("drive_access_ft.txt", error_str)
        # assert(pd.isnull(join_daps_stops[Stop.STOPS_COLUMN_STOP_ID]).sum() == len(join_daps_stops))

        # number them starting at self.max_stop_id_num
        idx = np.arange(daps_unique_df.shape[0], dtype=np.int64) + int(self.max_stop_id_num + 1)
        daps_unique_df = daps_unique_df.assign(**{Stop.STOPS_COLUMN_STOP_ID_NUM: idx})



        # rename DAP lot id to stop id
        daps_unique_df.rename(columns={dap_id_colname:Stop.STOPS_COLUMN_STOP_ID}, inplace=True)

        # append daps to stop ids
        self.stop_id_df = pd.concat([self.stop_id_df, daps_unique_df], axis=0)

        self.max_dap_id_num = self.stop_id_df[Stop.STOPS_COLUMN_STOP_ID_NUM].max()

        ##############################################################################################
        assert(len(taz_df.columns) == 1)

        # make sure the TAZ IDs are unique from Stop IDs
        tazs_unique_df  = taz_df.drop_duplicates().reset_index(drop=True)
        join_tazs_stops = pd.merge(left=tazs_unique_df,     right=self.stop_id_df,
                                       how="left",
                                       left_on=taz_id_colname,  right_on=Stop.STOPS_COLUMN_STOP_ID)
        # there should be only NaNs since TAZ IDs need to be unique from Stop IDs
        assert(pd.isnull(join_tazs_stops[Stop.STOPS_COLUMN_STOP_ID]).sum() == len(join_tazs_stops))

        # number them starting at self.max_stop_id_num
        tazs_unique_df[Stop.STOPS_COLUMN_STOP_ID_NUM] = tazs_unique_df.index + self.max_dap_id_num + 1

        # rename TAZ id to stop id
        tazs_unique_df.rename(columns={taz_id_colname:Stop.STOPS_COLUMN_STOP_ID}, inplace=True)

        # append daps to stop ids
        self.stop_id_df = pd.concat([self.stop_id_df, tazs_unique_df], axis=0)
        ##############################################################################################

        # write the stop ids and zone ids to numbering file
        stop_id_df = self.stop_id_df  # local copy with filled NA
        for col in [Stop.STOPS_COLUMN_ZONE_ID_NUM, Stop.STOPS_COLUMN_ZONE_ID]:
            if col not in stop_id_df.columns:
                stop_id_df[col] = -1
        stop_id_df.fillna(value={Stop.STOPS_COLUMN_ZONE_ID_NUM:-1,
                                 Stop.STOPS_COLUMN_ZONE_ID:"None"}, inplace=True)
        stop_id_df[Stop.STOPS_COLUMN_ZONE_ID_NUM] = stop_id_df[Stop.STOPS_COLUMN_ZONE_ID_NUM].astype(int)
        stop_id_df.to_csv(os.path.join(self.output_dir, Stop.OUTPUT_STOP_ID_NUM_FILE),
                          columns=[Stop.STOPS_COLUMN_STOP_ID_NUM, Stop.STOPS_COLUMN_STOP_ID,
                                   Stop.STOPS_COLUMN_ZONE_ID_NUM, Stop.STOPS_COLUMN_ZONE_ID],
                          sep=" ", index=False)
        FastTripsLogger.debug("Wrote %s" % os.path.join(self.output_dir, Stop.OUTPUT_STOP_ID_NUM_FILE))


    def add_numeric_stop_id(self, input_df, id_colname, numeric_newcolname, warn=False, warn_msg=None, drop_failures=True):
        """
        Passing a :py:class:`pandas.DataFrame` with a stop ID column called *id_colname*,
        adds the numeric stop id as a column named *numeric_newcolname* and returns it.
        """
        return Util.add_new_id(input_df, id_colname, numeric_newcolname,
                                   mapping_df=self.stop_id_df[[Stop.STOPS_COLUMN_STOP_ID, Stop.STOPS_COLUMN_STOP_ID_NUM]],
                                   mapping_id_colname=Stop.STOPS_COLUMN_STOP_ID,
                                   mapping_newid_colname=Stop.STOPS_COLUMN_STOP_ID_NUM,
                                   warn=warn, warn_msg=warn_msg, drop_failures=drop_failures)

    def add_stop_id_for_numeric_id(self, input_df, numeric_id, id_colname):
        """
        Passing a :py:class:`pandas.DataFrame` with a stop ID num column called *numeric_id*,
        adds the string stop id as a column named *id_colname* and returns it.
        """
        return Util.add_new_id(input_df, id_colname=numeric_id, newid_colname=id_colname,
                               mapping_df=self.stop_id_df[[Stop.STOPS_COLUMN_STOP_ID, Stop.STOPS_COLUMN_STOP_ID_NUM]],
                               mapping_id_colname=Stop.STOPS_COLUMN_STOP_ID_NUM,
                               mapping_newid_colname=Stop.STOPS_COLUMN_STOP_ID,
                               warn=True, warn_msg=None)

    def add_numeric_stop_zone_id(self, input_df, id_colname, numeric_newcolname, warn=False, warn_msg=None):
        """
        Passing a :py:class:`pandas.DataFrame` with a stop zone ID column called *id_colname*,
        adds the numeric stop zone id as a column named *numeric_newcolname* and returns it.
        """
        return Util.add_new_id(input_df, id_colname, numeric_newcolname,
                               mapping_df=self.zone_id_df[[Stop.STOPS_COLUMN_ZONE_ID, Stop.STOPS_COLUMN_ZONE_ID_NUM]],
                               mapping_id_colname=Stop.STOPS_COLUMN_ZONE_ID,
                               mapping_newid_colname=Stop.STOPS_COLUMN_ZONE_ID_NUM,
                               warn=warn, warn_msg=warn_msg)

    def add_stop_lat_lon(self, input_df, id_colname, new_lat_colname, new_lon_colname, new_stop_name_colname=None):
        """
        Passing a :py:class:`pandas.DataFrame` with a stop ID column called *id_colname*,
        adds the stop latitude and longitude as columns named *new_lat_colname* and *new_lon_colname*
        and returns it.

        Pass *new_stop_name_colname* to also get the stop name.
        """
        stop_cols = [Stop.STOPS_COLUMN_STOP_ID, Stop.STOPS_COLUMN_STOP_LATITUDE, Stop.STOPS_COLUMN_STOP_LONGITUDE]
        if new_stop_name_colname: stop_cols.append(Stop.STOPS_COLUMN_STOP_NAME)

        input_df = pd.merge(left    =input_df,
                                right   =self.stops_df[stop_cols],
                                how     ="left",
                                left_on =id_colname,
                                right_on=Stop.STOPS_COLUMN_STOP_ID)
        # don't want to add this column
        if Stop.STOPS_COLUMN_STOP_ID != id_colname:
            input_df.drop(Stop.STOPS_COLUMN_STOP_ID, axis=1, inplace=True)

        rename_dict = {Stop.STOPS_COLUMN_STOP_LATITUDE :new_lat_colname,
                       Stop.STOPS_COLUMN_STOP_LONGITUDE:new_lon_colname}
        if new_stop_name_colname: rename_dict[Stop.STOPS_COLUMN_STOP_NAME] = new_stop_name_colname
        input_df.rename(columns=rename_dict, inplace=True)
        return input_df

    def add_stop_zone_id(self, input_df, id_colname, zone_colname):
        """
        Passing a :py:class:`pandas.DataFrame` with a stop ID column called *id_colname*,
        adds the stop zone id as a column named *zone_colname* and returns it.

        If no zone_ids specified, this is a no-op.
        """
        if Stop.STOPS_COLUMN_ZONE_ID not in self.stops_df.columns.values:
            return input_df

        input_df = pd.merge(left    =input_df,
                                right   = self.stops_df[[Stop.STOPS_COLUMN_STOP_ID, Stop.STOPS_COLUMN_ZONE_ID]],
                                how     ="left",
                                left_on =id_colname,
                                right_on=Stop.STOPS_COLUMN_STOP_ID)
        # don't want to add this column
        if Stop.STOPS_COLUMN_STOP_ID != id_colname:
            input_df.drop(Stop.STOPS_COLUMN_STOP_ID, axis=1, inplace=True)

        input_df.rename(columns={Stop.STOPS_COLUMN_ZONE_ID:zone_colname}, inplace=True)
        return input_df

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
        latest_arrival_min = 60.0*latest_arrival.hour + latest_arrival.minute + (latest_arrival.second/60.0)
        # filter to stop
        df = self.trip_times_df.loc[stop_id]
        # arrive before latest arrival and arrive within time window
        df = df.loc[(df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN] < latest_arrival_min)&
                    (df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN] > (latest_arrival_min - (time_window.total_seconds()/60.0)))]

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
        earliest_departure_min = 60.0*earliest_departure.hour + earliest_departure.minute + (earliest_departure.second/60.0)
        # filter to stop
        df = self.trip_times_df.loc[stop_id]
        # depart after the earliest departure
        df = df.loc[(df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN] > earliest_departure_min)&
                    (df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN] < (earliest_departure_min + (time_window.total_seconds()/60.0)))]

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

    def stop_min_max_lat_lon(self):
        """
        Returns (min_stop_lat, max_stop_lat,
                 min_stop_lon, max_stop_lon)
        """
        return (self.stops_df[Stop.STOPS_COLUMN_STOP_LATITUDE].min(),
                self.stops_df[Stop.STOPS_COLUMN_STOP_LATITUDE].max(),
                self.stops_df[Stop.STOPS_COLUMN_STOP_LONGITUDE].min(),
                self.stops_df[Stop.STOPS_COLUMN_STOP_LONGITUDE].max())
