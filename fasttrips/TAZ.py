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
from .Route  import Route
from .Stop   import Stop
from .Util   import Util

class TAZ:
    """
    TAZ class.

    One instance represents all of the Transportation Analysis Zones as well as their access links.

    Stores access link information in :py:attr:`TAZ.walk_access`, and :py:attr:`TAZ.drive_access`,
    both instances of :py:class:`pandas.DataFrame`.
    """

    #: File with fasttrips walk access information.
    #: See `walk_access specification <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/walk_access_ft.md>`_.
    INPUT_WALK_ACCESS_FILE                  = "walk_access_ft.txt"

    #: Walk access links column name: TAZ Identifier. String.
    WALK_ACCESS_COLUMN_TAZ                  = 'taz'
    #: Walk access links column name: Stop Identifier. String.
    WALK_ACCESS_COLUMN_STOP                 = 'stop_id'
    #: Walk access links column name: Walk Distance
    WALK_ACCESS_COLUMN_DIST                 = 'dist'

    #: fasttrips Walk access links column name: Elevation Gain, feet gained along link.
    WALK_ACCESS_COLUMN_ELEVATION_GAIN       = 'elevation_gain'
    #: fasttrips Walk access links column name: Population Density, people per square mile.  Float.
    WALK_ACCESS_COLUMN_POPULATION_DENSITY   = 'population_density'
    #: fasttrips Walk access links column name: Retail Density, employees per square mile. Float.
    WALK_ACCESS_COLUMN_RETAIL_DENSITY       = 'retail_density'
    #: fasttrips Walk access links column name: Auto Capacity, vehicles per hour per mile. Float.
    WALK_ACCESS_COLUMN_AUTO_CAPACITY        = 'auto_capacity'
    #: fasttrips Walk access links column name: Indirectness, ratio of Manhattan distance to crow-fly distance. Float.
    WALK_ACCESS_COLUMN_INDIRECTNESS         = 'indirectness'

    # ========== Added by fasttrips =======================================================
    #: Walk access links column name: TAZ Numerical Identifier. Int.
    WALK_ACCESS_COLUMN_TAZ_NUM              = 'taz_num'
    #: Walk access links column name: Stop Numerical Identifier. Int.
    WALK_ACCESS_COLUMN_STOP_NUM             = 'stop_id_num'

    #: Walk access links column name: Link walk time.  This is a TimeDelta
    WALK_ACCESS_COLUMN_TIME                 = 'time'
    #: Walk access links column name: Link walk time in minutes.  This is float.
    WALK_ACCESS_COLUMN_TIME_MIN             = 'time_min'

    #: Walk acess cost column name: Link generic cost for accessing stop from TAZ. Float.
    WALK_ACCESS_COLUMN_ACC_COST             = 'access_cost'
    #: Egress cost column name: Link generic cost for egressing to TAZ from stop. Float.
    WALK_ACCESS_COLUMN_EGR_COST             = 'egress_cost'

    #: File with fasttrips drive access information.
    #: See `drive_access specification <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/drive_access_ft.md>`_.
    INPUT_DRIVE_ACCESS_FILE                  = "drive_access_ft.txt"

    #: Drive access links column name: TAZ Identifier. String.
    DRIVE_ACCESS_COLUMN_TAZ                  = WALK_ACCESS_COLUMN_TAZ
    #: Drive access links column name: Stop Identifier. String.
    DRIVE_ACCESS_COLUMN_LOT_ID               = 'lot_id'
    #: Drive access links column name: Direction ('access' or 'egress')
    DRIVE_ACCESS_COLUMN_DIRECTION            = 'direction'
    #: Drive access links column name: Drive distance
    DRIVE_ACCESS_COLUMN_DISTANCE             = 'dist'
    #: Drive access links column name: Drive cost in cents (integer)
    DRIVE_ACCESS_COLUMN_COST                 = 'cost'
    #: Drive access links column name: Driving time in minutes between TAZ and lot (float)
    DRIVE_ACCESS_COLUMN_TRAVEL_TIME_MIN      = 'travel_time_min'
    #: Drive access links column name: Driving time in minutes between TAZ and lot (TimeDelta)
    DRIVE_ACCESS_COLUMN_TRAVEL_TIME          = 'travel_time'
    #: Drive access links column name: Start time (open time for lot?), minutes after midnight
    DRIVE_ACCESS_COLUMN_START_TIME_MIN       = 'start_time_min'
    #: Drive access links column name: Start time (open time for lot?). A DateTime instance
    DRIVE_ACCESS_COLUMN_START_TIME           = 'start_time'
    #: Drive access links column name: End time (open time for lot?), minutes after midnight
    DRIVE_ACCESS_COLUMN_END_TIME_MIN         = 'end_time_min'
    #: Drive access links column name: End time (open time for lot?). A DateTime instance
    DRIVE_ACCESS_COLUMN_END_TIME             = 'end_time'

    #: fasttrips Drive access links column name: Elevation Gain, feet gained along link.
    DRIVE_ACCESS_COLUMN_ELEVATION_GAIN       = 'elevation_gain'
    #: fasttrips Drive access links column name: Population Density, people per square mile.  Float.
    DRIVE_ACCESS_COLUMN_POPULATION_DENSITY   = 'population_density'
    #: fasttrips Drive access links column name: Retail Density, employees per square mile. Float.
    DRIVE_ACCESS_COLUMN_RETAIL_DENSITY       = 'retail_density'
    #: fasttrips Drive access links column name: Auto Capacity, vehicles per hour per mile. Float.
    DRIVE_ACCESS_COLUMN_AUTO_CAPACITY        = 'auto_capacity'
    #: fasttrips Drive access links column name: Indirectness, ratio of Manhattan distance to crow-fly distance. Float.
    DRIVE_ACCESS_COLUMN_INDIRECTNESS         = 'indirectness'

    # ========== Added by fasttrips =======================================================
    #: fasttrips Drive access links column name: TAZ Numerical Identifier. Int.
    DRIVE_ACCESS_COLUMN_TAZ_NUM              = WALK_ACCESS_COLUMN_TAZ_NUM

    #: File with fasttrips drive access points information.
    #: See `Drive access points specification <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/drive_access_points_ft.md>`_.
    INPUT_DAP_FILE                           = 'drive_access_points_ft.txt'
    #: fasttrips DAP column name: Lot ID. String.
    DAP_COLUMN_LOT_ID                        = 'lot_id'
    #: fasttrips DAP column name: Lot Latitude (WGS 84)
    DAP_COLUMN_LOT_LATITUDE                  = 'lot_lat'
    #: fasttrips DAP column name: Lot Longitude (WGS 84)
    DAP_COLUMN_LOT_LONGITUDE                 = 'lot_long'
    #: fasttrips DAP column name: Name of the Lot. String.
    DAP_COLUMN_NAME                          = 'name'
    #: fasttrips DAP column name: Capacity (number of parking spaces)
    DAP_COLUMN_CAPACITY                      = 'capacity'
    #: fasttrips DAP column name: Overflow Capacity (hide and ride)
    DAP_COLUMN_OVERFLOW_CAPACITY             = 'overflow_capacity'
    #: fasttrips DAP column name: Hourly Cost in cents.  Integer.
    DAP_COLUMN_HOURLY_COST                   = 'hourly_cost'
    #: fasttrips DAP column name: Maximum Daily Cost in cents.  Integer.
    DAP_COLUMN_MAXIMUM_COST                  = 'max_cost'
    #: fasttrips DAP column name: Type
    DAP_COLUMN_TYPE                          = 'type'

    #: mode column
    MODE_COLUMN_MODE                         = 'mode'
    #: mode number
    MODE_COLUMN_MODE_NUM                     = 'mode_num'

    #: access and egress modes.  First is default.
    ACCESS_EGRESS_MODES = ["walk","bike_own","bike_share","PNR","KNR"]
    #: Access mode: Walk
    MODE_ACCESS_WALK                         = 101
    #: Access mode: Bike (own)
    MODE_ACCESS_BIKE_OWN                     = 102
    #: Access mode: Bike (share)
    MODE_ACCESS_BIKE_SHARE                   = 103
    #: Access mode: Drive to PNR
    MODE_ACCESS_PNR                          = 104
    #: Access mode: Drive to KNR
    MODE_ACCESS_KNR                          = 105
    #: Egress mode: Walk
    MODE_EGRESS_WALK                         = 201
    #: Egress mode: Bike (own)
    MODE_EGRESS_BIKE_OWN                     = 202
    #: Egress mode: Bike (share)
    MODE_EGRESS_BIKE_SHARE                   = 203
    #: Egress mode: Drive to PNR
    MODE_EGRESS_PNR                          = 204
    #: Egress mode: Drive to KNR
    MODE_EGRESS_KNR                          = 205
    #: Access mode number list, in order of ACCESS_EGRESS_MODES
    ACCESS_MODE_NUMS = [MODE_ACCESS_WALK,
                        MODE_ACCESS_BIKE_OWN, MODE_ACCESS_BIKE_SHARE,
                        MODE_ACCESS_PNR,      MODE_ACCESS_KNR]
    #: Egress mode number list, in order of ACCESS_EGRESS_MODES
    EGRESS_MODE_NUMS = [MODE_EGRESS_WALK,
                        MODE_EGRESS_BIKE_OWN, MODE_EGRESS_BIKE_SHARE,
                        MODE_EGRESS_PNR,      MODE_EGRESS_KNR]
    def __init__(self, input_dir, today, stops, routes):
        """
        Constructor.  Reads the TAZ data from the input files in *input_dir*.
        """
        self.access_modes_df = pandas.DataFrame(data={TAZ.MODE_COLUMN_MODE    :TAZ.ACCESS_EGRESS_MODES,
                                                      TAZ.MODE_COLUMN_MODE_NUM:TAZ.ACCESS_MODE_NUMS })
        self.access_modes_df[TAZ.MODE_COLUMN_MODE] = self.access_modes_df[TAZ.MODE_COLUMN_MODE]\
            .apply(lambda x:'%s_%s' % (x, Route.MODE_TYPE_ACCESS))

        self.egress_modes_df = pandas.DataFrame(data={TAZ.MODE_COLUMN_MODE    :TAZ.ACCESS_EGRESS_MODES,
                                                      TAZ.MODE_COLUMN_MODE_NUM:TAZ.EGRESS_MODE_NUMS })
        self.egress_modes_df[TAZ.MODE_COLUMN_MODE] = self.egress_modes_df[TAZ.MODE_COLUMN_MODE]\
            .apply(lambda x:'%s_%s' % (x, Route.MODE_TYPE_EGRESS))

        routes.add_access_egress_modes(self.access_modes_df, self.egress_modes_df)

        #: Walk access links table. Make sure TAZ ID and stop ID are read as strings.
        self.walk_access_df = pandas.read_csv(os.path.join(input_dir, TAZ.INPUT_WALK_ACCESS_FILE),
                                              dtype={TAZ.WALK_ACCESS_COLUMN_TAZ :object,
                                                     TAZ.WALK_ACCESS_COLUMN_STOP:object})
        # verify required columns are present
        walk_access_cols = list(self.walk_access_df.columns.values)
        assert(TAZ.WALK_ACCESS_COLUMN_TAZ      in walk_access_cols)
        assert(TAZ.WALK_ACCESS_COLUMN_STOP     in walk_access_cols)
        assert(TAZ.WALK_ACCESS_COLUMN_DIST     in walk_access_cols)

        # printing this before setting index
        FastTripsLogger.debug("=========== WALK ACCESS ===========\n" + str(self.walk_access_df.head()))
        FastTripsLogger.debug("As read\n"+str(self.walk_access_df.dtypes))

        # skipping index setting for now -- it's annoying for joins
        # self.walk_access_df.set_index([TAZ.WALK_ACCESS_COLUMN_TAZ,
        #                                TAZ.WALK_ACCESS_COLUMN_STOP], inplace=True, verify_integrity=True)

        # TODO: remove?
        self.walk_access_df[TAZ.WALK_ACCESS_COLUMN_TIME_MIN] = self.walk_access_df[TAZ.WALK_ACCESS_COLUMN_DIST]*60.0/3.0;
        # convert time column from float to timedelta
        self.walk_access_df[TAZ.WALK_ACCESS_COLUMN_TIME] = \
            self.walk_access_df[TAZ.WALK_ACCESS_COLUMN_TIME_MIN].map(lambda x: datetime.timedelta(minutes=x))

        FastTripsLogger.debug("Final\n"+str(self.walk_access_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s" %
                             (len(self.walk_access_df), "walk access", TAZ.INPUT_WALK_ACCESS_FILE))

        #: Drive access links table. Make sure TAZ ID and lot ID are read as strings.
        if os.path.exists(os.path.join(input_dir, TAZ.INPUT_DRIVE_ACCESS_FILE)):
            self.drive_access_df = pandas.read_csv(os.path.join(input_dir, TAZ.INPUT_DRIVE_ACCESS_FILE),
                                                   dtype={TAZ.DRIVE_ACCESS_COLUMN_TAZ   :object,
                                                          TAZ.DRIVE_ACCESS_COLUMN_LOT_ID:object})

            # verify required columns are present
            drive_access_cols = list(self.drive_access_df.columns.values)
            assert(TAZ.DRIVE_ACCESS_COLUMN_TAZ              in drive_access_cols)
            assert(TAZ.DRIVE_ACCESS_COLUMN_LOT_ID           in drive_access_cols)
            assert(TAZ.DRIVE_ACCESS_COLUMN_DIRECTION        in drive_access_cols)
            assert(TAZ.DRIVE_ACCESS_COLUMN_DISTANCE         in drive_access_cols)
            assert(TAZ.DRIVE_ACCESS_COLUMN_COST             in drive_access_cols)
            assert(TAZ.DRIVE_ACCESS_COLUMN_TRAVEL_TIME      in drive_access_cols)
            assert(TAZ.DRIVE_ACCESS_COLUMN_START_TIME       in drive_access_cols)
            assert(TAZ.DRIVE_ACCESS_COLUMN_END_TIME         in drive_access_cols)

            # printing this before setting index
            FastTripsLogger.debug("=========== DRIVE ACCESS ===========\n" + str(self.drive_access_df.head()))
            FastTripsLogger.debug("As read\n"+str(self.drive_access_df.dtypes))

            self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_TRAVEL_TIME_MIN] = self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_TRAVEL_TIME]

            # datetime version
            self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_START_TIME] = \
                self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_START_TIME].map(lambda x: \
                    datetime.datetime.combine(today, datetime.datetime.strptime(x, '%H:%M:%S').time()))
            self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_END_TIME] = \
                self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_END_TIME].map(lambda x: \
                    datetime.datetime.combine(today, datetime.datetime.strptime(x, '%H:%M:%S').time()))

            # float version
            self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_START_TIME_MIN] = \
                self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_START_TIME].map(lambda x: \
                    60*x.time().hour + x.time().minute + x.time().second/60.0 )
            self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_END_TIME_MIN] = \
                self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_END_TIME].map(lambda x: \
                    60*x.time().hour + x.time().minute + x.time().second/60.0 )

            # convert time column from number to timedelta
            self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_TRAVEL_TIME] = \
                self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_TRAVEL_TIME_MIN].map(lambda x: datetime.timedelta(minutes=float(x)))

            # We're going to join this with stops to get drive-to-stop
            drive_access = self.drive_access_df.loc[self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_DIRECTION] == 'access']
            drive_egress = self.drive_access_df.loc[self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_DIRECTION] == 'egress']

            # join with transfers
            drive_access = pandas.merge(left=drive_access,
                                        right=stops.transfers_df,
                                        left_on=TAZ.DRIVE_ACCESS_COLUMN_LOT_ID,
                                        right_on=Stop.TRANSFERS_COLUMN_FROM_STOP,
                                        how='left')
            drive_egress = pandas.merge(left=drive_egress,
                                        right=stops.transfers_df,
                                        left_on=TAZ.DRIVE_ACCESS_COLUMN_LOT_ID,
                                        right_on=Stop.TRANSFERS_COLUMN_TO_STOP,
                                        how='left')
            self.drive_access_df = pandas.concat([drive_access, drive_egress], axis=0)

            FastTripsLogger.debug("Final\n"+str(self.drive_access_df.dtypes))
            FastTripsLogger.info("Read %7d %15s from %25s" %
                                 (len(self.drive_access_df), "drive access", TAZ.INPUT_DRIVE_ACCESS_FILE))
            self.has_drive_access = True
        else:
            self.has_drive_access = False
            self.drive_access_df  = pandas.DataFrame(columns=[TAZ.DRIVE_ACCESS_COLUMN_TAZ, TAZ.DRIVE_ACCESS_COLUMN_LOT_ID])
            FastTripsLogger.debug("=========== NO DRIVE ACCESS ===========\n")

        # Add numeric stop ID to walk access links
        self.walk_access_df  = stops.add_numeric_stop_id(self.walk_access_df,
                                                         id_colname=TAZ.WALK_ACCESS_COLUMN_STOP,
                                                         numeric_newcolname=TAZ.WALK_ACCESS_COLUMN_STOP_NUM)

        # add DAPs IDs and TAZ IDs to stop ID list
        stops.add_daps_tazs_to_stops(self.drive_access_df[[TAZ.DRIVE_ACCESS_COLUMN_LOT_ID]],
                                     TAZ.DRIVE_ACCESS_COLUMN_LOT_ID,
                                     pandas.concat([self.walk_access_df[[TAZ.WALK_ACCESS_COLUMN_TAZ]],
                                                    self.drive_access_df[[TAZ.DRIVE_ACCESS_COLUMN_TAZ]]], axis=0),
                                     TAZ.WALK_ACCESS_COLUMN_TAZ)

        # Add TAZ stop ID to walk and drive access links
        self.walk_access_df  = stops.add_numeric_stop_id(self.walk_access_df,
                                                         id_colname=TAZ.WALK_ACCESS_COLUMN_TAZ,
                                                         numeric_newcolname=TAZ.WALK_ACCESS_COLUMN_TAZ_NUM)

        if self.has_drive_access:
            self.drive_access_df = stops.add_numeric_stop_id(self.drive_access_df,
                                                             id_colname=TAZ.DRIVE_ACCESS_COLUMN_TAZ,
                                                             numeric_newcolname=TAZ.DRIVE_ACCESS_COLUMN_TAZ_NUM)

        if os.path.exists(os.path.join(input_dir, TAZ.INPUT_DAP_FILE)):
            #: DAP table. Make sure TAZ ID and lot ID are read as strings.
            self.dap_df = pandas.read_csv(os.path.join(input_dir, TAZ.INPUT_DAP_FILE),
                                          dtype={TAZ.DAP_COLUMN_LOT_ID:object})
            # verify required columns are present
            dap_cols = list(self.dap_df.columns.values)
            assert(TAZ.DAP_COLUMN_LOT_ID            in dap_cols)
            assert(TAZ.DAP_COLUMN_LOT_LATITUDE      in dap_cols)
            assert(TAZ.DAP_COLUMN_LOT_LONGITUDE     in dap_cols)

        else:
            self.dap_df = pandas.DataFrame()

        FastTripsLogger.debug("=========== DAPS ===========\n" + str(self.dap_df.head()))
        FastTripsLogger.debug("\n"+str(self.dap_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s" %
                             (len(self.dap_df), "DAPs", TAZ.INPUT_DAP_FILE))

