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

class TAZ:
    """
    TAZ class.

    One instance represents all of the Transportation Analysis Zones as well as their access links.

    Stores access link information in :py:attr:`TAZ.walk_access`, and :py:attr:`TAZ.drive_access`,
    both instances of :py:class:`pandas.DataFrame`.
    """

    #: File with fasttrips walk access information.
    #: See `walk_access specification <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/walk_access.md>`_.
    INPUT_WALK_ACCESS_FILE                  = "walk_access.txt"

    #: Walk access links column name: TAZ Identifier
    WALK_ACCESS_COLUMN_TAZ                  = 'taz'
    #: Walk access links column name: Stop Identifier
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

    #: Walk access links column name: Link walk time.  This is a TimeDelta
    WALK_ACCESS_COLUMN_TIME                 = 'time'
    #: Walk access links column name: Link walk time in minutes.  This is float.
    WALK_ACCESS_COLUMN_TIME_MIN             = 'time_min'

    #: Walk acess cost column name: Link generic cost for accessing stop from TAZ. Float.
    WALK_ACCESS_COLUMN_ACC_COST             = 'access_cost'
    #: Egress cost column name: Link generic cost for egressing to TAZ from stop. Float.
    WALK_ACCESS_COLUMN_EGR_COST             = 'egress_cost'

    #: File with fasttrips drive access information.
    #: See `drive_access specification <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/drive_access.md>`_.
    INPUT_DRIVE_ACCESS_FILE                  = "drive_access.txt"

    #: Drive access links column name: TAZ Identifier
    DRIVE_ACCESS_COLUMN_TAZ                  = 'taz'
    #: Drive access links column name: Stop Identifier
    DRIVE_ACCESS_COLUMN_LOT_ID               = 'lot_id'
    #: Drive access links column name: Direction (Access or Egress)
    DRIVE_ACCESS_COLUMN_DIRECTION            = 'direction'
    #: Drive access links column name: Drive distance
    DRIVE_ACCESS_COLUMN_DISTANCE             = 'dist'
    #: Drive access links column name: Drive cost in cents (integer)
    DRIVE_ACCESS_COLUMN_COST                 = 'cost'
    #: Drive access links column name: Driving time in minutes between TAZ and lot (float)
    DRIVE_ACCESS_COLUMN_TRAVEL_TIME_MIN      = 'travel_time_min'
    #: Drive access links column name: Driving time in minutes between TAZ and lot (TimeDelta)
    DRIVE_ACCESS_COLUMN_TRAVEL_TIME          = 'travel_time'
    #: Drive access links column name: Start time (open time for lot?) 'HH:MM:SS' string
    DRIVE_ACCESS_COLUMN_START_TIME_STR       = 'start_time_str'
    #: Drive access links column name: Start time (open time for lot?), minutes after midnight
    DRIVE_ACCESS_COLUMN_START_TIME_MIN       = 'start_time_min'
    #: Drive access links column name: Start time (open time for lot?). A DateTime instance
    DRIVE_ACCESS_COLUMN_START_TIME           = 'start_time'
    #: Drive access links column name: End time (open time for lot?) 'HH:MM:SS' string
    DRIVE_ACCESS_COLUMN_END_TIME_STR         = 'end_time_str'
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


    def __init__(self, input_dir, today):
        """
        Constructor.  Reads the TAZ data from the input files in *input_dir*.
        """
        #: Walk access links table
        self.walk_access_df = pandas.read_csv(os.path.join(input_dir, "..", TAZ.INPUT_WALK_ACCESS_FILE))
        # verify required columns are present
        walk_access_cols = list(self.walk_access_df.columns.values)
        assert(TAZ.WALK_ACCESS_COLUMN_TAZ      in walk_access_cols)
        assert(TAZ.WALK_ACCESS_COLUMN_STOP     in walk_access_cols)
        assert(TAZ.WALK_ACCESS_COLUMN_DIST     in walk_access_cols)

        # printing this before setting index
        FastTripsLogger.debug("=========== WALK ACCESS ===========\n" + str(self.walk_access_df.head()))
        FastTripsLogger.debug("As read\n"+str(self.walk_access_df.dtypes))

        self.walk_access_df.set_index([TAZ.WALK_ACCESS_COLUMN_TAZ,
                                       TAZ.WALK_ACCESS_COLUMN_STOP], inplace=True, verify_integrity=True)

        # TODO: remove?
        self.walk_access_df[TAZ.WALK_ACCESS_COLUMN_TIME_MIN] = self.walk_access_df[TAZ.WALK_ACCESS_COLUMN_DIST]*60.0/3.0;
        # convert time column from float to timedelta
        self.walk_access_df[TAZ.WALK_ACCESS_COLUMN_TIME] = \
            self.walk_access_df[TAZ.WALK_ACCESS_COLUMN_TIME_MIN].map(lambda x: datetime.timedelta(minutes=x))

        FastTripsLogger.debug("Final\n"+str(self.walk_access_df.dtypes))
        FastTripsLogger.info("Read %7d walk access links" % len(self.walk_access_df))

        #: Drive access links table
        self.drive_access_df = pandas.read_csv(os.path.join(input_dir, "..", TAZ.INPUT_DRIVE_ACCESS_FILE))
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

        self.drive_access_df.set_index([TAZ.DRIVE_ACCESS_COLUMN_TAZ,
                                        TAZ.DRIVE_ACCESS_COLUMN_LOT_ID,
                                        TAZ.DRIVE_ACCESS_COLUMN_DIRECTION], inplace=True, verify_integrity=True)

        self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_TRAVEL_TIME_MIN] = self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_TRAVEL_TIME]

        # string version - we already have
        self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_START_TIME_STR] = self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_START_TIME]
        self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_END_TIME_STR  ] = self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_END_TIME]

        # datetime version
        self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_START_TIME] = \
            self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_START_TIME_STR].map(lambda x: \
                datetime.datetime.combine(today, datetime.datetime.strptime(x, '%H:%M:%S').time()))
        self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_END_TIME] = \
            self.drive_access_df[TAZ.DRIVE_ACCESS_COLUMN_END_TIME_STR].map(lambda x: \
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

        FastTripsLogger.debug("Final\n"+str(self.drive_access_df.dtypes))
        FastTripsLogger.info("Read %7d drive access links" % len(self.drive_access_df))