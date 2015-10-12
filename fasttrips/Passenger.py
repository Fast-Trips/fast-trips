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
from .TAZ    import TAZ
from .Util   import Util

class Passenger:
    """
    Passenger class.

    One instance represents all of the households and persons that could potentially make transit trips.

    Stores household information in :py:attr:`Passenger.households_df` and person information in
    :py:attr:`Passenger.persons_df`, which are both :py:class:`pandas.DataFrame` instances.
    """

    #: File with households
    INPUT_HOUSEHOLDS_FILE                       = "household.txt"
    #: Households column: Household ID
    HOUSEHOLDS_COLUMN_HOUSEHOLD_ID              = 'hh_id'

    #: File with persons
    INPUT_PERSONS_FILE                          = "person.txt"
    #: Persons column: Household ID
    PERSONS_COLUMN_HOUSEHOLD_ID                 = HOUSEHOLDS_COLUMN_HOUSEHOLD_ID
    #: Persons column: Person ID (string)
    PERSONS_COLUMN_PERSON_ID                    = 'person_id'

    # ========== Added by fasttrips =======================================================
    #: Persons column: Person ID number
    PERSONS_COLUMN_PERSON_ID_NUM                = 'person_id_num'

    #: File with trip list
    INPUT_TRIP_LIST_FILE                        = "trip_list.txt"
    #: Trip list column: Person ID
    TRIP_LIST_COLUMN_PERSON_ID                  = PERSONS_COLUMN_PERSON_ID
    #: Trip list column: Origin TAZ ID
    TRIP_LIST_COLUMN_ORIGIN_TAZ_ID              = "o_taz"
    #: Trip list column: Destination TAZ ID
    TRIP_LIST_COLUMN_DESTINATION_TAZ_ID         = "d_taz"
    #: Trip list column: Mode
    TRIP_LIST_COLUMN_MODE                       = "mode"
    #: Trip list column: Departure Time. DateTime.
    TRIP_LIST_COLUMN_DEPARTURE_TIME             = 'departure_time'
    #: Trip list column: Arrival Time. DateTime.
    TRIP_LIST_COLUMN_ARRIVAL_TIME               = 'arrival_time'
    #: Trip list column: Time Target (either 'arrival' or 'departure')
    TRIP_LIST_COLUMN_TIME_TARGET                = 'time_target'
    # ========== Added by fasttrips =======================================================
    #: Trip list column: Unique numeric ID for this passenger/trip
    TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM           = "trip_list_id_num"
    #: Trip list column: Origin TAZ Numeric ID
    TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM          = "o_taz_num"
    #: Trip list column: Destination Numeric TAZ ID
    TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM     = "d_taz_num"
    #: Trip list column: Departure Time. Float, minutes after midnight.
    TRIP_LIST_COLUMN_DEPARTURE_TIME_MIN         = 'departure_time_min'
    #: Trip list column: Departure Time. Float, minutes after midnight.
    TRIP_LIST_COLUMN_ARRIVAL_TIME_MIN           = 'arrival_time_min'

    def __init__(self, input_dir, today, stops):
        """
        Constructor from dictionary mapping attribute to value.
        """
        self.households_df  = pandas.read_csv(os.path.join(input_dir, Passenger.INPUT_HOUSEHOLDS_FILE))
        household_cols      = list(self.households_df.columns.values)

        FastTripsLogger.debug("=========== HOUSEHOLDS ===========\n" + str(self.households_df.head()))
        FastTripsLogger.debug("\n"+str(self.households_df.index.dtype)+"\n"+str(self.households_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s" %
                             (len(self.households_df), "households", Passenger.INPUT_HOUSEHOLDS_FILE))

        self.persons_df     = pandas.read_csv(os.path.join(input_dir, Passenger.INPUT_PERSONS_FILE))
        self.persons_id_df  = Util.add_numeric_column(self.persons_df[[Passenger.PERSONS_COLUMN_PERSON_ID]],
                                                      id_colname=Passenger.PERSONS_COLUMN_PERSON_ID,
                                                      numeric_newcolname=Passenger.PERSONS_COLUMN_PERSON_ID_NUM)
        self.persons_df     = pandas.merge(left=self.persons_df, right=self.persons_id_df,
                                           how="left")
        persons_cols        = list(self.persons_df.columns.values)

        FastTripsLogger.debug("=========== PERSONS ===========\n" + str(self.persons_df.head()))
        FastTripsLogger.debug("\n"+str(self.persons_df.index.dtype)+"\n"+str(self.persons_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s" %
                             (len(self.persons_df), "persons", Passenger.INPUT_PERSONS_FILE))

        self.trip_list_df  = pandas.read_csv(os.path.join(input_dir, Passenger.INPUT_TRIP_LIST_FILE))
        trip_list_cols     = list(self.trip_list_df.columns.values)

        FastTripsLogger.debug("=========== TRIP LIST ===========\n" + str(self.trip_list_df.head()))
        FastTripsLogger.debug("\n"+str(self.trip_list_df.index.dtype)+"\n"+str(self.trip_list_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s" %
                             (len(self.trip_list_df), "person trips", Passenger.INPUT_TRIP_LIST_FILE))

        # Create unique numeric index
        self.trip_list_df[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM] = self.trip_list_df.index + 1

        # datetime version
        self.trip_list_df[Passenger.TRIP_LIST_COLUMN_ARRIVAL_TIME] = \
            self.trip_list_df[Passenger.TRIP_LIST_COLUMN_ARRIVAL_TIME].map(lambda x: \
                datetime.datetime.combine(today, datetime.datetime.strptime(x, '%H:%M:%S').time()))
        self.trip_list_df[Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME] = \
            self.trip_list_df[Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME].map(lambda x: \
                datetime.datetime.combine(today, datetime.datetime.strptime(x, '%H:%M:%S').time()))

        # float version
        self.trip_list_df[Passenger.TRIP_LIST_COLUMN_ARRIVAL_TIME_MIN] = \
            self.trip_list_df[Passenger.TRIP_LIST_COLUMN_ARRIVAL_TIME].map(lambda x: \
                60*x.time().hour + x.time().minute + x.time().second/60.0 )
        self.trip_list_df[Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME_MIN] = \
            self.trip_list_df[Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME].map(lambda x: \
                60*x.time().hour + x.time().minute + x.time().second/60.0 )

        # TODO: validate fields?

        # Join trips to persons
        self.trip_list_df = pandas.merge(left=self.trip_list_df, right=self.persons_df,
                                         how='left',
                                         on=Passenger.TRIP_LIST_COLUMN_PERSON_ID)
        # And then to households
        self.trip_list_df = pandas.merge(left=self.trip_list_df, right=self.households_df,
                                         how='left',
                                         on=Passenger.PERSONS_COLUMN_HOUSEHOLD_ID)

        # add TAZ numeric ids (stored in the stop mapping)
        self.trip_list_df = stops.add_numeric_stop_id(self.trip_list_df,
            id_colname        =Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID,
            numeric_newcolname=Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM)
        self.trip_list_df = stops.add_numeric_stop_id(self.trip_list_df,
            id_colname        =Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID,
            numeric_newcolname=Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM)

        FastTripsLogger.debug("Final trip_list_df\n"+str(self.trip_list_df.index.dtype)+"\n"+str(self.trip_list_df.dtypes))

        #: Maps trip list ID num to :py:class:`Path` instance
        self.id_to_path = collections.OrderedDict()

    def add_path(self, trip_list_id, path):
        """
        Stores this path for the trip_list_id.
        """
        self.id_to_path[trip_list_id] = path

    def get_path(self, trip_list_id):
        """
        Retrieves a stored path for the given trip_list_id
        """
        # print self.id_to_path
        return self.id_to_path[trip_list_id]