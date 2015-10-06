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
import collections,os,sys
import pandas

from .Logger import FastTripsLogger
from .Path import Path

class Passenger:
    """
    Passenger class.

    One instance represents all of the households and persons that could potentially make transit trips.

    Stores household information in :py:attr:`Passenger.households_df` and person information in
    :py:attr:`Passenger.persons_df`, which are both :py:class:`pandas.DataFrame` instances.
    """

    #: File with households
    INPUT_HOUSEHOLDS_FILE                       = "household.txt"

    #: File with persons
    INPUT_PERSONS_FILE                          = "person.txt"
    #: Persons column: Household ID
    PERSONS_COLUMN_HOUSEHOLD_ID                 = 'hh_id'

    #: File with trip list
    INPUT_TRIP_LIST_FILE                        = "trip_list.txt"
    #: Trip list column: Person ID
    TRIP_LIST_COLUMN_PERSON_ID                  = 'person_id'

    def __init__(self, input_dir):
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

        # TODO: validate fields?

        # Join trips to persons
        self.trip_list_df = pandas.merge(left=self.trip_list_df, right=self.persons_df,
                                         how='left',
                                         on=Passenger.TRIP_LIST_COLUMN_PERSON_ID)
        # And then to households
        self.trip_list_df = pandas.merge(left=self.trip_list_df, right=self.households_df,
                                         how='left',
                                         on=Passenger.PERSONS_COLUMN_HOUSEHOLD_ID)
        print self.trip_list_df

        #: the remainder of the input is related to the :py:class:`Path`
        #: TODO: what about multiple trips for a single passenger?
        # self.path               = Path(passenger_record)

