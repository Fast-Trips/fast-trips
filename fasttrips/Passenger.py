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
    #: Trip list column: Transit Mode
    TRIP_LIST_COLUMN_TRANSIT_MODE               = "transit_mode"
    #: Trip list column: Numeric Transit Mode
    TRIP_LIST_COLUMN_TRANSIT_MODE_NUM           = "transit_mode_num"
    #: Trip list column: Access Mode
    TRIP_LIST_COLUMN_ACCESS_MODE                = "access_mode"
    #: Trip list column: Numeric Access Mode
    TRIP_LIST_COLUMN_ACCESS_MODE_NUM            = "access_mode_num"
    #: Trip list column: Egress Mode
    TRIP_LIST_COLUMN_EGRESS_MODE                = "egress_mode"
    #: Trip list column: Numeric Egress Mode
    TRIP_LIST_COLUMN_EGRESS_MODE_NUM            = "egress_mode_num"

    #: Generic transit.  Specify this for mode when you mean walk, any transit modes, walk
    #: TODO: get rid of this?  Maybe user should always specify.
    MODE_GENERIC_TRANSIT                        = "transit"
    #: Generic transit - Numeric mode number
    MODE_GENERIC_TRANSIT_NUM                    = 1000

    #: Trip list column: User class. String.
    TRIP_LIST_COLUMN_USER_CLASS                 = "user_class"

    def __init__(self, input_dir, output_dir, today, stops, routes):
        """
        Constructor from dictionary mapping attribute to value.
        """

        self.trip_list_df  = pandas.read_csv(os.path.join(input_dir, Passenger.INPUT_TRIP_LIST_FILE),
                                             dtype={Passenger.TRIP_LIST_COLUMN_PERSON_ID         :object,
                                                    Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID     :object,
                                                    Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID:object})
        trip_list_cols     = list(self.trip_list_df.columns.values)

        assert(Passenger.TRIP_LIST_COLUMN_PERSON_ID          in trip_list_cols)
        assert(Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID      in trip_list_cols)
        assert(Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID in trip_list_cols)

        FastTripsLogger.debug("=========== TRIP LIST ===========\n" + str(self.trip_list_df.head()))
        FastTripsLogger.debug("\n"+str(self.trip_list_df.index.dtype)+"\n"+str(self.trip_list_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s" %
                             (len(self.trip_list_df), "person trips", Passenger.INPUT_TRIP_LIST_FILE))

        non_null_person_ids = pandas.notnull(self.trip_list_df[Passenger.TRIP_LIST_COLUMN_PERSON_ID]).sum()
        # Make null person ids 'None'
        self.trip_list_df[Passenger.TRIP_LIST_COLUMN_PERSON_ID].fillna(value='', inplace=True)
        if non_null_person_ids > 0 and os.path.exists(os.path.join(input_dir, Passenger.INPUT_PERSONS_FILE)):

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

            self.households_df  = pandas.read_csv(os.path.join(input_dir, Passenger.INPUT_HOUSEHOLDS_FILE))
            household_cols      = list(self.households_df.columns.values)

            FastTripsLogger.debug("=========== HOUSEHOLDS ===========\n" + str(self.households_df.head()))
            FastTripsLogger.debug("\n"+str(self.households_df.index.dtype)+"\n"+str(self.households_df.dtypes))
            FastTripsLogger.info("Read %7d %15s from %25s" %
                                 (len(self.households_df), "households", Passenger.INPUT_HOUSEHOLDS_FILE))
        else:
            self.persons_df     = pandas.DataFrame()
            self.households_df  = pandas.DataFrame()

        # Create unique numeric index
        self.trip_list_df[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM] = self.trip_list_df.index + 1

        # datetime version
        self.trip_list_df[Passenger.TRIP_LIST_COLUMN_ARRIVAL_TIME] = \
            self.trip_list_df[Passenger.TRIP_LIST_COLUMN_ARRIVAL_TIME].map(lambda x: Util.read_time(x))
        self.trip_list_df[Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME] = \
            self.trip_list_df[Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME].map(lambda x: Util.read_time(x))

        # float version
        self.trip_list_df[Passenger.TRIP_LIST_COLUMN_ARRIVAL_TIME_MIN] = \
            self.trip_list_df[Passenger.TRIP_LIST_COLUMN_ARRIVAL_TIME].map(lambda x: \
                60*x.time().hour + x.time().minute + x.time().second/60.0 )
        self.trip_list_df[Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME_MIN] = \
            self.trip_list_df[Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME].map(lambda x: \
                60*x.time().hour + x.time().minute + x.time().second/60.0 )

        # TODO: validate fields?

        if len(self.persons_df) > 0:
            # Join trips to persons
            self.trip_list_df = pandas.merge(left=self.trip_list_df, right=self.persons_df,
                                             how='left',
                                             on=Passenger.TRIP_LIST_COLUMN_PERSON_ID)
            # And then to households
            self.trip_list_df = pandas.merge(left=self.trip_list_df, right=self.households_df,
                                             how='left',
                                             on=Passenger.PERSONS_COLUMN_HOUSEHOLD_ID)
        else:
            # Give each passenger a unique person ID num
            self.trip_list_df[Passenger.PERSONS_COLUMN_PERSON_ID_NUM] = self.trip_list_df.index + 1

        # add TAZ numeric ids (stored in the stop mapping)
        self.trip_list_df = stops.add_numeric_stop_id(self.trip_list_df,
            id_colname        =Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID,
            numeric_newcolname=Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM)
        self.trip_list_df = stops.add_numeric_stop_id(self.trip_list_df,
            id_colname        =Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID,
            numeric_newcolname=Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM)

        # figure out modes:
        if Passenger.TRIP_LIST_COLUMN_MODE not in trip_list_cols:
            # default to generic walk-transit-walk
            self.trip_list_df[Passenger.TRIP_LIST_COLUMN_MODE] = Passenger.MODE_GENERIC_TRANSIT
            self.trip_list_df['mode_dash_count'] = 0

        else:
            # count the dashes in the mode
            self.trip_list_df['mode_dash_count'] = self.trip_list_df[Passenger.TRIP_LIST_COLUMN_MODE]\
                .map(lambda x: x.count('-'))

        # The only modes allowed are access-transit-egress or MODE_GENERIC_TRANSIT
        bad_mode_df = self.trip_list_df.loc[((self.trip_list_df['mode_dash_count']!=2)&
                                             ((self.trip_list_df['mode_dash_count']!=0)|
                                              (self.trip_list_df[Passenger.TRIP_LIST_COLUMN_MODE]!=Passenger.MODE_GENERIC_TRANSIT)))]
        if len(bad_mode_df) > 0:
            FastTripsLogger.fatal("Could not understand column '%s' in the following: \n%s" %
                                  (Passenger.TRIP_LIST_COLUMN_MODE,
                                   bad_mode_df[[Passenger.TRIP_LIST_COLUMN_MODE,'mode_dash_count']].to_string()))
            sys.exit(2)

        # Take care of the transit generic
        self.trip_list_df.loc[self.trip_list_df['mode_dash_count']==0,
                              Passenger.TRIP_LIST_COLUMN_TRANSIT_MODE] = Passenger.MODE_GENERIC_TRANSIT
        self.trip_list_df.loc[self.trip_list_df['mode_dash_count']==0,
                              Passenger.TRIP_LIST_COLUMN_ACCESS_MODE ] = "%s" % TAZ.ACCESS_EGRESS_MODES[0]
        self.trip_list_df.loc[self.trip_list_df['mode_dash_count']==0,
                              Passenger.TRIP_LIST_COLUMN_EGRESS_MODE ] = "%s" % TAZ.ACCESS_EGRESS_MODES[0]

        # Take care of the access-transit-egress
        self.trip_list_df.loc[self.trip_list_df['mode_dash_count']==2,
                              Passenger.TRIP_LIST_COLUMN_ACCESS_MODE] = self.trip_list_df[Passenger.TRIP_LIST_COLUMN_MODE]\
            .map(lambda x: "%s" % x[:x.find('-')])
        self.trip_list_df.loc[self.trip_list_df['mode_dash_count']==2,
                              Passenger.TRIP_LIST_COLUMN_TRANSIT_MODE] = self.trip_list_df[Passenger.TRIP_LIST_COLUMN_MODE]\
            .map(lambda x: x[x.find('-')+1:x.rfind('-')])
        self.trip_list_df.loc[self.trip_list_df['mode_dash_count']==2,
                              Passenger.TRIP_LIST_COLUMN_EGRESS_MODE] = self.trip_list_df[Passenger.TRIP_LIST_COLUMN_MODE]\
            .map(lambda x: "%s" % x[x.rfind('-')+1:])

        # We're done with mode_dash_count, thanks for your service
        self.trip_list_df.drop('mode_dash_count', axis=1, inplace=True) # replace with cumsum

        # Set the user class for each trip
        from .Path import Path
        Path.set_user_class(self.trip_list_df, Passenger.TRIP_LIST_COLUMN_USER_CLASS)

        # Verify that Path has all the configuration for these user classes + transit modes + access modes + egress modes
        # => Figure out unique user class + mode combinations
        self.modes_df = self.trip_list_df[[Passenger.TRIP_LIST_COLUMN_USER_CLASS,
                                           Passenger.TRIP_LIST_COLUMN_TRANSIT_MODE,
                                           Passenger.TRIP_LIST_COLUMN_ACCESS_MODE,
                                           Passenger.TRIP_LIST_COLUMN_EGRESS_MODE]].set_index(Passenger.TRIP_LIST_COLUMN_USER_CLASS)
        # stack - so before we have three columns: transit_mode, access_mode, egress_mode
        # after, we have two columns: demand_mode_type and the value, demand_mode
        self.modes_df               = self.modes_df.stack().to_frame()
        self.modes_df.index.names   = [Passenger.TRIP_LIST_COLUMN_USER_CLASS, Path.WEIGHTS_COLUMN_DEMAND_MODE_TYPE]
        self.modes_df.columns       = [Path.WEIGHTS_COLUMN_DEMAND_MODE]
        self.modes_df.reset_index(inplace=True)
        self.modes_df.drop_duplicates(inplace=True)
        # fix demand_mode_type since transit_mode is just transit, etc
        self.modes_df[Path.WEIGHTS_COLUMN_DEMAND_MODE_TYPE] = self.modes_df[Path.WEIGHTS_COLUMN_DEMAND_MODE_TYPE].apply(lambda x: x[:-5])
        FastTripsLogger.debug("Demand mode types by class: \n%s" % str(self.modes_df))

        # Make sure we have all the weights required for these user_class/mode combinations
        Path.verify_weight_config(self.modes_df, output_dir, routes)

        FastTripsLogger.debug("Final trip_list_df\n"+str(self.trip_list_df.index.dtype)+"\n"+str(self.trip_list_df.dtypes))
        FastTripsLogger.debug("\n"+self.trip_list_df.head().to_string(formatters=
            {Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME:Util.datetime64_formatter,
             Passenger.TRIP_LIST_COLUMN_ARRIVAL_TIME  :Util.datetime64_formatter}))

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