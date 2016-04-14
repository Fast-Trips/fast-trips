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
from .TAZ    import TAZ
from .Trip   import Trip
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

    #: assignment results - Passenger table
    PASSENGERS_CSV                              = r"passengers_df_iter%d.csv"

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

    def setup_passenger_paths(self, output_dir, iteration, stop_id_df, trip_id_df):
        """
        Converts assignment results (which is stored in each Passenger :py:class:`Path`,
        in the :py:attr:`Path.states`) into a single :py:class:`pandas.DataFrame`.  Each row
        represents a link in the passenger's path.  The returned :py:class:`pandas.DataFrame`
        has the following columns:

        ==============  ===============  =====================================================================================================
        column name      column type     description
        ==============  ===============  =====================================================================================================
        `person_id               object  person unique ID
        `trip_list_id`            int64  trip list numerical ID
        `pathdir`                 int64  the :py:attr:`Path.direction`
        `pathmode`               object  the :py:attr:`Path.mode`
        `linkmode`               object  the mode of the link, one of :py:attr:`Path.STATE_MODE_ACCESS`, :py:attr:`Path.STATE_MODE_EGRESS`,
                                         :py:attr:`Path.STATE_MODE_TRANSFER` or :py:attr:`Path.STATE_MODE_TRIP`.  Paths will always start with
                                         access, followed by trips with transfers in between, and ending in an egress following the last trip.
        `trip_id`                object  the trip ID for trip links.  Set to :py:attr:`numpy.nan` for non-trip links.
        `trip_id_num`           float64  the numerical trip ID for trip links.  Set to :py:attr:`numpy.nan` for non-trip links.
        `A_id`                   object  the stop ID at the start of the link, or TAZ ID for access links
        `A_id_num`                int64  the numerical stop ID at the start of the link, or a numerical TAZ ID for access links
        `B_id`                   object  the stop ID at the end of the link, or a TAZ ID for access links
        `B_id_num`                int64  the numerical stop ID at the end of the link, or a numerical TAZ ID for access links
        `A_seq`,                  int64  the sequence number for the stop at the start of the link, or -1 for access links
        `B_seq`,                  int64  the sequence number for the stop at the start of the link, or -1 for access links
        `A_time`         datetime64[ns]  the time the passenger arrives at `A_id`
        `B_time`         datetime64[ns]  the time the passenger arrives at `B_id`
        `linktime`      timedelta64[ns]  the time spent on the link
        `cost`                  float64  the cost of the entire path
        ==============  ===============  =====================================================================================================

        Additionally, this method writes out the dataframe to a csv at :py:attr:`Passenger.PASSENGERS_CSV` in the given `output_dir`
        and labeled with the given `iteration`.
        """
        from .Path import Path
        mylist = []
        for trip_list_id,path in self.id_to_path.iteritems():
            if not path.goes_somewhere():   continue
            if not path.path_found():       continue

            # OUTBOUND passengers have states like this:
            #    stop:          label    departure   dep_mode  successor linktime
            # orig_taz                                 Access    b stop1
            #  b stop1                                  trip1    a stop2
            #  a stop2                               Transfer    b stop3
            #  b stop3                                  trip2    a stop4
            #  a stop4                                 Egress   dest_taz
            #
            #  stop:         label  dep_time    dep_mode   successor  seq  suc       linktime             cost  arr_time
            #   460:  0:20:49.4000  17:41:10      Access        3514   -1   -1   0:03:08.4000     0:03:08.4000  17:44:18
            #  3514:  0:17:41.0000  17:44:18     5131292        4313   30   40   0:06:40.0000     0:12:21.8000  17:50:59
            #  4313:  0:05:19.2000  17:50:59    Transfer        5728   -1   -1   0:00:19.2000     0:00:19.2000  17:51:18
            #  5728:  0:04:60.0000  17:57:00     5154302        5726   16   17   0:07:33.8000     0:03:02.4000  17:58:51
            #  5726:  0:01:57.6000  17:58:51      Egress         231   -1   -1   0:01:57.6000     0:01:57.6000  18:00:49

            # INBOUND passengers have states like this
            #   stop:          label      arrival   arr_mode predecessor linktime
            # dest_taz                                 Egress    a stop4
            #  a stop4                                  trip2    b stop3
            #  b stop3                               Transfer    a stop2
            #  a stop2                                  trip1    b stop1
            #  b stop1                                 Access   orig_taz
            #
            #  stop:         label  arr_time    arr_mode predecessor  seq pred       linktime             cost  dep_time
            #    15:  0:36:38.4000  17:30:38      Egress        3772   -1   -1   0:02:38.4000     0:02:38.4000  17:28:00
            #  3772:  0:34:00.0000  17:28:00     5123368        6516   22   14   0:24:17.2000     0:24:17.2000  17:05:50
            #  6516:  0:09:42.8000  17:03:42    Transfer        4766   -1   -1   0:00:16.8000     0:00:16.8000  17:03:25
            #  4766:  0:09:26.0000  17:03:25     5138749        5671    7    3   0:05:30.0000     0:05:33.2000  16:57:55
            #  5671:  0:03:52.8000  16:57:55      Access         943   -1   -1   0:03:52.8000     0:03:52.8000  16:54:03
            prev_linkmode = None
            prev_state_id = None

            state_list = path.states
            if not path.outbound(): state_list = list(reversed(state_list))

            for (state_id, state) in state_list:

                linkmode        = state[Path.STATE_IDX_DEPARRMODE]
                trip_id         = None
                if linkmode not in [Path.STATE_MODE_ACCESS, Path.STATE_MODE_TRANSFER, Path.STATE_MODE_EGRESS]:
                    trip_id     = state[Path.STATE_IDX_TRIP]
                    linkmode    = Path.STATE_MODE_TRIP

                if path.outbound():
                    a_id_num    = state_id
                    b_id_num    = state[Path.STATE_IDX_SUCCPRED]
                    a_seq       = state[Path.STATE_IDX_SEQ]
                    b_seq       = state[Path.STATE_IDX_SEQ_SUCCPRED]
                    b_time      = state[Path.STATE_IDX_ARRDEP]
                    a_time      = b_time - state[Path.STATE_IDX_LINKTIME]
                else:
                    a_id_num    = state[Path.STATE_IDX_SUCCPRED]
                    b_id_num    = state_id
                    a_seq       = state[Path.STATE_IDX_SEQ_SUCCPRED]
                    b_seq       = state[Path.STATE_IDX_SEQ]
                    b_time      = state[Path.STATE_IDX_DEPARR]
                    a_time      = b_time - state[Path.STATE_IDX_LINKTIME]

                # two trips in a row -- this shouldn't happen
                if linkmode == Path.STATE_MODE_TRIP and prev_linkmode == Path.STATE_MODE_TRIP:
                    FastTripsLogger.warn("Two trip links in a row... this shouldn't happen.  trip_list_id is %s\n%s\n" % (str(trip_list_id), str(path)))

                row = [path.person_id,
                       trip_list_id,
                       path.direction,
                       path.mode,
                       linkmode,
                       trip_id,
                       a_id_num,
                       b_id_num,
                       a_seq,
                       b_seq,
                       a_time,
                       b_time,
                       state[Path.STATE_IDX_LINKTIME],
                       path.cost,
                       ]
                mylist.append(row)

                prev_linkmode = linkmode
                prev_state_id = state_id

        df =  pandas.DataFrame(mylist,
                               columns=[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                        Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
                                        'pathdir',  # for debugging
                                        'pathmode', # for output
                                        'linkmode', 'trip_id_num',
                                        'A_id_num','B_id_num',
                                        'A_seq','B_seq',
                                        'A_time', 'B_time',
                                        'linktime', 'cost'])

        # get A_id and B_id and trip_id
        df = Util.add_new_id(  input_df=df,                 id_colname='A_id_num',                            newid_colname='A_id',
                             mapping_df=stop_id_df, mapping_id_colname=Stop.STOPS_COLUMN_STOP_ID_NUM, mapping_newid_colname=Stop.STOPS_COLUMN_STOP_ID)
        df = Util.add_new_id(  input_df=df,                 id_colname='B_id_num',                            newid_colname='B_id',
                             mapping_df=stop_id_df, mapping_id_colname=Stop.STOPS_COLUMN_STOP_ID_NUM, mapping_newid_colname=Stop.STOPS_COLUMN_STOP_ID)
        # get trip_id
        df = Util.add_new_id(  input_df=df,                 id_colname=Trip.TRIPS_COLUMN_TRIP_ID_NUM,         newid_colname=Trip.TRIPS_COLUMN_TRIP_ID,
                             mapping_df=trip_id_df, mapping_id_colname=Trip.TRIPS_COLUMN_TRIP_ID_NUM, mapping_newid_colname=Trip.TRIPS_COLUMN_TRIP_ID)

        FastTripsLogger.debug("Setup passengers dataframe:\n%s" % str(df.dtypes))
        df.to_csv(os.path.join(output_dir, Passenger.PASSENGERS_CSV % iteration), index=False)
        FastTripsLogger.info("Wrote passengers dataframe to %s" % os.path.join(output_dir, Passenger.PASSENGERS_CSV % iteration))
        return df
