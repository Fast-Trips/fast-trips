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
import collections
import os
import sys

import numpy as np
import pandas as pd

from .Error  import DemandInputError
from .Logger import FastTripsLogger
from .Route  import Route
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
    #: Trip list column: Person Trip ID
    TRIP_LIST_COLUMN_PERSON_TRIP_ID             = "person_trip_id"
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
    #: Trip list column: Access Mode
    TRIP_LIST_COLUMN_ACCESS_MODE                = "access_mode"
    #: Trip list column: Egress Mode
    TRIP_LIST_COLUMN_EGRESS_MODE                = "egress_mode"
    #: Trip list column: Outbound (bool), true iff time target is arrival
    TRIP_LIST_COLUMN_OUTBOUND                   = "outbound"

    #: Option for :py:attr:`Passenger.TRIP_LIST_COLUMN_TIME_TARGET` (arrival time)
    TIME_TARGET_ARRIVAL                         = "arrival"
    #: Option for :py:attr:`Passenger.TRIP_LIST_COLUMN_TIME_TARGET` (departure time)
    TIME_TARGET_DEPARTURE                       = "departure"

    #: Generic transit.  Specify this for mode when you mean walk, any transit modes, walk
    #: TODO: get rid of this?  Maybe user should always specify.
    MODE_GENERIC_TRANSIT                        = "transit"
    #: Generic transit - Numeric mode number
    MODE_GENERIC_TRANSIT_NUM                    = 1000

    #: Minumum Value of Time: 1 dollar shouldn't be worth 180 minutes
    MIN_VALUE_OF_TIME                           = 60.0/180.0

    #: Trip list column: User class. String.
    TRIP_LIST_COLUMN_USER_CLASS                 = "user_class"
    #: Trip list column: Purpose. String.
    TRIP_LIST_COLUMN_PURPOSE                    = "purpose"
    #: Trip list column: Value of time. Float.
    TRIP_LIST_COLUMN_VOT                        = "vot"
    #: Trip list column: Trace. Boolean.
    TRIP_LIST_COLUMN_TRACE                      = "trace"

    #: Column names from pathfinding
    PF_COL_PF_ITERATION             = 'pf_iteration' #: 0.01*pathfinding_iteration + iteration during which this path was found
    PF_COL_PAX_A_TIME               = 'pf_A_time'    #: time path-finder thinks passenger arrived at A
    PF_COL_PAX_B_TIME               = 'pf_B_time'    #: time path-finder thinks passenger arrived at B
    PF_COL_LINK_TIME                = 'pf_linktime'  #: time path-finder thinks passenger spent on link
    PF_COL_LINK_FARE                = 'pf_linkfare'  #: fare path-finder thinks passenger spent on link
    PF_COL_LINK_COST                = 'pf_linkcost'  #: cost (generalized) path-finder thinks passenger spent on link
    PF_COL_LINK_DIST                = 'pf_linkdist'  #: dist path-finder thinks passenger spent on link
    PF_COL_WAIT_TIME                = 'pf_waittime'  #: time path-finder thinks passenger waited for vehicle on trip links

    PF_COL_PATH_NUM                 = 'pathnum'      #: path number, starting from 0
    PF_COL_LINK_NUM                 = 'linknum'      #: link number, starting from access
    PF_COL_LINK_MODE                = 'linkmode'     #: link mode (Access, Trip, Egress, etc)

    PF_COL_MODE                     = TRIP_LIST_COLUMN_MODE        #: supply mode
    PF_COL_ROUTE_ID                 = Trip.TRIPS_COLUMN_ROUTE_ID   #: link route ID
    PF_COL_TRIP_ID                  = Trip.TRIPS_COLUMN_TRIP_ID    #: link trip ID
    PF_COL_DESCRIPTION              = 'description'                #: path text description
    #: todo replace/rename ??
    PF_COL_PAX_A_TIME_MIN           = 'pf_A_time_min'

    #: pathfinding results
    PF_PATHS_CSV                    = r"enumerated_paths.csv"
    PF_LINKS_CSV                    = r"enumerated_links.csv"

    #: results - PathSets
    PATHSET_PATHS_CSV               = r"pathset_paths.csv"
    PATHSET_LINKS_CSV               = r"pathset_links.csv"

    def __init__(self, input_dir, output_dir, today, stops, routes, capacity_constraint):
        """
        Constructor from dictionary mapping attribute to value.
        """

        # if no demand dir, nothing to do
        if input_dir == None:
            self.trip_list_df = pd.DataFrame()
            return

        FastTripsLogger.info("-------- Reading demand --------")
        FastTripsLogger.info("Capacity constraint? %x" % capacity_constraint )

        self.trip_list_df  = pd.read_csv(os.path.join(input_dir, Passenger.INPUT_TRIP_LIST_FILE),
                                             skipinitialspace=True,
                                             dtype={Passenger.TRIP_LIST_COLUMN_PERSON_ID         :object,
                                                    Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID    :object,
                                                    Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID     :object,
                                                    Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID:object,
                                                    Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME    :object,
                                                    Passenger.TRIP_LIST_COLUMN_ARRIVAL_TIME      :object,
                                                    Passenger.TRIP_LIST_COLUMN_PURPOSE           :object})
        trip_list_cols     = list(self.trip_list_df.columns.values)

        assert(Passenger.TRIP_LIST_COLUMN_PERSON_ID          in trip_list_cols)
        assert(Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID     in trip_list_cols)
        assert(Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID      in trip_list_cols)
        assert(Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID in trip_list_cols)
        assert(Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME     in trip_list_cols)
        assert(Passenger.TRIP_LIST_COLUMN_ARRIVAL_TIME       in trip_list_cols)
        assert(Passenger.TRIP_LIST_COLUMN_TIME_TARGET        in trip_list_cols)
        assert(Passenger.TRIP_LIST_COLUMN_VOT                in trip_list_cols)

        FastTripsLogger.debug("=========== TRIP LIST ===========\n" + str(self.trip_list_df.head()))
        FastTripsLogger.debug("\n"+str(self.trip_list_df.index.dtype)+"\n"+str(self.trip_list_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s" %
                             (len(self.trip_list_df), "person trips", Passenger.INPUT_TRIP_LIST_FILE))

        # Error on missing person ids or person_trip_ids
        missing_person_ids = self.trip_list_df[pd.isnull(self.trip_list_df[Passenger.TRIP_LIST_COLUMN_PERSON_ID])|
                                               pd.isnull(self.trip_list_df[Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID])]
        if len(missing_person_ids)>0:
            error_msg = "Missing person_id or person_trip_id fields:\n%s\n" % str(missing_person_ids)
            error_msg += "Use 0 for person_id for trips without corresponding person."
            FastTripsLogger.fatal(error_msg)
            raise DemandInputError(Passenger.INPUT_TRIP_LIST_FILE, error_msg)

        # Drop (warn) on missing origins or destinations
        missing_ods = self.trip_list_df[ pd.isnull(self.trip_list_df[Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID])|
                                         pd.isnull(self.trip_list_df[Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID]) ]
        if len(missing_ods)>0:
            FastTripsLogger.warn("Missing origin or destination for the following trips. Dropping.\n%s" % str(missing_ods))
            self.trip_list_df = self.trip_list_df.loc[ pd.notnull(self.trip_list_df[Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID     ])&
                                                       pd.notnull(self.trip_list_df[Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID]) ].reset_index(drop=True)
            FastTripsLogger.warn("=> Have %d person trips" % len(self.trip_list_df))

        non_zero_person_ids = len(self.trip_list_df.loc[self.trip_list_df[Passenger.TRIP_LIST_COLUMN_PERSON_ID]!="0"])
        if non_zero_person_ids > 0 and os.path.exists(os.path.join(input_dir, Passenger.INPUT_PERSONS_FILE)):

            self.persons_df     = pd.read_csv(os.path.join(input_dir, Passenger.INPUT_PERSONS_FILE),
                                                  skipinitialspace=True,
                                                  dtype={Passenger.PERSONS_COLUMN_PERSON_ID:object})
            self.persons_id_df  = Util.add_numeric_column(self.persons_df[[Passenger.PERSONS_COLUMN_PERSON_ID]],
                                                          id_colname=Passenger.PERSONS_COLUMN_PERSON_ID,
                                                          numeric_newcolname=Passenger.PERSONS_COLUMN_PERSON_ID_NUM)
            self.persons_df     = pd.merge(left=self.persons_df, right=self.persons_id_df,
                                               how="left")
            persons_cols        = list(self.persons_df.columns.values)

            FastTripsLogger.debug("=========== PERSONS ===========\n" + str(self.persons_df.head()))
            FastTripsLogger.debug("\n"+str(self.persons_df.index.dtype)+"\n"+str(self.persons_df.dtypes))
            FastTripsLogger.info("Read %7d %15s from %25s" %
                                 (len(self.persons_df), "persons", Passenger.INPUT_PERSONS_FILE))

            self.households_df  = pd.read_csv(os.path.join(input_dir, Passenger.INPUT_HOUSEHOLDS_FILE), skipinitialspace=True)
            household_cols      = list(self.households_df.columns.values)

            FastTripsLogger.debug("=========== HOUSEHOLDS ===========\n" + str(self.households_df.head()))
            FastTripsLogger.debug("\n"+str(self.households_df.index.dtype)+"\n"+str(self.households_df.dtypes))
            FastTripsLogger.info("Read %7d %15s from %25s" %
                                 (len(self.households_df), "households", Passenger.INPUT_HOUSEHOLDS_FILE))
        else:
            self.persons_df     = pd.DataFrame()
            self.households_df  = pd.DataFrame()

        # make sure that each tuple TRIP_LIST_COLUMN_PERSON_ID, TRIP_LIST_COLUMN_PERSON_TRIP_ID is unique
        self.trip_list_df["ID_dupes"] = self.trip_list_df.duplicated(subset=[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                             Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID],
                                                                     keep=False)
        if self.trip_list_df["ID_dupes"].sum() > 0:
            error_msg = "Duplicate IDs (%s, %s) found:\n%s" % \
                (Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                 Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                 self.trip_list_df.loc[self.trip_list_df["ID_dupes"]==True].to_string())
            FastTripsLogger.fatal(error_msg)
            raise DemandInputError(Passenger.INPUT_TRIP_LIST_FILE, error_msg)

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

        # value of time must be greater than a threshhold or any fare becomes prohibitively expensive
        low_vot = self.trip_list_df.loc[ self.trip_list_df[Passenger.TRIP_LIST_COLUMN_VOT] < Passenger.MIN_VALUE_OF_TIME ]
        if len(low_vot) > 0:
            FastTripsLogger.warn("These trips have value of time lower than the minimum threshhhold (%f): raising to minimum.\n%s" %
                (Passenger.MIN_VALUE_OF_TIME, str(low_vot) ))
        self.trip_list_df.loc[ self.trip_list_df[Passenger.TRIP_LIST_COLUMN_VOT] < Passenger.MIN_VALUE_OF_TIME,
            Passenger.TRIP_LIST_COLUMN_VOT] = Passenger.MIN_VALUE_OF_TIME

        if len(self.persons_df) > 0:
            # Join trips to persons
            self.trip_list_df = pd.merge(left=self.trip_list_df, right=self.persons_df,
                                             how='left',
                                             on=Passenger.TRIP_LIST_COLUMN_PERSON_ID)

            # are any null?
            no_person_ids = self.trip_list_df.loc[ pd.isnull(self.trip_list_df[Passenger.PERSONS_COLUMN_PERSON_ID_NUM])&
                                                   (self.trip_list_df[Passenger.PERSONS_COLUMN_PERSON_ID]!="0")]
            if len(no_person_ids) > 0:
                error_msg = "Even though a person list is given, failed to find person information for %d trips" % len(no_person_ids)
                FastTripsLogger.fatal(error_msg)
                FastTripsLogger.fatal("\n%s\n" % no_person_ids.to_string())
                raise DemandInputError(Passenger.INPUT_TRIP_LIST_FILE, error_msg)

            # And then to households
            self.trip_list_df = pd.merge(left=self.trip_list_df, right=self.households_df,
                                             how='left',
                                             on=Passenger.PERSONS_COLUMN_HOUSEHOLD_ID)
        else:
            # Give each passenger a unique person ID num
            self.trip_list_df[Passenger.PERSONS_COLUMN_PERSON_ID_NUM] = self.trip_list_df.index + 1

        # add TAZ numeric ids (stored in the stop mapping)
        self.trip_list_df = stops.add_numeric_stop_id(self.trip_list_df,
            id_colname        =Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID,
            numeric_newcolname=Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM,
            warn              =True,
            warn_msg          ="TAZ numbers configured as origins in demand file are not found in the network")
        self.trip_list_df = stops.add_numeric_stop_id(self.trip_list_df,
            id_colname        =Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID,
            numeric_newcolname=Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM,
            warn              =True,
            warn_msg          ="TAZ numbers configured as destinations in demand file are not found in the network")
        # trips with invalid TAZs have been dropped
        FastTripsLogger.debug("Have %d person trips" % len(self.trip_list_df))

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

        # validate time_target
        invalid_time_target = self.trip_list_df.loc[ self.trip_list_df[Passenger.TRIP_LIST_COLUMN_TIME_TARGET].isin(
                                                        [Passenger.TIME_TARGET_ARRIVAL, Passenger.TIME_TARGET_DEPARTURE])==False ]
        if len(invalid_time_target) > 0:
            error_msg = "Invalid value in column %s:\n%s" % (Passenger.TRIP_LIST_COLUMN_TIME_TARGET, str(invalid_time_target))
            FastTripsLogger.fatal(error_msg)
            raise DemandInputError(Passenger.INPUT_TRIP_LIST_FILE, error_msg)

        # set outbound
        self.trip_list_df[Passenger.TRIP_LIST_COLUMN_OUTBOUND] = (self.trip_list_df[Passenger.TRIP_LIST_COLUMN_TIME_TARGET] == Passenger.TIME_TARGET_ARRIVAL)

        # Set the user class for each trip
        from .PathSet import PathSet
        PathSet.set_user_class(self.trip_list_df, Passenger.TRIP_LIST_COLUMN_USER_CLASS)

        # Verify that PathSet has all the configuration for these user classes + transit modes + access modes + egress modes
        # => Figure out unique user class + mode combinations
        self.modes_df = self.trip_list_df[[Passenger.TRIP_LIST_COLUMN_USER_CLASS,
                                           Passenger.TRIP_LIST_COLUMN_PURPOSE,
                                           Passenger.TRIP_LIST_COLUMN_TRANSIT_MODE,
                                           Passenger.TRIP_LIST_COLUMN_ACCESS_MODE,
                                           Passenger.TRIP_LIST_COLUMN_EGRESS_MODE]].set_index([Passenger.TRIP_LIST_COLUMN_USER_CLASS, Passenger.TRIP_LIST_COLUMN_PURPOSE])
        # stack - so before we have three columns: transit_mode, access_mode, egress_mode
        # after, we have two columns: demand_mode_type and the value, demand_mode
        self.modes_df               = self.modes_df.stack().to_frame()
        self.modes_df.index.names   = [Passenger.TRIP_LIST_COLUMN_USER_CLASS, Passenger.TRIP_LIST_COLUMN_PURPOSE, PathSet.WEIGHTS_COLUMN_DEMAND_MODE_TYPE]
        self.modes_df.columns       = [PathSet.WEIGHTS_COLUMN_DEMAND_MODE]
        self.modes_df.reset_index(inplace=True)
        self.modes_df.drop_duplicates(inplace=True)
        # fix demand_mode_type since transit_mode is just transit, etc
        self.modes_df[PathSet.WEIGHTS_COLUMN_DEMAND_MODE_TYPE] = self.modes_df[PathSet.WEIGHTS_COLUMN_DEMAND_MODE_TYPE].apply(lambda x: x[:-5])
        FastTripsLogger.debug("Demand mode types by class & purpose: \n%s" % str(self.modes_df))

        # Make sure we have all the weights required for these user_class/mode combinations
        self.trip_list_df = PathSet.verify_weight_config(self.modes_df, output_dir, routes, capacity_constraint, self.trip_list_df)

        # add column trace
        from .Assignment import Assignment
        if len(Assignment.TRACE_IDS) > 0:
            trace_df = pd.DataFrame.from_records(data=Assignment.TRACE_IDS,
                                                     columns=[Passenger.TRIP_LIST_COLUMN_PERSON_ID, Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID])
            trace_df[Passenger.TRIP_LIST_COLUMN_TRACE] = True

            # combine
            self.trip_list_df = pd.merge(left  =self.trip_list_df,
                                             right =trace_df,
                                             how   ="left",
                                             on    =[Passenger.TRIP_LIST_COLUMN_PERSON_ID, Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID])
            # make nulls into False
            self.trip_list_df.loc[pd.isnull(self.trip_list_df[Passenger.TRIP_LIST_COLUMN_TRACE]), Passenger.TRIP_LIST_COLUMN_TRACE] = False
        else:
            self.trip_list_df[Passenger.TRIP_LIST_COLUMN_TRACE] = False


        FastTripsLogger.info("Have %d person trips" % len(self.trip_list_df))
        FastTripsLogger.debug("Final trip_list_df\n"+str(self.trip_list_df.index.dtype)+"\n"+str(self.trip_list_df.dtypes))
        FastTripsLogger.debug("\n"+self.trip_list_df.head().to_string())

        #: Maps trip_list_id to :py:class:`PathSet` instance.  Use trip_list_id instead of (person_id, person_trip_id) for simplicity and to iterate sequentially
        #: in setup_passenger_pathsets()
        self.id_to_pathset = collections.OrderedDict()

    def add_pathset(self, trip_list_id, pathset):
        """
        Stores this path set for the trip_list_id.
        """
        self.id_to_pathset[trip_list_id] = pathset

    def get_pathset(self, trip_list_id):
        """
        Retrieves a stored path set for the given trip_list_id
        """
        return self.id_to_pathset[trip_list_id]

    def get_person_id(self, trip_list_id):
        to_ret = self.trip_list_df.loc[self.trip_list_df[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM]==trip_list_id,
                                        [Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                         Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID]]
        return(to_ret.iloc[0,0], to_ret.iloc[0,1])

    def read_passenger_pathsets(self, pathset_dir, stops, modes_df, include_asgn=True):
        """
        Reads the dataframes described in :py:meth:`Passenger.setup_passenger_pathsets` and returns them.

        :param pathset_dir: Location of csv files to read
        :type pathset_dir: string
        :param include_asgn: If true, read from files called :py:attr:`Passenger.PF_PATHS_CSV` and :py:attr:`Passenger.PF_LINKS_CSV`.
                             Otherwise read from files called :py:attr:`Passenger.PATHSET_PATHS_CSV` and :py:attr:`Passenger.PATHSET_LINKS_CSV` which include assignment results.

        :return: See :py:meth:`Assignment.setup_passengers`
                 for documentation on the passenger paths :py:class:`pandas.DataFrame`
        :rtype: a tuple of (:py:class:`pandas.DataFrame`, :py:class:`pandas.DataFrame`)
        """
        # read existing paths
        paths_file = os.path.join(pathset_dir, Passenger.PATHSET_PATHS_CSV if include_asgn else Passenger.PF_PATHS_CSV)
        pathset_paths_df = pd.read_csv(paths_file,
                                           skipinitialspace=True,
                                           dtype={Passenger.TRIP_LIST_COLUMN_PERSON_ID     :object,
                                                  Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID:object})
        FastTripsLogger.info("Read %s" % paths_file)
        FastTripsLogger.debug("pathset_paths_df.dtypes=\n%s" % str(pathset_paths_df.dtypes))

        from .Assignment import Assignment

        date_cols = [Passenger.PF_COL_PAX_A_TIME, Passenger.PF_COL_PAX_B_TIME]
        if include_asgn:
            date_cols.extend([Assignment.SIM_COL_PAX_BOARD_TIME,
                              Assignment.SIM_COL_PAX_ALIGHT_TIME,
                              Assignment.SIM_COL_PAX_A_TIME,
                              Assignment.SIM_COL_PAX_B_TIME])
        links_dtypes = {Passenger.TRIP_LIST_COLUMN_PERSON_ID     :object,
                        Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID:object,
                        Trip.TRIPS_COLUMN_TRIP_ID                :object,
                        "A_id"                                   :object,
                        "B_id"                                   :object,
                        Passenger.PF_COL_ROUTE_ID                :object,
                        Passenger.PF_COL_TRIP_ID                 :object}
        # read datetimes as string initially
        for date_col in date_cols:
            links_dtypes[date_col] = object

        links_file = os.path.join(pathset_dir, Passenger.PATHSET_LINKS_CSV if include_asgn else Passenger.PF_LINKS_CSV)
        pathset_links_df = pd.read_csv(links_file, skipinitialspace=True, dtype=links_dtypes)

        # convert time strings to datetimes
        for date_col in date_cols:
            if date_col in pathset_links_df.columns.values:
                pathset_links_df[date_col] = pathset_links_df[date_col].map(lambda x: Util.read_time(x))

        # convert time duration columns to time durations
        link_cols = list(pathset_links_df.columns.values)
        if Passenger.PF_COL_LINK_TIME in link_cols:
            pathset_links_df[Passenger.PF_COL_LINK_TIME]       = pd.to_timedelta(pathset_links_df[Passenger.PF_COL_LINK_TIME])
        elif "%s min" % Passenger.PF_COL_LINK_TIME in link_cols:
            pathset_links_df[Passenger.PF_COL_LINK_TIME]       = pd.to_timedelta(pathset_links_df["%s min" % Passenger.PF_COL_LINK_TIME], unit='m')

        if Passenger.PF_COL_WAIT_TIME in link_cols:
            pathset_links_df[Passenger.PF_COL_WAIT_TIME]       = pd.to_timedelta(pathset_links_df[Passenger.PF_COL_WAIT_TIME])
        elif "%s min" % Passenger.PF_COL_WAIT_TIME in link_cols:
            pathset_links_df[Passenger.PF_COL_WAIT_TIME]       = pd.to_timedelta(pathset_links_df["%s min" % Passenger.PF_COL_WAIT_TIME], unit='m')

        # if simulation results are available
        if Assignment.SIM_COL_PAX_LINK_TIME in link_cols:
            pathset_links_df[Assignment.SIM_COL_PAX_LINK_TIME] = pd.to_timedelta(pathset_links_df[Assignment.SIM_COL_PAX_LINK_TIME])
        elif "%s min" % Assignment.SIM_COL_PAX_WAIT_TIME in link_cols:
            pathset_links_df[Assignment.SIM_COL_PAX_LINK_TIME] = pd.to_timedelta(pathset_links_df["%s min" % Assignment.SIM_COL_PAX_LINK_TIME], unit='m')

        if Assignment.SIM_COL_PAX_WAIT_TIME in link_cols:
            pathset_links_df[Assignment.SIM_COL_PAX_WAIT_TIME] = pd.to_timedelta(pathset_links_df[Assignment.SIM_COL_PAX_WAIT_TIME])
        elif "%s min" % Assignment.SIM_COL_PAX_WAIT_TIME in link_cols:
            pathset_links_df[Assignment.SIM_COL_PAX_WAIT_TIME] = pd.to_timedelta(pathset_links_df["%s min" % Assignment.SIM_COL_PAX_WAIT_TIME], unit='m')

        # and drop the numeric version
        if "%s min" % Passenger.PF_COL_LINK_TIME in link_cols:
            pathset_links_df.drop(["%s min" % Passenger.PF_COL_LINK_TIME,
                                   "%s min" % Passenger.PF_COL_WAIT_TIME], axis=1, inplace=True)
        if "%s min" % Assignment.SIM_COL_PAX_LINK_TIME in link_cols:
            pathset_links_df.drop(["%s min" % Assignment.SIM_COL_PAX_LINK_TIME,
                                   "%s min" % Assignment.SIM_COL_PAX_WAIT_TIME], axis=1, inplace=True)

        # if A_id_num isn't there, add it
        if "A_id_num" not in pathset_links_df.columns.values:
            pathset_links_df = stops.add_numeric_stop_id(pathset_links_df, id_colname="A_id", numeric_newcolname="A_id_num",
                                                         warn=True, warn_msg="read_passenger_pathsets: invalid stop ID", drop_failures=False)
        if "B_id_num" not in pathset_links_df.columns.values:
            pathset_links_df = stops.add_numeric_stop_id(pathset_links_df, id_colname="B_id", numeric_newcolname="B_id_num",
                                                         warn=True, warn_msg="read_passenger_pathsets: invalid stop ID", drop_failures=False)

        # if trip_list_id_num is in trip list and not in these, add it
        if Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM in self.trip_list_df.columns.values:
            if Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM not in pathset_paths_df.columns.values:
                pathset_paths_df = pd.merge(left  =pathset_paths_df,
                                                right =self.trip_list_df[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                          Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                                          Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM]],
                                                how   ="left")
            if Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM not in pathset_links_df.columns.values:
                pathset_links_df = pd.merge(left  =pathset_links_df,
                                                right =self.trip_list_df[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                          Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                                          Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM]],
                                                how   ="left")
        # add mode_num if it's not there
        if Route.ROUTES_COLUMN_MODE_NUM not in pathset_links_df.columns.values:
            pathset_links_df = pd.merge(left=pathset_links_df, right=modes_df[[Route.ROUTES_COLUMN_MODE_NUM, Route.ROUTES_COLUMN_MODE]], how="left")

        FastTripsLogger.info("Read %s" % links_file)
        FastTripsLogger.debug("pathset_links_df head=\n%s" % str(pathset_links_df.head()))
        FastTripsLogger.debug("pathset_links_df.dtypes=\n%s" % str(pathset_links_df.dtypes))

        return (pathset_paths_df, pathset_links_df)


    def setup_passenger_pathsets(self, iteration, pathfinding_iteration, stops, trip_id_df, trips_df, modes_df,
                                 transfers, tazs, prepend_route_id_to_trip_id):
        """
        Converts pathfinding results (which is stored in each Passenger :py:class:`PathSet`) into two
        :py:class:`pandas.DataFrame` instances.

        Returns two :py:class:`pandas.DataFrame` instances: pathset_paths_df and pathset_links_df.
        These only include pathsets for person trips which have just been sought (e.g. those in
        :py:attr:`Passenger.pathfind_trip_list_df`)

        pathset_paths_df has path set information, where each row represents a passenger's path:

        ==================  ===============  =====================================================================================================
        column name          column type     description
        ==================  ===============  =====================================================================================================
        `person_id`                  object  person ID
        `person_trip_id`             object  person trip ID
        `trip_list_id_num`            int64  trip list numerical ID
        `trace`                        bool  Are we tracing this person trip?
        `pathdir`                     int64  the :py:attr:`PathSet.direction`
        `pathmode`                   object  the :py:attr:`PathSet.mode`
        `pf_iteration`              float64  iteration + 0.01*pathfinding_iteration in which these paths were found
        `pathnum`                     int64  the path number for the path within the pathset
        `pf_cost`                   float64  the cost of the entire path
        `pf_fare`                   float64  the fare of the entire path
        `pf_probability`            float64  the probability of the path
        `pf_initcost`               float64  the initial cost of the entire path
        `pf_initfare`               float64  the initial fare of the entire path
        `description`                object  string representation of the path
        ==================  ===============  =====================================================================================================

        pathset_links_df has path link information, where each row represents a link in a passenger's path:

        ==================  ===============  =====================================================================================================
        column name          column type     description
        ==================  ===============  =====================================================================================================
        `person_id`                  object  person ID
        `person_trip_id`             object  person trip ID
        `trip_list_id_num`            int64  trip list numerical ID
        `trace`                        bool  Are we tracing this person trip?
        `pf_iteration`              float64  iteration + 0.01*pathfinding_iteration in which these paths were found
        `pathnum`                     int64  the path number for the path within the pathset
        `linkmode`                   object  the mode of the link, one of :py:attr:`PathSet.STATE_MODE_ACCESS`, :py:attr:`PathSet.STATE_MODE_EGRESS`,
                                             :py:attr:`PathSet.STATE_MODE_TRANSFER` or :py:attr:`PathSet.STATE_MODE_TRIP`.  PathSets will always start with
                                             access, followed by trips with transfers in between, and ending in an egress following the last trip.
        `mode_num`                    int64  the mode number for the link
        `mode`                       object  the supply mode for the link
        `route_id`                   object  the route ID for trip links.  Set to :py:attr:`numpy.nan` for non-trip links.
        `trip_id`                    object  the trip ID for trip links.  Set to :py:attr:`numpy.nan` for non-trip links.
        `trip_id_num`               float64  the numerical trip ID for trip links.  Set to :py:attr:`numpy.nan` for non-trip links.
        `A_id`                       object  the stop ID at the start of the link, or TAZ ID for access links
        `A_id_num`                    int64  the numerical stop ID at the start of the link, or a numerical TAZ ID for access links
        `B_id`                       object  the stop ID at the end of the link, or a TAZ ID for access links
        `B_id_num`                    int64  the numerical stop ID at the end of the link, or a numerical TAZ ID for access links
        `A_seq`                       int64  the sequence number for the stop at the start of the link, or -1 for access links
        `B_seq`                       int64  the sequence number for the stop at the start of the link, or -1 for access links
        `pf_A_time`          datetime64[ns]  the time the passenger arrives at `A_id`
        `pf_B_time`          datetime64[ns]  the time the passenger arrives at `B_id`
        `pf_linktime`       timedelta64[ns]  the time spent on the link
        `pf_linkfare`               float64  the fare of the link
        `pf_linkcost`               float64  the generalized cost of the link
        `pf_linkdist`               float64  the distance for the link
        `A_lat`                     float64  the latitude of A (if it's a stop)
        `A_lon`                     float64  the longitude of A (if it's a stop)
        `B_lat`                     float64  the latitude of B (if it's a stop)
        `B_lon`                     float64  the longitude of B (if it's a stop)
        ==================  ===============  =====================================================================================================

        """
        from .Assignment import Assignment
        from .PathSet import PathSet
        pathlist = []
        linklist = []

        trip_list_id_nums = self.pathfind_trip_list_df[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM].tolist()

        for trip_list_id,pathset in self.id_to_pathset.iteritems():
            # only process if we just did pathfinding for this person trip
            if trip_list_id not in trip_list_id_nums: continue

            if not pathset.goes_somewhere():   continue
            if not pathset.path_found():       continue

            for pathnum in range(pathset.num_paths()):
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

                state_list = pathset.pathdict[pathnum][PathSet.PATH_KEY_STATES]
                if not pathset.outbound: state_list = list(reversed(state_list))

                pathlist.append([\
                    pathset.person_id,
                    pathset.person_trip_id,
                    trip_list_id,
                    (pathset.person_id,pathset.person_trip_id) in Assignment.TRACE_IDS,
                    pathset.direction,
                    pathset.mode,
                    0.01*pathfinding_iteration+iteration,
                    pathnum,
                    pathset.pathdict[pathnum][PathSet.PATH_KEY_COST],
                    pathset.pathdict[pathnum][PathSet.PATH_KEY_FARE],
                    pathset.pathdict[pathnum][PathSet.PATH_KEY_PROBABILITY],
                    pathset.pathdict[pathnum][PathSet.PATH_KEY_INIT_COST],
                    pathset.pathdict[pathnum][PathSet.PATH_KEY_INIT_FARE]
                ])

                link_num   = 0
                for (state_id, state) in state_list:

                    linkmode        = state[PathSet.STATE_IDX_DEPARRMODE]
                    mode_num        = None
                    trip_id         = None
                    waittime        = None

                    if linkmode in [PathSet.STATE_MODE_ACCESS, PathSet.STATE_MODE_TRANSFER, PathSet.STATE_MODE_EGRESS]:
                        mode_num    = state[PathSet.STATE_IDX_TRIP]
                    else:
                        # trip mode_num will need to be joined
                        trip_id     = state[PathSet.STATE_IDX_TRIP]
                        linkmode    = PathSet.STATE_MODE_TRIP

                    if pathset.outbound:
                        a_id_num    = state_id
                        b_id_num    = state[PathSet.STATE_IDX_SUCCPRED]
                        a_seq       = state[PathSet.STATE_IDX_SEQ]
                        b_seq       = state[PathSet.STATE_IDX_SEQ_SUCCPRED]
                        b_time      = state[PathSet.STATE_IDX_ARRDEP]
                        a_time      = b_time - state[PathSet.STATE_IDX_LINKTIME]
                        trip_time   = state[PathSet.STATE_IDX_ARRDEP] - state[PathSet.STATE_IDX_DEPARR]
                    else:
                        a_id_num    = state[PathSet.STATE_IDX_SUCCPRED]
                        b_id_num    = state_id
                        a_seq       = state[PathSet.STATE_IDX_SEQ_SUCCPRED]
                        b_seq       = state[PathSet.STATE_IDX_SEQ]
                        b_time      = state[PathSet.STATE_IDX_DEPARR]
                        a_time      = b_time - state[PathSet.STATE_IDX_LINKTIME]
                        trip_time   = state[PathSet.STATE_IDX_DEPARR] - state[PathSet.STATE_IDX_ARRDEP]

                    # trips: linktime includes wait
                    if linkmode == PathSet.STATE_MODE_TRIP:
                        waittime    = state[PathSet.STATE_IDX_LINKTIME] - trip_time

                    # two trips in a row -- this shouldn't happen
                    if linkmode == PathSet.STATE_MODE_TRIP and prev_linkmode == PathSet.STATE_MODE_TRIP:
                        FastTripsLogger.warn("Two trip links in a row... this shouldn't happen. person_id is %s trip is %s\npathnum is %d\nstatelist (%d): %s\n" % (person_id, person_trip_id, pathnum, len(state_list), str(state_list)))
                        sys.exit()

                    linklist.append([\
                        pathset.person_id,
                        pathset.person_trip_id,
                        trip_list_id,
                        (pathset.person_id,pathset.person_trip_id) in Assignment.TRACE_IDS,
                        0.01*pathfinding_iteration + iteration,
                        pathnum,
                        linkmode,
                        mode_num,
                        trip_id,
                        a_id_num,
                        b_id_num,
                        a_seq,
                        b_seq,
                        a_time,
                        b_time,
                        state[PathSet.STATE_IDX_LINKTIME],
                        state[PathSet.STATE_IDX_LINKFARE],
                        state[PathSet.STATE_IDX_LINKCOST],
                        state[PathSet.STATE_IDX_LINKDIST],
                        waittime,
                        link_num ])

                    prev_linkmode = linkmode
                    prev_state_id = state_id
                    link_num     += 1

        FastTripsLogger.debug("setup_passenger_pathsets(): pathlist and linklist constructed")

        pathset_paths_df = pd.DataFrame(pathlist, columns=[\
            Passenger.TRIP_LIST_COLUMN_PERSON_ID,
            Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
            Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
            Passenger.TRIP_LIST_COLUMN_TRACE,
            'pathdir',  # for debugging
            'pathmode', # for output
            Passenger.PF_COL_PF_ITERATION,
            Passenger.PF_COL_PATH_NUM,
            PathSet.PATH_KEY_COST,
            PathSet.PATH_KEY_FARE,
            PathSet.PATH_KEY_PROBABILITY,
            PathSet.PATH_KEY_INIT_COST,
            PathSet.PATH_KEY_INIT_FARE])

        pathset_links_df = pd.DataFrame(linklist, columns=[\
            Passenger.TRIP_LIST_COLUMN_PERSON_ID,
            Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
            Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
            Passenger.TRIP_LIST_COLUMN_TRACE,
            Passenger.PF_COL_PF_ITERATION,
            Passenger.PF_COL_PATH_NUM,
            Passenger.PF_COL_LINK_MODE,
            Route.ROUTES_COLUMN_MODE_NUM,
            Trip.TRIPS_COLUMN_TRIP_ID_NUM,
            'A_id_num','B_id_num',
            'A_seq','B_seq',
            Passenger.PF_COL_PAX_A_TIME,
            Passenger.PF_COL_PAX_B_TIME,
            Passenger.PF_COL_LINK_TIME,
            Passenger.PF_COL_LINK_FARE,
            Passenger.PF_COL_LINK_COST,
            Passenger.PF_COL_LINK_DIST,
            Passenger.PF_COL_WAIT_TIME,
            Passenger.PF_COL_LINK_NUM ])

        FastTripsLogger.debug("setup_passenger_pathsets(): pathset_paths_df(%d) and pathset_links_df(%d) dataframes constructed" % (len(pathset_paths_df), len(pathset_links_df)))

        # get A_id and B_id and trip_id
        pathset_links_df = stops.add_stop_id_for_numeric_id(pathset_links_df,'A_id_num','A_id')
        pathset_links_df = stops.add_stop_id_for_numeric_id(pathset_links_df,'B_id_num','B_id')

        # get A_lat, A_lon, B_lat, B_lon
        pathset_links_df = stops.add_stop_lat_lon(pathset_links_df, id_colname="A_id", new_lat_colname="A_lat", new_lon_colname="A_lon")
        pathset_links_df = stops.add_stop_lat_lon(pathset_links_df, id_colname="B_id", new_lat_colname="B_lat", new_lon_colname="B_lon")

        # get trip_id
        pathset_links_df = Util.add_new_id(  input_df=pathset_links_df,          id_colname=Trip.TRIPS_COLUMN_TRIP_ID_NUM,         newid_colname=Trip.TRIPS_COLUMN_TRIP_ID,
                                           mapping_df=trip_id_df,        mapping_id_colname=Trip.TRIPS_COLUMN_TRIP_ID_NUM, mapping_newid_colname=Trip.TRIPS_COLUMN_TRIP_ID)

        # get route id
        # mode_num will appear in left (for non-transit links) and right (for transit link) both, so we need to consolidate
        pathset_links_df = pd.merge(left=pathset_links_df, right=trips_df[[Trip.TRIPS_COLUMN_TRIP_ID, Trip.TRIPS_COLUMN_ROUTE_ID, Route.ROUTES_COLUMN_MODE_NUM]],
                                        how="left", on=Trip.TRIPS_COLUMN_TRIP_ID)

        pathset_links_df[Route.ROUTES_COLUMN_MODE_NUM] = pathset_links_df["%s_x" % Route.ROUTES_COLUMN_MODE_NUM]
        pathset_links_df.loc[pd.notnull(pathset_links_df["%s_y" % Route.ROUTES_COLUMN_MODE_NUM]), Route.ROUTES_COLUMN_MODE_NUM] = pathset_links_df["%s_y" % Route.ROUTES_COLUMN_MODE_NUM]
        pathset_links_df.drop(["%s_x" % Route.ROUTES_COLUMN_MODE_NUM,
                               "%s_y" % Route.ROUTES_COLUMN_MODE_NUM], axis=1, inplace=True)
        # verify it's always set
        FastTripsLogger.debug("Have %d links with no mode number set" % len(pathset_links_df.loc[ pd.isnull(pathset_links_df[Route.ROUTES_COLUMN_MODE_NUM]) ]))

        # get supply mode
        pathset_links_df = pd.merge(left=pathset_links_df, right=modes_df[[Route.ROUTES_COLUMN_MODE_NUM, Route.ROUTES_COLUMN_MODE]], how="left")

        FastTripsLogger.debug("setup_passenger_pathsets(): pathset_paths_df and pathset_links_df dataframes constructed")
        # FastTripsLogger.debug("\n%s" % pathset_links_df.head().to_string())

        if len(pathset_paths_df) > 0:
            # create path description
            pathset_links_df[Passenger.PF_COL_DESCRIPTION] = pathset_links_df["A_id"] + " " + pathset_links_df[Route.ROUTES_COLUMN_MODE]
            if prepend_route_id_to_trip_id:
                pathset_links_df.loc[ pd.notnull(pathset_links_df[Trip.TRIPS_COLUMN_TRIP_ID]), Passenger.PF_COL_DESCRIPTION ] = pathset_links_df[Passenger.PF_COL_DESCRIPTION] + " " + pathset_links_df[Trip.TRIPS_COLUMN_ROUTE_ID] + "_"
            else:
                pathset_links_df.loc[ pd.notnull(pathset_links_df[Trip.TRIPS_COLUMN_TRIP_ID]), Passenger.PF_COL_DESCRIPTION ] = pathset_links_df[Passenger.PF_COL_DESCRIPTION] + " "
            pathset_links_df.loc[ pd.notnull(pathset_links_df[Trip.TRIPS_COLUMN_TRIP_ID]),     Passenger.PF_COL_DESCRIPTION ] = pathset_links_df[Passenger.PF_COL_DESCRIPTION] + pathset_links_df[Trip.TRIPS_COLUMN_TRIP_ID]
            pathset_links_df.loc[ pathset_links_df[Passenger.PF_COL_LINK_MODE]==PathSet.STATE_MODE_EGRESS, Passenger.PF_COL_DESCRIPTION ] = pathset_links_df[Passenger.PF_COL_DESCRIPTION] + " " + pathset_links_df["B_id"]

            descr_df = pathset_links_df[[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
                                         Passenger.PF_COL_PF_ITERATION,
                                         Passenger.PF_COL_PATH_NUM,
                                         Passenger.PF_COL_DESCRIPTION]].groupby([Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
                                                                                 Passenger.PF_COL_PF_ITERATION,
                                                                                 Passenger.PF_COL_PATH_NUM])[Passenger.PF_COL_DESCRIPTION].apply(lambda x:" ".join(x))
            descr_df = descr_df.to_frame().reset_index()
            # join it to pathset_paths and drop from pathset_links
            pathset_paths_df = pd.merge(left=pathset_paths_df, right=descr_df, how="left")
            pathset_links_df.drop([Passenger.PF_COL_DESCRIPTION], axis=1, inplace=True)
        else:
            pathset_paths_df[Passenger.PF_COL_DESCRIPTION] = ""

        return (pathset_paths_df, pathset_links_df)

    @staticmethod
    def write_paths(output_dir, iteration, pathfinding_iteration, simulation_iteration, pathset_df, links, output_pathset_per_sim_iter, drop_debug_columns, drop_pathfinding_columns):
        """
        Write either pathset paths (if links=False) or pathset links (if links=True) as the case may be
        """
        # if simulation_iteration < 0, then this is the pathfinding result
        if simulation_iteration < 0:
            pathset_df[            "iteration"] = iteration
            pathset_df["pathfinding_iteration"] = pathfinding_iteration
            Util.write_dataframe(df=pathset_df,
                                 name="pathset_links_df" if links else "pathset_paths_df",
                                 output_file=os.path.join(output_dir, Passenger.PF_LINKS_CSV if links else Passenger.PF_PATHS_CSV),
                                 append=True if ((iteration > 1) or (pathfinding_iteration > 1)) else False,
                                 keep_duration_columns=True,
                                 drop_debug_columns=drop_debug_columns,
                                 drop_pathfinding_columns=drop_pathfinding_columns)
            pathset_df.drop(["iteration","pathfinding_iteration"], axis=1, inplace=True)
            return

        # otherwise, add columns and write it
        pathset_df[            "iteration"] = iteration
        pathset_df["pathfinding_iteration"] = pathfinding_iteration
        pathset_df[ "simulation_iteration"] = simulation_iteration

        # mostly we append
        do_append = True
        # but sometimes we ovewrite
        if output_pathset_per_sim_iter:
            if (iteration == 1) and (pathfinding_iteration == 1) and (simulation_iteration == 0): do_append = False
        else:
            if (iteration == 1) and (pathfinding_iteration == 1): do_append = False

        Util.write_dataframe(df=pathset_df,
                             name="pathset_links_df" if links else "pathset_paths_df",
                             output_file=os.path.join(output_dir, Passenger.PATHSET_LINKS_CSV if links else Passenger.PATHSET_PATHS_CSV),
                             append=do_append,
                             drop_debug_columns=drop_debug_columns,
                             drop_pathfinding_columns=drop_pathfinding_columns)
        pathset_df.drop(["iteration","pathfinding_iteration","simulation_iteration"], axis=1, inplace=True)


    @staticmethod
    def choose_paths(choose_for_everyone, iteration, pathfinding_iteration, simulation_iteration, pathset_paths_df, pathset_links_df):
        """
        Returns the same dataframes as input, but with a new/updated column,
        :py:attr:`Assignment.SIM_COL_PAX_CHOSEN`.  This column is set to:

        * :py:attr:`Assignment.CHOSEN_NOT_CHOSEN_YET` for not chosen yet
        * 'iter%d.%02d sim%d' % (iteration, pathfinding_iteration, simulation_iteration for chosen)
        * :py:attr:`Assignment.CHOSEN_REJECTED` for chosen but then rejected

        If *choose_for_everyone* is True, this will attempt to choose for every passenger trip.
        Otherwise, this will attempt to choose for just those passenger trips that still need it.

        Returns (TOTAL num passenger trips chosen, NEW num passenger trips chosen, updated pathset_paths_df, updated pathset_links_df)
        """
        from .Assignment import Assignment
        from .PathSet    import PathSet


        # If choose_for_everyone, we need to do all of them.
        if choose_for_everyone:
            pathset_paths_df[Assignment.SIM_COL_PAX_CHOSEN] = pd.Categorical([Assignment.CHOSEN_NOT_CHOSEN_YET]*len(pathset_paths_df), categories=Assignment.CHOSEN_CATEGORIES, ordered=True)
        else:
            # set chosen to ordered categories if needed
            if pathset_paths_df[Assignment.SIM_COL_PAX_CHOSEN].dtype.name != "category":
                pathset_paths_df[Assignment.SIM_COL_PAX_CHOSEN] = pathset_paths_df[Assignment.SIM_COL_PAX_CHOSEN].astype('category')
                pathset_paths_df[Assignment.SIM_COL_PAX_CHOSEN].cat.set_categories(Assignment.CHOSEN_CATEGORIES, ordered=True, inplace=True)

            # Otherwise, just choose for those that still need it
            rejected_paths = pathset_paths_df.loc[ (pathset_paths_df[Assignment.SIM_COL_PAX_CHOSEN] >  Assignment.CHOSEN_NOT_CHOSEN_YET)&
                                                   (pathset_paths_df[Assignment.SIM_COL_PAX_COST  ] >= PathSet.HUGE_COST) ]
            FastTripsLogger.info("          Rejecting %d previously chosen paths for huge costs" % len(rejected_paths))
            FastTripsLogger.debug("rejected_paths head(20): \n%s" % rejected_paths.head(20))

            # why doesn't this translate to pathset_links_df ?
            if len(rejected_paths) > 0:
                # first invalidate any high cost choices
                pathset_paths_df.loc[ (pathset_paths_df[Assignment.SIM_COL_PAX_CHOSEN] >  Assignment.CHOSEN_NOT_CHOSEN_YET)&
                                      (pathset_paths_df[Assignment.SIM_COL_PAX_COST  ] >= PathSet.HUGE_COST),
                                      Assignment.SIM_COL_PAX_CHOSEN ] = Assignment.CHOSEN_REJECTED
            #     # do the same to links
            #     rejected_paths.groupby([Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM, Passenger.PF_COL_PATH_NUM])

        # add this as a category
        CHOSEN_VALUE = "iter%d.%02d sim%d" % (iteration, pathfinding_iteration, simulation_iteration)
        if CHOSEN_VALUE not in Assignment.CHOSEN_CATEGORIES:
            Assignment.CHOSEN_CATEGORIES.append(CHOSEN_VALUE)
            pathset_paths_df[Assignment.SIM_COL_PAX_CHOSEN].cat.add_categories(CHOSEN_VALUE, inplace=True)

        # group to passenger trips
        pathset_paths_df_grouped = pathset_paths_df[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                     Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                     Assignment.SIM_COL_PAX_CHOSEN]].groupby([Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                                              Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID]).aggregate("max").reset_index()
        pathset_paths_df_grouped[Assignment.SIM_COL_PAX_CHOSEN] = pd.Categorical(pathset_paths_df_grouped[Assignment.SIM_COL_PAX_CHOSEN], categories=Assignment.CHOSEN_CATEGORIES, ordered=True)

        # if there's no chosen AND one of the unchosen options is choosable then we can choose
        num_rejected = len(pathset_paths_df_grouped.loc[ pathset_paths_df_grouped[Assignment.SIM_COL_PAX_CHOSEN]==Assignment.CHOSEN_REJECTED       ])  # everything is rejected
        num_unchosen = len(pathset_paths_df_grouped.loc[ pathset_paths_df_grouped[Assignment.SIM_COL_PAX_CHOSEN]==Assignment.CHOSEN_NOT_CHOSEN_YET ])
        num_chosen   = len(pathset_paths_df_grouped) - num_rejected - num_unchosen

        # count how many passenger trips have pathsets with valid paths (logsum > 0) AND no path chosen (chosen < 0)
        pax_choose_df = pathset_paths_df_grouped.loc[ pathset_paths_df_grouped[Assignment.SIM_COL_PAX_CHOSEN]==Assignment.CHOSEN_NOT_CHOSEN_YET ].copy()
        num_unchosen  = len(pax_choose_df)

        FastTripsLogger.info("          Have %6d total passenger-trips, with %6d chosen paths, %6d fully rejected and %6d needing a choice" % (len(pathset_paths_df_grouped), num_chosen, num_rejected, num_unchosen))

        # If we have nothing to do, return
        if len(pax_choose_df) == 0:
            return (num_chosen, 0, pathset_paths_df, pathset_links_df)

        # flag it
        pax_choose_df["to_choose"] = 1

        # Choose a random number for them now
        # todo: do this differently?
        np.random.seed(iteration*1000 + simulation_iteration)
        pax_choose_df["rand"] = np.random.rand(len(pax_choose_df))
        # FastTripsLogger.debug("\n%s" % pax_choose_df.head().to_string())

        # add to_choose flag and rand to pathset_paths_df
        pathset_paths_df = pd.merge(left =pathset_paths_df,
                                        right=pax_choose_df[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                             Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                             "to_choose", "rand"]],
                                        how  ="left")

        # select out just those pathsets we're choosing, and eligible
        paths_choose_df = pathset_paths_df.loc[ (pathset_paths_df["to_choose"]==1) &
                                                (pathset_paths_df[Assignment.SIM_COL_PAX_COST] < PathSet.HUGE_COST) &
                                                (pathset_paths_df[Assignment.SIM_COL_PAX_CHOSEN] == Assignment.CHOSEN_NOT_CHOSEN_YET) ].copy()

        if len(paths_choose_df) == 0:
            FastTripsLogger.info("          No choosable paths")
            pathset_paths_df.drop(["to_choose","rand"], axis=1, inplace=True)
            return (num_chosen, 0, pathset_paths_df, pathset_links_df)

        # Use updated probability -- create cumulative probability
        paths_choose_df["prob_cum"] = paths_choose_df.groupby([Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                               Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID])[Assignment.SIM_COL_PAX_PROBABILITY].cumsum()
        # verify cumsum is ok
        # FastTripsLogger.debug("choose_path() paths_choose_df=\n%s\n" % paths_choose_df.head(100).to_string())

        # use it to choose the path based on the cumulative probability
        paths_choose_df["rand_less"] = False
        paths_choose_df.loc[paths_choose_df["rand"] < paths_choose_df["prob_cum"], "rand_less"] = True
        if num_unchosen < 10:
            FastTripsLogger.debug("choose_path() paths_choose_df=\n%s\n" % paths_choose_df.to_string())
        else:
            FastTripsLogger.debug("choose_path() paths_choose_df=\n%s\n" % paths_choose_df.head(100).to_string())

        # this will now be person id, trip list id num, index for chosen path
        chosen_path_df = paths_choose_df[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                          Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                          "rand_less"]].groupby([Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                 Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID]).idxmax(axis=0).reset_index()
        chosen_path_df.rename(columns={"rand_less":"chosen_idx"}, inplace=True)
        # FastTripsLogger.debug("choose_path() chosen_path_df=\n%s\n" % chosen_path_df.head(30).to_string())
        num_chosen += len(chosen_path_df)

        # mark it as chosen
        pathset_paths_df = pd.merge(left=pathset_paths_df, right=chosen_path_df, how="left")
        pathset_paths_df.loc[pathset_paths_df["chosen_idx"]==pathset_paths_df.index, Assignment.SIM_COL_PAX_CHOSEN] = CHOSEN_VALUE

        if len(Assignment.TRACE_IDS) > 0:
            FastTripsLogger.debug("choose_path() pathset_paths_df=\n%s\n" % pathset_paths_df.loc[ pathset_paths_df[Passenger.TRIP_LIST_COLUMN_TRACE]==True].to_string())

        FastTripsLogger.info("          Chose %d out of %d paths from the pathsets => total chosen %d" %
                             (len(chosen_path_df), len(pathset_paths_df_grouped), num_chosen))

        # drop the intermediates
        pathset_paths_df.drop(["to_choose","rand","chosen_idx"], axis=1, inplace=True)

        # give the chosen index to pathset_links_df
        if Assignment.SIM_COL_PAX_CHOSEN in list(pathset_links_df.columns.values):
            pathset_links_df.drop(Assignment.SIM_COL_PAX_CHOSEN, axis=1, inplace=True)

        pathset_links_df = pd.merge(left=pathset_links_df,
                                        right=pathset_paths_df[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                                Passenger.PF_COL_PATH_NUM,
                                                                Assignment.SIM_COL_PAX_CHOSEN]],
                                        how="left")

        pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN] = pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN].astype('category')
        pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN].cat.set_categories(Assignment.CHOSEN_CATEGORIES, ordered=True, inplace=True)

        return (num_chosen, len(chosen_path_df), pathset_paths_df, pathset_links_df)


    @staticmethod
    def get_chosen_links(pathset_links_df):
        """
        Given the pathset paths and pathset links, returns the pathset links for the ones marked as chosen.
        """
        # gather the links for the chosen paths
        from .Assignment import Assignment

        # set to ordered categories if needed
        if pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN].dtype.name != "category":
            pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN] = pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN].astype('category')
            pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN].cat.set_categories(Assignment.CHOSEN_CATEGORIES, ordered=True, inplace=True)

        return pathset_links_df.loc[pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN]>Assignment.CHOSEN_NOT_CHOSEN_YET,].copy()
