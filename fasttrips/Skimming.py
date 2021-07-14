from __future__ import division
from builtins import str
from builtins import range
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
import sys

import numpy as np
import pandas as pd

from .Logger import FastTripsLogger
from .Route import Route
from .Passenger import Passenger
from .Trip import Trip
from .Util import Util


# TODO Jan: WIP
# Copy of passengers for now because I want to use this to create paths, etc.
class Skimming(object):
    """
    Skimming class.

    One instance represents all the skims we could ever want.

    OLD: Stores household information in :py:attr:`Passenger.households_df` and person information in
    :py:attr:`Passenger.persons_df`, which are both :py:class:`pandas.DataFrame` instances.
    """

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

    #: Generic transit.  Specify this for mode when you mean walk, any transit modes, walk
    #: TODO: get rid of this?  Maybe user should always specify.
    MODE_GENERIC_TRANSIT                        = "transit"
    #: Generic transit - Numeric mode number
    MODE_GENERIC_TRANSIT_NUM                    = 1000

    #: Trip list column: User class. String.
    TRIP_LIST_COLUMN_USER_CLASS                 = "user_class"
    #: Trip list column: Purpose. String.
    TRIP_LIST_COLUMN_PURPOSE                    = "purpose"
    #: Trip list column: Value of time. Float.
    TRIP_LIST_COLUMN_VOT                        = "vot"
    #: Trip list column: Trace. Boolean.
    TRIP_LIST_COLUMN_TRACE                      = "trace"

    #: Column names from pathfinding -> see Passenger.py. Keep for now to minimise code duplication.
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

    # def __init__(self, output_dir):
    #     """
    #     """

    def setup_pathsets(self, id_to_pathset, stops, modes_df):
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


        # id_to_pathset: THIS IS pathset!

        for trip_list_id, pathset in id_to_pathset.items():

            # TODO Jan: check:
            # if not pathset.goes_somewhere():   continue

            if not pathset.path_found():
                # Does this happen for disconnected zones? Or do we only use those that
                # have associated stops by definition?
                raise NotImplementedError("TODO Jan")

            for pathnum in range(pathset.num_paths()):
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

                state_list = pathset.pathdict[pathnum][PathSet.PATH_KEY_STATES]
                if not pathset.outbound: state_list = list(reversed(state_list))

                pathlist.append([
                    trip_list_id,
                    pathset.direction,
                    pathset.mode,
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

                    linklist.append([
                        trip_list_id,
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
                        link_num])

                    prev_linkmode = linkmode
                    link_num     += 1

        FastTripsLogger.debug("setup_passenger_pathsets(): pathlist and linklist constructed")

        pathset_paths_df = pd.DataFrame(pathlist, columns=[
            Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
            'pathdir',  # for debugging
            'pathmode', # for output
            Passenger.PF_COL_PF_ITERATION,
            Passenger.PF_COL_PATH_NUM,
            PathSet.PATH_KEY_COST,
            PathSet.PATH_KEY_FARE,
            PathSet.PATH_KEY_PROBABILITY,
            PathSet.PATH_KEY_INIT_COST,
            PathSet.PATH_KEY_INIT_FARE])

        pathset_links_df = pd.DataFrame(linklist, columns=[
            Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
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
            Passenger.PF_COL_LINK_NUM])

        FastTripsLogger.debug("setup_skimming_pathsets(): pathset_paths_df(%d) and pathset_links_df(%d) dataframes constructed" % (len(pathset_paths_df), len(pathset_links_df)))

        # get A_id and B_id and trip_id
        pathset_links_df = stops.add_stop_id_for_numeric_id(pathset_links_df,'A_id_num','A_id')
        pathset_links_df = stops.add_stop_id_for_numeric_id(pathset_links_df,'B_id_num','B_id')

        # get A_lat, A_lon, B_lat, B_lon
        pathset_links_df = stops.add_stop_lat_lon(pathset_links_df, id_colname="A_id", new_lat_colname="A_lat", new_lon_colname="A_lon")
        pathset_links_df = stops.add_stop_lat_lon(pathset_links_df, id_colname="B_id", new_lat_colname="B_lat", new_lon_colname="B_lon")

        ## get trip_id
        #pathset_links_df = Util.add_new_id(  input_df=pathset_links_df,          id_colname=Trip.TRIPS_COLUMN_TRIP_ID_NUM,         newid_colname=Trip.TRIPS_COLUMN_TRIP_ID,
        #                                   mapping_df=trip_id_df,        mapping_id_colname=Trip.TRIPS_COLUMN_TRIP_ID_NUM, mapping_newid_colname=Trip.TRIPS_COLUMN_TRIP_ID)

        # get route id
        # mode_num will appear in left (for non-transit links) and right (for transit link) both, so we need to consolidate
        #pathset_links_df = pd.merge(left=pathset_links_df, right=trips_df[[Trip.TRIPS_COLUMN_TRIP_ID, Trip.TRIPS_COLUMN_ROUTE_ID, Route.ROUTES_COLUMN_MODE_NUM]],
        #                                how="left", on=Trip.TRIPS_COLUMN_TRIP_ID)

        # pathset_links_df[Route.ROUTES_COLUMN_MODE_NUM] = pathset_links_df["%s_x" % Route.ROUTES_COLUMN_MODE_NUM]
        # pathset_links_df.loc[pd.notnull(pathset_links_df["%s_y" % Route.ROUTES_COLUMN_MODE_NUM]), Route.ROUTES_COLUMN_MODE_NUM] = pathset_links_df["%s_y" % Route.ROUTES_COLUMN_MODE_NUM]
        # pathset_links_df.drop(["%s_x" % Route.ROUTES_COLUMN_MODE_NUM,
        #                        "%s_y" % Route.ROUTES_COLUMN_MODE_NUM], axis=1, inplace=True)
        # # verify it's always set
        # FastTripsLogger.debug("Have %d links with no mode number set" % len(pathset_links_df.loc[ pd.isnull(pathset_links_df[Route.ROUTES_COLUMN_MODE_NUM]) ]))
        #
        # # get supply mode
        # pathset_links_df = pd.merge(left=pathset_links_df, right=modes_df[[Route.ROUTES_COLUMN_MODE_NUM, Route.ROUTES_COLUMN_MODE]], how="left")
        #
        # FastTripsLogger.debug("setup_passenger_pathsets(): pathset_paths_df and pathset_links_df dataframes constructed")
        # # FastTripsLogger.debug("\n%s" % pathset_links_df.head().to_string())

        # if len(pathset_paths_df) > 0:
        #     # create path description
        #     pathset_links_df[Passenger.PF_COL_DESCRIPTION] = pathset_links_df["A_id"] + " " + pathset_links_df[Route.ROUTES_COLUMN_MODE]
        #     pathset_links_df.loc[ pd.notnull(pathset_links_df[Trip.TRIPS_COLUMN_TRIP_ID]), Passenger.PF_COL_DESCRIPTION ] = pathset_links_df[Passenger.PF_COL_DESCRIPTION] + " "
        #     pathset_links_df.loc[ pd.notnull(pathset_links_df[Trip.TRIPS_COLUMN_TRIP_ID]),     Passenger.PF_COL_DESCRIPTION ] = pathset_links_df[Passenger.PF_COL_DESCRIPTION] + pathset_links_df[Trip.TRIPS_COLUMN_TRIP_ID]
        #     pathset_links_df.loc[ pathset_links_df[Passenger.PF_COL_LINK_MODE]==PathSet.STATE_MODE_EGRESS, Passenger.PF_COL_DESCRIPTION ] = pathset_links_df[Passenger.PF_COL_DESCRIPTION] + " " + pathset_links_df["B_id"]
        #
        #     descr_df = pathset_links_df[[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
        #                                  Passenger.PF_COL_PF_ITERATION,
        #                                  Passenger.PF_COL_PATH_NUM,
        #                                  Passenger.PF_COL_DESCRIPTION]].groupby([Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
        #                                                                          Passenger.PF_COL_PF_ITERATION,
        #                                                                          Passenger.PF_COL_PATH_NUM])[Passenger.PF_COL_DESCRIPTION].apply(lambda x:" ".join(x))
        #     descr_df = descr_df.to_frame().reset_index()
        #     # join it to pathset_paths and drop from pathset_links
        #     pathset_paths_df = pd.merge(left=pathset_paths_df, right=descr_df, how="left")
        #     pathset_links_df.drop([Passenger.PF_COL_DESCRIPTION], axis=1, inplace=True)
        # else:
        #     pathset_paths_df[Passenger.PF_COL_DESCRIPTION] = ""

        pathset_links_df.loc[:, pathset_links_df.dtypes == np.float64] = \
            pathset_links_df.loc[:, pathset_links_df.dtypes == np.float64].astype(np.float32)
        pathset_links_df.loc[:, pathset_links_df.dtypes == np.int64] = \
            pathset_links_df.loc[:, pathset_links_df.dtypes == np.int64].apply(pd.to_numeric,
                                                                               downcast='integer')

        pathset_paths_df.loc[:, pathset_paths_df.dtypes == np.float64] = \
            pathset_paths_df.loc[:, pathset_paths_df.dtypes == np.float64].astype(np.float32)
        pathset_paths_df.loc[:, pathset_paths_df.dtypes == np.int64] = \
            pathset_paths_df.loc[:, pathset_paths_df.dtypes == np.int64].apply(pd.to_numeric,
                                                                               downcast='integer')

        return pathset_paths_df, pathset_links_df


    # @staticmethod
    # def write_paths(output_dir, pathset_df, links, output_pathset_per_sim_iter, drop_debug_columns, drop_pathfinding_columns):
    #     """
    #     Write either pathset paths (if links=False) or pathset links (if links=True) as the case may be
    #     """
    #     # if simulation_iteration < 0, then this is the pathfinding result
    #     if simulation_iteration < 0:
    #         pathset_df[            "iteration"] = iteration
    #         pathset_df["pathfinding_iteration"] = pathfinding_iteration
    #         Util.write_dataframe(df=pathset_df,
    #                              name="pathset_links_df" if links else "pathset_paths_df",
    #                              output_file=os.path.join(output_dir, Passenger.PF_LINKS_CSV if links else Passenger.PF_PATHS_CSV),
    #                              append=True if ((iteration > 1) or (pathfinding_iteration > 1)) else False,
    #                              keep_duration_columns=True,
    #                              drop_debug_columns=drop_debug_columns,
    #                              drop_pathfinding_columns=drop_pathfinding_columns)
    #         pathset_df.drop(["iteration","pathfinding_iteration"], axis=1, inplace=True)
    #         return
    #
    #     # otherwise, add columns and write it
    #     pathset_df[            "iteration"] = iteration
    #     pathset_df["pathfinding_iteration"] = pathfinding_iteration
    #     pathset_df[ "simulation_iteration"] = simulation_iteration
    #
    #     # mostly we append
    #     do_append = True
    #     # but sometimes we ovewrite
    #     if output_pathset_per_sim_iter:
    #         if (iteration == 1) and (pathfinding_iteration == 1) and (simulation_iteration == 0): do_append = False
    #     else:
    #         if (iteration == 1) and (pathfinding_iteration == 1): do_append = False
    #
    #     Util.write_dataframe(df=pathset_df,
    #                          name="pathset_links_df" if links else "pathset_paths_df",
    #                          output_file=os.path.join(output_dir, Passenger.PATHSET_LINKS_CSV if links else Passenger.PATHSET_PATHS_CSV),
    #                          append=do_append,
    #                          drop_debug_columns=drop_debug_columns,
    #                          drop_pathfinding_columns=drop_pathfinding_columns)
    #     pathset_df.drop(["iteration","pathfinding_iteration","simulation_iteration"], axis=1, inplace=True)