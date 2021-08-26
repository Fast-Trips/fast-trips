from __future__ import division
from builtins import range
from builtins import object

__copyright__ = "Copyright 2015 Contributing Entities"
__license__ = """
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
import sys
import datetime

import numpy as np
import pandas as pd

import _fasttrips
from .Assignment import Assignment
from .Logger import FastTripsLogger
from .Route import Route
from .Passenger import Passenger
from .PathSet import PathSet
from .Performance import Performance
from .TAZ import TAZ
from .Trip import Trip
from .Util import Util


# TODO
#  - sort out cases for when the access and egress links do not represent all skims (i.e. there are disconnected zones),
#     this needs external input
#  - add missing o,d,val during Skim creation. If there are no paths, they will be empty and we need to fill these in
#     for pandas unstack to work. I think this is faster than working with a dictionary and processing each o,d
#     individually
#  - time, sample interval, user class specific values
#  - add tests

class Skimming(object):
    """
    Skimming class.

    One instance represents all the skims we could ever want, in accordance with design of the rest of the software.

    OLD: Stores household information in :py:attr:`Passenger.households_df` and person information in
    :py:attr:`Passenger.persons_df`, which are both :py:class:`pandas.DataFrame` instances.
    """

    # TODO Jan: add
    # time_period_start = 960
    # time_period_end = 1020
    # time_sampling_size = 5 minutes
    # which components
    # vot - use mean for now, but add option to pass in list with values, each of which will lead to calc
    #

    @staticmethod
    def generate_skims(output_dir, FT):

        veh_trips_df = FT.trips.get_full_trips()
        # write 0-iter vehicle trips
        Assignment.write_vehicle_trips(output_dir, 0, 0, 0, veh_trips_df)

        # FOR NOW: we're starting over with empty vehicles
        Trip.reset_onboard(veh_trips_df)

        # run c++ extension
        skim_path_set = Skimming.generate_pathsets_skimming(output_dir, FT, veh_trips_df)

        # extract path and path link dataframes
        pathset_paths_per_sample_time, pathset_links_per_sample_time = Skimming.setup_pathsets(skim_path_set,
                                                                                               FT.stops,
                                                                                               FT.trips.trip_id_df,
                                                                                               FT.trips.trips_df,
                                                                                               FT.routes.modes_df)

        pathset_links_per_sample_time = Skimming.attach_fare_component(pathset_links_per_sample_time, veh_trips_df, FT)

        skim_matrices = Skimming.extract_matrices(pathset_links_per_sample_time, FT)

        # TODO: average skims over time sample points

        return pathset_paths_per_sample_time, pathset_links_per_sample_time, skim_matrices

    @staticmethod
    def create_index_mapping(FT):
        # TODO Jan: mapping here is from 0-based index to integer taz_num, which in turn are a mapping from whatever the
        #  original input is. Need to include that second mapping here.
        #  This is also where disconnected zones come in, so need the user to specify all TAZs. This is done for tableau
        #  outputs in some of the tests, but not required as of yet.

        # uniq_ids = np.union1d(pathset_paths_df[Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM].values,
        #                       pathset_paths_df[Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM].values)
        # index_dict = dict(zip(np.arange(len(uniq_ids)), uniq_ids))

        acc_eggr_links = FT.tazs.merge_access_egress()
        all_taz = np.sort(acc_eggr_links[TAZ.WALK_ACCESS_COLUMN_TAZ_NUM].unique())
        index_dict = dict(zip(np.arange(len(all_taz)), all_taz))
        return index_dict

    @staticmethod
    def extract_matrices(pathset_links_per_sample_time, FT):

        index_mapping = Skimming.create_index_mapping(FT)
        num_zones = len(index_mapping)

        # skim_components = ['transfers', 'ivt', 'fare']
        # for component in skim_components:
        skim_matrices = {t: [] for t in pathset_links_per_sample_time.keys()}

        for sample_time, pathset_links_df in pathset_links_per_sample_time.items():
            # calculate fare skim:
            fare_skim = Skimming.calculate_fare_skim(pathset_links_df, num_zones, index_mapping)
            skim_matrices[sample_time].append(fare_skim)
            # calculate transfer skim
            transfer_skim = Skimming.calculate_transfer_skim(pathset_links_df, num_zones, index_mapping)
            skim_matrices[sample_time].append(transfer_skim)

        # calculate ivt skim

        return skim_matrices

    @staticmethod
    def calculate_fare_skim(pathset_links_df, num_zones, index_mapping):
        # TODO Jan: this depends on there being values for each o and d
        # (technically for the cross product because if one is missing unstack will nan fill)
        # -> add missing values!
        component_name = "fare"

        fares = pathset_links_df.groupby(
            [Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM, Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM])[
            Assignment.SIM_COL_PAX_FARE].sum().to_frame('skim_value').reset_index()

        fares = fares.sort_values(
            by=[Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM, Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM],
            axis=0).set_index([Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM, 'd_taz_num']).unstack(
            Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM).fillna(Skim.skim_component_default_vals[component_name])

        transfer_values = fares.values.astype(Skim.skim_component_types[component_name])
        transfer_skim = Skim(component_name, num_zones, transfer_values, index_mapping)

        return transfer_skim

    @staticmethod
    def calculate_transfer_skim(pathset_links_df, num_zones, index_mapping):
        # TODO Jan: this depends on there being values for each o and d
        # (technically for the cross product because if one is missing unstack will nan fill)
        # -> add missing values!
        component_name = "transfer"

        num_transfers = pathset_links_df.groupby(
            [Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM, Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM]).apply(
            lambda group: group.loc[group['linkmode'] == 'transfer'].shape[0]).to_frame('skim_value').reset_index()

        transfer_values = num_transfers.sort_values(
            by=[Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM, Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM],
            axis=0).set_index([Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM, 'd_taz_num']).unstack(
            Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM).fillna(Skim.skim_component_default_vals[component_name])

        transfer_values = transfer_values.values.astype(Skim.skim_component_types[component_name])
        transfer_skim = Skim(component_name, num_zones, transfer_values, index_mapping)

        return transfer_skim

    @staticmethod
    def attach_destination_number(pathset_paths_df, pathset_links_df):
        # attach destination zone. 'egress' seems to be hard-coded in TAZ
        temp = pathset_links_df.groupby([Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM,
                                         Passenger.PF_COL_PATH_NUM]).apply(
            lambda group: group.loc[group.linkmode == 'egress'].B_id_num).to_frame(
            Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM).reset_index()[
            [Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM, Passenger.PF_COL_PATH_NUM,
             Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM]]
        pathset_paths_df = pathset_paths_df.merge(temp, on=[Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM,
                                                            Passenger.PF_COL_PATH_NUM])
        pathset_links_df = pathset_links_df.merge(temp, on=[Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM,
                                                            Passenger.PF_COL_PATH_NUM])
        return pathset_paths_df, pathset_links_df

    @staticmethod
    def attach_fare_component(pathset_links_per_sample_time, veh_trips_df, FT):
        # cost calc stuff, see Ass .2027: choose_paths_without_simulation

        for sample_time, pathset_links_df in pathset_links_per_sample_time.items():

            pathset_links_df = Assignment.find_passenger_vehicle_times(pathset_links_df, veh_trips_df, is_skimming=True)

            # instead of flag_missed_transfers(), set these to pathfinding results
            pathset_links_df[Assignment.SIM_COL_PAX_ALIGHT_DELAY_MIN] = 0
            pathset_links_df[Assignment.SIM_COL_PAX_A_TIME] = pathset_links_df[Passenger.PF_COL_PAX_A_TIME]
            pathset_links_df[Assignment.SIM_COL_PAX_B_TIME] = pathset_links_df[Passenger.PF_COL_PAX_B_TIME]
            pathset_links_df[Assignment.SIM_COL_PAX_LINK_TIME] = pathset_links_df[Passenger.PF_COL_LINK_TIME]
            pathset_links_df[Assignment.SIM_COL_PAX_WAIT_TIME] = pathset_links_df[Passenger.PF_COL_WAIT_TIME]
            pathset_links_df[Assignment.SIM_COL_PAX_MISSED_XFER] = 0

            # Add fares -- need stop zones first if they're not there.
            # We only need to do this once per pathset.
            # todo -- could remove non-transit links for this?
            stops = FT.stops
            if "A_zone_id" not in list(pathset_links_df.columns.values):
                assert (stops is not None)
                pathset_links_df = stops.add_stop_zone_id(pathset_links_df, "A_id", "A_zone_id")
                pathset_links_df = stops.add_stop_zone_id(pathset_links_df, "B_id", "B_zone_id")

            # This needs to be done fresh each time since simulation might change the board times and therefore the fare
            # periods
            pathset_links_df = FT.routes.add_fares(pathset_links_df, is_skimming=True)
            pathset_links_per_sample_time[sample_time] = pathset_links_df

        return pathset_links_per_sample_time

    @staticmethod
    def find_trip_based_pathset_skimming(origin, mean_vot, start_time, user_class, purpose,
                                         access_mode, transit_mode, egress_mode, trace):
        """
        Stuff/
        """
        (ret_ints, ret_doubles, path_costs, process_num, pf_returnstatus,
         label_iterations, num_labeled_stops, max_label_process_count,
         ms_labeling, ms_enumerating,
         bytes_workingset, bytes_privateusage, mem_timestamp) = \
            _fasttrips.find_pathset_skimming(user_class, purpose, access_mode, transit_mode, egress_mode, origin,
                                             start_time, mean_vot, 1 if trace else 0)
        FastTripsLogger.debug("Finished finding path for origin %s" % (origin))
        pathdict = {}
        row_num = 0

        for path_num in range(path_costs.shape[0]):

            pathdict[path_num] = {}
            pathdict[path_num][PathSet.PATH_KEY_COST] = path_costs[path_num, 0]
            pathdict[path_num][PathSet.PATH_KEY_FARE] = path_costs[path_num, 1]
            pathdict[path_num][PathSet.PATH_KEY_PROBABILITY] = path_costs[path_num, 2]
            pathdict[path_num][PathSet.PATH_KEY_INIT_COST] = path_costs[path_num, 3]
            pathdict[path_num][PathSet.PATH_KEY_INIT_FARE] = path_costs[path_num, 4]
            # List of (stop_id, stop_state)
            pathdict[path_num][PathSet.PATH_KEY_STATES] = []

            # print "path_num %d" % path_num

            # while we have unprocessed rows and the row is still relevant for this path_num
            while (row_num < ret_ints.shape[0]) and (ret_ints[row_num, 0] == path_num):
                # print row_num

                mode = ret_ints[row_num, 2]
                # todo
                if mode == -100:
                    mode = PathSet.STATE_MODE_ACCESS
                elif mode == -101:
                    mode = PathSet.STATE_MODE_EGRESS
                elif mode == -102:
                    mode = PathSet.STATE_MODE_TRANSFER
                elif mode == -103:
                    mode = Passenger.MODE_GENERIC_TRANSIT_NUM

                pathdict[path_num][PathSet.PATH_KEY_STATES].append((ret_ints[row_num, 1], [
                    datetime.timedelta(minutes=ret_doubles[row_num, 0]),  # label,
                    Assignment.NETWORK_BUILD_DATE_START_TIME + datetime.timedelta(minutes=ret_doubles[row_num, 1]),
                    # departure/arrival time
                    mode,  # departure/arrival mode
                    ret_ints[row_num, 3],  # trip id
                    ret_ints[row_num, 4],  # successor/predecessor
                    ret_ints[row_num, 5],  # sequence
                    ret_ints[row_num, 6],  # sequence succ/pred
                    datetime.timedelta(minutes=ret_doubles[row_num, 2]),  # link time
                    ret_doubles[row_num, 3],  # link fare
                    datetime.timedelta(minutes=ret_doubles[row_num, 4]),  # link cost
                    ret_doubles[row_num, 5],  # link dist
                    datetime.timedelta(minutes=ret_doubles[row_num, 6]),  # cost
                    Assignment.NETWORK_BUILD_DATE_START_TIME + datetime.timedelta(minutes=ret_doubles[row_num, 7])
                    # arrival/departure time
                ]))
                row_num += 1

        perf_dict = {
            Performance.PERFORMANCE_PF_COL_PROCESS_NUM: process_num,
            Performance.PERFORMANCE_PF_COL_PATHFINDING_STATUS: pf_returnstatus,
            Performance.PERFORMANCE_PF_COL_LABEL_ITERATIONS: label_iterations,
            Performance.PERFORMANCE_PF_COL_NUM_LABELED_STOPS: num_labeled_stops,
            Performance.PERFORMANCE_PF_COL_MAX_STOP_PROCESS_COUNT: max_label_process_count,
            Performance.PERFORMANCE_PF_COL_TIME_LABELING_MS: ms_labeling,
            Performance.PERFORMANCE_PF_COL_TIME_ENUMERATING_MS: ms_enumerating,
            Performance.PERFORMANCE_PF_COL_TRACED: trace,
            Performance.PERFORMANCE_PF_COL_WORKING_SET_BYTES: bytes_workingset,
            Performance.PERFORMANCE_PF_COL_PRIVATE_USAGE_BYTES: bytes_privateusage,
            Performance.PERFORMANCE_PF_COL_MEM_TIMESTAMP: datetime.datetime.fromtimestamp(mem_timestamp)
        }
        return pathdict, perf_dict

    @staticmethod
    def generate_pathsets_skimming(output_dir, FT, veh_trips_df):
        """
        Protoyping skimming. Mean vot, walk access and egress, one origin to all destinations at first.
        """
        FastTripsLogger.info("Skimming")
        start_time = datetime.datetime.now()

        Assignment.initialize_fasttrips_extension(0, output_dir, veh_trips_df)

        ####### VOT
        # this should be configurable, if a list do for each, if not provided use mean
        mean_vot = FT.passengers.trip_list_df[Passenger.TRIP_LIST_COLUMN_VOT].mean()
        #######


        # TODO: taking these for now, but if there are TAZs that don't have acc/eggr link this will have missing values
        # However, don't we want disconnections for the other ones? So maybe this is correct for the C++ extension and
        # then adding in skim values will happen outside of that?
        acc_eggr_links = FT.tazs.merge_access_egress()
        all_taz = acc_eggr_links[TAZ.WALK_ACCESS_COLUMN_TAZ_NUM].unique()

        do_trace = True
        # TEST: one departure time - this will need to be done every X mins, user specified or default 5 maybe

        ########### TIMES - skim time period and sampling frequency
        # TODO: parse somewhere else, ensure consistency. Create SkimSampler?
        # skim_period_start = 900 # 3 to 5pm
        # skim_period_end = 1020
        # time_sample_step = 30 # let's do 30mins for now. should we do sampling frequency instead?
        # # dep_time = 960  # make it 4pm for now

        time_sampling_points = [900, 930, 960, 990, 1020]
        ############
        # c++ results; departure_time: origin_taz_num: result_dict
        skims_path_set = {t: {} for t in time_sampling_points}

        ####### USER CLASS AND MODES
        # TODO: either from file or from provided data
        user_class = 'all'
        purpose = 'work'
        access_mode = 'walk'
        transit_mode = 'transit'
        egress_mode = 'walk'
        #######

        for d_t in time_sampling_points:
            d_t = int(d_t)  # TODO: this should happen during creation and validation of time sampler

            time_elapsed = datetime.datetime.now() - start_time
            FastTripsLogger.info("Skiming for time period %  Time elapsed: %2dh:%2dm:%2ds" % (
                d_t,
                int(time_elapsed.total_seconds() / 3600),
                int((time_elapsed.total_seconds() % 3600) / 60),
                time_elapsed.total_seconds() % 60))

            path_dict = {Passenger.TRIP_LIST_COLUMN_TIME_TARGET: "departure",
                         Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME: datetime.time(d_t // 60, (d_t % 60)),
                         Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME_MIN: d_t}

            for origin in all_taz:
                (pathdict, perf_dict) = \
                    Skimming.find_trip_based_pathset_skimming(
                        origin,
                        mean_vot,
                        d_t,
                        user_class,
                        purpose,
                        access_mode,
                        transit_mode,
                        egress_mode,
                        trace=do_trace
                    )

                FT.performance.add_info(-1, -1, -1, origin, perf_dict)

                pathset_this_o = PathSet(path_dict)
                pathset_this_o.pathdict = pathdict
                skims_path_set[d_t][origin] = pathset_this_o

        FastTripsLogger.info("Finished skimming.  Time elapsed: %2dh:%2dm:%2ds" % (
            int(time_elapsed.total_seconds() / 3600),
            int((time_elapsed.total_seconds() % 3600) / 60),
            time_elapsed.total_seconds() % 60))

        return skims_path_set

    # TODO Jan: replace with passenger method.
    # would need to add pathmode (use None? just a string. No there's a generic transit mode called "transit
    # somewhere, use that); and pathset.outbound = False. Note neither is in constructor.
    # Also use is_skimming specific columns obviously
    @staticmethod
    def setup_pathsets(skim_path_set, stops, trip_id_df, trips_df, modes_df):
        """ docstring from corresponding passenger method
        Converts pathfinding results (which is stored in each Passenger :py:class:`PathSet`) into two
        :py:class:`pandas.DataFrame` instances.

        Returns two :py:class:`pandas.DataFrame` instances: pathset_paths_df and pathset_links_df.

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
        from .PathSet import PathSet

        skim_sample_times = skim_path_set.keys()
        path_dfs = []
        link_dfs = []

        for sample_time in skim_sample_times:
            pathlist = []
            linklist = []
            for origin, pathset in skim_path_set[sample_time].items():

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
                    # skimming is always inbound
                    # if not pathset.outbound: state_list = list(reversed(state_list))
                    state_list = list(reversed(state_list))

                    pathlist.append([
                        origin,
                        pathset.direction,
                        # pathset.mode,  # where does this come from?
                        pathnum,
                        pathset.pathdict[pathnum][PathSet.PATH_KEY_COST],
                        pathset.pathdict[pathnum][PathSet.PATH_KEY_FARE],
                        pathset.pathdict[pathnum][PathSet.PATH_KEY_PROBABILITY],
                        pathset.pathdict[pathnum][PathSet.PATH_KEY_INIT_COST],
                        pathset.pathdict[pathnum][PathSet.PATH_KEY_INIT_FARE]
                    ])

                    link_num = 0
                    for (state_id, state) in state_list:

                        linkmode = state[PathSet.STATE_IDX_DEPARRMODE]
                        mode_num = None
                        trip_id = None
                        waittime = None

                        if linkmode in [PathSet.STATE_MODE_ACCESS, PathSet.STATE_MODE_TRANSFER, PathSet.STATE_MODE_EGRESS]:
                            mode_num = state[PathSet.STATE_IDX_TRIP]
                        else:
                            # trip mode_num will need to be joined
                            trip_id = state[PathSet.STATE_IDX_TRIP]
                            linkmode = PathSet.STATE_MODE_TRIP

                        a_id_num = state[PathSet.STATE_IDX_SUCCPRED]
                        b_id_num = state_id
                        a_seq = state[PathSet.STATE_IDX_SEQ_SUCCPRED]
                        b_seq = state[PathSet.STATE_IDX_SEQ]
                        b_time = state[PathSet.STATE_IDX_DEPARR]
                        a_time = b_time - state[PathSet.STATE_IDX_LINKTIME]
                        trip_time = state[PathSet.STATE_IDX_DEPARR] - state[PathSet.STATE_IDX_ARRDEP]

                        # trips: linktime includes wait
                        if linkmode == PathSet.STATE_MODE_TRIP:
                            waittime = state[PathSet.STATE_IDX_LINKTIME] - trip_time

                        # two trips in a row -- this shouldn't happen
                        if linkmode == PathSet.STATE_MODE_TRIP and prev_linkmode == PathSet.STATE_MODE_TRIP:
                            FastTripsLogger.warn("Two trip links in a row... this shouldn't happen.")
                            sys.exit()

                        linklist.append([
                            origin,
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
                        link_num += 1

            FastTripsLogger.debug("setup_passenger_pathsets(): pathlist and linklist constructed")

            pathset_paths_df = pd.DataFrame(pathlist, columns=[
                Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM,
                'pathdir',  # for debugging
                # ''pathmode', # for output
                # Passenger.PF_COL_PF_ITERATION,
                Passenger.PF_COL_PATH_NUM,
                PathSet.PATH_KEY_COST,
                PathSet.PATH_KEY_FARE,
                PathSet.PATH_KEY_PROBABILITY,
                PathSet.PATH_KEY_INIT_COST,
                PathSet.PATH_KEY_INIT_FARE])

            pathset_links_df = pd.DataFrame(linklist, columns=[
                Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM,
                Passenger.PF_COL_PATH_NUM,
                Passenger.PF_COL_LINK_MODE,
                Route.ROUTES_COLUMN_MODE_NUM,
                Trip.TRIPS_COLUMN_TRIP_ID_NUM,
                'A_id_num', 'B_id_num',
                'A_seq', 'B_seq',
                Passenger.PF_COL_PAX_A_TIME,
                Passenger.PF_COL_PAX_B_TIME,
                Passenger.PF_COL_LINK_TIME,
                Passenger.PF_COL_LINK_FARE,
                Passenger.PF_COL_LINK_COST,
                Passenger.PF_COL_LINK_DIST,
                Passenger.PF_COL_WAIT_TIME,
                Passenger.PF_COL_LINK_NUM])

            FastTripsLogger.debug(
                "setup_skimming_pathsets(): pathset_paths_df(%d) and pathset_links_df(%d) dataframes constructed" % (
                    len(pathset_paths_df), len(pathset_links_df)))

            # get A_id and B_id and trip_id
            pathset_links_df = stops.add_stop_id_for_numeric_id(pathset_links_df, 'A_id_num', 'A_id')
            pathset_links_df = stops.add_stop_id_for_numeric_id(pathset_links_df, 'B_id_num', 'B_id')

            # get A_lat, A_lon, B_lat, B_lon
            pathset_links_df = stops.add_stop_lat_lon(pathset_links_df, id_colname="A_id", new_lat_colname="A_lat",
                                                      new_lon_colname="A_lon")
            pathset_links_df = stops.add_stop_lat_lon(pathset_links_df, id_colname="B_id", new_lat_colname="B_lat",
                                                      new_lon_colname="B_lon")

            ## get trip_id
            pathset_links_df = Util.add_new_id(input_df=pathset_links_df, id_colname=Trip.TRIPS_COLUMN_TRIP_ID_NUM,
                                               newid_colname=Trip.TRIPS_COLUMN_TRIP_ID,
                                               mapping_df=trip_id_df, mapping_id_colname=Trip.TRIPS_COLUMN_TRIP_ID_NUM,
                                               mapping_newid_colname=Trip.TRIPS_COLUMN_TRIP_ID)

            # get route id. mode_num will appear in left (for non-transit links) and right (for transit link) both,
            # so we need to consolidate
            pathset_links_df = pd.merge(left=pathset_links_df, right=trips_df[
                [Trip.TRIPS_COLUMN_TRIP_ID, Trip.TRIPS_COLUMN_ROUTE_ID, Route.ROUTES_COLUMN_MODE_NUM]],
                                        how="left", on=Trip.TRIPS_COLUMN_TRIP_ID)
            pathset_links_df[Route.ROUTES_COLUMN_MODE_NUM] = pathset_links_df["%s_x" % Route.ROUTES_COLUMN_MODE_NUM]
            pathset_links_df.loc[
                pd.notnull(pathset_links_df["%s_y" % Route.ROUTES_COLUMN_MODE_NUM]), Route.ROUTES_COLUMN_MODE_NUM] = \
                pathset_links_df["%s_y" % Route.ROUTES_COLUMN_MODE_NUM]
            pathset_links_df.drop(["%s_x" % Route.ROUTES_COLUMN_MODE_NUM,
                                   "%s_y" % Route.ROUTES_COLUMN_MODE_NUM], axis=1, inplace=True)

            # get supply mode
            pathset_links_df = pd.merge(left=pathset_links_df,
                                        right=modes_df[[Route.ROUTES_COLUMN_MODE_NUM, Route.ROUTES_COLUMN_MODE]],
                                        how="left")

            # attach destination zone numbers
            pathset_paths_df, pathset_links_df = Skimming.attach_destination_number(pathset_paths_df, pathset_links_df)

            FastTripsLogger.debug(
                "setup_skimming_pathsets(): pathset_paths_df and pathset_links_df dataframes constructed")

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
            path_dfs[sample_time] = pathset_paths_df
            link_dfs[sample_time] = pathset_links_df

        return path_dfs, link_dfs



class Skim(object):
    """
    A single skim matrix, wraps a numpy array and has a write to omx method
    """
    skim_component_types = {
        "transfer": np.int32,
        "ivt": np.float32,
        "fare": np.float32
    }
    skim_component_default_vals = {
        "transfer": -1,  # TODO: is this a good idea?
        "ivt": np.inf,
        "fare": np.inf
    }

    def __init__(self, name, num_zones, values=None, zone_index_mapping=None):
        assert name in Skim.skim_component_types.keys(), \
            f"Skim component {name} not implemented yet, choose from {Skim.skim_component_types.keys()}"
        self.name = name
        self.num_zones = num_zones
        self.zone_index_mapping = zone_index_mapping

        if values is None:
            self.matrix = np.full((self.num_zones, self.num_zones), Skim.skim_component_default_vals[self.name],
                                  dtype=Skim.skim_component_types[self.name])
        else:
            self.set_matrix(values)

    def set_matrix(self, values):
        # TODO: check for right type, etc.
        self.matrix = values

    def set_value(self, origin, destination, value):
        # TODO: do we coerce here?
        self.matrix[origin, destination] = value

    def set_row(self, origin, values):
        """
        Sets skim row, i.e. value for a given origin to all destinations. Expects numpy array for now, maybe
        change that
        """
        assert values.shape[0] == self.num_zones
        self.matrix[origin] = values

    def set_column(self, destination, values):
        """
        Sets skim column, i.e. value for a given destination to all origins. Expects numpy array for now, maybe
        change that
        """
        assert values.shape[0] == self.num_zones
        self.matrix[:,destination] = values

    def write_to_file(self, file_root, name=None):
        """
        write skim matrix to omx file. if no name provided use default.
        """
        # if name is None:
        #     name = f"{self.name}_skim.omx"
        pass

