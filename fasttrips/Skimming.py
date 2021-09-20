from __future__ import division

import json
import multiprocessing as mp
import os
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

from collections import defaultdict

MANDATORY_NO_DEFAULT = object()  # default in case None is a legal default in config parsing

import sys
import datetime

import configparser
from typing import Dict, Union, Callable, Optional, List, Tuple

import numpy as np
import pandas as pd

import _fasttrips

from .Assignment import Assignment
from .Logger import FastTripsLogger, setupLogging
from .Route import Route
from .Passenger import Passenger
from .PathSet import PathSet
from .Performance import Performance
from .TAZ import TAZ
from .Trip import Trip

from collections import namedtuple

#TODO better name
SkimConfig = namedtuple(
    "SkimConfig", field_names=["user_class", "purpose", "access_mode", "transit_mode", "egress_mode"]
)

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

    start_time: int  # Skimming start time in minutes past midnight
    end_time: int  # Skimming end time in minutes past midnight
    sample_interval: int  # Sampling interval length in minutes
    skim_set : List[SkimConfig]

    # TODO Jan: add
    # time_period_start = 960
    # time_period_end = 1020
    # time_sampling_size = 5 minutes
    # which components
    # vot - use mean for now, but add option to pass in list with values, each of which will lead to calc
    #
    @staticmethod
    def read_skimming_configuration(config_fullpath):
        """
        Read the skimming related configuration parameters from :py:attr:`Assignment.CONFIGURATION_FILE`
        """

        # Note we use configobj to support nested ini structure used for skimming, not supported by
        # configparser stdlib used in Assignment.py
        from configobj import ConfigObj

        # _inspec=False is to allow stuff like "[(0, "pnr_7")]" from assignment to parse without error
        # means we lose fancy parsing and validation though
        parser = ConfigObj(config_fullpath, list_values=False, _inspec=True)
        # Read configuration from specified configuration directory
        FastTripsLogger.info("Reading configuration file %s for skimming" % config_fullpath)

        if "skimming" not in parser.keys():
            # TODO point user to documentation?
            e = "Skimming requires additional config file section '[skimming]'"
            FastTripsLogger.error(e)
            raise ValueError(e)
        elif "user_classes" not in parser["skimming"]:
            # TODO point user to documentation?
            e = "[skimming] config section requires subsection '[[user_classes]]'"
            FastTripsLogger.error(e)
            raise ValueError(e)

        start_time = Skimming._read_config_value(parser['skimming'], "time_period_start", int)
        end_time = Skimming._read_config_value(parser['skimming'], "time_period_end", int)
        sample_interval = Skimming._read_config_value(parser['skimming'], "time_period_sampling_interval", int, default=10)

        if start_time < 0:
            e = f"Start time must be specified as non-negative minutes after midnight, got {start_time}."
            FastTripsLogger.error(e)
            raise ValueError(e)

        if end_time < start_time:
            e = "Skimming sampling end time is before start time."
            FastTripsLogger.error(e)
            raise ValueError(e)
        elif sample_interval > (end_time - start_time):
            e = "Skimming sampling interval is longer than total specified duration."
            FastTripsLogger.error(e)
            raise ValueError(e)
        elif sample_interval <= 0:
            e = f"Skimming sampling interval must be a positive integer, got {sample_interval}."
            FastTripsLogger.error(e)
            raise ValueError(e)


        if not (end_time - start_time / sample_interval).is_integer():
            FastTripsLogger.warning(f"Total duration from {start_time} to {end_time} is not a multiple of \n"
                                    f"sample interval {sample_interval}. Final Skimming interval will be shorter than"
                                    "all others.")

        Skimming.start_time = start_time
        Skimming.end_time = end_time
        Skimming.sample_interval = sample_interval

        user_class_section: dict = Skimming._read_config_value(parser['skimming'], 'user_classes', typecast_func=None)

        Skimming.skim_set = Skimming._parse_skimming_user_class_options(user_class_section)

    @staticmethod
    def _read_config_value(config_dict: dict, key: str, typecast_func: Optional[Callable],
                           default=MANDATORY_NO_DEFAULT):
        if default == MANDATORY_NO_DEFAULT:
            if key not in config_dict:
                raise KeyError(f"Required key {key} missing from Skimming config")
            value = config_dict[key]
        else:
            value = config_dict.get(key, default)
        if typecast_func is not None:
            try:
                value = typecast_func(value)
            except (TypeError, ValueError):
                msg = f"Value {value} for key {key} in has non-permitted type {type(value)}"
                expected_type = getattr(typecast_func, "__name__", None)
                if expected_type is not None:
                    msg += f" (expected {expected_type})"
                FastTripsLogger.error(msg)
                raise TypeError(msg)
        return value

    @staticmethod
    def _parse_skimming_user_class_options(user_class_section: Dict) -> List[SkimConfig]:
        """
        Parse sections like
                [[user_classes]]
                    [[[real]]]
                    personal_business = [ "walk-commuter_rail-walk", "walk-commuter_rail-walk" ]

                    [[[not_real]]]
                    meal = [ "PNR-local_bus-walk", "walk-local_bus-PNR" ]
        in the skimming config
        """
        # this variable gets set as part of Run.py::run_fasttrips_skimming::run_setup()
        # so we can use it here
        weights_df = PathSet.WEIGHTS_DF
        pathweight_user_classes = weights_df['user_class'].unique()
        skims_to_produce: List[SkimConfig] = []

        if len(user_class_section) == 0:
            raise ValueError(f"Skimming config user class list is empty")

        for user_class_name, section_dict in user_class_section.items():

            if user_class_name not in pathweight_user_classes:
                raise ValueError(f"User class {user_class_name} not supplied in path weights file")

            user_class_skims = Skimming._parse_skimming_user_class_modes(user_class_name, section_dict, weights_df)
            skims_to_produce.extend(user_class_skims)
        return skims_to_produce

    @staticmethod
    def _parse_skimming_user_class_modes(user_class_name: str, user_class_dict: Dict, weights_df) -> List[SkimConfig]:
        """Parse lines like
            meal = [ "PNR-local_bus-walk", "walk-local_bus-PNR" ]
        in the skimming config.
        """
        user_class_skims = []

        legal_purposes = weights_df.loc[weights_df['user_class'] == user_class_name, 'purpose'].unique()
        if len(user_class_dict) == 0:
            raise ValueError(f"User Class {user_class_name} contains no purposes")
        for purpose, mode_list in user_class_dict.items():
            purpose_subset = weights_df[weights_df['purpose'] == purpose]
            legal_access = purpose_subset.loc[purpose_subset['demand_mode_type'] == "access", 'demand_mode'].unique()
            legal_transit = purpose_subset.loc[purpose_subset['demand_mode_type'] == "transit", 'demand_mode'].unique()
            legal_egress = purpose_subset.loc[purpose_subset['demand_mode_type'] == "egress", 'demand_mode'].unique()

            # Input validation
            if purpose not in legal_purposes:
                raise ValueError(f"Purpose {purpose} not supplied in path weights file")
            try:
                mode_list = json.loads(mode_list)
            except:
                raise ValueError(f"Mode-list for {user_class_name}:{purpose} could not be parsed. "
                                 "Should be a (valid JSON) list.")
            if not isinstance(mode_list, list):
                raise ValueError(f"Mode-list for {user_class_name}:{purpose}  could not be parsed. "
                                 "Should be a (valid JSON) list.")
            if len(mode_list) == 0:
                raise ValueError(f"Mode-list for  {user_class_name}:{purpose}  is empty")
            else:
                for i in mode_list:
                    mode_list_err = f"Mode-list {i} should be a access-transit-egress hypen delimited string, got {i}"
                    if isinstance(i, str) is False:
                        raise ValueError(
                            mode_list_err
                        )
                    sub_mode_list = i.split("-")
                    if len(sub_mode_list) != 3:
                        raise ValueError(mode_list_err)
                    for value, sub_mode, legal_list in zip(sub_mode_list, ("access", "transit", "egress"),
                                                           (legal_access, legal_transit, legal_egress)):
                        if value not in legal_list:
                            raise ValueError(f"Value {value} not in path weights file for mode sub-leg '{sub_mode}'")
                    user_class_skims.append(SkimConfig(user_class_name, purpose, *sub_mode_list))
        return user_class_skims

    @staticmethod
    def generate_skims(output_dir, FT):

        veh_trips_df = FT.trips.get_full_trips()
        # write 0-iter vehicle trips
        Assignment.write_vehicle_trips(output_dir, 0, 0, 0, veh_trips_df)

        # FOR NOW: we're starting over with empty vehicles
        Trip.reset_onboard(veh_trips_df)
        # run c++ extension
        skim_config: SkimConfig
        skim_results = {}
        for skim_config in Skimming.skim_set:
            skim_matrices = Skimming.generate_aggregated_skims(output_dir, FT, veh_trips_df, skim_config=skim_config)
            skim_results[tuple(skim_config)] = skim_matrices

        return "pathset_paths_per_sample_time", "pathset_links_per_sample_time", skim_results

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
    def extract_matrices(pathset_links_df: pd.DataFrame, FT) -> Dict[str, "Skim"]:

        index_mapping = Skimming.create_index_mapping(FT)
        num_zones = len(index_mapping)

        # skim_components = ['transfers', 'ivt', 'fare']
        # for component in skim_components:
        skim_matrices = {}

        # calculate fare skim:
        fare_skim = Skimming.calculate_skim(pathset_links_df, num_zones, index_mapping, component_name="fare")
        skim_matrices['fare'] = fare_skim
        # calculate transfer skim
        transfer_skim = Skimming.calculate_skim(pathset_links_df, num_zones, index_mapping, component_name="transfer")
        skim_matrices['transfer'] = transfer_skim

        # calculate ivt skim

        return skim_matrices

    @staticmethod
    def calculate_skim(pathset_links_df, num_zones, index_mapping, *, component_name) -> "Skim":
        od_colnames = [Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM, Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM]

        zone_list = pd.Series(index_mapping.values()).to_frame()
        zone_ods = zone_list.merge(zone_list, how='cross')
        zone_ods.columns = od_colnames
        zone_ods = zone_ods.set_index(od_colnames)
        zone_ods['skim_value'] = Skim.skim_component_default_vals[component_name]
        if component_name == "fare":
            skim_vals_coo = pathset_links_df.groupby(od_colnames)[Assignment.SIM_COL_PAX_FARE].sum().to_frame('skim_value')
        elif component_name == "transfer":
            skim_vals_coo = pathset_links_df.groupby(od_colnames).apply(
                lambda group: group.loc[group['linkmode'] == 'transfer'].shape[0]).to_frame('skim_value')#.reset_index()
        else:
            raise NotImplementedError(f"missing skim type {component_name}")

        # zone_ods has all the entries we would expect, but none of the values, so we fill them in
        zone_ods.loc[skim_vals_coo.index, 'skim_value'] = skim_vals_coo['skim_value']
        skim_vals_coo = zone_ods.reset_index()
        # TODO sort by the multindex so we don't need to reset and unset
        #   Jan I think they are already going to be in stable order because of construction of zone_ods?

        skim_values_dense = skim_vals_coo.sort_values(
            by=od_colnames,
            axis=0).set_index(od_colnames).unstack(
            Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM).fillna(Skim.skim_component_default_vals[component_name])

        skim_values_dense = skim_values_dense.values.astype(Skim.skim_component_types[component_name])
        transfer_skim = Skim(component_name, num_zones, skim_values_dense, index_mapping)

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
    def attach_fare_component(pathset_links_df: pd.DataFrame, veh_trips_df, FT) -> pd.DataFrame:
        # cost calc stuff, see Ass .2027: choose_paths_without_simulation

        # for sample_time, pathset_links_df in pathset_links_per_sample_time.items():

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

        return pathset_links_df

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
    def generate_aggregated_skims(output_dir, FT, veh_trips_df, skim_config: SkimConfig) -> Dict[str, "Skim"]:
        """
        Aggregate skims for all time periods together to produce a single average skim per skim type.

        Creates pathsets and extracts output (which are used internally and discarded)

        Protoyping skimming. Mean vot, walk access and egress, one origin to all destinations at first.


        Skimming.read_skimming_configuration(Assignment.CONFIGURATION_FILE) is a pre-requisite function call
         as it will set Attributes for sampling, and user_class/ mode configurations to run

        """
        from .Queue import ProcessManager
        FastTripsLogger.info("Skimming")
        start_time = datetime.datetime.now()
        num_processes = Assignment.NUMBER_OF_PROCESSES

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

        ########### TIMES - skim time period and sampling frequency
        # TODO: parse somewhere else, ensure consistency. Create SkimSampler?
        # skim_period_start = 900 # 3 to 5pm
        # skim_period_end = 1020
        # time_sample_step = 30 # let's do 30mins for now. should we do sampling frequency instead?
        # # dep_time = 960  # make it 4pm for now
        time_sampling_points = np.arange(Skimming.start_time, Skimming.end_time, Skimming.sample_interval)
        ############
        # c++ results; departure_time: origin_taz_num: result_dict

        # This is semi-redundant, we just need a mutable variable to have broad enough scope to get values from
        # finalizer.
        skims_path_set = {t: {} for t in time_sampling_points}



        time_indexed_skims = []

        for d_t in time_sampling_points:
            time_elapsed = datetime.datetime.now() - start_time
            FastTripsLogger.info("Skimming for time sampling point %2d. Time elapsed: %2dh:%2dm:%2ds" % (
                d_t,
                int(time_elapsed.total_seconds() / 3600),
                int((time_elapsed.total_seconds() % 3600) / 60),
                time_elapsed.total_seconds() % 60))

            path_dict = {Passenger.TRIP_LIST_COLUMN_TIME_TARGET: "departure",
                         Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME: datetime.time(d_t // 60, (d_t % 60)),
                         Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME_MIN: d_t}
            process_manager = ProcessManager(num_processes,
                                             process_worker_task=SkimmingWorkerTask(),
                                             process_worker_task_args=(output_dir, veh_trips_df)
                                             )
            try:
                # Note we thread at the origin level, rather than (origin, time), as
                # fine grained time sampling would become a memory issue - this way we can
                # store a a few skims per timestep, rather than a collection of orig- all dests pathsets per timestep.
                # Note that cost of setting up cpp extension multiple times is relatively small
                for origin in all_taz:
                    # populate tasks to process
                    orig_data = SkimmingQueueInputData(QueueData.TO_PROCESS, origin, mean_vot, d_t, skim_config,
                                                       trace=do_trace)
                    process_manager.todo_queue.put(orig_data)

                # start the processes
                process_manager.add_work_done_sentinels()
                process_manager.start_processes()

                # This function is the pass-through mechanism to get results from processes.
                def origin_finalizer(queue_data: SkimmingQueueOutputData):
                    FT.performance.add_info(-1, -1, -1, queue_data.origin, queue_data.perf_dict)
                    # we are reusing PathSet with skimming specific args above
                    # PathSet(path_dict) is generic across origins
                    pathset_this_o = PathSet(path_dict)
                    # queue_data.pathdict specific to origin
                    pathset_this_o.pathdict = queue_data.pathdict
                    skims_path_set[d_t][queue_data.origin] = pathset_this_o

                process_manager.wait_and_finalize(task_finalizer=origin_finalizer)
            except Exception as e:
                process_manager.exception_handler(e)

            # end of the time step collate paths into skims

            # extract path and path link dataframes
            # TODO pathset_paths not used for now
            _pathset_paths_df, pathset_links_df = Skimming.setup_pathsets(skims_path_set[d_t],
                                                                          FT.stops,
                                                                          FT.trips.trip_id_df,
                                                                          FT.trips.trips_df,
                                                                          FT.routes.modes_df)
            pathset_links_df = Skimming.attach_fare_component(pathset_links_df, veh_trips_df, FT)
            skim_matrices = Skimming.extract_matrices(pathset_links_df, FT)
            # now that we have skim matrix for d_t, we can drop the pathsets for this time_sample
            skims_path_set.pop(d_t)
            time_indexed_skims.append(skim_matrices)

        agg_skims = Skimming._aggregate_skims(time_indexed_skims, op=np.mean)

        time_elapsed = datetime.datetime.now() - start_time

        FastTripsLogger.info("Finished skimming.  Time elapsed: %2dh:%2dm:%2ds" % (
            int(time_elapsed.total_seconds() / 3600),
            int((time_elapsed.total_seconds() % 3600) / 60),
            time_elapsed.total_seconds() % 60))

        return agg_skims

    @staticmethod
    def _aggregate_skims(time_indexed_skims: List[Dict[str, "Skim"]], op=np.mean) -> Dict[str, "Skim"]:
        """Combine skims over time periods down to an aggregated skim."""
        # TODO will op also become a dict, with different op per skim type?

        # assume the keys are all the same, if they're not something bad has happened
        skim_types = list(time_indexed_skims[0].keys())
        skim_averages_by_type = {t: [] for t in skim_types}
        # group skims by type instead of time
        for t in time_indexed_skims:
            for skim_type, skim in t.items():
                skim_averages_by_type[skim_type].append(skim)
        # aggregate the skims to one per type
        return {skim_type: op(skim_list, axis=0) for skim_type, skim_list in skim_averages_by_type.items()}

    # TODO Jan: replace with passenger method.
    # would need to add pathmode (use None? just a string. No there's a generic transit mode called "transit
    # somewhere, use that); and pathset.outbound = False. Note neither is in constructor.
    # Also use is_skimming specific columns obviously
    @staticmethod
    def setup_pathsets(skim_path_set, stops, trip_id_df, trips_df, modes_df) -> Tuple[pd.DataFrame, pd.DataFrame]:
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

        # skim_sample_times = skim_path_set.keys()
        # path_dfs = {}
        # link_dfs = {}


        pathlist, linklist = Passenger.setup_pathsets_generic(
            pathset_dict=skim_path_set,
            pf_iteration=None,
            is_skimming=True
        )

        FastTripsLogger.debug("Skimming_setup__pathsets(): pathlist and linklist constructed")

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

        pathset_paths_df, pathset_links_df = Passenger.clean_pathset_dfs(
            pathset_paths_df,
            pathset_links_df,
            stops,
            trip_id_df,
            trips_df,
            modes_df,
            prepend_route_id_to_trip_id=False,
            is_skimming=True
        )

        return pathset_paths_df, pathset_links_df


from .Queue import QueueData, ProcessWorkerTask, ExceptionQueueData


class SkimmingQueueInputData(QueueData):
    def __init__(self, state, origin, mean_vot, d_t, skim_config:SkimConfig, trace):
        super().__init__(state)
        self.origin = origin
        self.mean_vot = mean_vot
        self.d_t = d_t
        self.user_class = skim_config.user_class
        self.purpose = skim_config.purpose
        self.access_mode = skim_config.access_mode
        self.transit_mode = skim_config.transit_mode
        self.egress_mode = skim_config.egress_mode
        self.trace = trace


class SkimmingQueueOutputData(QueueData):
    def __init__(self, state, worker_num, pathdict, perf_dict, origin):
        super().__init__(state)
        self.worker_num = worker_num
        self.pathdict = pathdict
        self.perf_dict = perf_dict
        self.origin = origin
        self.identifier = f"Origin {origin}"


class SkimmingWorkerTask(ProcessWorkerTask):

    def __call__(self,
                 output_dir,
                 veh_trips_df,
                 *args,
                 worker_num: int,
                 in_queue: "mp.Queue[SkimmingQueueInputData]",
                 out_queue: "mp.Queue[SkimmingQueueOutputData]",
                 **kwargs
                 ):
        worker_str = "_worker%02d" % worker_num

        from .FastTrips import FastTrips
        setupLogging(infoLogFilename=None,
                     debugLogFilename=os.path.join(output_dir, FastTrips.DEBUG_LOG % worker_str),
                     logToConsole=False,
                     append=False  #TODO check this, assuming false since log will be generated by assignment?
                     )
        FastTripsLogger.info("Skimming worker %2d starting" % (worker_num))

        # TODO are these used in the skimming context?
        # the child process doesn't have these set so read them
        # Assignment.CONFIGURATION_FILE = run_config
        # Assignment.CONFIGURATION_FUNCTIONS_FILE = func_file
        # Assignment.read_functions(func_file)
        # Assignment.read_configuration(run_config)

        Assignment.initialize_fasttrips_extension(0, output_dir, veh_trips_df)

        while True:
            res_flag = self.poll_queue(in_queue, out_queue, worker_num)
            if res_flag:  # exception or finished
                return

    @staticmethod
    def poll_queue(
            in_queue: "mp.Queue[SkimmingQueueInputData]",
            out_queue: "mp.Queue[QueueData]",
            worker_num: int
    ) -> bool:
        state_obj: SkimmingQueueInputData = in_queue.get()

        if state_obj.state == QueueData.WORK_DONE:
            FastTripsLogger.debug(f"Worker {worker_num} received sentinel that work is done. Terminating process.")
            out_queue.put(
                QueueData(QueueData.WORK_DONE, worker_num)
            )
            return True

        FastTripsLogger.info("Processing origin %20s" % (state_obj.origin))
        out_queue.put(
            SkimmingQueueOutputData(
                QueueData.STARTING, worker_num, pathdict=None, perf_dict=None, origin=state_obj.origin
            )
        )

        try:
            (pathdict, perf_dict) = Skimming.find_trip_based_pathset_skimming(
                state_obj.origin,
                state_obj.mean_vot,
                state_obj.d_t,
                state_obj.user_class,
                state_obj.purpose,
                state_obj.access_mode,
                state_obj.transit_mode,
                state_obj.egress_mode,
                state_obj.trace
            )
            out_queue.put(
                SkimmingQueueOutputData(QueueData.COMPLETED, worker_num, pathdict, perf_dict, origin=state_obj.origin)
            )
            return False
        except:
            FastTripsLogger.exception("Exception")
            # call it a day
            out_queue.put(ExceptionQueueData(QueueData.EXCEPTION, worker_num, message=str(sys.exc_info())))
            return True






class Skim(np.ndarray):
    """
    A single skim matrix, subclasses numpy array and has a write to omx method.
    See https://numpy.org/doc/stable/user/basics.subclassing.html for details on numpy array subclassing.
    """

    # TODO Jan: where do we define these?
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

    def __new__(cls, name, num_zones, values=None, zone_index_mapping=None):
        assert name in Skim.skim_component_types.keys(), \
            f"Skim component {name} not implemented yet, choose from {Skim.skim_component_types.keys()}"

        if values is None:
            values = np.full((num_zones, num_zones), Skim.skim_component_default_vals[name],
                             dtype=Skim.skim_component_types[name])
        obj = np.asarray(values).view(cls)

        obj.name = name
        obj.num_zones = num_zones
        obj.zone_index_mapping = zone_index_mapping

        return obj

    def __array_finalize__(self, obj):
        if obj is None: return
        self.name = getattr(obj, 'name', None)
        self.num_zones = getattr(obj, 'num_zones', None)
        self.zone_index_mapping = getattr(obj, 'zone_index_mapping', None)

    def write_to_file(self, file_root, name=None):
        """
        write skim matrix to omx file. if no name provided use default.
        """
        # if name is None:
        #     name = f"{self.name}_skim.omx"
        pass