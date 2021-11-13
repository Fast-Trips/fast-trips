from __future__ import division

import json
import multiprocessing as mp
import os
from pathlib import Path
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
from .Stop import Stop
from .TAZ import TAZ
from .Trip import Trip
from .Util import Util

# from collections import namedtuple
# SkimClasses = namedtuple(
#     "SkimClasses", field_names=["start_time", "end_time", "sampling_interval", "user_class", "purpose", "access_mode",
#                                 "transit_mode", "egress_mode", "vot"]
# )

class SkimConfig(object):
    """Skimming config options.
    """
    def __init__(self, start_time, end_time, sampling_interval, vot, user_class, purpose, access_mode, transit_mode,
                 egress_mode):
        #: Skimming start time in minutes past midnight
        self.start_time = start_time
        #: Skimming end time in minutes past midnight
        self.end_time = end_time
        #: Sampling interval length in minutes
        self.sampling_interval = sampling_interval
        self.vot = vot
        self.user_class = user_class
        self.purpose = purpose
        self.access_mode = access_mode
        self.transit_mode = transit_mode
        self.egress_mode = egress_mode

        self.validate_options()

        # self.derive_properties()

    def validate_options(self):
        if self.start_time < 0:
            e = f"Start time must be specified as non-negative minutes after midnight, got {self.start_time}."
            FastTripsLogger.error(e)
            raise ValueError(e)

        if self.end_time < self.start_time:
            e = "Skimming sampling end time is before start time."
            FastTripsLogger.error(e)
            raise ValueError(e)
        elif self.sample_interval > (self.end_time - self.start_time):
            e = "Skimming sampling interval is longer than total specified duration."
            FastTripsLogger.error(e)
            raise ValueError(e)
        elif self.sample_interval <= 0:
            e = f"Skimming sampling interval must be a positive integer, got {self.sample_interval}."
            FastTripsLogger.error(e)
            raise ValueError(e)

        if not (self.end_time - self.start_time / self.sample_interval).is_integer():
            FastTripsLogger.warning(f"Total duration from {self.start_time} to {self.end_time} is not a multiple of "
                                    f"sample interval {self.sample_interval}. Final Skimming interval will be shorter "
                                    "than all others.")

        # this variable gets set as part of Run.py::run_fasttrips_skimming::run_setup() so we can use it here
        weights_df = PathSet.WEIGHTS_DF
        pathweight_user_classes = weights_df['user_class'].unique()
        if self.user_class not in pathweight_user_classes:
            raise ValueError(f"User class {self.user_class} not supplied in path weights file")

        legal_purposes = weights_df.loc[weights_df['user_class'] == self.user_class, 'purpose'].unique()
        if self.purpose not in legal_purposes:
            raise ValueError(f"Purpose {self.purpose} not supplied in path weights file")

        purpose_subset = weights_df[weights_df['purpose'] == self.purpose]
        legal_access = purpose_subset.loc[purpose_subset['demand_mode_type'] == "access", 'demand_mode'].unique()
        legal_transit = purpose_subset.loc[purpose_subset['demand_mode_type'] == "transit", 'demand_mode'].unique()
        legal_egress = purpose_subset.loc[purpose_subset['demand_mode_type'] == "egress", 'demand_mode'].unique()
        for value, sub_mode, legal_list in zip((self.access_mode, self.transit_mode, self.egress_mode),
                                               ("access", "transit", "egress"),
                                               (legal_access, legal_transit, legal_egress)):
            if value not in legal_list:
                raise ValueError(f"Value {value} not in path weights file for mode sub-leg '{sub_mode}'")

class Skimming(object):
    """
    Skimming class.

    One instance represents all the skims we could ever want, in accordance with design of the rest of the software.
    """

    #: All currently implemented skim components.
    components = ['fare', 'num_transfers', 'invehicle_time', 'access_time', 'egress_time', 'transfer_time',
                  'wait_time', 'adaption_time', 'gen_cost']

    #: List of skim configs for which separate skims will be produced, provided in skim_config_ft.csv
    skim_set: List[SkimConfig]
    # mapping from 0-based indexes to fasttrips stops_id
    index_mapping: Dict
    # number of tazs
    num_zones: int
    #: Name of the output sub-directory all skimming related data is written to
    OUTPUT_DIR = "skims"
    # name of skimming config file. TODO: make parameter
    SKIMMING_CONFIG_FILE = None

    @staticmethod
    def read_skimming_configuration(FT):
        """
        Read the skimming related configuration parameters from :py:attr:`Skimming.SKIMMING_CONFIG_FILE`
        """
        assert Skimming.SKIMMING_CONFIG_FILE is not None, f"Missing skimming config, please specify skim_config_file"

        # Read configuration from specified configuration directory
        FastTripsLogger.info("Reading configuration file %s for skimming" % Skimming.SKIMMING_CONFIG_FILE)

        skim_config_df = pd.read_csv(Skimming.SKIMMING_CONFIG_FILE)

        # name and order of fields that will be passed to SkimConfig
        field_order = ["start_time", "end_time", "sampling_interval", "vot", "user_class", "purpose", "access_mode",
                       "transit_mode", "egress_mode"]

        Skimming.skim_set = skim_config_df.apply(lambda x: SkimConfig(*x[field_order].values), axis=1).to_list()

        # create mapping from fasttrips zone index to 0-based skim array index, and also from original zone id names to
        # 0-based skimming indexes
        Skimming.index_mapping = Skimming.create_index_mapping(FT)
        Skimming.num_zones = len(Skimming.index_mapping)
        Skimming.index_to_zone_ids = Skimming.create_stop_to_zone_id_mapping(FT, Skimming.index_mapping)

    @staticmethod
    def generate_skims(output_dir, FT, veh_trips_df=None):
        """

        veh_trips_df: optional, blah. When running w/o assignment, can get from files.

        """
        if veh_trips_df is None:
            veh_trips_df = FT.trips.get_full_trips()
        # For skimming we do not want denied boardings due to capacity constraints. This also resets on board capacities
        # so if a future implementation of crowding ever uses that in deterministic pathfinding, this has to change.
        Trip.reset_onboard(veh_trips_df)

        # run c++ extension
        skim_config: SkimClasses
        skim_results = {}
        for skim_config in Skimming.skim_set:
            skim_matrices = Skimming.generate_aggregated_skims(output_dir, FT, veh_trips_df, skim_config=skim_config)
            skim_results[tuple(skim_config)] = skim_matrices

        return skim_results

    # Would it be better to have one omx file? It's just a convention but might be cleaner on disk. Would need an
    # append option in Skim.write_to_file, and handling of cases where file already exists.
    @staticmethod
    def write_skim_matrices(skim_matrices):
        """
        writes
        :param skim_matrices: {tuple(SkimClasses): {skim_type: Skim}}
        :return: nil
        """
        # omx mapping of taz ids to indexes cannot be anything but integers, however that's not what we have
        #  here, see e.g. the Springfield example. We dump the taz id - index relation to disk.
        Skimming.write_zone_id_to_index_mapping()

        output_dir = Path(Assignment.OUTPUT_DIR) / Skimming.OUTPUT_DIR

        skim_attribs = {"start_time": Skimming.start_time,
                        "end_time": Skimming.end_time,
                        "sample_interval": Skimming.sample_interval}

        for skim_config_tuple, skims in skim_matrices.items():
            # TODO Jan: Remove once debug output gets removed
            skims = skims[0]

            purp_user_output_dir = output_dir / "_".join(skim_config_tuple)
            purp_user_output_dir.mkdir(parents=True, exist_ok=True)

            FastTripsLogger.debug(f"Writing skims")
            for k, v in skims.items():
                skim_name = f"{k}_{Skimming.start_time}_{Skimming.end_time}_{Skimming.sample_interval}.omx"
                v.write_to_file(skim_name, purp_user_output_dir, attributes=skim_attribs)

    @classmethod
    def write_zone_id_to_index_mapping(cls):
        """ Necessary because omx cannot have non-integers in mapping and we might have strings"""
        out_dir = Path(Assignment.OUTPUT_DIR) / Skimming.OUTPUT_DIR
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "skim_index_to_zone_id_mapping.csv"
        pd.DataFrame(Skimming.index_to_zone_ids, columns=['zone_id']).to_csv(out_file)
        FastTripsLogger.info(f"Wrote index to zone id mapping to {out_file}, use this in case of string ids for zones")

    @staticmethod
    def create_index_mapping(FT):
        """mapping from internal fasttrips stop_id to 0-based skim index"""
        acc_eggr_links = FT.tazs.merge_access_egress()
        all_taz = np.sort(acc_eggr_links[TAZ.WALK_ACCESS_COLUMN_TAZ_NUM].unique())
        index_dict = dict(zip(np.arange(len(all_taz)), all_taz))
        return index_dict

    @staticmethod
    def create_stop_to_zone_id_mapping(FT, index_dict):
        """ fasttrips maps all stops to integer indexes internally. We need a mapping from these to the original zone
        names, and combine this with our 0-based skim mapping. This method returns a list where the entry at position i
        corresponds to skim index i, i.e. a list of zone ids that are in order of the skim mapping"""
        stops_zones_df = FT.stops.stop_id_df[[Stop.STOPS_COLUMN_STOP_ID, Stop.STOPS_COLUMN_STOP_ID_NUM]]
        stops_zones_df = stops_zones_df.loc[stops_zones_df[Stop.STOPS_COLUMN_STOP_ID_NUM].isin(index_dict.values())]
        stops_zones_df['skim_index'] = stops_zones_df[Stop.STOPS_COLUMN_STOP_ID_NUM].map(
            {v: k for k, v in index_dict.items()})
        return stops_zones_df.sort_values(by=['skim_index'])[Stop.STOPS_COLUMN_STOP_ID].values


    @staticmethod
    def extract_matrices(pathset_links_df, d_t_datetime):
        skim_matrices = {skim_name: Skimming.calculate_skim(pathset_links_df, d_t_datetime, component_name=skim_name)
                         for skim_name in Skimming.components}
        return skim_matrices

    @staticmethod
    def calculate_skim(pathset_links_df, d_t_datetime, component_name):
        od_colnames = [Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM, Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM]

        zone_list = pd.Series(Skimming.index_mapping.values()).to_frame()
        zone_ods = zone_list.merge(zone_list, how='cross')
        zone_ods.columns = od_colnames
        zone_ods = zone_ods.set_index(od_colnames)
        zone_ods['skim_value'] = Skim.skim_default_value

        if component_name == "fare":
            skim_vals_coo = pathset_links_df.groupby(od_colnames)[Assignment.SIM_COL_PAX_FARE].sum().to_frame(
                'skim_value')
        elif component_name == "num_transfers":
            skim_vals_coo = pathset_links_df.groupby(od_colnames).apply(
                lambda group: group.loc[group[Passenger.PF_COL_LINK_MODE] == PathSet.STATE_MODE_TRANSFER].shape[
                    0]).to_frame(
                'skim_value')
        elif component_name == "invehicle_time":
            skim_vals_coo = pathset_links_df.groupby(od_colnames).apply(
                lambda group: group.loc[group[Passenger.PF_COL_LINK_MODE] == PathSet.STATE_MODE_TRIP][
                    Passenger.PF_COL_LINK_TIME].sum().seconds).to_frame(
                'skim_value')
        elif component_name == "access_time":
            skim_vals_coo = pathset_links_df.groupby(od_colnames).apply(
                lambda group: group.loc[group[Passenger.PF_COL_LINK_MODE] == PathSet.STATE_MODE_ACCESS][
                    Passenger.PF_COL_LINK_TIME].sum().seconds).to_frame(
                'skim_value')
        elif component_name == "egress_time":
            skim_vals_coo = pathset_links_df.groupby(od_colnames).apply(
                lambda group: group.loc[group[Passenger.PF_COL_LINK_MODE] == PathSet.STATE_MODE_EGRESS][
                    Passenger.PF_COL_LINK_TIME].sum().seconds).to_frame(
                'skim_value')
        elif component_name == "transfer_time":
            skim_vals_coo = pathset_links_df.groupby(od_colnames).apply(
                lambda group: group.loc[group[Passenger.PF_COL_LINK_MODE] == PathSet.STATE_MODE_TRANSFER][
                    Passenger.PF_COL_LINK_TIME].sum().seconds).to_frame(
                'skim_value')
        elif component_name == "wait_time":
            skim_vals_coo = pathset_links_df.groupby(od_colnames).apply(
                lambda group: group[Passenger.PF_COL_WAIT_TIME].sum().seconds).to_frame(
                'skim_value')
        elif component_name == "adaption_time":
            skim_vals_coo = pathset_links_df.groupby(od_colnames).apply(
                lambda group: (group.loc[group[Passenger.PF_COL_LINK_MODE] == PathSet.STATE_MODE_ACCESS][
                                  Passenger.PF_COL_PAX_A_TIME].min() - d_t_datetime).seconds
            ).to_frame(
                'skim_value')
        elif component_name == "gen_cost":
            skim_vals_coo = pathset_links_df.groupby(od_colnames).apply(lambda group: group[
                Assignment.SIM_COL_PAX_COST].sum()).to_frame('skim_value')
        else:
            raise NotImplementedError(f"missing skim type {component_name}")

        # zone_ods has all the entries we would expect, but none of the values, so we fill them in
        zone_ods.loc[skim_vals_coo.index, 'skim_value'] = skim_vals_coo['skim_value']
        skim_vals_coo = zone_ods.reset_index()
        skim_values_dense = skim_vals_coo.sort_values(
            by=od_colnames,
            axis=0).set_index(od_colnames).unstack(
            Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM).fillna(Skim.skim_default_value)
        skim_values_dense = skim_values_dense.values.astype(Skim.skim_type)
        return Skim(component_name, Skimming.num_zones, skim_values_dense, Skimming.index_to_zone_ids)

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
    def trip_list_for_skimming(pathset_paths_df, d_t, skim_classes):
        """ Trip list for skimming path cost calculations with Passenger.calculate_cost.
        """
        trip_list = pathset_paths_df[[Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM,
                                      Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM]].drop_duplicates()
        trip_list[Passenger.TRIP_LIST_COLUMN_USER_CLASS] = skim_classes.user_class
        trip_list[Passenger.TRIP_LIST_COLUMN_PURPOSE] = skim_classes.purpose
        trip_list[Passenger.TRIP_LIST_COLUMN_VOT] = skim_classes.vot
        trip_list[Passenger.TRIP_LIST_COLUMN_ACCESS_MODE] = skim_classes.access_mode
        trip_list[Passenger.TRIP_LIST_COLUMN_EGRESS_MODE] = skim_classes.egress_mode
        trip_list[Passenger.TRIP_LIST_COLUMN_TRANSIT_MODE] = skim_classes.transit_mode
        trip_list[Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME_MIN] = d_t
        trip_list[Passenger.TRIP_LIST_COLUMN_ARRIVAL_TIME_MIN] = 0  # should not be used because of time target
        trip_list[Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME] = Util.parse_minutes_to_time(int(d_t)) # need
        # typecasting for np.int64, see e.g. discussion in https://github.com/pandas-dev/pandas/issues/8757
        trip_list[Passenger.TRIP_LIST_COLUMN_ARRIVAL_TIME] = Util.parse_minutes_to_time(0)  # should not be used
        # because of time target
        trip_list[Passenger.TRIP_LIST_COLUMN_TIME_TARGET] = Passenger.TIME_TARGET_DEPARTURE  # always for skimming

        trip_list[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM] = np.arange(1, trip_list.shape[0] + 1)
        trip_list[Passenger.TRIP_LIST_COLUMN_TRACE] = False

        return trip_list

    @staticmethod
    def attach_path_information_for_cost_calc(pathset_paths_df, pathset_links_df, veh_trips_df, trip_list):

        pathset_links_df = Assignment.find_passenger_vehicle_times(pathset_links_df, veh_trips_df, is_skimming=True)

        pathset_links_df[Assignment.SIM_COL_PAX_ALIGHT_DELAY_MIN] = 0
        pathset_links_df[Assignment.SIM_COL_PAX_A_TIME] = pathset_links_df[Passenger.PF_COL_PAX_A_TIME]
        pathset_links_df[Assignment.SIM_COL_PAX_B_TIME] = pathset_links_df[Passenger.PF_COL_PAX_B_TIME]
        pathset_links_df[Assignment.SIM_COL_PAX_LINK_TIME] = pathset_links_df[Passenger.PF_COL_LINK_TIME]
        pathset_links_df[Assignment.SIM_COL_PAX_WAIT_TIME] = pathset_links_df[Passenger.PF_COL_WAIT_TIME]
        pathset_links_df[Assignment.SIM_COL_PAX_MISSED_XFER] = 0

        pathset_links_df[Passenger.TRIP_LIST_COLUMN_TRACE] = False
        pathset_paths_df[Passenger.TRIP_LIST_COLUMN_TRACE] = False

        # need to attach TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM for cost calculations
        pathset_links_df = pathset_links_df.merge(trip_list[[Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM,
                                                             Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM,
                                                             Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM]],
                                                  on=[Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM,
                                                      Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM],
                                                  how='left')
        pathset_paths_df = pathset_paths_df.merge(trip_list[[Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM,
                                                             Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM,
                                                             Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM]],
                                                  on=[Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID_NUM,
                                                      Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID_NUM],
                                                  how='left')
        return pathset_paths_df, pathset_links_df

    @staticmethod
    def attach_costs(pathset_paths_df, pathset_links_df, veh_trips_df, d_t, skim_config, FT):
        # cost calc stuff, see Ass .2027: choose_paths_without_simulation
        trip_list = Skimming.trip_list_for_skimming(pathset_paths_df, d_t, skim_config)

        pathset_paths_df, pathset_links_df = Skimming.attach_path_information_for_cost_calc(pathset_paths_df,
                                                                                            pathset_links_df,
                                                                                            veh_trips_df, trip_list)
        pathset_paths_df, pathset_links_df = PathSet.calculate_cost(
            Assignment.STOCH_DISPERSION, pathset_paths_df, pathset_links_df, veh_trips_df,
            trip_list, FT.routes, FT.tazs, FT.transfers, stops=FT.stops,
            reset_bump_iter=True, is_skimming=True)

        return pathset_paths_df, pathset_links_df

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
    def generate_aggregated_skims(output_dir, FT, veh_trips_df, skim_config: SkimClasses) -> Dict[str, "Skim"]:
        """
        Aggregate skims for all time periods together to produce a single average skim per skim type.

        Creates pathsets and extracts output (which are used internally and discarded)

        Protoyping skimming. Mean vot, walk access and egress, one origin to all destinations at first.


        Skimming.read_skimming_configuration(Assignment.CONFIGURATION_FILE, FT) is a pre-requisite function call
         as it will set Attributes for sampling, and user_class/ mode configurations to run

        """
        from .Queue import ProcessManager
        FastTripsLogger.info("Skimming")
        start_time = datetime.datetime.now()
        num_processes = Assignment.NUMBER_OF_PROCESSES

        all_taz = list(Skimming.index_mapping.values())
        FastTripsLogger.debug(f"running origins {all_taz}")

        # Do we want to expose this variable for debugging?
        do_trace = False

        time_sampling_points = np.arange(Skimming.start_time, Skimming.end_time, Skimming.sample_interval)
        FastTripsLogger.debug(f"doing time points {time_sampling_points}")

        # This is semi-redundant, we just need a mutable variable to have broad enough scope to get values from
        # finalizer.
        skims_path_set = {t: {} for t in time_sampling_points}

        time_indexed_skims = {}  # {t: [] for t in time_sampling_points}
        stuff = {t: [] for t in time_sampling_points}

        for d_t in time_sampling_points:
            d_t_datetime = Util.parse_minutes_to_time(int(d_t))
            time_elapsed = datetime.datetime.now() - start_time
            FastTripsLogger.info("Skimming for time sampling point %2d. Time elapsed: %2dh:%2dm:%2ds" % (
                d_t,
                int(time_elapsed.total_seconds() / 3600),
                int((time_elapsed.total_seconds() % 3600) / 60),
                time_elapsed.total_seconds() % 60))

            path_dict = {Passenger.TRIP_LIST_COLUMN_TIME_TARGET: "departure",
                         Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME: d_t_datetime, # datetime.time(d_t // 60,(d_t % 60))
                         Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME_MIN: d_t}
            process_manager = ProcessManager(num_processes,
                                             process_worker_task=SkimmingWorkerTask(),
                                             process_worker_task_args=(output_dir, veh_trips_df,
                                                                       Assignment.CONFIGURATION_FILE,
                                                                       Assignment.CONFIGURATION_FUNCTIONS_FILE)
                                             )
            try:
                # Note we thread at the origin level, rather than (origin, time), as
                # fine grained time sampling could become a memory issue - this way we can
                # store a few skims per timestep, rather than a collection of orig- all dests pathsets per timestep.
                for origin in all_taz:
                    # populate tasks to process
                    orig_data = SkimmingQueueInputData(QueueData.TO_PROCESS, origin, d_t, skim_config,
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

            # extract path and path link dataframes. pathset_paths not used for now.
            pathset_paths_df, pathset_links_df = Skimming.setup_pathsets(skims_path_set[d_t],
                                                                          FT.stops,
                                                                          FT.trips.trip_id_df,
                                                                          FT.trips.trips_df,
                                                                          FT.routes.modes_df)

            pathset_paths_df, pathset_links_df = Skimming.attach_costs(pathset_paths_df, pathset_links_df,
                                                                       veh_trips_df, d_t, skim_config, FT)

            # TODO Jan: do we want to do some health checks here? links shouldn't have missed xfers, paths should be
            #  inbound (dir==2) and have probability one (only deterministic skimming for now)

            skim_matrices = Skimming.extract_matrices(pathset_links_df, d_t_datetime)
            # now that we have skim matrix for d_t, we can drop the pathsets for this time_sample
            skims_path_set.pop(d_t)
            #time_indexed_skims[d_t].append(skim_matrices)
            time_indexed_skims[d_t] = skim_matrices

            stuff[d_t].append(pathset_paths_df)
            stuff[d_t].append(pathset_links_df)

        # average skims over time periods.
        agg_skims = Skimming._aggregate_skims(time_indexed_skims, op="mean")

        time_elapsed = datetime.datetime.now() - start_time

        FastTripsLogger.info("Finished skimming.  Time elapsed: %2dh:%2dm:%2ds" % (
            int(time_elapsed.total_seconds() / 3600),
            int((time_elapsed.total_seconds() % 3600) / 60),
            time_elapsed.total_seconds() % 60))

        # FIXME TESTING don't return stuff and remove it
        return agg_skims, stuff

    @staticmethod
    def _aggregate_skims(time_indexed_skims: Dict[int, List[Dict[str, "Skim"]]], op="mean") -> Dict[str, "Skim"]:
        """Combine skims over time periods down to an aggregated skim."""

        # grab the keys of the first time sampling point. assume the keys are all the same, if they're not something
        # bad has happened
        skim_types = list(time_indexed_skims[next(iter(time_indexed_skims))].keys())

        skim_averages_by_type = {t: [] for t in skim_types}
        # group skims by type instead of time
        for time, t in time_indexed_skims.items():
            for skim_type, skim in t.items():
                skim_averages_by_type[skim_type].append(skim)

        # aggregate the skims to one per type.
        if op == "mean":
            # We cannot use np.mean on a list of Skims because numpy will convert the list to a 3D array and
            # therefore we'll loose Skim __dict__. However, we can use python's sum, which is just as fast or faster
            # for a few elements.
            skims = {skim_type: sum(skim_list) / len(skim_list) for skim_type, skim_list in
                     skim_averages_by_type.items()}
        else:
            # If we want to be able to use any numpy function on a list of Skims, we could create a Skim with the
            # resulting values and update the Skim.__dict__.
            raise NotImplementedError(f"Skim aggregation method {op} not implemented yet")

        return skims

    @staticmethod
    def setup_pathsets(skim_path_set, stops, trip_id_df, trips_df, modes_df) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """ Generalizes Passengers' path extraction method to convert c++ pathfinding result datastructure into
        dataframes for all origin-destination pairs.
        """
        from .PathSet import PathSet

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
    def __init__(self, state, origin, mean_vot, d_t, skim_config: SkimClasses, trace):
        super().__init__(state)
        self.origin = origin
        self.d_t = d_t
        self.user_class = skim_config.user_class
        self.purpose = skim_config.purpose
        self.access_mode = skim_config.access_mode
        self.transit_mode = skim_config.transit_mode
        self.egress_mode = skim_config.egress_mode
        self.vot = skim_config.vot
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
                 run_config,
                 func_file,
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

        # TODO Jan: are some of these unused in the skimming context?
        # the child process doesn't have these set so read them
        Assignment.CONFIGURATION_FILE = run_config
        Assignment.CONFIGURATION_FUNCTIONS_FILE = func_file
        Assignment.read_functions(func_file)
        Assignment.read_configuration(run_config)

        # TODO Jan: Think about a better way here? Don't hard-code anything in the C++ part.
        # we set the earliest departure time to the start of the time slice, do not allow any earlier departures.
        PathSet.DEPART_EARLY_ALLOWED_MIN = datetime.timedelta(minutes=0)

        Assignment.initialize_fasttrips_extension(worker_num, output_dir, veh_trips_df)

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
                state_obj.vot,
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
    A single skim matrix, subclasses numpy array to keep track of the number of zones, the name, and a list called
    index_to_zone_ids, which is a list of zone_ids as per the input files, where the position in the list corresponds to
    the skim array index.
    """

    # TODO: using inf is convenient, so we are using floats for now. Should we be using masked arrays?
    skim_type = np.float32
    skim_default_value = np.inf

    def __new__(cls, name, num_zones, values=None, index_to_zone_ids=None):
        if values is None:
            values = np.full((num_zones, num_zones), Skim.skim_default_value, dtype=Skim.skim_type)
        obj = np.asarray(values).view(cls)
        obj.name = name
        obj.num_zones = num_zones
        obj.index_to_zone_ids = index_to_zone_ids
        return obj

    def __array_finalize__(self, obj):
        if obj is None: return
        self.name = getattr(obj, 'name', None)
        self.num_zones = getattr(obj, 'num_zones', None)
        self.index_to_zone_ids = getattr(obj, 'index_to_zone_ids', None)

    def write_to_file(self, name=None, file_root=None, attributes={}):
        """
        write skim matrix to omx file. if no name provided use default.
        use mapping for zones to index
        """
        import openmatrix as omx

        if name is None:
            name = f"{self.name}.omx"

        if file_root is not None:
            name = os.path.join(file_root, name)

        with omx.open_file(name, 'w') as skim_file:
            skim_file[self.name] = self

            for k, v in self.__dict__.items():
                skim_file[self.name].attrs[k] = v

            for k, v in attributes.items():
                skim_file[self.name].attrs[k] = v

            # # omx enforces tables.UInt32Atom for mappings, but the Springfield example uses strings like Z1.
            # # could do the following, but let's just save a dict in the attributes.
            # try:
            #     int_mapping = list(map(lambda x: int(x), self.index_to_zone_ids))
            #     skim_file.create_mapping('taz', int_mapping)
            # except ValueError as e:
            #     FastTripsLogger.debug(f"Caught {e}, which probably means you have string zone ids. Therefore, "
            #                           f"openmatrix cannot create index - zone id mapping. Please use mapping "
            #                           f"provided in output directory.")
