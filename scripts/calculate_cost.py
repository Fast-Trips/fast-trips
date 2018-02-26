import os

import pandas as pd

from fasttrips import PathSet, Run


BASE_DIR            = os.path.join(os.getcwd(), 'fasttrips', 'Examples')
TEST_FOLDER         = 'calculate_cost'

# DIRECTORY LOCATIONS
OUTPUT_DIR          = os.path.join(BASE_DIR, 'output')
INPUT_NETWORK       = os.path.join(BASE_DIR, 'networks', 'simple')
INPUT_DEMAND        = os.path.join(BASE_DIR, 'demand', 'demand_twopaths')
DF_DIR              = os.path.join(BASE_DIR, 'misc', TEST_FOLDER)

# INPUT FILE LOCATIONS
CONFIG_FILE         = os.path.join(INPUT_DEMAND, 'config_ft.txt')
INPUT_FUNCTIONS     = os.path.join(INPUT_DEMAND, 'config_ft.py')
INPUT_WEIGHTS       = os.path.join(INPUT_DEMAND, 'pathweight_ft.txt')
PATHSET_PATHS_OUT   = os.path.join(DF_DIR, 'output_pathset_paths_calculate_cost.csv')
PATHSET_LINKS_OUT   = os.path.join(DF_DIR, 'output_pathset_links_calculate_cost.csv')


STOCHASTIC_DISPERSION = 0.5


def init_fasttrips(capacity_constrained=True, split_transit=False):
    """
    Initialize the FastTrips object. The FastTrips object is necessary to
    run the static calculate_cost method.

    TODO: Remove the FastTrips object dependency in the calculate_cost function
    :param capacity_constrained: Whether the calculate_cost method should be capacity constrained
    :type capacity_constrained: bool.
    :param split_transit: Whether split_transit_links should be called as part of the calculate_cost method
    :type split_transit:
    :return: FastTrips Object

    """

    GLOBAL_ITERATIONS = 4

    ft = Run.run_setup(
        INPUT_NETWORK,
        INPUT_DEMAND,
        INPUT_WEIGHTS,
        CONFIG_FILE,
        GLOBAL_ITERATIONS,
        OUTPUT_DIR,
        pathfinding_type='stochastic',
        capacity=capacity_constrained,
        overlap_split_transit=split_transit,
        input_functions=INPUT_FUNCTIONS,
        output_folder=TEST_FOLDER,
        trace_only=False,
    )

    ft.read_input_files()

    return ft


def run_calculate_cost(ft):
    """
    Runs PathSet.calculate_cost with as a static method with configured pathset_links,
    pathset_paths, and veh_trips.

    :param ft: An initialized and loaded FastTrips object.
    :type ft: py:class:FastTrips
    :return: (str, str) system paths to pathset_links and pathset_paths csv outputs.

    """

    ######## LOAD IN PATHSET PATHS #################
    pathset_paths_loc = os.path.join(DF_DIR, 'input_pathset_paths.csv')
    pathset_paths_cols = [
        'person_id', 'person_trip_id', 'trip_list_id_num','trace','pathdir', 'pathmode', 'pathnum'
    ]
    pathset_paths_df = pd.read_csv(pathset_paths_loc, usecols=pathset_paths_cols,
                                   dtype={'person_id':str, 'person_trip_id':str})

    ######## LOAD IN PATHSET LINKS #################
    pathset_links_loc = os.path.join(DF_DIR, 'input_pathset_links.csv')
    pathset_links_cols = [
        'person_id', 'person_trip_id', 'trip_list_id_num', 'trace', 'pathnum', 'linkmode',
        'trip_id_num', 'A_seq', 'B_seq', 'A_id_num', 'B_id_num','pf_A_time','pf_B_time','pf_linkdist','linknum', 'A_id',
        'B_id', 'trip_id', 'route_id', 'mode_num', 'mode', 'bump_iter', 'board_state','new_A_time',
        'new_B_time', 'new_linktime', 'pf_linktime','missed_xfer','board_time', 'overcap', 'alight_time',
    ]

    pathset_links_df = pd.read_csv(pathset_links_loc, usecols=pathset_links_cols,
                                   parse_dates=['new_A_time', 'new_B_time', 'pf_A_time', 'pf_B_time', 'pf_linktime', 'board_time'],
                                   infer_datetime_format=True,
                                   dtype={'person_id':str, 'person_trip_id': str})

    pathset_links_df['pf_linktime'] = pd.to_timedelta(pathset_links_df['pf_linktime'], 'm')
    pathset_links_df['new_linktime'] = pd.to_timedelta(pathset_links_df['new_linktime'], 'm')

    ######## LOAD IN VEHICLES #################
    veh_trips_loc = os.path.join(DF_DIR, 'input_veh_trips.csv')
    veh_trips_col = [
        'mode', 'mode_num', 'route_id', 'route_id_num',
        'trip_id', 'trip_id_num', 'stop_id', 'stop_id_num',
        'stop_sequence', 'arrival_time', 'departure_time'
    ]

    veh_trips_df = pd.read_csv(veh_trips_loc, usecols=veh_trips_col,
                               infer_datetime_format=True, parse_dates=['arrival_time','departure_time'])

    (paths_df, links_df) = PathSet.calculate_cost(ft, STOCHASTIC_DISPERSION, pathset_paths_df, pathset_links_df,
                                                  veh_trips_df, reset_bump_iter=False)

    paths_df.to_csv(PATHSET_PATHS_OUT, index=False)
    links_df.to_csv(PATHSET_LINKS_OUT, index=False)

    return (PATHSET_PATHS_OUT, PATHSET_LINKS_OUT)


if __name__ == '__main__':
    ft = init_fasttrips(split_transit=False)
    run_calculate_cost(ft)
