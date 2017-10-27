import os
import pandas as pd

from fasttrips import Passenger, PathSet, Run
from shutil import copyfile
from test_convergence import run_capacity_test


GENERATE_FILES_FROM_TWO_PATH_EXAMPLE = False

BASE_DIR            = os.path.join(os.getcwd(), 'fasttrips', 'Examples', 'test_scenario')
TEST_FOLDER         = 'calculate_cost'

# DIRECTORY LOCATIONS
OUTPUT_DIR          = os.path.join(BASE_DIR, 'output')
INPUT_NETWORK       = os.path.join(BASE_DIR, 'network')
INPUT_DEMAND        = os.path.join(BASE_DIR, 'demand_twopaths')

# INPUT FILE LOCATIONS
CONFIG_FILE         = os.path.join(INPUT_DEMAND, 'config_ft.txt')
INPUT_FUNCTIONS     = os.path.join(INPUT_DEMAND, 'config_ft.py')
INPUT_WEIGHTS       = os.path.join(INPUT_DEMAND, 'pathweight_ft.txt')

GLOBAL_ITERATIONS   = 4

def run_calculate_cost_test():

    ft = Run.run_setup(
        INPUT_NETWORK,
        INPUT_DEMAND,
        INPUT_WEIGHTS,
        CONFIG_FILE,
        GLOBAL_ITERATIONS,
        OUTPUT_DIR,
        pathfinding_type='stochastic',
        capacity=True,
        input_functions=INPUT_FUNCTIONS,
        output_folder=TEST_FOLDER,
        trace_only=False,
    )

    ft.read_input_files()

    # Calculate Cost Data Frames
    DF_DIR = os.path.join(BASE_DIR, TEST_FOLDER)

    ######## LOAD IN PATHSET PATHS #################
    pathset_paths_loc = os.path.join(DF_DIR, 'pathset_paths.csv')
    #IGNORE: 'pf_cost','pf_fare', 'pf_probability', 'pf_initcost', 'pf_initfare', 'description', 'chosen',
    #'bump_iter', 'missed_xfer','sim_cost', 'ln_PS', 'logsum_component','logsum', 'probability',
    pathset_paths_cols = [
        'person_id', 'person_trip_id', 'trip_list_id_num','trace','pathdir', 'pathmode', 'pf_iteration', 'pathnum',
        'iteration', 'pathfinding_iteration', 'simulation_iteration'
    ]
    pathset_paths_df = pd.read_csv(pathset_paths_loc, usecols=pathset_paths_cols,
                                   dtype={'person_id':str, 'person_trip_id':str})
    pathset_paths_df = pathset_paths_df.loc[pathset_paths_df.iteration == pathset_paths_df.iteration.max()]
    pathset_paths_df = pathset_paths_df.loc[pathset_paths_df.pathfinding_iteration == pathset_paths_df.pathfinding_iteration.max()]
    pathset_paths_df = pathset_paths_df.loc[pathset_paths_df.simulation_iteration == pathset_paths_df.simulation_iteration.max()]

    ######## LOAD IN PATHSET LINKS #################
    pathset_links_loc = os.path.join(DF_DIR, 'pathset_links.csv')
    #IGNORE: 'A_seq', 'B_seq', 'pf_linktime min', 'pf_linkfare', 'pf_linkcost', 'pf_waittime min', 'A_lat', 'A_lon',
    # 'B_lat', 'B_lon', 'A_zone_id', 'B_zone_id', 'split_first', 'chosen', 'alight_delay_min', 'new_waittime min',
    # 'fare', 'fare_period', 'from_fare_period', 'transfer_fare_type', 'transfer_fare', 'free_transfer',
    # 'distance', 'sim_cost', 'overcap_frac',
    pathset_links_cols = [
        'person_id', 'person_trip_id', 'trip_list_id_num', 'trace', 'pf_iteration', 'pathnum', 'linkmode',
        'trip_id_num', 'A_id_num', 'B_id_num','pf_A_time','pf_B_time','pf_linkdist','linknum', 'A_id',
        'B_id', 'trip_id', 'route_id', 'mode_num', 'mode', 'bump_iter', 'board_state','new_A_time',
        'new_B_time', 'new_linktime min','missed_xfer','board_time', 'overcap', 'alight_time',
        'iteration', 'pathfinding_iteration', 'simulation_iteration',
    ]

    pathset_links_df = pd.read_csv(pathset_links_loc, usecols=pathset_links_cols,
                                   parse_dates=['new_A_time', 'new_B_time', 'pf_A_time', 'pf_B_time', 'board_time'],
                                   infer_datetime_format=True,
                                   dtype={'person_id':str, 'person_trip_id': str})
    pathset_links_df.rename(
        columns={'new_linktime min':'new_linktime'},
        inplace=True
    )
    pathset_links_df['pf_linktime'] = pd.to_timedelta(pathset_links_df['new_linktime'], 'm')

    pathset_links_df = pathset_links_df.loc[pathset_links_df.iteration == pathset_links_df.iteration.max()]
    pathset_links_df = pathset_links_df.loc[pathset_links_df.pathfinding_iteration == pathset_links_df.pathfinding_iteration.max()]
    pathset_links_df = pathset_links_df.loc[pathset_links_df.simulation_iteration == pathset_links_df.simulation_iteration.max()]

    ######## LOAD IN VEHICLES #################
    veh_trips_loc = os.path.join(DF_DIR, 'veh_trips.csv')
    veh_trips_df = pd.read_csv(veh_trips_loc)
    #Filter out the vehicle trip table to just the last useful iteration, pathfinding iteration, and simulation iteration
    veh_trips_df = veh_trips_df.loc[veh_trips_df.iteration == veh_trips_df.iteration.max()]
    veh_trips_df = veh_trips_df.loc[veh_trips_df.pathfinding_iteration == veh_trips_df.pathfinding_iteration.max()]
    veh_trips_df = veh_trips_df.loc[veh_trips_df.simulation_iteration == 'final']

    if Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM not in pathset_paths_df.columns.values:
        pathset_paths_df = pd.merge(left=pathset_paths_df,
                                        right=ft.passengers.trip_list_df[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                 Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                                 Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM]],
                                        how="left")
    if Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM not in pathset_links_df.columns.values:
        pathset_links_df = pd.merge(left=pathset_links_df,
                                        right=ft.passengers.trip_list_df[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                 Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                                 Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM]],
                                        how="left")

    # Calculate Cost Parameters
    stochastic_dispersion = 0.5

    (paths_df, links_df) = PathSet.calculate_cost(ft, stochastic_dispersion, pathset_paths_df, pathset_links_df,
                                                  veh_trips_df, reset_bump_iter=False)
    paths_df.to_csv(os.path.join(DF_DIR, 'pathset_paths_cost.csv'))
    links_df.to_csv(os.path.join(DF_DIR, 'pathset_links_cost.csv'))


if __name__ == '__main__':
    if GENERATE_FILES_FROM_TWO_PATH_EXAMPLE:
        out_dir, r = run_capacity_test()

        if not os.path.exists(os.path.join(BASE_DIR, TEST_FOLDER)):
            os.makedirs(os.path.join(BASE_DIR, TEST_FOLDER))

        copyfile(os.path.join(out_dir, 'pathset_paths.csv'), os.path.join(BASE_DIR, TEST_FOLDER, 'pathset_paths.csv'))
        copyfile(os.path.join(out_dir, 'pathset_links.csv'), os.path.join(BASE_DIR, TEST_FOLDER, 'pathset_links.csv'))
        copyfile(os.path.join(out_dir, 'veh_trips.csv'), os.path.join(BASE_DIR, TEST_FOLDER, 'veh_trips.csv'))

    run_calculate_cost_test()
