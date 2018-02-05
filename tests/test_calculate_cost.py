import os
import pytest


import numpy as np
import pandas as pd

from fasttrips import PathSet, Run, Util

EXAMPLE_DIR    = os.path.join(os.getcwd(), 'fasttrips', 'Examples', 'Springfield')

# DIRECTORY LOCATIONS
INPUT_NETWORK       = os.path.join(EXAMPLE_DIR, 'networks', 'vermont')
INPUT_DEMAND        = os.path.join(EXAMPLE_DIR, 'demand', 'simpson_zorn')
INPUT_CONFIG        = os.path.join(EXAMPLE_DIR, 'configs', 'B')
OUTPUT_DIR          = os.path.join(EXAMPLE_DIR, 'output')
TEST_FOLDER         = os.path.join(EXAMPLE_DIR, 'output','calculate_cost')
DF_DIR              = os.path.join(EXAMPLE_DIR, 'misc', 'test_controls', 'calculate_cost')

# INPUT FILE LOCATIONS
CONFIG_FILE         = os.path.join(INPUT_CONFIG, 'config_ft.txt')
INPUT_FUNCTIONS     = os.path.join(INPUT_CONFIG, 'config_ft.py')
INPUT_WEIGHTS       = os.path.join(INPUT_CONFIG, 'pathweight_ft.txt')
PATHSET_PATHS_OUT   = os.path.join(DF_DIR, 'output_pathset_paths_calculate_cost.csv')
PATHSET_LINKS_OUT   = os.path.join(DF_DIR, 'output_pathset_links_calculate_cost.csv')

STOCHASTIC_DISPERSION = 0.5

PATHSET_PATHS_CTL   = os.path.join(DF_DIR, 'control_result_pathset_paths.csv')
PATHSET_LINKS_CTL   = os.path.join(DF_DIR, 'control_result_pathset_links.csv')

@pytest.mark.travis
def test_growth_type_cost_calculation():
    sample_data = {
        'weight_value': [3.93, .03, .3, .56],
        'var_value': [24., 3, 2.35, 15.431],
        'growth_type': ['constant', 'exponential', 'logarithmic', 'logistic'],
        'log_base': [np.nan, np.nan, np.exp(1), np.nan],
        'logistic_max': [np.nan, np.nan, np.nan, 10.1],
        'logistic_mid': [np.nan, np.nan, np.nan, 2.45]
    }

    compare_dtypes = {
            'sim_cost': np.float64,
    }

    verify_dataframe(PATHSET_LINKS_CTL, links_csv, Util.merge_two_dicts(join_dtypes, compare_dtypes),
                         list(join_dtypes.keys()), list(compare_dtypes.keys()))

    os.unlink(paths_csv)
    os.unlink(links_csv)


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

    scenario_dir = os.path.join(INPUT_NETWORK, 'simple')

    ft = Run.run_setup(
        scenario_dir,
        INPUT_DEMAND,
        INPUT_WEIGHTS,
        CONFIG_FILE,
        GLOBAL_ITERATIONS,
        OUTPUT_DIR,
        pathfinding_type='stochastic',
        learning_convergence=False,
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
        'B_id', 'trip_id', 'route_id', 'mode_num', 'mode','new_A_time', # 'bump_iter', 'board_state',
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

    sample_df = pd.DataFrame(data=sample_data)

    Util.calculate_pathweight_costs(sample_df, 'test_cost')
    np.testing.assert_almost_equal(sample_df['test_cost'].values.tolist(), result_set, decimal=5)


if __name__ == '__main__':
    import traceback
    try:
        test_growth_type_cost_calculation()
    except Exception, err:
        traceback.print_exc()
