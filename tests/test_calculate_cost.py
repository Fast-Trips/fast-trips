import os
import pytest


import numpy as np
import pandas as pd

from fasttrips import PathSet, Run, Util

EXAMPLE_DIR    = os.path.join(os.getcwd(), 'fasttrips', 'Examples', 'Springfield')

# DIRECTORY LOCATIONS
OUTPUT_DIR          = os.path.join(EXAMPLE_DIR, 'output')
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

    result_set = [94.32, 3.13704, 0.51001, 127.04425]

    sample_df = pd.DataFrame(data=sample_data)

    Util.calculate_pathweight_costs(sample_df, 'test_cost')
    np.testing.assert_almost_equal(sample_df['test_cost'].values.tolist(), result_set, decimal=5)


def verify_dataframe(ctl_path, test_path, dtypes, join_cols, compare_cols):
    '''
    Method to verify that a test dataframe is equal (or nearly equal for floats)
    to a known control dataframe with specified join columns and comparison columns.

    :param ctl_path: Path to known datafame csv
    :param test_path: Path to unknown dataframe csv
    :param dtypes: (key=colname, value=datatype) Specify datatype dictionary to ensure proper comparison
    :param join_cols: List of columns to join dataframes for comparison
    :param compare_cols: List of columns to compare values.
    :return: AssertionError if dataframes do not match on compare_cols.
    '''

    df_test = pd.read_csv(ctl_path, usecols=list(dtypes.keys()), dtype=dtypes)
    df_control = pd.read_csv(test_path, usecols=list(dtypes.keys()), dtype=dtypes)

    assert len(df_control) == len(df_test), \
        'test dataframe contains unexpected number of records'

    df_join = pd.merge(left=df_control, right=df_test, how='inner',
             on=join_cols, suffixes=['_ctl', '_test'])

    assert len(df_control) == len(df_join), \
        'Test and Control have mismatched records'
    assert len(df_test) == len(df_join), \
        'Test and Control have mismatched records'

    for col in compare_cols:
        col_ctl = '{}_ctl'.format(col)
        col_test = '{}_test'.format(col)
        if ~np.isclose(df_join[col_ctl], df_join[col_test]).any():
            print '{}: Column values do not match.'.format(col)
            print df_join[~np.isclose(df_join[col_ctl], df_join[col_test])]
            assert False

@pytest.mark.skip(reason='Need to refresh the comparison csv')
def test_calculate_cost():
    '''Organizing script for Nostests to run to test calculate_cost'''

    ft = init_fasttrips(split_transit=False)
    paths_csv, links_csv = run_calculate_cost(ft)

    join_dtypes = {
        'trip_list_id_num': int,
        'pathnum': np.int64,
        'person_id': str,
    }

    compare_dtypes = {
        'sim_cost': np.float64,
        'ln_PS': np.float64,
        'logsum_component': np.float64,
        'logsum': np.float64,
        'probability': np.float64
    }

    verify_dataframe(PATHSET_PATHS_CTL, paths_csv, Util.merge_two_dicts(join_dtypes, compare_dtypes),
                         list(join_dtypes.keys()), list(compare_dtypes.keys()))

    join_dtypes = {
            'trip_list_id_num': int,
            'pathnum': np.int64,
            'linknum': np.int64,
            'person_id': str,
    }

    compare_dtypes = {
            'sim_cost': np.float64,
    }

    verify_dataframe(PATHSET_LINKS_CTL, links_csv, Util.merge_two_dicts(join_dtypes, compare_dtypes),
                         list(join_dtypes.keys()), list(compare_dtypes.keys()))

    os.unlink(paths_csv)
    os.unlink(links_csv)


def init_fasttrips(capacity_constrained=True, split_transit=False):
    '''
    Initialize the FastTrips object. The FastTrips object is necessary to
    run the static calculate_cost method.

    :param capacity_constrained: Whether the calculate_cost method should be capacity constrained
    :type capacity_constrained: bool.
    :param split_transit: Whether split_transit_links should be called as part of the calculate_cost method
    :type split_transit:
    :return: FastTrips Object

    '''

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
    '''
    Runs PathSet.calculate_cost with as a static method with configured pathset_links,
    pathset_paths, and veh_trips.

    :param ft: An initialized and loaded FastTrips object.
    :type ft: py:class:FastTrips
    :return: (str, str) system paths to pathset_links and pathset_paths csv outputs.

    '''

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

    veh_trips_df = pd.read_csv(veh_trips_loc, usecols=veh_trips_col,
                               infer_datetime_format=True, parse_dates=['arrival_time','departure_time'])

    (paths_df, links_df) = PathSet.calculate_cost(STOCHASTIC_DISPERSION, pathset_paths_df, pathset_links_df,
                                                  veh_trips_df, ft.passengers.trip_list_df, ft.routes,
                                                  ft.tazs, ft.transfers, stops=ft.stops,
                                                  reset_bump_iter=False)

    paths_df.to_csv(PATHSET_PATHS_OUT, index=False)
    links_df.to_csv(PATHSET_LINKS_OUT, index=False)

    return (PATHSET_PATHS_OUT, PATHSET_LINKS_OUT)

if __name__ == '__main__':
    import traceback
    try:
        test_growth_type_cost_calculation()
        test_calculate_cost()
    except Exception, err:
        traceback.print_exc()
