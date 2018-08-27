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

    result_set = [94.32, 3.13704, 0.51001, 127.04425]

    sample_df = pd.DataFrame(data=sample_data)

    Util.calculate_pathweight_costs(sample_df, 'test_cost')
    np.testing.assert_almost_equal(sample_df['test_cost'].values.tolist(), result_set, decimal=5)


if __name__ == '__main__':
    import traceback
    try:
        test_growth_type_cost_calculation()
    except Exception, err:
        traceback.print_exc()
