import os
import pytest
import sys

import numpy as np
import pandas as pd

from fasttrips import Util

sys.path.insert(0, os.path.join(os.getcwd(), 'scripts'))
from calculate_cost import calculate_cost

# DIRECTORY LOCATIONS
BASE_DIR            = os.path.join(os.getcwd(), 'fasttrips', 'Examples', 'misc')
TEST_FOLDER         = 'calculate_cost'
DF_DIR              = os.path.join(BASE_DIR, TEST_FOLDER)

PATHSET_PATHS_CTL   = os.path.join(DF_DIR, 'control_result_pathset_paths.csv')
PATHSET_LINKS_CTL   = os.path.join(DF_DIR, 'control_result_pathset_links.csv')


def verify_dataframe(ctl_path, test_path, dtypes, join_cols, compare_cols):
    """
    Method to verify that a test dataframe is equal (or nearly equal for floats)
    to a known control dataframe with specified join columns and comparison columns.

    :param ctl_path: Path to known datafame csv
    :param test_path: Path to unknown dataframe csv
    :param dtypes: (key=colname, value=datatype) Specify datatype dictionary to ensure proper comparison
    :param join_cols: List of columns to join dataframes for comparison
    :param compare_cols: List of columns to compare values.
    :return: AssertionError if dataframes do not match on compare_cols.
    """

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

@pytest.mark.skip(reason="Need to re-evaluate setup of this test fixture")
def test_calculate_cost():
    """Organizing script for Nostests to run to test calculate_cost"""

    paths_csv, links_csv = calculate_cost(use_ft=False)

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


if __name__ == '__main__':
    test_calculate_cost()
