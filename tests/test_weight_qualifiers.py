import pytest
from pytest import raises

import numpy as np
import pandas as pd

from fasttrips import Assignment
from fasttrips import PathSet


@pytest.fixture(scope='module')
def sample_dataframe():
    sample_dict = {
        'user_class': ['all'] * 10,
        'purpose': ['other'] * 8 + ['work'] * 2,
        'demand_mode_type': ['access'] * 4 + ['egress'] * 4 + ['access'] * 2,
        'demand_mode': ['walk'] * 10,
        'supply_mode': ['walk_access'] * 4 + ['walk_egress'] * 4 + ['walk_access'] * 2,
        'weight_name': [
            'depart_early_cost_min',
            'depart_early_cost_min.logistic.growth_rate',
            'depart_early_cost_min.logistic.logistic_max',
            'depart_early_cost_min.logistic.logistic_mid',
            'time_min',
            'arrive_late_cost_min',
            'arrive_late_cost_min.logarithmic.growth_rate',
            'arrive_late_cost_min.logarithmic.log_base',
            'depart_early_cost_min',
            'depart_early_cost_min.exponential.growth_rate',
        ],
        'weight_value': [
            4.0,
            0.2,
            10,
            9,
            3.93,
            4.0,
            0.3,
            2.71828,
            4.0,
            0.02,
        ]
    }

    yield pd.DataFrame(data=sample_dict)


@pytest.fixture(scope='module')
def expected_dataframe():
    expected_dict = {
        'user_class': ['all'] * 4,
        'purpose': ['other'] * 3 + ['work'] * 1,
        'demand_mode_type': ['access'] * 1 + ['egress'] * 2 + ['access'] * 1,
        'demand_mode': ['walk'] * 4,
        'supply_mode': ['walk_access'] * 1 + ['walk_egress'] * 2 + ['walk_access'] * 1,
        'weight_name': [
            'depart_early_cost_min',
            'time_min',
            'arrive_late_cost_min',
            'depart_early_cost_min',
        ],
        'weight_value': [
            4.0,
            3.93,
            4.0,
            4.0,
        ],
        'growth_rate': [0.2, np.nan, 0.3, 0.02],
        'logistic_max': [10, np.nan, np.nan, np.nan],
        'logistic_mid': [9, np.nan, np.nan, np.nan],
        'log_base': [np.nan, np.nan, 2.71828, np.nan],
    }

    yield pd.DataFrame(data=expected_dict)


def test_parse_weight_qualifiers(sample_dataframe, expected_dataframe):
    """
    Test to ensure that the pathweight_ft pivot for qualifiers is working as expected.
    """
    results_df = Assignment.process_weight_qualifiers(sample_dataframe)

    pd.testing.assert_frame_equal(results_df, expected_dataframe, check_like=True)


def test_parse_weight_qualifiers_bad_key(sample_dataframe):
    """
    Test to ensure the validation on qualifier types is working.
    """
    sample_dataframe.loc[sample_dataframe[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME].str.contains('logistic') , 'weight_name'] = \
        sample_dataframe[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME].str.replace('logistic', 'logging')

    with raises(KeyError, message="Expecting KeyError"):
        Assignment.process_weight_qualifiers(sample_dataframe)


def test_no_qualifiers(sample_dataframe):
    """
     Test to ensure that dataframe without qualifiers is return the same as it went in.
    """
    weights = sample_dataframe[~sample_dataframe[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME].str.contains('\.')].copy()
    pd.testing.assert_frame_equal(Assignment.process_weight_qualifiers(weights), weights, check_exact=True, check_frame_type=True)
