import pytest
from pytest import raises

import numpy as np
import pandas as pd

from fasttrips import Assignment
from fasttrips import PathSet

sort_cols = ['user_class', 'purpose', 'demand_mode_type', 'demand_mode', 'supply_mode', 'weight_name']

@pytest.fixture
def sample_dataframe():
    sample_dict = {
        'user_class': ['all'] * 7,
        'purpose': ['other'] * 6 + ['work'] * 1,
        'demand_mode_type': ['access'] * 3 + ['egress'] * 3 + ['access'] * 1,
        'demand_mode': ['walk'] * 7,
        'supply_mode': ['walk_access'] * 3 + ['walk_egress'] * 3 + ['walk_access'] * 1,
        'weight_name': [
            'depart_early_cost_min.logistic',
            'depart_early_cost_min.logistic.logistic_max',
            'depart_early_cost_min.logistic.logistic_mid',
            'time_min',
            'arrive_late_cost_min.logarithmic',
            'arrive_late_cost_min.logarithmic.log_base',
            'depart_early_cost_min.exponential',
        ],
        'weight_value': [
            0.2,
            10,
            9,
            3.93,
            0.3,
            2.71828,
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
            0.2,
            3.93,
            0.3,
            0.02,
        ],
        'growth_type': ['logistic', 'constant', 'logarithmic', 'exponential'],
        'logistic_max': [10, np.nan, np.nan, np.nan],
        'logistic_mid': [9, np.nan, np.nan, np.nan],
        'log_base': [np.nan, np.nan, 2.71828, np.nan],
    }

    yield pd.DataFrame(data=expected_dict)

@pytest.mark.travis
def test_parse_weight_qualifiers(sample_dataframe, expected_dataframe):
    """
    Test to ensure that the pathweight_ft pivot for qualifiers is working as expected.
    """
    results_df = Assignment.process_weight_qualifiers(sample_dataframe)

    pd.testing.assert_frame_equal(results_df.sort_values(sort_cols).reset_index(drop=True),
                                  expected_dataframe.sort_values(sort_cols).reset_index(drop=True),
                                  check_like=True)

@pytest.mark.travis
def test_parse_weight_qualifiers_bad_key(sample_dataframe):
    """
    Test to ensure the validation on qualifier types is working.
    """
    sample_dataframe.loc[sample_dataframe[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME].str.contains('logistic') , 'weight_name'] = \
        sample_dataframe[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME].str.replace('logistic', 'logging')

    with raises(KeyError, message="Expecting KeyError"):
        Assignment.process_weight_qualifiers(sample_dataframe)

@pytest.mark.travis
def test_parse_weight_qualifiers_bad_logrithmic_qualifier(sample_dataframe):
    """
    Test to ensure the validation on qualifier types is working.
    """
    sample_dataframe.loc[sample_dataframe[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME] == 'arrive_late_cost_min.logarithmic.log_base' , 'weight_name'] = \
        'arrive_late_cost_min.logarithmic.log_base_bad'

    with raises(AssertionError, message="Expecting AssertionError"):
        Assignment.process_weight_qualifiers(sample_dataframe)

@pytest.mark.travis
def test_parse_weight_qualifiers_bad_logrithmic_qualifier_value(sample_dataframe):
    """
    Test to ensure the validation on qualifier types is working.
    """
    sample_dataframe.loc[sample_dataframe[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME] ==
                         'arrive_late_cost_min.logarithmic.log_base', 'weight_value'] = -1
    with raises(AssertionError, message="Expecting AssertionError"):
        Assignment.process_weight_qualifiers(sample_dataframe)

@pytest.mark.travis
def test_parse_weight_qualifiers_bad_logistic_qualifier(sample_dataframe):
    """
    Test to ensure the validation on qualifier types is working.
    """
    sample_dataframe.loc[sample_dataframe[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME] == 'depart_early_cost_min.logistic.logistic_max' , 'weight_name'] = \
        'depart_early_cost_min.logistic.logistic_max_bad'

    with raises(AssertionError, message="Expecting AssertionError"):
        Assignment.process_weight_qualifiers(sample_dataframe)

@pytest.mark.travis
def test_parse_weight_qualifiers_bad_logistic_max_qualifier_value(sample_dataframe):
    """
    Test to ensure the validation on qualifier types is working.
    """
    sample_dataframe.loc[sample_dataframe[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME] ==
                         'depart_early_cost_min.logistic.logistic_max', 'weight_value'] = -1
    with raises(AssertionError, message="Expecting AssertionError"):
        Assignment.process_weight_qualifiers(sample_dataframe)

@pytest.mark.travis
def test_parse_weight_qualifiers_bad_logistic_mid_qualifier_value(sample_dataframe):
    """
    Test to ensure the validation on qualifier types is working.
    """
    sample_dataframe.loc[sample_dataframe[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME] ==
                         'depart_early_cost_min.logistic.logistic_mid', 'weight_value'] = -1
    with raises(AssertionError, message="Expecting AssertionError"):
        Assignment.process_weight_qualifiers(sample_dataframe)

@pytest.mark.travis
def test_no_qualifiers(sample_dataframe):
    """
     Test to ensure that dataframe without qualifiers is return the same as it went in.
    """
    weights = sample_dataframe[~sample_dataframe[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME].str.contains('\.')].copy()
    result_df = Assignment.process_weight_qualifiers(weights)
    weights['growth_type'] = PathSet.CONSTANT_GROWTH_MODEL
    pd.testing.assert_frame_equal(result_df, weights, check_exact=True, check_frame_type=True)
