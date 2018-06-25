import pandas as pd

import pytest

from fasttrips import Util

@pytest.mark.travis
def test_exponential_integral():
    test_series = pd.Series(
        data=[6., 5./3, 111./11, 45./7, 524./13],
        index=[1, 4, 7, 25, 627],
        name='cost_series'
    )

    validation_series = pd.Series(
        data=[6.9705779108, 1.7363055299, 13.0381931819, 7.5509512918, 125.9777034592],
        index=[1, 4, 7, 25, 627],
        name='cost_series'
    )

    output = Util.exponential_integration(test_series, .05)

    pd.testing.assert_series_equal(validation_series, output, check_less_precise=6)

@pytest.mark.travis
def test_logarithmic_integral():
    test_series = pd.Series(
        data=[6., 5. / 3, 111. / 11, 45. / 7, 524. / 13],
        index=[1, 4, 7, 25, 627],
        name='cost_series'
    )

    validation_series = pd.Series(
        data=[0.5497657105, 0.0684470798, 1.1970915826, 0.6108509468, 8.1800984816],
        index=[1, 4, 7, 25, 627],
        name='cost_series'
    )

    output = Util.logarithmic_integration(test_series, .05, 2)

    pd.testing.assert_series_equal(validation_series, output, check_less_precise=6)

@pytest.mark.travis
def test_logistic_integral():
    test_series = pd.Series(
        data=[6., 5. / 3, 111. / 11, 45. / 7, 524. / 13],
        index=[1, 4, 7, 25, 627],
        name='cost_series'
    )

    validation_series = pd.Series(
        data=[14.88742622, 3.873967421, 26.56146076, 16.05182001, 148.7918347],
        index=[1, 4, 7, 25, 627],
        name='cost_series'
    )

    output = Util.logistic_integration(test_series, .05, 6, 10)

    pd.testing.assert_series_equal(validation_series, output, check_less_precise=6)
