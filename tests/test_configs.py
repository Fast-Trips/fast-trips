"""Test config changes introduced by skimming. Test options work as intended and configs
are backwards compatible with the non-skimming use case.

Ideally the non-skimming options would be explicitly validated as well



"""
import os
import tempfile

import numpy as np
import pytest

from fasttrips.Skimming import Skimming
from fasttrips.Assignment import Assignment

no_skimming_config = """
[fasttrips]
network_build_date            = 06/30/2015
trace_ids                     = [(0, "pnr_7")]
number_of_processes           = 3
debug_output_columns          = True

[pathfinding]
user_class_function           = generic_user_class
pathweights_fixed_width       = True
overlap_chunk_size            = 50
utils_conversion_factor       = 20
"""

skimming_header_config = no_skimming_config + (
    """
[skimming]
"""
)

partial_skimming_config = no_skimming_config + (
    """
[skimming]
time_period_start             =900
time_period_end               =960
"""
)


full_skimming_config = no_skimming_config + (
    """
[skimming]
time_period_start             =900
time_period_end               =960
time_period_sampling_interval =30
"""
)

bad_skimming_config0 = no_skimming_config + (
    """
[skimming]
time_period_start             =900
time_period_end               =960
time_period_sampling_interval =30.1
"""
)


bad_skimming_config1 = no_skimming_config + (
    """
[skimming]
time_period_start             =900
time_period_end               =800
time_period_sampling_interval =30
"""
)

bad_skimming_config2 = no_skimming_config + (
    """
[skimming]
time_period_start             =-39
time_period_end               =960
time_period_sampling_interval =30
"""
)

bad_skimming_config3 = no_skimming_config + (
    """
[skimming]
time_period_start             =900
time_period_end               =960
time_period_sampling_interval =0.5
"""
)

bad_skimming_config4 = no_skimming_config + (
    """
[skimming]
time_period_start             =900
time_period_end               =960
time_period_sampling_interval =2000
"""
)

test_cases = [
    no_skimming_config,
    skimming_header_config,
    partial_skimming_config,
    full_skimming_config,
    bad_skimming_config0,
    bad_skimming_config1,
    bad_skimming_config2,
    bad_skimming_config3,
    bad_skimming_config4
]

test_ids = [
    "no skimming section",
    "empty skimming section",
    "partial skimming section",
    "full skimming section",
    "bad interval float",
    "bad end-start",
    "bad start",
    "bad interval short",
    "bad interval long"
]

error_matches = {
    "no skimming section": r"Skimming requires additional config file section '\[skimming\]'",
    "bad end-start": "Skimming sampling end time is before start time.",
    "bad start": "Start time must be specified as non-negative minutes after midnight",
    "bad interval short": "Skimming requires sampling interval of at least 1 minute",
    "bad interval long": "Skimming sampling interval is longer than total specified duration.",
    "bad interval float": "Skimming requires sampling interval to be an integer."
}

warning_matches = {
    "full skimming float interval": "Skimming sampling interval converted to integer"
}


@pytest.fixture(params=test_cases, ids=test_ids)
def config_file(request):
    config_str = request.param
    # note can't use StringIO with current Assignment parser, file is hard coded as expected.

    fp = tempfile.NamedTemporaryFile(mode="w", delete=False)
    print(config_str, file=fp)
    fp.close()
    yield fp.name, request.node.callspec.id
    os.remove(fp.name)


def test_assignment_config_parsing_works(config_file):
    # ideally this test would explicitly test the assignment related options
    config_file_name, test_id = config_file
    # with open(config_file, 'r') as f:
    #     print(f.readlines())
    print(config_file_name)
    Assignment.read_configuration(config_file_name)


def test_skimming_config_parsing_values(caplog, config_file):
    config_file, test_id = config_file

    if test_id in error_matches:
        with pytest.raises(ValueError, match=error_matches[test_id]):
            Skimming.read_skimming_configuration(config_file)
    else:
        Skimming.read_skimming_configuration(config_file)
        assert Skimming.start_time == 900
        assert Skimming.end_time == 960
        assert Skimming.sample_interval == 30
