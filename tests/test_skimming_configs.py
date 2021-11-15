"""Test skimming config parsing.
"""
import os
import io
import re
import tempfile

import pytest

from fasttrips import FastTrips, PathSet
from fasttrips.Skimming import Skimming, SkimConfig
from fasttrips.Assignment import Assignment

# need a path-weights file to validate skimming options against
# This is coupled to the setup process so we need to run that to get this setup
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ""))
EXAMPLE_DIR = os.path.join(ROOT_DIR, "fasttrips", "Examples", "Springfield")

# DIRECTORY LOCATIONS
INPUT_NETWORK = os.path.join(EXAMPLE_DIR, "networks", "vermont")
INPUT_DEMAND = os.path.join(EXAMPLE_DIR, "demand", "general")
INPUT_CONFIG = os.path.join(EXAMPLE_DIR, "configs", "A")
OUTPUT_DIR = os.path.join(EXAMPLE_DIR, "output")

# INPUT FILE LOCATIONS
CONFIG_FILE = os.path.join(INPUT_CONFIG, "config_skimming_ft.txt")
INPUT_WEIGHTS = os.path.join(INPUT_CONFIG, "pathweight_ft.txt")
SKIMMING_CONFIG = os.path.join(INPUT_CONFIG, "skim_classes_ft.csv")

skimming_cases = []
skimming_fail_reasons = []

skim_config_works = \
  """start_time, end_time, sampling_interval, vot, user_class, purpose, access_mode, transit_mode, egress_mode
900, 910, 5, 10, all, other, walk, transit, walk
"""
skimming_cases.append((skim_config_works, "working"))
skimming_fail_reasons.append((None, None))

skim_config_no_vot = \
  """start_time, end_time, sampling_interval, user_class, purpose, access_mode, transit_mode, egress_mode
900, 910, 5, all, other, walk, transit, walk
"""
skimming_cases.append((skim_config_no_vot, "no_vot"))
skimming_fail_reasons.append((KeyError, re.escape("['vot'] not in index")))

skim_config_vot_string = \
  """start_time, end_time, sampling_interval, vot, user_class, purpose, access_mode, transit_mode, egress_mode
900, 910, 5, very large, all, other, walk, transit, walk
"""
skimming_cases.append((skim_config_vot_string, "vot_string"))
skimming_fail_reasons.append(
    (ValueError, re.escape("VoT needs to be a positive number, got  very large")))

skim_config_end_before_start = \
  """start_time, end_time, sampling_interval, vot, user_class, purpose, access_mode, transit_mode, egress_mode
920, 910, 5, 10, all, other, walk, transit, walk
"""
skimming_cases.append((skim_config_end_before_start, "end_before_start"))
skimming_fail_reasons.append((ValueError, re.escape("Skimming sampling end time is before start time.")))

skim_config_sampling_longer_period = \
  """start_time, end_time, sampling_interval, vot, user_class, purpose, access_mode, transit_mode, egress_mode
900, 910, 20, 10, all, other, walk, transit, walk
"""
skimming_cases.append((skim_config_sampling_longer_period, "sampling_too_large"))
skimming_fail_reasons.append(
    (ValueError, re.escape("Skimming sampling interval is longer than total specified duration.")))

skim_config_neg_sampling = \
  """start_time, end_time, sampling_interval, vot, user_class, purpose, access_mode, transit_mode, egress_mode
900, 910, -2, 10, all, other, walk, transit, walk
"""
skimming_cases.append((skim_config_neg_sampling, "neg_sampling"))
skimming_fail_reasons.append(
    (ValueError, re.escape("Skimming sampling interval must be a positive integer, got -2.")))


skim_config_missing_purpose = \
  """start_time, end_time, sampling_interval, vot, user_class, purpose, access_mode, transit_mode, egress_mode
900, 910, 5, 10, all, shopping, walk, transit, walk
"""

skim_config_missing_transit = \
  """start_time, end_time, sampling_interval, vot, user_class, purpose, access_mode, transit_mode, egress_mode
920, 910, 5, 10, all, shopping, walk, super_fast_rail, walk
"""

skim_config_missing_access = \
  """start_time, end_time, sampling_interval, vot, user_class, purpose, access_mode, transit_mode, egress_mode
920, 910, 5, 10, all, shopping, kiss, transit, walk
"""

skim_config_missing_access = \
  """start_time, end_time, sampling_interval, vot, user_class, purpose, access_mode, transit_mode, egress_mode
920, 910, 5, 10, all, shopping, kiss, transit, walk
"""







test_ids = [i[1] for i in skimming_cases]


@pytest.fixture(params=zip(skimming_cases, skimming_fail_reasons), ids=test_ids)
def config_file_bundle(request):
    cases, reasons = request.param
    config_str, test_id = cases
    err, err_msg = reasons
    # ON CI, PathSet.WEIGHTS_DF seems to get interfered with by other tests because it's not a local variable...
    # make sure we have the right set of global variables loaded ... need weights_df
    # We need this other global variable for that to work
    PathSet.WEIGHTS_FIXED_WIDTH = True #False
    Assignment.read_weights(weights_file=INPUT_WEIGHTS)
    # pass config_str through to help with debugging
    #Skimming.SKIMMING_CONFIG_FILE = io.StringIO(config_str)
    ft_ = FastTrips(INPUT_NETWORK, INPUT_DEMAND, INPUT_WEIGHTS, CONFIG_FILE, OUTPUT_DIR,
                    skim_config_file=io.StringIO(config_str))
    ft_.read_configuration()
    ft_.read_input_files()
    yield ft_, config_str, test_id, err, err_msg

#
# @pytest.fixture()
# def ft(skim_stuff):
#     ft_ = FastTrips(INPUT_NETWORK, INPUT_DEMAND, INPUT_WEIGHTS, CONFIG_FILE, OUTPUT_DIR,
#                     skim_config_file=skim_stuff)
#     ft_.read_configuration()
#     ft_.read_input_files()
#     yield ft_

def test_skimming_config_parsing_catches_errors(config_file_bundle):
    ft_, config_str, test_id, expected_err, expected_err_msg = config_file_bundle

    if expected_err is not None:
        with pytest.raises(expected_err, match=expected_err_msg):
            Skimming.read_skimming_configuration(ft_)
    else:
        # Check that it runs without crashing
        Skimming.read_skimming_configuration(ft_)
        vals_ = Skimming.skim_set[0]
        assert vals_.start_time == 900
        assert vals_.end_time == 910
        assert vals_.sampling_interval == 5
        assert vals_.vot == 10
        assert vals_.user_class == "all"
        assert vals_.purpose == "other"
        assert vals_.access_mode == "walk"
        assert vals_.transit_mode == "transit"
        assert vals_.egress_mode == "walk"
