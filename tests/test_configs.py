"""Test skimming config parsing.
"""
import os
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
INPUT_DEMAND = os.path.join(EXAMPLE_DIR, "demand", "simpson_zorn")
INPUT_CONFIG = os.path.join(EXAMPLE_DIR, "configs", "B")
OUTPUT_DIR = os.path.join(EXAMPLE_DIR, "output")

# INPUT FILE LOCATIONS
CONFIG_FILE = os.path.join(INPUT_CONFIG, "config_ft.txt")
INPUT_FUNCTIONS = os.path.join(INPUT_CONFIG, "config_ft.py")
INPUT_WEIGHTS = os.path.join(INPUT_CONFIG, "pathweight_ft.txt")


skimming_cases = []
skimming_fail_reasons = []

#
# # note this one should not fail, is also checked specially that values are right
# skimming_cases.append((no_skimming_config, "no_skimming_config"))
# skimming_fail_reasons.append((None, None))
#
# skimming_cases.append((partial_skimming_config, "start_end_only"))
# skimming_fail_reasons.append(
#     (ValueError, re.escape("[skimming] config section requires subsection '[[user_classes]]'"))
# )
# # make changes to a working config, to introduce a specific error to test
# bad_skimming_config0 = full_skimming_config.replace(
#     "time_period_sampling_interval =30", "time_period_sampling_interval =30.34"
# )
# skimming_cases.append((bad_skimming_config0, "float_sample_interval"))
# skimming_fail_reasons.append((TypeError, re.escape("has non-permitted type")))
#
#
# bad_skimming_config1 = full_skimming_config.replace(
#     "time_period_start             =900", "time_period_start             =1200"
# )
# skimming_cases.append((bad_skimming_config1, "start > end"))
# skimming_fail_reasons.append((ValueError, re.escape("")))
#
# bad_skimming_config2 = full_skimming_config.replace(
#     "time_period_start             =900", "time_period_start             =-42"
# )
# skimming_cases.append((bad_skimming_config2, "start < 0"))
# skimming_fail_reasons.append(
#     (ValueError, "Start time must be specified as non-negative minutes after midnight, got -42")
# )
#
# bad_skimming_config2a = full_skimming_config.replace(
#     "time_period_sampling_interval =30", "time_period_sampling_interval =-1"
# )
# skimming_cases.append((bad_skimming_config2a, "sample <0"))
# skimming_fail_reasons.append((ValueError, "Skimming sampling interval must be a positive integer"))
#
#
# bad_skimming_config3 = full_skimming_config.replace(
#     "time_period_sampling_interval =30", "time_period_sampling_interval =0"
# )
# skimming_cases.append((bad_skimming_config2a, "sample =0"))
# skimming_fail_reasons.append((ValueError, "Skimming sampling interval must be a positive integer"))
#
# bad_skimming_config4 = full_skimming_config.replace(
#     "time_period_sampling_interval =30", "time_period_sampling_interval =2000"
# )
# skimming_cases.append((bad_skimming_config4, "sample_too_large"))
# skimming_fail_reasons.append((ValueError, "Skimming sampling interval is longer than total specified duration"))
#
# bad_skimming_config5 = full_skimming_config.replace("real", "foo")
# skimming_cases.append((bad_skimming_config5, "bad user class"))
# skimming_fail_reasons.append((ValueError, "User class foo not supplied in path weights file"))
#
# bad_skimming_config6 = full_skimming_config.replace("meal", "foo")
# skimming_cases.append((bad_skimming_config6, "bad purpose"))
# skimming_fail_reasons.append((ValueError, "Purpose foo not supplied in path weights file"))
#
# bad_skimming_config7 = full_skimming_config.replace("PNR", "foo")
# skimming_cases.append((bad_skimming_config7, "bad mode"))
# skimming_fail_reasons.append((ValueError, "Value foo not in path weights file for mode sub-leg 'access'"))
#
#
# test_ids = [i[1] for i in skimming_cases]
#
#
# @pytest.fixture(params=zip(skimming_cases, skimming_fail_reasons), ids=test_ids)
# def config_file_bundle(request):
#     cases, reasons = request.param
#     config_str, test_id = cases
#     err, err_msg = reasons
#     # note can't use StringIO with current Assignment parser, file is hard coded as expected.
#
#     # ON CI, PathSet.WEIGHTS_DF seems to get interfered with by other tests because it's not a local variable...
#     # make sure we have the right set of global variables loaded ... need weights_df
#     # We need this other global variable for that to work
#     PathSet.WEIGHTS_FIXED_WIDTH = False
#     Assignment.read_weights(weights_file=INPUT_WEIGHTS)
#
#     fp = tempfile.NamedTemporaryFile(mode="w", delete=False)
#     print(config_str, file=fp)
#     fp.close()
#     # pass config_str through to help with debugging
#     yield fp.name, config_str, test_id, err, err_msg
#     os.remove(fp.name)
#
#
# def test_assignment_config_parsing_works(config_file_bundle):
#     # ideally there would be tests validating these options
#     config_file_name = config_file_bundle[0]
#     config_file_str = config_file_bundle[1]
#     print(config_file_str)
#
#     Assignment.read_configuration(config_file_name)
#
#
# @pytest.fixture()
# def ft():
#     ft_ = FastTrips(INPUT_NETWORK, INPUT_DEMAND, INPUT_WEIGHTS, CONFIG_FILE, OUTPUT_DIR,
#                     input_functions=INPUT_FUNCTIONS)
#     ft_.read_configuration()
#     ft_.read_input_files()
#     yield ft_
#
#
# def test_skimming_config_parsing_catches_errors(config_file_bundle, ft):
#     config_file_name, config_str, test_id, expected_err, expected_err_msg = config_file_bundle
#
#     if expected_err is not None:
#         with pytest.raises(expected_err, match=expected_err_msg):
#             Skimming.read_skimming_configuration(config_file_name, ft)
#     else:
#         # Check that it runs without crashing (detailed checking of a legitimate log is below)
#         Skimming.read_skimming_configuration(config_file_name, ft)
#         assert Skimming.start_time == 900
#         assert Skimming.end_time == 960
#         assert Skimming.sample_interval == 30
#
#
# @pytest.fixture(params=full_skimming_config, ids=test_ids)
# def legal_config(request):
#     config_str = request.param
#     # note can't use StringIO with current Assignment parser, file is hard coded.
#
#     fp = tempfile.NamedTemporaryFile(mode="w", delete=False)
#     print(config_str, file=fp)
#     fp.close()
#     # pass config_str through to help with debugging
#     yield fp.name, config_str
#     os.remove(fp.name)
