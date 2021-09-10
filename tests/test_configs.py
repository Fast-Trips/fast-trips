"""Test config changes introduced by skimming. Test options work as intended and configs
are backwards compatible with the non-skimming use case.

Ideally the non-skimming options would be explicitly validated as well.

Note that temp files are required because the standard library configparser (used in assignment) doesn't support
io.StringIO properly.

"""
import os
import re
import tempfile

import numpy as np
import pytest

from fasttrips import Run
from fasttrips.Skimming import Skimming, SkimConfig
from fasttrips.Assignment import Assignment

# need a path-weights file to validate skimming options against
# This is coupled to the setup process so we need to run that to get this setup
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ""))
EXAMPLE_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples", "Springfield")

# DIRECTORY LOCATIONS
INPUT_NETWORK = os.path.join(EXAMPLE_DIR, "networks", "vermont")
INPUT_DEMAND = os.path.join(EXAMPLE_DIR, "demand", "general")
INPUT_CONFIG = os.path.join(EXAMPLE_DIR, "configs", "B")
OUTPUT_DIR = os.path.join(EXAMPLE_DIR, "output")

# INPUT FILE LOCATIONS
CONFIG_FILE = os.path.join(INPUT_CONFIG, "config_ft.txt")
INPUT_FUNCTIONS = os.path.join(INPUT_CONFIG, "config_ft.py")
INPUT_WEIGHTS = os.path.join(INPUT_CONFIG, "pathweight_ft.txt")

print(120 * "=")
print("config paths:")
print(os.path.abspath(CONFIG_FILE))
print(os.path.abspath(INPUT_FUNCTIONS))
print(os.path.abspath(INPUT_WEIGHTS))
print(120 * "=")

Run.run_setup(
    input_network_dir=INPUT_NETWORK,
    input_demand_dir=INPUT_DEMAND,
    run_config=CONFIG_FILE,
    input_weights=INPUT_WEIGHTS,
    output_dir=OUTPUT_DIR,
    output_folder="example",
    pathfinding_type="stochastic",
    overlap_variable="count",
    overlap_split_transit=True,
    iters=1,
    dispersion=0.50,
    trace_ids=[("0", "trip_4")],
    debug_trace_only=True,
    input_functions=INPUT_FUNCTIONS,
)


# TODO stuff less than 0?
# Test a series of input configs, validate that they don't crash the assignment parsing
# and that they catch errors and produce sensible messages

skimming_cases = []
skimming_fail_reasons = []

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

full_skimming_config = no_skimming_config + (
    """
[skimming]
time_period_start             =900
time_period_end               =960
time_period_sampling_interval =30

    [[user_classes]]
        [[[real]]]
        meal = [ "walk-local_bus-walk", "PNR-local_bus-walk" ]
        personal_business = [ "walk-commuter_rail-walk", "PNR-commuter_rail-walk" ]

        [[[not_real]]]
        meal = [ "PNR-commuter_rail-walk", "walk-commuter_rail-PNR" ]
        personal_business = [ "PNR-local_bus-walk"]

"""
)

# note this one should not fail, is also checked specially that values are right
skimming_cases.append((full_skimming_config, "full_skimming_config"))
skimming_fail_reasons.append((None, None))


skimming_cases.append((no_skimming_config, "assignment_config_no_skimming"))
skimming_fail_reasons.append((ValueError, re.escape("Skimming requires additional config file section '[skimming]'")))


skimming_header_config = no_skimming_config + (
    """
[skimming]
"""
)
skimming_cases.append((skimming_header_config, "header_only"))
skimming_fail_reasons.append(
    (ValueError, re.escape("[skimming] config section requires subsection '[[user_classes]]'"))
)

partial_skimming_config = no_skimming_config + (
    """
[skimming]
time_period_start             =900
time_period_end               =960
"""
)
skimming_cases.append((partial_skimming_config, "start_end_only"))
skimming_fail_reasons.append(
    (ValueError, re.escape("[skimming] config section requires subsection '[[user_classes]]'"))
)
# make changes to a working config, to introduce a specific error to test
bad_skimming_config0 = full_skimming_config.replace(
    "time_period_sampling_interval =30", "time_period_sampling_interval =30.34"
)
skimming_cases.append((bad_skimming_config0, "float_sample_interval"))
skimming_fail_reasons.append((TypeError, re.escape("has non-permitted type")))


bad_skimming_config1 = full_skimming_config.replace(
    "time_period_start             =900", "time_period_start             =1200"
)
skimming_cases.append((bad_skimming_config1, "start > end"))
skimming_fail_reasons.append((ValueError, re.escape("")))

bad_skimming_config2 = full_skimming_config.replace(
    "time_period_start             =900", "time_period_start             =-42"
)
skimming_cases.append((bad_skimming_config2, "start < 0"))
skimming_fail_reasons.append(
    (ValueError, "Start time must be specified as non-negative minutes after midnight, got -42")
)

bad_skimming_config2a = full_skimming_config.replace(
    "time_period_sampling_interval =30", "time_period_sampling_interval =-1"
)
skimming_cases.append((bad_skimming_config2a, "sample <0"))
skimming_fail_reasons.append((ValueError, "Skimming sampling interval must be a positive integer"))


bad_skimming_config3 = full_skimming_config.replace(
    "time_period_sampling_interval =30", "time_period_sampling_interval =0"
)
skimming_cases.append((bad_skimming_config2a, "sample =0"))
skimming_fail_reasons.append((ValueError, "Skimming sampling interval must be a positive integer"))

bad_skimming_config4 = full_skimming_config.replace(
    "time_period_sampling_interval =30", "time_period_sampling_interval =2000"
)
skimming_cases.append((bad_skimming_config4, "sample_too_large"))
skimming_fail_reasons.append((ValueError, "Skimming sampling interval is longer than total specified duration"))

bad_skimming_config5 = full_skimming_config.replace("real", "foo")
skimming_cases.append((bad_skimming_config5, "bad user class"))
skimming_fail_reasons.append((ValueError, "User class foo not supplied in path weights file"))

bad_skimming_config6 = full_skimming_config.replace("meal", "foo")
skimming_cases.append((bad_skimming_config6, "bad purpose"))
skimming_fail_reasons.append((ValueError, "Purpose foo not supplied in path weights file"))

bad_skimming_config7 = full_skimming_config.replace("PNR", "foo")
skimming_cases.append((bad_skimming_config7, "bad mode"))
skimming_fail_reasons.append((ValueError, "Value foo not in path weights file for mode sub-leg 'access'"))

bad_skimming_config7 = full_skimming_config.replace("-", "<>")
skimming_cases.append((bad_skimming_config7, "bad delimiter"))
skimming_fail_reasons.append(
    (ValueError, "Mode-list walk<>local_bus<>walk should be a access-transit-egress hypen delimited string")
)

test_ids = [i[1] for i in skimming_cases]


@pytest.fixture(params=zip(skimming_cases, skimming_fail_reasons), ids=test_ids)
def config_file_bundle(request):
    cases, reasons = request.param
    config_str, test_id = cases
    err, err_msg = reasons
    # note can't use StringIO with current Assignment parser, file is hard coded as expected.

    fp = tempfile.NamedTemporaryFile(mode="w", delete=False)
    print(config_str, file=fp)
    fp.close()
    # pass config_str through to help with debugging
    yield fp.name, config_str, test_id, err, err_msg
    os.remove(fp.name)


def test_assignment_config_parsing_works(config_file_bundle):
    # ideally there would be tests validating these options
    config_file_name = config_file_bundle[0]
    config_file_str = config_file_bundle[1]
    print(config_file_str)

    Assignment.read_configuration(config_file_name)


def test_skimming_config_parsing_catches_errors(config_file_bundle):
    config_file_name, config_str, test_id, expected_err, expected_err_msg = config_file_bundle
    print(ROOT_DIR)
    print(EXAMPLE_DIR)
    print(config_str)
    print(INPUT_WEIGHTS, os.path.exists(INPUT_WEIGHTS), os.path.isfile(INPUT_WEIGHTS))
    print(os.listdir(os.path.dirname(INPUT_WEIGHTS)))
    print("path weights")
    with open(INPUT_WEIGHTS, 'r') as f:
        print(f.read())


    if expected_err is not None:
        with pytest.raises(expected_err, match=expected_err_msg):
            Skimming.read_skimming_configuration(config_file_name)
    else:
        # Check that it runs without crashing (detailed checking of a legitimate log is below)
        Skimming.read_skimming_configuration(config_file_name)
        assert Skimming.start_time == 900
        assert Skimming.end_time == 960
        assert Skimming.sample_interval == 30



@pytest.fixture(params=full_skimming_config, ids=test_ids)
def legal_config(request):
    config_str = request.param
    # note can't use StringIO with current Assignment parser, file is hard coded as expected.

    fp = tempfile.NamedTemporaryFile(mode="w", delete=False)
    print(config_str, file=fp)
    fp.close()
    # pass config_str through to help with debugging
    yield fp.name, config_str
    os.remove(fp.name)


def check_legal_config_values(legal_config):
    Skimming.read_skimming_configuration(legal_config)

    assert Skimming.start_time == 900
    assert Skimming.end_time == 960
    assert Skimming.sample_interval == 30
    expected_skim_set = [
        SkimConfig("real", "meal", "walk", "local_bus", "walk"),
        SkimConfig("real", "meal", "PNR", "local_bus", "walk"),
        SkimConfig("real", "personal_business", "walk", "commuter_rail", "walk"),
        SkimConfig("real", "personal_business", "PNR", "commuter_rail", "walk"),
        SkimConfig("not_real", "meal", "PNR", "commuter_rail", "walk"),
        SkimConfig("not_real", "meal", "walk", "commuter_rail", "PNR"),
        SkimConfig("not_real", "personal_business", "PNR", "local_bus", "walk"),
    ]
    assert Skimming.skim_set == expected_skim_set
