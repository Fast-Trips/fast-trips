"""Test skimming config parsing.
"""
import os
from pathlib import Path

import pytest

from fasttrips import FastTrips, Run
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
OUTPUT_FOLDER = "skimming_test"

# INPUT FILE LOCATIONS
CONFIG_FILE = os.path.join(INPUT_CONFIG, "config_skimming_ft.txt")
#INPUT_FUNCTIONS = os.path.join(INPUT_CONFIG, "config_ft.py")
INPUT_WEIGHTS = os.path.join(INPUT_CONFIG, "pathweight_ft.txt")
SKIMMING_CONFIG = os.path.join(INPUT_CONFIG, "skim_classes_ft.csv")


# skimming config is provided
# runs post-assignment w/o error
# compare results? run w/o assignment and compare to golden vals
# Skim: write method: need all fields, check data? -> maybe in test_skim_matrix?


@pytest.fixture()
def ft():
    ft_ = FastTrips(INPUT_NETWORK, INPUT_DEMAND, INPUT_WEIGHTS, CONFIG_FILE, OUTPUT_DIR, OUTPUT_FOLDER,
                   skim_config_file=SKIMMING_CONFIG)
    ft_.read_configuration()
    ft_.read_input_files()
    yield ft_


def test_runs_post_assignment():
    Run.run_fasttrips(
        input_network_dir=INPUT_NETWORK,
        input_demand_dir=INPUT_DEMAND,
        run_config=CONFIG_FILE,
        input_weights=INPUT_WEIGHTS,
        output_dir=OUTPUT_DIR,
        output_folder=OUTPUT_FOLDER,
        skim_config_file=SKIMMING_CONFIG,
        iters=1)

    files_exist()


def files_exist():
    skim_dir = Path(OUTPUT_DIR) / OUTPUT_FOLDER / "skims"
    skim_params_1 = skim_dir / "user_class_all_purpose_other_access_walk_transit_transit_egress_walk_vot_10"
    skim_params_2 = skim_dir / "user_class_all_purpose_other_access_walk_transit_transit_egress_walk_vot_20"
    assert skim_dir.is_dir()
    # assert mapping exists
    assert (skim_dir / "skim_index_to_zone_id_mapping.csv").is_file()
    assert skim_params_1.is_dir()
    assert skim_params_2.is_dir()

    all_components = Skimming.components
    for component in all_components:
        assert (skim_params_1 / f"{component}_900_910_5.omx").is_file()
        assert (skim_params_2 / f"{component}_910_920_5.omx").is_file()
