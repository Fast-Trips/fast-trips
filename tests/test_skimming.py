"""Test skimming config parsing.
"""
import io
import os
from pathlib import Path
import uuid

import numpy as np
import pandas as pd
import pytest

from fasttrips import FastTrips, Run
from fasttrips.Skimming import Skimming

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ""))
EXAMPLE_DIR = os.path.join(ROOT_DIR, "fasttrips", "Examples", "Springfield")

# DIRECTORY LOCATIONS
INPUT_NETWORK = os.path.join(EXAMPLE_DIR, "networks", "vermont")
INPUT_DEMAND = os.path.join(EXAMPLE_DIR, "demand", "general")
INPUT_CONFIG = os.path.join(EXAMPLE_DIR, "configs", "A")
OUTPUT_DIR = os.path.join(EXAMPLE_DIR, "output")
OUTPUT_FOLDER = str(uuid.uuid4())

# INPUT FILE LOCATIONS
CONFIG_FILE = os.path.join(INPUT_CONFIG, "config_skimming_ft.txt")
INPUT_WEIGHTS = os.path.join(INPUT_CONFIG, "pathweight_ft.txt")
SKIMMING_CONFIG = os.path.join(INPUT_CONFIG, "skim_classes_ft.csv")


@pytest.fixture()
def ft():
    ft_ = FastTrips(INPUT_NETWORK, INPUT_DEMAND, INPUT_WEIGHTS, CONFIG_FILE, OUTPUT_DIR, OUTPUT_FOLDER,
                   skim_config_file=SKIMMING_CONFIG)
    ft_.read_configuration()
    ft_.read_input_files()
    yield ft_


# simple test to see if skimming runs post assignment and produces expected files (w/o content check)
@pytest.mark.skip(reason="segfaults on CI")
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
        assert (skim_params_1 / f"{component}_900_930_5.omx").is_file()
        assert (skim_params_2 / f"{component}_930_960_5.omx").is_file()


inf = np.inf
golden_vals = \
    {'fare': np.array([[inf, 2.75, 2.75, 2.75, 3.3],
                       [2.75, inf, 2.75, 0.9166667, 3.4166667],
                       [2.75, 2.75, inf, 2.9583333, 4.],
                       [2.75, 0.9166667, 2.75, inf, 7.2166667],
                       [4., 4., 4., 6.6666665, inf]], dtype=np.float32),
     'num_transfers': np.array([[np.inf, 0.16666667, 0., 0., 1.],
                                [0., inf, 0., 0.16666667, 0.8333333],
                                [0., 0., inf, 0., 0.],
                                [0., 0., 0., inf, 1.],
                                [1., 0.33333334, 0., 1., inf]], dtype=np.float32),
     'invehicle_time': np.array([[inf, 850., 420., 120., 886.],
                                 [820., inf, 400., 570., 808.3333],
                                 [320., 480., inf, 280., 180.],
                                 [120., 580., 300., inf, 837.6667],
                                 [760., 440., 180., 760., inf]], dtype=np.float32),
     'access_time': np.array([[inf, 533., 533., 533., 533.],
                              [444., inf, 444., 444., 521.8333],
                              [355.33334, 133., inf, 199.66667, 533.],
                              [133., 133., 133., inf, 133.],
                              [800., 800., 800., 800., inf]], dtype=np.float32),
     'egress_time': np.array([[inf, 312.33334, 134., 134., 801.],
                              [534., inf, 134., 134., 801.],
                              [534., 268., inf, 134., 801.],
                              [534., 445.33334, 134., inf, 801.],
                              [534., 756.6667, 534., 134., inf]], dtype=np.float32),
     'transfer_time': np.array([[inf, 0., 0., 0., 133.],
                                [0., inf, 0., 0., 110.833336],
                                [0., 0., inf, 0., 0.],
                                [0., 0., 0., inf, 22.166666],
                                [300., 100., 0., 100., inf]], dtype=np.float32),
     'wait_time': np.array([[inf, 0., 0., 0., 286.],
                            [0., inf, 0., 20., 268.33334],
                            [0., 0., inf, 0., 0.],
                            [0., 0., 0., inf, 457.66666],
                            [160., 20., 0., 360., inf]], dtype=np.float32),
     'adaption_time': np.array([[inf, 196., 196., 196., 196.],
                                [125., inf, 125., 65., 107.333336],
                                [154., 216., inf, 229.33333, 336.],
                                [336., 136., 236., inf, 256.],
                                [250., 250., 250., 250., inf]], dtype=np.float32),
     'gen_cost': np.array([[inf, 1.8970689, 1.34508, 1.24508, 3.742813],
                           [1.8859688, inf, 1.2219689, 1.2395355, 3.506891],
                           [1.6028577, 1.0157467, inf, 0.8867467, 2.2884133],
                           [1.24508, 1.0619688, 0.78108, inf, 3.5708797],
                           [3.9704132, 3.153858, 2.2884133, 3.5557468, inf]], dtype=np.float32),
     }

skim_index_mapping_golden = """,zone_id
0,Z1
1,Z2
2,Z3
3,Z4
4,Z5"""
skim_index_array = np.array(["Z1", "Z2", "Z3", "Z4", "Z5"])
num_zones = 5

# test w/o assignment, including value checks
def test_skimming_no_assignment():
    # make this a session level fixture and then write individual test functions for better logging and
    # possible failure comparison?
    skims = Run.run_fasttrips_skimming(
        input_network_dir=INPUT_NETWORK,
        input_demand_dir=INPUT_DEMAND,
        run_config=CONFIG_FILE,
        input_weights=INPUT_WEIGHTS,
        output_dir=OUTPUT_DIR,
        output_folder=OUTPUT_FOLDER,
        skim_config_file=SKIMMING_CONFIG,
        iters=1
    )

    files_exist()

    assert len(skims) == 2  # we have two skims

    # let's compare to golden values
    # first make sure index mappings are identical
    map_test = pd.read_csv(Path(OUTPUT_DIR) / OUTPUT_FOLDER / "skims" / "skim_index_to_zone_id_mapping.csv")
    map_golden = pd.read_csv(io.StringIO(skim_index_mapping_golden))
    pd.testing.assert_frame_equal(map_test, map_golden)
    # second compare skim values of first skim time period
    skims_1 = skims[list(skims.keys())[0]]
    for k, v in skims_1.items():
        assert np.array_equal(v, golden_vals[k]), f"{k} is not equal"
        check_attributes(v, k)


def check_attributes(skim, name):
    assert skim.name == name
    assert skim.num_zones == 5
    assert np.array_equal(skim.index_to_zone_ids, skim_index_array)