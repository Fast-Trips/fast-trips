import os
from pathlib import Path
import uuid

import pytest

from fasttrips import Run
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


def test_skimming_post_assignment():
    """
    Test skimming runs after assignment and produces files in expected locations.
    """

    Run.run_fasttrips(
        input_network_dir=INPUT_NETWORK,
        input_demand_dir=INPUT_DEMAND,
        run_config=CONFIG_FILE,
        input_weights=INPUT_WEIGHTS,
        output_dir=OUTPUT_DIR,
        output_folder=OUTPUT_FOLDER,
        skim_config_file=SKIMMING_CONFIG,
        iters=1,
    )

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


if __name__ == '__main__':
    test_skimming_post_assignment()
