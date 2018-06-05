import os
import pytest
from fasttrips import Run


def test_psrc():
    """
    Test to ensure that more complex network, PSRC, is working. Also a
    useful benchmark for Partridge loader compared to transitfeed.
    """
    EXAMPLES_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples")

    INPUT_NETWORK = os.path.join(EXAMPLES_DIR, "networks", 'psrc_1_1')
    INPUT_DEMAND = os.path.join(EXAMPLES_DIR, "demand", "psrc_1_1")
    OUTPUT_DIR = os.path.join(EXAMPLES_DIR, "output")

    Run.run_fasttrips(
        input_network_dir=INPUT_NETWORK,
        input_demand_dir=INPUT_DEMAND,
        run_config=os.path.join(INPUT_DEMAND, "config_ft.txt"),
        input_functions=os.path.join(INPUT_DEMAND, 'config_ft.py'),
        input_weights=os.path.join(INPUT_DEMAND, "pathweight_ft.txt"),
        output_dir=OUTPUT_DIR,
        max_stop_process_count = 2,
        pf_iters = 2,
        overlap_variable = "None",
        output_folder="test_psrc",
        pathfinding_type="stochastic",
        capacity=True,
        iters=1,
        OVERLAP = "None",
        dispersion=1.0,
        test_size  = 2
    )


if __name__ == '__main__':
    test_psrc()
