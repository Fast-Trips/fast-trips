import os
import pytest
from fasttrips import Run

OVERLAP_VARIABLES = ["None","count","distance","time"]

@pytest.mark.parametrize("overlap_var", OVERLAP_VARIABLES)
@pytest.mark.parametrize("split_links", [False, True])

@pytest.mark.travis
def test_overlap(overlap_var, split_links ):
    EXAMPLES_DIR  = os.path.join(os.getcwd(), "fasttrips", "Examples")

    INPUT_NETWORK = os.path.join(EXAMPLES_DIR, "networks", 'simple')
    INPUT_DEMAND  = os.path.join(EXAMPLES_DIR, 'demand', "demand_reg")
    OUTPUT_DIR    = os.path.join(EXAMPLES_DIR, "output")


    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORK,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND, "config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND, "pathweight_ft.txt"),
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_overlap_var-%s_split-%s" % (overlap_var, split_links),
        overlap_variable = overlap_var,
        max_stop_process_count = 2,
        pf_iters         = 2,
        pathfinding_type = "stochastic",
        overlap_split_transit = split_links,
        iters            = 1,
        dispersion       = 0.50,
        num_trips        = 5)

    assert r["passengers_arrived"] > 0
