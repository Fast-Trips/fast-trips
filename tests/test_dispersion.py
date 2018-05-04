import os
import pytest
from fasttrips import Run

@pytest.mark.parametrize("dispersion_parameter", [1.0,0.3])

def test_dispersion(dispersion_parameter):

    EXAMPLES_DIR   = os.path.join(os.getcwd(), "fasttrips", "Examples",)

    INPUT_NETWORK  = os.path.join(EXAMPLES_DIR, "networks", 'simple')
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR, 'demand', "demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR, "output")

    full_output_dir = os.path.join(OUTPUT_DIR, "test_dispers_%4.2f" % dispersion_parameter)
    if not os.path.exists(full_output_dir):
        os.mkdir(full_output_dir)

    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORK,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND,"config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
        output_dir       = OUTPUT_DIR,
        max_stop_process_count = 2,
        pf_iters         = 2,
        overlap_variable = "None",
        output_folder    = "test_dispers_%4.2f" % dispersion_parameter,
        pathfinding_type = "stochastic",
        iters            = 1,
        dispersion       = dispersion_parameter,
        test_size        = 2)

    assert r["passengers_arrived"] > 0

if __name__ == "__main__":
    for dp in [1.0,0.3]:
        test_dispersion(dp)
