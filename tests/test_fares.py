import os
import pytest
from fasttrips import Run


ignore_PF_fares_options = [True]
ignore_EN_fares_options = [False, True]

@pytest.mark.parametrize("ignore_PF_fares", ignore_PF_fares_options)
@pytest.mark.parametrize("ignore_EN_fares", ignore_EN_fares_options)

@pytest.mark.travis
def test_fares(ignore_PF_fares,ignore_EN_fares):

    EXAMPLES_DIR   = os.path.join(os.getcwd(), "fasttrips", "Examples")

    INPUT_NETWORK = os.path.join(EXAMPLES_DIR, "networks", 'simple')
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR, 'demand', "demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR,"output")

    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORK,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND,"config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_ignore_fares_PF-%s_EN-%s" % (ignore_PF_fares,ignore_EN_fares),
        pathfinding_type = "stochastic",
        max_stop_process_count = 2,
        pf_iters         = 2,
        overlap_variable = "None",
        iters            = 1,
        dispersion       = 0.50,
        num_trips        = 5,
        transfer_fare_ignore_pathfinding = ignore_PF_fares,
        transfer_fare_ignore_pathenum    = ignore_EN_fares)

    assert r["passengers_arrived"] > 0

if __name__ == '__main__':
    import itertools
    for ignore_PF_fares,ignore_EN_fares in list(itertools.product(ignore_PF_fares_options, ignore_EN_fares_options)):
        print("running %s %s" % (ignore_PF_fares,ignore_EN_fares))
        test_fares(ignore_PF_fares,ignore_EN_fares)
