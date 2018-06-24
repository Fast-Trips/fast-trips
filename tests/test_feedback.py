import os
import pytest
from fasttrips import Run

EXAMPLE_DIR    = os.path.join(os.getcwd(), 'fasttrips', 'Examples', 'Springfield')

# DIRECTORY LOCATIONS
INPUT_NETWORK       = os.path.join(EXAMPLE_DIR, 'networks', 'vermont')
INPUT_DEMAND        = os.path.join(EXAMPLE_DIR, 'demand', 'general')
INPUT_CONFIG        = os.path.join(EXAMPLE_DIR, 'configs', 'A')
OUTPUT_DIR          = os.path.join(EXAMPLE_DIR, 'output')

# INPUT FILE LOCATIONS
CONFIG_FILE         = os.path.join(INPUT_CONFIG, 'config_ft.txt')
INPUT_WEIGHTS       = os.path.join(INPUT_CONFIG, 'pathweight_ft.txt')

# LIST OF RUN PARAMETERS
ITERS = [3]
CAPACITY_CONSTRAINT = [False, True]


@pytest.mark.parametrize("feedback_iters", ITERS)
@pytest.mark.parametrize("capacity_const", CAPACITY_CONSTRAINT)

@pytest.mark.travis
def test_feedback(feedback_iters,capacity_const):

    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORK,
        input_demand_dir = INPUT_DEMAND,
        run_config       = CONFIG_FILE,
        input_weights    = INPUT_WEIGHTS,
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_feedback_iters-%d_capConst-%s" % (feedback_iters,capacity_const),
        max_stop_process_count = 2,
        pf_iters         = 2,
        overlap_variable = "None",
        pathfinding_type = "stochastic",
        capacity         = capacity_const,
        iters            = feedback_iters,
        num_trips        = 5,
        dispersion       = 0.50)

    assert r["passengers_arrived"] > 0

if __name__ == '__main__':
    import itertools
    for iter, cap_const in list(itertools.product(ITERS,CAPACITY_CONSTRAINT)):
        print("running %s %s" % (iter, cap_const))
        test_feedback(iter, cap_const)
