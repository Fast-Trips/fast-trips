import os
import pytest
from fasttrips import Run

"""
Run just the tests labeled basic using `pytest -v -m basic`
"""

demand_options  = ["backward_bunnies","forward_bunnies"]
network_options = ["bunny_hop","many_bunny_hops"]

@pytest.mark.parametrize("demand", demand_options)
@pytest.mark.parametrize("network", network_options)

@pytest.mark.basic
@pytest.mark.travis
def test_bunny(demand, network):
    """
    Test to ensure that the most simple of networks and demand is working.
    """
    EXAMPLE_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples","Bunny_Hop")

    INPUT_NETWORK = os.path.join(EXAMPLE_DIR, "networks", network)
    INPUT_DEMAND  = os.path.join(EXAMPLE_DIR, "demand"  , demand)
    INPUT_CONFIG  = os.path.join(EXAMPLE_DIR, "configs","base")
    OUTPUT_DIR    = os.path.join(EXAMPLE_DIR, "output")

    Run.run_fasttrips(
        input_network_dir = INPUT_NETWORK,
        input_demand_dir  = INPUT_DEMAND,
        run_config        = os.path.join(INPUT_CONFIG, "config_ft.txt"),
        input_functions   = os.path.join(INPUT_CONFIG, 'config_ft.py'),
        input_weights     = os.path.join(INPUT_CONFIG, "pathweight_ft.txt"),
        output_dir        = OUTPUT_DIR,
        output_folder     = demand+"-"+network,
        pathfinding_type  = "stochastic",
        capacity          = False,
        iters             = 1,
        OVERLAP           = "None",
        dispersion        = 0.5
    )

if __name__ == '__main__':
    import itertools
    for demand,network in list(itertools.product(demand_options, network_options)):
        print("running %s %s" % (demand,network))
        test_bunny(demand, network)
