import os
import pytest
from fasttrips import Run

"""
Run just the tests labeled basic using `pytest -v -m basic`
"""

@pytest.mark.parametrize("demand", ["backward_bunnies","forward_bunnies"])
@pytest.mark.parametrize("network", ["bunny_hop","many_bunny_hops"])


@pytest.mark.basic
def test_bunny(demand, network):
    """
    Test to ensure that the most simple of networks and demand is working.
    """
    EXAMPLES_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples")

    INPUT_NETWORK = os.path.join(EXAMPLES_DIR, "networks", network)
    INPUT_DEMAND = os.path.join(EXAMPLES_DIR, "demand", demand)
    OUTPUT_DIR = os.path.join(EXAMPLES_DIR, "output")

    Run.run_fasttrips(
        input_network_dir=INPUT_NETWORK,
        input_demand_dir=INPUT_DEMAND,
        run_config=os.path.join(INPUT_DEMAND, "config_ft.txt"),
        input_functions=os.path.join(INPUT_DEMAND, 'config_ft.py'),
        input_weights=os.path.join(INPUT_DEMAND, "pathweight_ft.txt"),
        output_dir=OUTPUT_DIR,
        output_folder=demand+"-"+network,
        pathfinding_type="stochastic",
        capacity=False,
        iters=1,
        OVERLAP = "None",
        dispersion=0.2
    )


if __name__ == '__main__':
    test_bunny()
