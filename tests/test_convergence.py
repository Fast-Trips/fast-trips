import os
import pytest
from fasttrips import Run


def test_convergence():
    EXAMPLES_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR, "networks")
    INPUT_DEMAND = os.path.join(EXAMPLES_DIR, "demand", "demand_converge")
    OUTPUT_DIR = os.path.join(EXAMPLES_DIR, "output")

    INPUT_NETWORK  = os.path.join(EXAMPLE_DIR, "networks", "vermont")
    INPUT_DEMAND   = os.path.join(EXAMPLE_DIR, "demand", "simpson_zorn")
    INPUT_CONFIG   = os.path.join(EXAMPLE_DIR, "configs", "B")
    OUTPUT_DIR     = os.path.join(EXAMPLE_DIR, "output")

    Run.run_fasttrips(
        input_network_dir=scenario_dir,
        input_demand_dir=INPUT_DEMAND,
        run_config=os.path.join(INPUT_DEMAND, "config_ft.txt"),
        input_functions=os.path.join(INPUT_DEMAND, 'config_ft.py'),
        input_weights=os.path.join(INPUT_DEMAND, "pathweight_ft.txt"),
        output_dir=OUTPUT_DIR,
        output_folder="test_convergence",
        pathfinding_type="stochastic",
        capacity=True,
        iters=10,
        dispersion=0.50
    )


if __name__ == '__main__':
    test_convergence()
