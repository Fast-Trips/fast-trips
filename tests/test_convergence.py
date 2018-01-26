import os

from fasttrips import Run


def run_convergence():
    EXAMPLES_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR, "networks")
    INPUT_DEMAND = os.path.join(EXAMPLES_DIR, "demand", "demand_twopaths")
    OUTPUT_DIR = os.path.join(EXAMPLES_DIR, "output")

    scenario_dir = os.path.join(INPUT_NETWORKS, 'simple')

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
        iters=4,
        dispersion=0.50
    )


if __name__ == '__main__':
    run_convergence()
