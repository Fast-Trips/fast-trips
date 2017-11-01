import os

from fasttrips import Run


def run_capacity_test():
    EXAMPLES_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples", "test_scenario")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR, "network")
    INPUT_DEMAND = os.path.join(EXAMPLES_DIR, "demand_twopaths")
    OUTPUT_DIR = os.path.join(EXAMPLES_DIR, "output")

    return (OUTPUT_DIR, Run.run_fasttrips(
        input_network_dir=INPUT_NETWORKS,
        input_demand_dir=INPUT_DEMAND,
        run_config=os.path.join(INPUT_DEMAND, "config_ft.txt"),
        input_functions=os.path.join(INPUT_DEMAND, 'config_ft.py'),
        input_weights=os.path.join(INPUT_DEMAND, "pathweight_ft.txt"),
        output_dir=OUTPUT_DIR,
        output_folder="test_convergence",
        pathfinding_type="stochastic",
        capacity=True,
        iters=4,
        dispersion=0.50))

if __name__ == '__main__':
    run_capacity_test()
