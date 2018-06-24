import os
from fasttrips import Run

network = "psrc_1_1"
demand  = "psrc_1_1"
config  = "base"
out_folder = "seattle_base"

ex_dir   = os.path.abspath(os.path.dirname(__file__))
print "Running Fast-Trips in %s" % (ex_dir.split(os.sep)[-1:])

Run.run_fasttrips(
    input_network_dir    = os.path.join(ex_dir,"networks",network),
    input_demand_dir = os.path.join(ex_dir,"demand",demand),
    run_config       = os.path.join(ex_dir,"configs",config,"config_ft.txt"),
    input_weights    = os.path.join(ex_dir,"configs",config,"pathweight_ft.txt"),
    input_functions  = os.path.join(ex_dir,"configs",config,'config_ft.py'),
    output_dir       = os.path.join(ex_dir,"output"),
    output_folder    = out_folder,
    pathfinding_type = "stochastic",
    overlap_variable = "count",
    overlap_split_transit = True,
    iters            = 1,
    dispersion       = 0.50)
