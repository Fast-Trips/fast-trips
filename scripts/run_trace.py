"""
This script shows how to run a Fast-Trips trace for a single person-trip by adding two keywords to the call to run_fasttrips():
 - trace_ids = list of tuples identifying which trips to trace ("<person_id>","<trip_id>")
 - debug_trace_only = boolean value for whether to only run fast-trips for the traces [ versus whole trip list ]
 
Run from within fast-trips/scripts folder.

In addition to normal fast-trips outputs, outputs: 
- fasttrips_trace_<person_id>-<trip_id>.log  
- fasttrips_labels_<person_id>-<trip_id>.csv
- fasttrips_labels_ids_<person_id>-<trip_id>.csv

"""

USAGE = """
python run_trace.py
"""

import os

from fasttrips import Run

EXAMPLES_DIR   = os.path.join(os.path.dirname(os.getcwd()),"fasttrips","Examples","test_scenario")

Run.run_fasttrips(
    input_network_dir= os.path.join(EXAMPLES_DIR,"network"),
    input_demand_dir = os.path.join(EXAMPLES_DIR,"demand_reg"),
    run_config       = os.path.join(EXAMPLES_DIR,"demand_reg","config_ft.txt"),
    input_weights    = os.path.join(EXAMPLES_DIR,"demand_reg","pathweight_ft.txt"),
    output_dir       = os.path.join(EXAMPLES_DIR,"output"),
    output_folder    = "example",
    pathfinding_type = "stochastic",
    overlap_variable = "count",
    overlap_split_transit = True,
    iters            = 1,
    dispersion       = 0.50,
    trace_ids        = [("0","trip_4")],
    debug_trace_only = True)

    