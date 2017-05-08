import fasttrips
import argparse, os, pandas, re, sys

USAGE = r"""

  Run Fast-Trips from the command line with required inputs as command line parameters.

"""

if __name__ == "__main__":

    def str2bool(v):
        #susendberg's function
        return v.lower() in ("yes", "true", "t", "1")

    parser = argparse.ArgumentParser(usage=USAGE)
    parser.register('type','bool',str2bool)
    parser.add_argument('-t','--trace_only', action='store_true', help="Run only the trace persons?")
    parser.add_argument('-n','--num_trips',  type=int,  help="Number of person trips to run, if you don't want to run the whole demand.")
    parser.add_argument('-d','--dispersion', type=float,help="Stochastic dispersion parameter")
    parser.add_argument('-m','--max_stop_process_count', type=int, help="Max times to process a stop in stochastic pathfinding")
    parser.add_argument('-c','--capacity',      action='store_true', help="Enable capacity constraint")
    parser.add_argument('-o','--output_folder', type=str,  help="Directory within output_loc to write fasttrips outtput.  If none specified, will construct one.")
    parser.add_argument('--debug_output_columns',             action='store_true', help="Include debug columns in output")
    parser.add_argument('--overlap_variable',                 choices=['None','count','distance','time'], help="Variable to use for overlap penalty calculation")
    parser.add_argument('--overlap_split_transit',            action='store_true', help="Split transit for path overlap penalty calculation")
    parser.add_argument('--transfer_fare_ignore_pathfinding', action='store_true', help="In path-finding, suppress trying to adjust fares using transfer rules.  For performance.")
    parser.add_argument('--transfer_fare_ignore_pathenum',    action='store_true', help="In path-enumeration, suppress trying to adjust fares using transfer rules.  For performance.")
    parser.add_argument("pathfinding_type",  choices=['deterministic','stochastic','file'], help="Type of pathfinding")
    parser.add_argument("iters",             type=int,  help="Number of iterations to run")
    parser.add_argument("run_config",        type=str,  help="The run configuration file")
    parser.add_argument("input_network_dir", type=str,  help="Location of the input network")
    parser.add_argument("input_demand_dir",  type=str,  help="Location of the input demand")
    parser.add_argument("input_weights",     type=str,  help="Location of the pathweights file")
    parser.add_argument("output_dir",        type=str,  help="Location to write fasttrips output")

    args = parser.parse_args(sys.argv[1:])

    # don't pass on items that aren't set
    args_dict = vars(args)
    for key in args_dict.keys():
        if args_dict[key]==None: del args_dict[key]

    # if config_ft.py exists in demand dir, specify it for input_functions
    func_file = os.path.join(args.input_demand_dir, "config_ft.py")
    if os.path.exists(func_file):
        args_dict["input_functions"] = func_file
    print args_dict

    r = fasttrips.Run.run_fasttrips(**args_dict)
