import fasttrips
import argparse, os, pandas, re, sys

USAGE = r"""

  python runTest.py [--trace_only|-t] [--num_trips|-n #trips] [-c|--capacity] [-o|--output_dir dir] pathfinding_type iters input_network_dir input_demand_dir output_loc

  Where pathfinding_type is one of 'deterministic','stochastic' or 'file'

  e.g.

  python scripts\runTest.py --capacity deterministic 2 "C:\Users\lzorn\Box Sync\SHRP C-10\7-Test Case Development\test_net_export_20151005" Examples\test_net_20151005

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
    parser.add_argument('-c','--capacity',   action='store_true', help="Enable capacity constraint")
    parser.add_argument('-o','--output_dir', type=str,  help="Directory within output_loc to write fasttrips outtput.  If none specified, will construct one.")
    parser.add_argument('--overlap_variable',      choices=['None','count','distance','time'], help="Variable to use for overlap penalty calculation")
    parser.add_argument('--overlap_split_transit', action='store_true', help="Split transit for path overlap penalty calculation")
    parser.add_argument("pathfinding_type",  choices=['deterministic','stochastic','file'], help="Type of pathfinding")
    parser.add_argument("iters",             type=int,  help="Number of iterations to run")
    parser.add_argument("input_network_dir", type=str,  help="Location of the input network")
    parser.add_argument("input_demand_dir",  type=str,  help="Location of the input demand")
    parser.add_argument("output_loc",        type=str,  help="Location to write fasttrips output")

    args = parser.parse_args(sys.argv[1:])

    if not os.path.exists(args.output_loc):
        os.mkdir(args.output_loc)

    if args.output_dir:
        test_dir = args.output_dir
    else:
        test_dir = "%s%s_iter%d_%s" % ("" if args.input_network_dir == args.input_demand_dir else "%s_" % os.path.basename(args.input_demand_dir),
                                       args.pathfinding_type, args.iters,
                                      "cap" if args.capacity else "nocap")

        # don't override full run results
        if args.trace_only:
            test_dir = "%s_trace" % test_dir

    full_output_dir = os.path.join(args.output_loc, test_dir)
    if not os.path.exists(full_output_dir):
        print "Creating full output dir [%s]" % full_output_dir
        os.mkdir(full_output_dir)

    ft = fasttrips.FastTrips(args.input_network_dir, args.input_demand_dir, full_output_dir)

    # Read the configuration here so we can overwrite options below
    ft.read_configuration()

    fasttrips.Assignment.PATHFINDING_TYPE        = args.pathfinding_type
    fasttrips.Assignment.MAX_ITERATIONS          = int(args.iters)

    if args.max_stop_process_count:
        fasttrips.Assignment.STOCH_MAX_STOP_PROCESS_COUNT = args.max_stop_process_count

    if args.overlap_variable:
        fasttrips.PathSet.OVERLAP_VARIABLE       = args.overlap_variable

    if args.overlap_split_transit:
        fasttrips.PathSet.OVERLAP_SPLIT_TRANSIT  = args.overlap_split_transit

    if args.dispersion:
        fasttrips.Assignment.STOCH_DISPERSION    = args.dispersion

    if args.num_trips:
        fasttrips.Assignment.DEBUG_NUM_TRIPS     = args.num_trips

    if args.capacity:
        fasttrips.Assignment.CAPACITY_CONSTRAINT = True
    else:
        fasttrips.Assignment.CAPACITY_CONSTRAINT = False

    if args.trace_only:
        if len(fasttrips.Assignment.TRACE_PERSON_IDS) == 0:
            print "Trace only requested but no trace IDs are specified in configuration."
            sys.exit(2)
        fasttrips.Assignment.DEBUG_TRACE_ONLY    = True
        fasttrips.Assignment.NUMBER_OF_PROCESSES = 1

    # Readthe networks and demand
    ft.read_input_files()

    ft.run_assignment(full_output_dir)
