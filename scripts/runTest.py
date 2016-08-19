import fasttrips
import argparse, os, pandas, re, sys

USAGE = r"""

  python runTest.py [--trace_only|-t] [--num_trips|-n #trips] asgn_type iters capacity input_network_dir input_demand_dir output_dir

  Where asgn_type is one of 'deterministic','stochastic' or 'simulation'

  Use capacity='yes', 'true', 't', or 1 to enable a capacity constraint.

  e.g.

  python scripts\runTest.py deterministic 2 true "C:\Users\lzorn\Box Sync\SHRP C-10\7-Test Case Development\test_net_export_20151005" Examples\test_net_20151005
"""

if __name__ == "__main__":

    def str2bool(v):
        #susendberg's function
        return v.lower() in ("yes", "true", "t", "1")

    parser = argparse.ArgumentParser(usage=USAGE)
    parser.register('type','bool',str2bool)
    parser.add_argument('-t','--trace_only', action='store_true')
    parser.add_argument('-n','--num_trips',  type=int)
    parser.add_argument("asgn_type",         choices=['deterministic','stochastic','simulation'])
    parser.add_argument("iters",             type=int)
    parser.add_argument("capacity",          type='bool')
    parser.add_argument("input_network_dir", type=str)
    parser.add_argument("input_demand_dir",  type=str)
    parser.add_argument("output_dir",        type=str)

    args = parser.parse_args(sys.argv[1:])

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)

    test_dir = "%s%s_iter%d_%s" % ("" if args.input_network_dir == args.input_demand_dir else "%s_" % os.path.basename(args.input_demand_dir),
                                   args.asgn_type, args.iters,
                                  "cap" if args.capacity else "nocap")

    # don't override full run results
    if args.trace_only:
        test_dir = "%s_trace" % test_dir

    full_output_dir = os.path.join(args.output_dir, test_dir)
    if not os.path.exists(full_output_dir):
        print "Creating full output dir [%s]" % full_output_dir
        os.mkdir(full_output_dir)

    ft = fasttrips.FastTrips(args.input_network_dir, args.input_demand_dir, full_output_dir)

    # Read the configuration here so we can overwrite options below
    ft.read_configuration()

    if args.asgn_type == "deterministic":
        fasttrips.Assignment.ASSIGNMENT_TYPE     = fasttrips.Assignment.ASSIGNMENT_TYPE_DET_ASGN
    elif args.asgn_type == "stochastic":
        fasttrips.Assignment.ASSIGNMENT_TYPE     = fasttrips.Assignment.ASSIGNMENT_TYPE_STO_ASGN
    elif args.asgn_type == "simulation":
        fasttrips.Assignment.ASSIGNMENT_TYPE     = fasttrips.Assignment.ASSIGNMENT_TYPE_SIM_ONLY

    fasttrips.Assignment.ITERATION_FLAG          = int(args.iters)

    if args.num_trips:
        fasttrips.Assignment.DEBUG_NUM_TRIPS     = args.num_trips

    if args.capacity == 0:
        fasttrips.Assignment.CAPACITY_CONSTRAINT = False
    else:
        fasttrips.Assignment.CAPACITY_CONSTRAINT = True

    if args.trace_only:
        if len(fasttrips.Assignment.TRACE_PERSON_IDS) == 0:
            print "Trace only requested but no trace IDs are specified in configuration."
            sys.exit(2)
        fasttrips.Assignment.DEBUG_TRACE_ONLY    = True
        fasttrips.Assignment.NUMBER_OF_PROCESSES = 1

    # Readthe networks and demand
    ft.read_input_files()

    ft.run_assignment(full_output_dir)
