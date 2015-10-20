import fasttrips
import argparse, os, pandas, re, sys

USAGE = r"""

  python runTest.py asgn_type iters capacity input_network_dir input_demand_dir output_dir

  Where asgn_type is one of 'deterministic' or 'stochastic'

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
    parser.add_argument("asgn_type",         choices=['deterministic','stochastic'])
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

    full_output_dir = os.path.join(args.output_dir, test_dir)
    if not os.path.exists(full_output_dir):
        print "Creating full output dir [%s]" % full_output_dir
        os.mkdir(full_output_dir)

    ft = fasttrips.FastTrips(args.input_network_dir, args.input_demand_dir, full_output_dir)

    if args.asgn_type == "deterministic":
        fasttrips.Assignment.ASSIGNMENT_TYPE     = fasttrips.Assignment.ASSIGNMENT_TYPE_DET_ASGN
    elif args.asgn_type == "stochastic":
        fasttrips.Assignment.ASSIGNMENT_TYPE     = fasttrips.Assignment.ASSIGNMENT_TYPE_STO_ASGN

    fasttrips.Assignment.ITERATION_FLAG          = int(args.iters)

    if args.capacity == 0:
        fasttrips.Assignment.CAPACITY_CONSTRAINT = False
    else:
        fasttrips.Assignment.CAPACITY_CONSTRAINT = True

    ft.run_assignment(full_output_dir)
