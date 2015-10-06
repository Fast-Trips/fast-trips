import fasttrips
import argparse, os, pandas, re, sys

USAGE = r"""

  python runTest.py num_passengers asgn_type iters capacity input_dir output_dir

  Where asgn_type is one of 'deterministic' or 'stochastic'

  Use capacity=0 to leave it unmodified in trips and configure no capacity constraint.

"""
if __name__ == "__main__":

    parser = argparse.ArgumentParser(usage=USAGE)
    parser.add_argument("num_passengers", type=int,
                        help="Number of passengers to assign and simuluate")
    parser.add_argument("asgn_type",      choices=['deterministic','stochastic'])
    parser.add_argument("iters",          type=int)
    parser.add_argument("capacity",       type=int)
    parser.add_argument("input_dir",      type=str)
    parser.add_argument("output_dir",     type=str)

    args = parser.parse_args(sys.argv[1:])

    test_dir = "pax%d_%s_iter%d_%s" % (args.num_passengers, args.asgn_type, args.iters,
                                       "cap%d" % args.capacity if args.capacity > 0 else "nocap")

    full_output_dir = os.path.join(args.output_dir, test_dir)
    if not os.path.exists(full_output_dir):
        print "Creating full output dir [%s]" % full_output_dir
        os.mkdir(full_output_dir)

    ft = fasttrips.FastTrips(args.input_dir, full_output_dir)

    if args.asgn_type == "deterministic":
        fasttrips.Assignment.ASSIGNMENT_TYPE     = fasttrips.Assignment.ASSIGNMENT_TYPE_DET_ASGN
    elif args.asgn_type == "stochastic":
        fasttrips.Assignment.ASSIGNMENT_TYPE     = fasttrips.Assignment.ASSIGNMENT_TYPE_STO_ASGN

    fasttrips.Assignment.ITERATION_FLAG          = int(args.iters)

    if args.capacity == 0:
        fasttrips.Assignment.CAPACITY_CONSTRAINT = False
    else:
        fasttrips.Assignment.CAPACITY_CONSTRAINT = True

    sys.exit()
    ft.run_assignment(os.path.join(OUTPUT_DIR, subdir))

    # Original output files are in input_dir
    for output_file in ["ft_output_passengerPaths.dat",
                        "ft_output_passengerTimes.dat",
                        "ft_output_loadProfile.dat"]:
        compare_output.compare_file(os.path.join(OUTPUT_DIR, subdir),
                                    os.path.join(INPUT_DIR,  subdir), output_file)