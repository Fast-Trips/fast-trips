import fasttrips
import os, sys

USAGE = r"""

  python test_run.py input_dir output_dir

"""
if __name__ == "__main__":

    if len(sys.argv) != 3:
        print USAGE
        print sys.argv
        sys.exit(2)

    INPUT_DIR   = sys.argv[1]
    OUTPUT_DIR  = sys.argv[2]

    fasttrips.setupLogging("ft_info.log", "ft_debug.log", logToConsole=True, debug_noisy=False)

    ft = fasttrips.FastTrips(INPUT_DIR)
    fasttrips.Assignment.CAPACITY_CONSTRAINT = True
    fasttrips.Assignment.ASSIGNMENT_TYPE     = fasttrips.Assignment.ASSIGNMENT_TYPE_STO_ASGN
    ft.run_assignment(OUTPUT_DIR)
