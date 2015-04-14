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

    fasttrips.setupLogging("ft_info.log", "ft_debug.log", logToConsole=True)

    ft = fasttrips.FastTrips(INPUT_DIR)
    ft.run_assignment(OUTPUT_DIR)
