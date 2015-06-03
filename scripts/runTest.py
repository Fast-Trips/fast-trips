import fasttrips
import os, pandas, re, sys
import compare_output

USAGE = r"""

  python test_run.py input_dir output_dir

  e.g. python test_run.py ..\FAST-TrIPs-1\Examples\PSRC\pax100_deterministic_iter1_nocap Examples\PSRC

  Creates subdir in output_dir matching subdir in input_dir and places results there.
  Parses that subdir to determine test settings.

"""
if __name__ == "__main__":

    if len(sys.argv) != 3:
        print USAGE
        print sys.argv
        sys.exit(2)

    INPUT_DIR, subdir = os.path.split(sys.argv[1])
    OUTPUT_DIR  = sys.argv[2]
    test_pat    = re.compile(r"pax(\d+)_(deterministic|stochastic)_iter(\d+)_(cap\d+|nocap)")
    if not os.path.exists(OUTPUT_DIR): os.mkdir(OUTPUT_DIR)
    pandas.set_option('display.width', 300)

    m = test_pat.match(subdir)

    print "subdir = [%s]" % subdir
    if not os.path.exists(os.path.join(OUTPUT_DIR, subdir)):
        os.mkdir(os.path.join(OUTPUT_DIR, subdir))

    fasttrips.setupLogging(os.path.join(OUTPUT_DIR,subdir,"ft_info.log"),
                           os.path.join(OUTPUT_DIR,subdir,"ft_debug.log"),
                           logToConsole=True, debug_noisy=False)

    ft = fasttrips.FastTrips(os.path.join(INPUT_DIR, subdir))

    # Adjust settings
    if m.group(2) == "deterministic":
        fasttrips.Assignment.ASSIGNMENT_TYPE     = fasttrips.Assignment.ASSIGNMENT_TYPE_DET_ASGN
    elif m.group(2) == "stochastic":
        fasttrips.Assignment.ASSIGNMENT_TYPE     = fasttrips.Assignment.ASSIGNMENT_TYPE_STO_ASGN

    fasttrips.Assignment.ITERATION_FLAG          = int(m.group(3))

    if m.group(4) == "nocap":
        fasttrips.Assignment.CAPACITY_CONSTRAINT = False
    else:
        fasttrips.Assignment.CAPACITY_CONSTRAINT = True

    ft.run_assignment(os.path.join(OUTPUT_DIR, subdir))

    # Original output files are in input_dir
    for output_file in ["ft_output_passengerPaths.dat",
                        "ft_output_passengerTimes.dat",
                        "ft_output_loadProfile.dat"]:
        compare_output.compare_file(os.path.join(OUTPUT_DIR, subdir),
                                    os.path.join(INPUT_DIR,  subdir), output_file)