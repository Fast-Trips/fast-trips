import collections, csv, os, pandas, sys
import fasttrips
from fasttrips import FastTripsLogger

USAGE = """

  python compare_output.py output_dir1 output_dir2

  Compares the fasttrips output files in output_dir1 to those in output_dir2 and tallies up information on differences.

  Output is logged to ft_compare_info.log (for summary output) and ft_compare_debug.log (for detailed output).

"""

SPLIT_COLS = {"ft_output_loadProfile.dat"   :[],
              "ft_output_passengerTimes.dat":['arrivalTimes','boardingTimes','alightingTimes'],
              "ft_output_passengerPaths.dat":['boardingStops','boardingTrips','alightingStops','walkingTimes']}

def compare_file(dir1, dir2, filename):
    """
    Reads these files and looks at their differences, reporting on those differences.

    Splits comma-delimited fields.
    """
    filename1 = os.path.join(dir1, filename)
    filename2 = os.path.join(dir2, filename)
    FastTripsLogger.info("============== Comparing %s to %s" % (filename1, filename2))

    df1 = pandas.read_csv(filename1, sep="\t")
    df2 = pandas.read_csv(filename2, sep="\t")
    if filename == "ft_output_loadProfile.dat":
        # the keys are not unique since some stops come up twice; add rownum columns
        df1['rownum'] = range(1, len(df1)+1)
        df1.set_index(keys=['rownum','routeId', 'shapeId', 'tripId', 'direction', 'stopId'], inplace=True)
        df2['rownum'] = range(1, len(df2)+1)
        df2.set_index(keys=['rownum','routeId', 'shapeId', 'tripId', 'direction', 'stopId'], inplace=True)
    else:
        df1.set_index(keys=['passengerId','mode','originTaz','destinationTaz'], inplace=True)
        df2.set_index(keys=['passengerId','mode','originTaz','destinationTaz'], inplace=True)

    # split the columns that have multiple items in them
    split_cols = SPLIT_COLS[filename]
    for col in split_cols:
        split_df1 = df1[col].apply(lambda x: pandas.Series(x.split(',')))
        split_df2 = df2[col].apply(lambda x: pandas.Series(x.split(',')))
        if col.endswith('Times'):
            split_df1 = split_df1.astype('float')
            split_df2 = split_df2.astype('float')
        rename_cols1 = dict( (k,"%s%s" % (col,str(k))) for k in list(split_df1.columns.values))
        rename_cols2 = dict( (k,"%s%s" % (col,str(k))) for k in list(split_df2.columns.values))
        split_df1.rename(columns=rename_cols1, inplace=True)
        split_df2.rename(columns=rename_cols2, inplace=True)
        split_df1['num_%s' % col] = split_df1.notnull().sum(axis=1)
        split_df2['num_%s' % col] = split_df2.notnull().sum(axis=1)
        df1 = pandas.concat(objs=[df1, split_df1], axis=1)
        df2 = pandas.concat(objs=[df2, split_df2], axis=1)

    FastTripsLogger.info("Read   %10d rows from %s" % (len(df1), filename1))
    FastTripsLogger.info("Read   %10d rows from %s" % (len(df2), filename2))

    df_diff  = df1.merge(right=df2, how='outer', left_index=True, right_index=True, suffixes=('_1','_2'))
    FastTripsLogger.info("Merged %10d rows" % len(df_diff))

    # print df1.columns.values
    # print df2.columns.values
    # print df_diff.columns.values

    new_cols = []
    for colname in list(df1.columns.values):

        col1 = "%s_1" % colname
        col2 = "%s_2" % colname

        # deal with the split versions instead
        if colname in SPLIT_COLS[filename]: continue

        # if it's a time field, mod by 1440 minutes
        if colname.find("Time") >= 0:
            df_diff[col1] = df_diff[col1] % 1440
            df_diff[col2] = df_diff[col2] % 1440

        coldiff     = "%s_diff" % colname
        colabsdiff  = "%s_absdiff" % colname
        if str(df_diff[col1].dtype) == 'object':
            df_diff[coldiff] = ((df_diff[col1] != df_diff[col2]) & (df_diff[col1].notnull() | df_diff[col2].notnull()))
        else:
            df_diff[coldiff] = df_diff[col1] - df_diff[col2]
        df_diff[colabsdiff] = abs(df_diff[coldiff])

        # Detailed output
        FastTripsLogger.debug("============================================ %s ============================================" % colname)
        FastTripsLogger.debug(" -- head --\n" + str(df_diff[[col1, col2, coldiff]].head()) + "\n")
        FastTripsLogger.debug(" -- describe --\n" + str(df_diff[[col1, col2, coldiff]].describe()) + "\n")
        if df_diff[colabsdiff].max() == 0:
            FastTripsLogger.debug("-- no diffs --")
        else:
            FastTripsLogger.debug(" -- diffs --\n" + \
                                  str(df_diff.sort(columns=colabsdiff, ascending=False).loc[:,[col1, col2, coldiff, colabsdiff]].head()) + "\n")

        # Quick output
        status = ""
        if df_diff[colabsdiff].max() == 0:
            status = "Match"
        elif str(df_diff[col1].dtype) == 'object':
            status = "%d/%d objects differ" % (len(df_diff.loc[df_diff[coldiff]==True]), len(df_diff))
        else:
            status = "Values differ by [%6.2f,%6.2f]" % (df_diff[coldiff].min(), df_diff[coldiff].max())
        FastTripsLogger.info(" %-20s  %s" % (colname, status))


        new_cols.extend([col1, col2, coldiff])

    # df_diff = df_diff[new_cols]

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print USAGE
        print sys.argv
        sys.exit(2)

    OUTPUT_DIR1 = sys.argv[1]
    OUTPUT_DIR2 = sys.argv[2]

    fasttrips.setupLogging("ft_compare_info.log", "ft_compare_debug.log", logToConsole=True, debug_noisy=False)

    pandas.set_option('display.width', 300)
    for output_file in ["ft_output_passengerPaths.dat",
                        "ft_output_passengerTimes.dat",
                        "ft_output_loadProfile.dat"]:
        compare_file(OUTPUT_DIR1, OUTPUT_DIR2, output_file)
