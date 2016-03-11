import collections, csv, os, numpy, pandas, sys
import fasttrips
from fasttrips import FastTripsLogger

USAGE = """

  python compare_output.py output_dir1 output_dir2

  Compares the fasttrips output files in output_dir1 to those in output_dir2 and tallies up information on differences.

  Output is logged to ft_compare_info.log (for summary output) and ft_compare_debug.log (for detailed output).

"""

SPLIT_COLS = {"ft_output_loadProfile.txt"   :[],
              "ft_output_passengerTimes.txt":['arrivalTimes','boardingTimes','alightingTimes'],
              "ft_output_passengerPaths.txt":['boardingStops','boardingTrips','alightingStops','walkingTimes']}

def compare_file(dir1, dir2, filename):
    """
    Reads these files and looks at their differences, reporting on those differences.

    Splits comma-delimited fields.
    """
    filename1 = os.path.join(dir1, filename)
    filename2 = os.path.join(dir2, filename)
    FastTripsLogger.info("============== Comparing %s to %s" % (filename1, filename2))

    sep = "\t"
    if filename == "ft_output_loadProfile.txt": sep = ","

    df1 = pandas.read_csv(filename1, sep=sep)
    df2 = pandas.read_csv(filename2, sep=sep)
    if filename == "ft_output_loadProfile.txt":
        index_cols = ['rownum','route_id', 'trip_id', 'direction', 'stop_id']
        if 'direction' not in df1.columns.values: index_cols.remove('direction')
        # the keys are not unique since some stops come up twice; add rownum columns
        df1['rownum'] = range(1, len(df1)+1)
        df1.set_index(keys=index_cols, inplace=True)
        df2['rownum'] = range(1, len(df2)+1)
        df2.set_index(keys=index_cols, inplace=True)
    else:
        df1.set_index(keys=['person_id','trip_list_id_num','mode','originTaz','destinationTaz'], inplace=True)
        df2.set_index(keys=['person_id','trip_list_id_num','mode','originTaz','destinationTaz'], inplace=True)

    # startTime needs to be read as a time
    if 'startTime' in df1.columns.values:
        df1['startTime'] = df1['startTime'].map(lambda x: fasttrips.Util.read_time(x))
        df2['startTime'] = df2['startTime'].map(lambda x: fasttrips.Util.read_time(x))

    # split the columns that have multiple items in them
    split_cols = SPLIT_COLS[filename]
    for col in split_cols:
        if str(df1[col].dtype) == 'float64':
            df1[col] = df1[col].map(lambda x: '%f' % x)
        if str(df2[col].dtype) == 'float64':
            df2[col] = df2[col].map(lambda x: '%f' % x)

        split_df1 = df1[col].apply(lambda x: pandas.Series(x.split(',')))
        split_df2 = df2[col].apply(lambda x: pandas.Series(x.split(',')))

        if col.endswith('Times'):
            # these are formatted 11:12:13
            if filename=='ft_output_passengerTimes.txt':
                split_df1 = split_df1.applymap(lambda x: fasttrips.Util.read_time(x))
                split_df2 = split_df2.applymap(lambda x: fasttrips.Util.read_time(x))
            else:
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
        if len(rename_cols1) < len(rename_cols2):
            for k,v in rename_cols2.iteritems():
                if k not in rename_cols1: df1[v] = numpy.NaN
        if len(rename_cols2) < len(rename_cols1):
            for k,v in rename_cols1.iteritems():
                if k not in rename_cols2: df2[v] = numpy.NaN

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
        if colname.find("Time") >= 0 and str(df_diff[col1].dtype) == 'float':
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
        FastTripsLogger.debug(" -- dtypes --\n" + str(df_diff[[col1, col2, coldiff]].dtypes))
        FastTripsLogger.debug(" -- head --\n" + str(df_diff[[col1, col2, coldiff]].head()) + "\n")
        FastTripsLogger.debug(" -- describe --\n" + str(df_diff[[col1, col2, coldiff]].describe()) + "\n")
        if df_diff[colabsdiff].max() == 0:
            FastTripsLogger.debug("-- no diffs --")
        elif filename=="ft_output_passengerPaths.txt":
            FastTripsLogger.debug(" -- diffs --\n" + \
                                  str(df_diff.reset_index().sort(columns=[colabsdiff, 'trip_list_id_num'], ascending=[False,True]).loc[:,['trip_list_id_num','mode','originTaz','destinationTaz',col1, col2, coldiff, colabsdiff]].head()) + "\n")
        else:
            FastTripsLogger.debug(" -- diffs --\n" + \
                                  str(df_diff.sort(columns=colabsdiff, ascending=False).loc[:,[col1, col2, coldiff, colabsdiff]].head()) + "\n")

        # Quick output
        status = ""
        if df_diff[colabsdiff].max() == 0:
            status = "Match"
        elif str(df_diff[col1].dtype) == 'object':
            status = "%d/%d objects differ" % (len(df_diff.loc[df_diff[coldiff]==True]), len(df_diff))
        elif str(df_diff[col1].dtype)[:3] == 'int':
            status = "Values differ by [% 8.2f,% 8.2f] with %d values differing" % (df_diff[coldiff].min(), df_diff[coldiff].max(), len(df_diff.loc[df_diff[coldiff]!=0]))
        elif str(df_diff[col1].dtype) == 'datetime64[ns]':
            status = "Values differ by [%s,%s] mins with mean %s" % (str(df_diff[coldiff].min()), str(df_diff[coldiff].max()), str(df_diff[coldiff].mean()))
        else:
            status = "Values differ by [% 8.2f,% 8.2f] with mean % 8.2f" % (df_diff[coldiff].min(), df_diff[coldiff].max(), df_diff[coldiff].mean())
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

    fasttrips.setupLogging("ft_compare_info.log", "ft_compare_debug.log", logToConsole=True)

    pandas.set_option('display.width', 300)
    for output_file in ["ft_output_passengerPaths.txt",
                        "ft_output_passengerTimes.txt",
                        "ft_output_loadProfile.txt"]:
        compare_file(OUTPUT_DIR1, OUTPUT_DIR2, output_file)
