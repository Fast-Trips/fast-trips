import os, sys

import numpy as np
import pandas as pd

import fasttrips
from fasttrips import FastTripsLogger

USAGE = """

  python compare_output.py output_dir1 output_dir2

  Compares the fasttrips output files in output_dir1 to those in output_dir2 and tallies up information on differences.

  Output is logged to output_dir1:
    * ft_compare_info.log (for summary output)
    * ft_compare_debug.log (for detailed output)
    * ft_join_pathset.csv has the joined pathset results
    * ft_compare_pathset.csv has an aggregate summary of the pathset results (one line per trip_list_id_num)

"""

# TODO: this should match the input in question
STOCH_DISPERSION = 1.0

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

    df1 = pd.read_csv(filename1, sep=sep)
    df2 = pd.read_csv(filename2, sep=sep)
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

        split_df1 = df1[col].apply(lambda x: pd.Series(x.split(',')))
        split_df2 = df2[col].apply(lambda x: pd.Series(x.split(',')))

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
        df1 = pd.concat(objs=[df1, split_df1], axis=1)
        df2 = pd.concat(objs=[df2, split_df2], axis=1)
        if len(rename_cols1) < len(rename_cols2):
            for k,v in rename_cols2.iteritems():
                if k not in rename_cols1: df1[v] = np.NaN
        if len(rename_cols2) < len(rename_cols1):
            for k,v in rename_cols1.iteritems():
                if k not in rename_cols2: df2[v] = np.NaN

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
        if df_diff[col1].dtype != df_diff[col2].dtype:
            FastTripsLogger.debug("mismatching dtypes for %s and %s: %s vs %s" % (col1, col2, str(df_diff[col1].dtype), str(df_diff[col2].dtype)))
            df_diff[coldiff] = 0
        elif str(df_diff[col1].dtype) == 'object':
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

def compare_pathset(dir1, dir2):
    """
    Compare the pathset to see if the pathset is 'better'
    """
    filename = "ft_pathset.txt"
    filename1 = os.path.join(dir1, filename)
    filename2 = os.path.join(dir2, filename)
    FastTripsLogger.info("============== Comparing %s to %s" % (filename1, filename2))

    df1 = pd.read_csv(filename1, sep="\s+")
    df2 = pd.read_csv(filename2, sep="\s+")

    merge_cols = ['iteration','passenger_id_num','trip_list_id_num','path_board_stops','path_trips','path_alight_stops']

    # outer join
    df_diff  = df1.merge(right=df2, how='outer', on=merge_cols, suffixes=('_1','_2'))

    # make a single path_cost column
    df_diff['path_cost'] = df_diff[['path_cost_1','path_cost_2']].min(axis=1)

    # create probabilities based on the union of the path sets
    df_diff['exp_util'] = np.exp(-1.0*STOCH_DISPERSION*df_diff['path_cost'])

    # aggregate it to each person trip
    df_diff['num total paths'] = 1
    df_diff_counts = df_diff.groupby(['iteration','passenger_id_num','trip_list_id_num']).agg({'num total paths':'count', 'exp_util':'sum'})
    df_diff_counts.rename(columns={'exp_util':'sum_exp_util'}, inplace=True)

    # join it back to get the probability given a union pathset
    df_diff = df_diff.merge(df_diff_counts.reset_index()[['iteration','passenger_id_num','trip_list_id_num','sum_exp_util']], how='left')
    df_diff['union pathset probability'] = df_diff['exp_util']/df_diff['sum_exp_util']
    # drop these
    df_diff.drop(['exp_util','sum_exp_util'], axis=1, inplace=True)
    df_diff_counts.drop(['sum_exp_util'], axis=1, inplace=True)

    # write the joined one
    join_filename = os.path.join(dir1, "ft_join_pathset.csv")
    df_diff.to_csv(join_filename, sep=",", index=False)
    FastTripsLogger.info("Wrote joined pathset diff info to %s" % join_filename)

    # look at the nulls
    df1_only = df_diff.loc[pd.isnull(df_diff.path_cost_2)].groupby(['iteration','passenger_id_num','trip_list_id_num']).agg({'union pathset probability':'max','path_cost_1':'count'})
    df1_only.rename(columns={'union pathset probability':'max prob missing from file2',
                             'path_cost_1':'num paths missing from file2'}, inplace=True)
    df2_only = df_diff.loc[pd.isnull(df_diff.path_cost_1)].groupby(['iteration','passenger_id_num','trip_list_id_num']).agg({'union pathset probability':'max','path_cost_2':'count'})
    df2_only.rename(columns={'union pathset probability':'max prob missing from file1',
                             'path_cost_2':'num paths missing from file1'}, inplace=True)

    df_diff_summary = df_diff_counts.merge(df1_only, how='left', left_index=True, right_index=True)
    df_diff_summary = df_diff_summary.merge(df2_only, how='left', left_index=True, right_index=True)

    # note paths for which we didn't find ANY in one or the other run
    df_diff_summary['only in file1'] = 0
    df_diff_summary.loc[df_diff_summary['num paths missing from file2']==df_diff_summary['num total paths'],'only in file1'] = 1
    df_diff_summary['only in file2'] = 0
    df_diff_summary.loc[df_diff_summary['num paths missing from file1']==df_diff_summary['num total paths'],'only in file2'] = 1
    # NaN means zero
    df_diff_summary.loc[pd.isnull(df_diff_summary['num paths missing from file1']), 'num paths missing from file1'] = 0
    df_diff_summary.loc[pd.isnull(df_diff_summary['num paths missing from file2']), 'num paths missing from file2'] = 0

    # write detailed output
    detail_file = os.path.join(dir1, "ft_compare_pathset.csv")
    df_diff_summary.reset_index().to_csv(detail_file, index=False)
    FastTripsLogger.info("Wrote detailed pathset diff info to %s" % detail_file)

    # Report
    FastTripsLogger.info("                        Average pathset size: %.1f" % df_diff_summary['num total paths'].mean())
    FastTripsLogger.info("          Trips with paths ONLY in pathset 1: %d" % df_diff_summary['only in file1'].sum())
    FastTripsLogger.debug(" -- diffs --\n" + \
                          str(df_diff_summary.loc[df_diff_summary['only in file1']==1]) + "\n")

    FastTripsLogger.info("          Trips with paths ONLY in pathset 2: %d" % df_diff_summary['only in file2'].sum())
    FastTripsLogger.debug(" -- diffs --\n" + \
                          str(df_diff_summary.loc[df_diff_summary['only in file2']==1]) + "\n")

    FastTripsLogger.info("        Average paths missing from pathset 1: %.1f" % df_diff_summary['num paths missing from file1'].mean())
    FastTripsLogger.info("      Max probability missing from pathset 1: %.3f%%" % (100*df_diff_summary['max prob missing from file1'].max()))
    FastTripsLogger.info(" # trips w/ paths>10%% missing from pathset 1: %d" % len(df_diff_summary.loc[df_diff_summary['max prob missing from file1']>0.10]))
    FastTripsLogger.info(" # trips w/ paths> 1%% missing from pathset 1: %d" % len(df_diff_summary.loc[df_diff_summary['max prob missing from file1']>0.01]))
    temp_df = df_diff_summary[['max prob missing from file1','num paths missing from file1']].reset_index().sort_values(by=['max prob missing from file1', 'trip_list_id_num'], ascending=[False,True])
    FastTripsLogger.debug(" trips w/ paths> 1%% missing from pathset 1\n%s\n" %
        str(temp_df.loc[temp_df['max prob missing from file1']>0.01]))

    FastTripsLogger.info("        Average paths missing from pathset 2: %.1f" % df_diff_summary['num paths missing from file2'].mean())
    FastTripsLogger.info("      Max probability missing from pathset 2: %.3f%%" % (100*df_diff_summary['max prob missing from file2'].max()))
    FastTripsLogger.info(" # trips w/ paths>10%% missing from pathset 2: %d" % len(df_diff_summary.loc[df_diff_summary['max prob missing from file2']>0.10]))
    FastTripsLogger.info(" # trips w/ paths> 1%% missing from pathset 2: %d" % len(df_diff_summary.loc[df_diff_summary['max prob missing from file2']>0.01]))
    temp_df = df_diff_summary[['max prob missing from file2','num paths missing from file2']].reset_index().sort_values(by=['max prob missing from file2', 'trip_list_id_num'], ascending=[False,True])
    FastTripsLogger.debug(" trips w/ paths> 1%% missing from pathset 2\n%s\n" %
        str(temp_df.loc[temp_df['max prob missing from file2']>0.01]))


def compare_performance(dir1, dir2):
    """
    Compare performance output
    """
    filename = "ft_output_performance.txt"
    filename1 = os.path.join(dir1, filename)
    filename2 = os.path.join(dir2, filename)
    FastTripsLogger.info("============== Comparing %s to %s" % (filename1, filename2))

    df1 = pd.read_csv(filename1, sep="\t")
    df2 = pd.read_csv(filename2, sep="\t")

    # drop the text-y ones
    df1.drop([fasttrips.Performance.PERFORMANCE_COLUMN_TIME_LABELING, fasttrips.Performance.PERFORMANCE_COLUMN_TIME_ENUMERATING], axis=1, inplace=True)
    df2.drop([fasttrips.Performance.PERFORMANCE_COLUMN_TIME_LABELING, fasttrips.Performance.PERFORMANCE_COLUMN_TIME_ENUMERATING], axis=1, inplace=True)

    df_perf_diff  = df1.merge(right=df2, how='outer', on=['iteration','trip_list_id_num'], suffixes=('_1','_2'))

    # write the joined one
    join_filename = os.path.join(dir1, "ft_compare_performance.csv")
    df_perf_diff.to_csv(join_filename, sep=",", index=False)
    FastTripsLogger.info("Wrote joined performance info to %s" % join_filename)

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print USAGE
        print sys.argv
        sys.exit(2)

    OUTPUT_DIR1 = sys.argv[1]
    OUTPUT_DIR2 = sys.argv[2]

    fasttrips.setupLogging(os.path.join(OUTPUT_DIR1, "ft_compare_info.log"),
                           os.path.join(OUTPUT_DIR1, "ft_compare_debug.log"), logToConsole=True)

    pd.set_option('display.width', 300)
    for output_file in ["ft_output_passengerPaths.txt",
                        "ft_output_passengerTimes.txt",
                        "ft_output_loadProfile.txt"]:
        compare_file(OUTPUT_DIR1, OUTPUT_DIR2, output_file)

    compare_pathset(OUTPUT_DIR1, OUTPUT_DIR2)
    compare_performance(OUTPUT_DIR1, OUTPUT_DIR2)
