__copyright__ = "Copyright 2015 Contributing Entities"
__license__   = """
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
"""

import csv, datetime, logging, os

import numpy
import pandas

from .Error  import UnexpectedError
from .Logger import FastTripsLogger

class Util:
    """
    Util class.

    Collect useful stuff here that doesn't belong in any particular existing class.
    """
    #: Use this as the date
    SIMULATION_DAY                  = datetime.date(2016,1,1)
    #: Use this for the start time - the start of :py:attr:`Util.SIMULATION_DAY`
    SIMULATION_DAY_START            = datetime.datetime.combine(SIMULATION_DAY, datetime.time())

    #: Maps timedelta columns to units for :py:meth:`Util.write_dataframe`
    TIMEDELTA_COLUMNS_TO_UNITS      = {
        'time enumerating'  : 'milliseconds',  # performance
        'time labeling'     : 'milliseconds',  # performance
        'pf_linktime'       : 'min',
        'pf_linkcost'       : 'min',
        'pf_waittime'       : 'min',
        'new_linktime'      : 'min',
        'new_waittime'      : 'min'
    }

    #: Debug columns to drop
    DROP_DEBUG_COLUMNS = [
        # drop these?
        "A_lat","A_lon","B_lat","B_lon",# "distance",
        # numeric versions of other columns
        "trip_list_id_num","trip_id_num","A_id_num","B_id_num","mode_num",
        # simulation debugging
        "bump_iter","bumpstop_boarded","alight_delay_min"
    ]
    DROP_PATHFINDING_COLUMNS = [
        # pathfinding debugging
        "pf_iteration","pf_A_time","pf_B_time","pf_linktime","pf_linkcost","pf_linkdist","pf_waittime","pf_linkfare","pf_cost","pf_fare","pf_initcost","pf_initfare"
    ]

    @staticmethod
    def add_numeric_column(input_df, id_colname, numeric_newcolname):
        """
        Method to create numerical ID to map to an existing ID.
        Pass in a dataframe with JUST an ID column and we'll add a numeric ID column for ya!

        Returns the dataframe with the new column.
        """
        assert(len(input_df.columns) == 1)

        # drop duplicates - this is an ID and drop the index since it's not useful
        return_df = input_df.drop_duplicates().reset_index(drop=True)
        # create numerical version
        try:
            # if it converts, use that
            return_df[numeric_newcolname] = return_df[id_colname].astype(int)
        except:
            # if it doesn't, use index but start at 1
            return_df[numeric_newcolname] = return_df.index + 1
        return return_df

    @staticmethod
    def add_new_id(input_df,   id_colname,         newid_colname,
                   mapping_df, mapping_id_colname, mapping_newid_colname,
                   warn=False, warn_msg=None,      drop_failures=True):
        """
        Passing a :py:class:`pandas.DataFrame` *input_df* with an ID column called *id_colname*,
        adds the numeric id as a column named *newid_colname* and returns it.

        *mapping_df* is defines the mapping from an ID (*mapping_id_colname*) 
        to a numeric ID (*mapping_newid_colname*).

        If *warn* is True, then don't worry if some fail.  Just log and move on.  Otherwise, raise an exception.
        """
        input_cols = list(input_df.columns.values)
        # add the new id column
        return_df = pandas.merge(left=input_df, right=mapping_df,
                                 how='left',
                                 left_on=id_colname,
                                 right_on=mapping_id_colname,
                                 suffixes=("","_mapping"))

        # print "RETURN_DF=================="
        # print return_df.head()

        # first check if mapping_newid_colname was already in input_df; if it was, check needs to be performed on "_mapping"
        mapping_newid_colname_chk = mapping_id_colname + "_mapping" if mapping_newid_colname in input_cols else mapping_newid_colname

        # Make sure all ids were mapped to numbers.  If not warn or error
        if pandas.isnull(return_df[mapping_newid_colname_chk]).sum() != pandas.isnull(input_df[id_colname]).sum():

            msg_level = logging.CRITICAL
            if warn: msg_level = logging.WARN

            if warn_msg: FastTripsLogger.log(msg_level, warn_msg)
            FastTripsLogger.log(msg_level,"Util.add_new_id failed to map all ids to numbers")
            # FastTripsLogger.log(msg_level,"pandas.isnull(return_df[%s]).sum() = %d" % (mapping_newid_colname_chk, pandas.isnull(return_df[mapping_newid_colname_chk]).sum()))
            FastTripsLogger.log(msg_level,"\n%s\n" % str(return_df.loc[pandas.isnull(return_df[mapping_newid_colname_chk]),[id_colname,mapping_newid_colname_chk]].drop_duplicates()))
            # FastTripsLogger.log(msg_level,"pandas.isnull(input_df[%s]).sum() = %d" % (id_colname, pandas.isnull(input_df[id_colname]).sum()))


            if drop_failures:
                # remove them
                return_df = return_df.loc[pandas.notnull(return_df[mapping_newid_colname_chk])]
                # make it an int
                return_df[mapping_newid_colname_chk] = return_df[mapping_newid_colname_chk].astype(int)

            if not warn:
                raise UnexpectedError("Util.add_new_id failed to map all ids to numbers")

        # remove the redundant id column if necessary (it's redundant)
        if id_colname != mapping_id_colname:
            if mapping_id_colname in input_cols:
                return_df.drop("%s_mapping" % mapping_id_colname, axis=1, inplace=True)
            else:
                return_df.drop(mapping_id_colname, axis=1, inplace=True)

        # rename it as requested (if necessary)
        if newid_colname != mapping_newid_colname:
            if mapping_newid_colname in input_cols:
                return_df.rename(columns={"%s_mapping" % mapping_newid_colname:newid_colname}, inplace=True)
            else:
                return_df.rename(columns={mapping_newid_colname:newid_colname}, inplace=True)

        # print "FINAL RETURN_DF=================="
        # print return_df.head()

        return return_df

    @staticmethod
    def remove_null_columns(input_df, inplace=True):
        """
        Remove columns from the dataframe if they're *all* null since they're not useful and make thinks harder to look at.
        """
        # a column full of "None" isn't useful
        for colname in list(input_df.columns.values):
            if input_df.dtypes[colname] != 'object': continue

            null_count = pandas.isnull(input_df[colname]).sum()
            if null_count == len(input_df):
                FastTripsLogger.debug("Dropping null column [%s]" % colname)
                input_df.drop(colname, axis=1, inplace=inplace)

        return input_df

    @staticmethod
    def datetime64_formatter(x):
        """
        Formatter to convert :py:class:`numpy.datetime64` to string that looks like `HH:MM:SS`
        """
        return pandas.to_datetime(x).strftime('%H:%M:%S') if pandas.notnull(x) else ""

    @staticmethod
    def pretty(df):
        """
        Make a pretty version of the dataframe and return it.
        """
        df_cp = df.copy()
        df_cols = list(df.columns.values)
        for col_idx in range(len(df_cols)):
            if str(df.dtypes[col_idx]) == "datetime64[ns]": # print as HH:MM:SS
                df_cp[df_cols[col_idx]] = df[df_cols[col_idx]].apply(Util.datetime64_formatter)
        return df_cp

    @staticmethod
    def datetime64_min_formatter(x):
        """
        Formatter to convert :py:class:`numpy.datetime64` to minutes after midnight
        (with two decimal places)
        """
        return '%.2f' % (pandas.to_datetime(x).hour*60.0 + \
                         pandas.to_datetime(x).minute + \
                         pandas.to_datetime(x).second/60.0)

    @staticmethod
    def timedelta_formatter(x):
        """
        Formatter to convert :py:class:`numpy.timedelta64` to string that looks like `4m 35.6s`
        """
        seconds = x/numpy.timedelta64(1,'s')
        minutes = int(seconds/60)
        seconds -= minutes*60
        return '%4dm %04.1fs' % (minutes,seconds)

    @staticmethod
    def read_time(x, end_of_day=False):
        try:
            if x=='' or x.lower()=='default':
                x = '24:00:00' if end_of_day else '00:00:00'
        except:
            if pandas.isnull(x):
                x = '24:00:00' if end_of_day else '00:00:00'
        time_split = x.split(':')
        hour = int(time_split[0])
        day = Util.SIMULATION_DAY
        if hour >= 24: 
            time_split[0] = '%02d' %(hour-24)
            day += datetime.timedelta(days=1)
        x = ':'.join(time_split)
        return datetime.datetime.combine(day, datetime.datetime.strptime(x, '%H:%M:%S').time())

    @staticmethod
    def write_dataframe(df, name, output_file, append=False, keep_duration_columns=False, drop_debug_columns=True, drop_pathfinding_columns=True):
        """
        Convenience method to write a dataframe but make some of the fields more usable.

        :param df:          The dataframe to write
        :type  df:          :py:class:`pandas.DataFrame`
        :param name:        Name of the dataframe. Just used for logging.
        :type  name:        str
        :param output_file: The name of the file to which the dataframe will be written
        :type  output_file: str
        :param append:      Pass true to append to the existing output file, false otherwise
        :type  append:      bool
        :param keep_duration_columns: Pass True to keep the original duration columns (e.g. "0 days 00:12:00.000000000")
        :type  keep_duration_columns: bool
        :param drop_debug_columns:    Pass True to drop debug columns specified in :py:attr:`Util.DROP_DEBUG_COLUMNS`
        :type  drop_debug_columns:    bool
        :param drop_pathfinding_columns: Pass True to drop pathfinding columns specified in :py:attr:`Util.DROP_PATHFINDING_COLUMNS`
        :type  drop_pathfinding_columns: bool

        For columns that are :py:class:`numpy.timedelta64` fields, instead of writing "0 days 00:12:00.000000000",
        times will be converted to the units specified in :py:attr:`Util.TIMEDELTA_COLUMNS_TO_UNITS`.  The original
        duration columns will be kept if *keep_duration_columns* is True.

        """
        if len(df) == 0:
            FastTripsLogger.info("No rows of %s dataframe to write to %s" % (name, output_file))
            return

        df_cols = list(df.columns.values)

        if drop_debug_columns:
            # drop the columns from the list
            for debug_col in Util.DROP_DEBUG_COLUMNS:
                if debug_col in df_cols: df_cols.remove(debug_col)

        if drop_pathfinding_columns:
            # drop the columns from the list
            for debug_col in Util.DROP_PATHFINDING_COLUMNS:
                if debug_col in df_cols: df_cols.remove(debug_col)

        df_toprint = df[df_cols].copy()

        # if we're appending, figure out the header row
        header_row = None
        if append and os.path.exists(output_file):
            # get the columns
            df_file = open(output_file, 'rb')
            df_reader = csv.reader(df_file, delimiter=",")
            header_row = df_reader.next()
            df_file.close()

        for col_idx in range(len(df_cols)):
            old_colname = df_cols[col_idx]
            # FastTripsLogger.debug("%s -> %s" % (old_colname, df_toprint.dtypes[col_idx]))

            # convert timedelta untils because the string version is just awful
            if str(df_toprint.dtypes[col_idx]) == "timedelta64[ns]":

                # lookup timedelta units
                units_str   = Util.TIMEDELTA_COLUMNS_TO_UNITS[old_colname]
                new_colname = "%s %s" % (old_colname, units_str)
                if units_str == "milliseconds":
                    units = numpy.timedelta64(1,'ms')
                elif units_str == "min":
                    units = numpy.timedelta64(1,'m')
                else:
                    raise

                # if the column already exists, continue
                if new_colname in df_cols: continue

                # otherwise make the new one and add or replace it
                df_toprint[new_colname] = df_toprint[old_colname]/units
                if keep_duration_columns:           # add
                    df_cols.append(new_colname)
                else:                               # replace
                    df_cols[col_idx] = new_colname

            elif str(df_toprint.dtypes[col_idx]) == "datetime64[ns]":
                # print as HH:MM:SS
                df_toprint[df_cols[col_idx]] = df_toprint[df_cols[col_idx]].apply(Util.datetime64_formatter)

            # print df_toprint.dtypes[col_idx]

        # the cols have new column names instead of old
        df_toprint = df_toprint[df_cols]

        # append
        if header_row:
            df_file = open(output_file, "a")
            df_toprint[header_row].to_csv(df_file, index=False, header=False, float_format="%.10f")
            df_file.close()

            FastTripsLogger.info("Appended %s dataframe to %s" % (name, output_file))

        else:
            df_toprint.to_csv(output_file, index=False, float_format="%.10f")
            FastTripsLogger.info("Wrote %s dataframe to %s" % (name, output_file))

    @staticmethod
    def calculate_distance_miles(dataframe, origin_lat, origin_lon, destination_lat, destination_lon, distance_colname):
        """
        Given a dataframe with columns origin_lat, origin_lon, destination_lat, destination_lon, calculates the distance
        in miles between origin and destination based on Haversine.  Results are added to the dataframe in a column called *distance_colname*.
        """
        radius = 3959.0 # mi

        # assume these aren't in here
        dataframe["dist_lat" ] = numpy.radians(dataframe[destination_lat]-dataframe[origin_lat])
        dataframe["dist_lon" ] = numpy.radians(dataframe[destination_lon]-dataframe[origin_lon])
        dataframe["dist_hava"] = (numpy.sin(dataframe["dist_lat"]/2) * numpy.sin(dataframe["dist_lat"]/2)) + \
                                 (numpy.cos(numpy.radians(dataframe[origin_lat])) * numpy.cos(numpy.radians(dataframe[destination_lat])) * numpy.sin(dataframe["dist_lon"]/2.0) * numpy.sin(dataframe["dist_lon"]/2.0))
        dataframe["dist_havc"] = 2.0*numpy.arctan2(numpy.sqrt(dataframe["dist_hava"]), numpy.sqrt(1.0-dataframe["dist_hava"]))
        dataframe[distance_colname] = radius * dataframe["dist_havc"]

        # FastTripsLogger.debug("calculate_distance_miles\n%s", dataframe.to_string())

        # check
        min_dist = dataframe[distance_colname].min()
        max_dist = dataframe[distance_colname].max()
        if min_dist < 0:
            FastTripsLogger.warn("calculate_distance_miles: min is negative\n%s" % dataframe.loc[dataframe[distance_colname]<0].to_string())
        if max_dist > 1000:
            FastTripsLogger.warn("calculate_distance_miles: max is greater than 1k\n%s" % dataframe.loc[dataframe[distance_colname]>1000].to_string())

        dataframe.drop(["dist_lat","dist_lon","dist_hava","dist_havc"], axis=1, inplace=True)

    @staticmethod
    def get_process_mem_use_str():
        """
        Returns a string representing the process memory use.
        """
        try:
            import psutil

        except ImportError:
            return "Uknown; please install python package psutil"

        p = psutil.Process()
        bytes = p.memory_info().rss
        if bytes < 1024:
            return "%d bytes" % bytes
        if bytes < 1024*1024:
            return "%.1f KB" % (bytes/1024.0)
        if bytes < 1024*1024*1024:
            return "%.1f MB" % (bytes/(1024.0*1024.0))
        return "%.1f GB" % (bytes/(1024.0*1024.0*1024.0))
