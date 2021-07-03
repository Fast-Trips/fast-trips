from __future__ import absolute_import
from __future__ import division
from builtins import next
from builtins import str
from builtins import range
from builtins import object
from .utils.numeric_parser import vparse_numeric
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
import csv
import datetime
import logging
import os

import numpy as np
import pandas as pd
import partridge as ptg

from .Error  import UnexpectedError
from .Logger import FastTripsLogger


class Util(object):
    """
    Util class.

    Collect useful stuff here that doesn't belong in any particular existing class.
    """
    #: Use this as the date
    #SIMULATION_DAY                  = datetime.datetime(year=2016,day=1,month=1, hour=0, minute=0, second=0)
    #: Use this for the start time - the start of :py:attr:`Util.SIMULATION_DAY`
    #SIMULATION_DAY_START            = datetime.datetime.combine(SIMULATION_DAY, datetime.time())

    #: Maps timedelta columns to units for :py:meth:`Util.write_dataframe`
    TIMEDELTA_COLUMNS_TO_UNITS      = {
        'time enumerating'  : 'milliseconds',  # performance
        'time labeling'     : 'milliseconds',  # performance
        'step_duration'     : 'seconds',       # performance
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
        return_df = pd.merge(left=input_df, right=mapping_df,
                                 how='left',
                                 left_on=id_colname,
                                 right_on=mapping_id_colname,
                                 suffixes=("","_mapping"))

        # print "RETURN_DF=================="
        # print return_df.head()

        # first check if mapping_newid_colname was already in input_df; if it was, check needs to be performed on "_mapping"
        mapping_newid_colname_chk = mapping_id_colname + "_mapping" if mapping_newid_colname in input_cols else mapping_newid_colname

        # Make sure all ids were mapped to numbers.  If not warn or error
        if pd.isnull(return_df[mapping_newid_colname_chk]).sum() != pd.isnull(input_df[id_colname]).sum():

            msg_level = logging.CRITICAL
            if warn: msg_level = logging.WARN

            if warn_msg: FastTripsLogger.log(msg_level, warn_msg)
            FastTripsLogger.log(msg_level,"Util.add_new_id failed to map all ids to numbers")
            # FastTripsLogger.log(msg_level,"pd.isnull(return_df[%s]).sum() = %d" % (mapping_newid_colname_chk, pd.isnull(return_df[mapping_newid_colname_chk]).sum()))
            FastTripsLogger.log(msg_level,"\n%s\n" % str(return_df.loc[pd.isnull(return_df[mapping_newid_colname_chk]),[id_colname,mapping_newid_colname_chk]].drop_duplicates()))
            # FastTripsLogger.log(msg_level,"pd.isnull(input_df[%s]).sum() = %d" % (id_colname, pd.isnull(input_df[id_colname]).sum()))


            if drop_failures:
                # remove them
                return_df = return_df.loc[pd.notnull(return_df[mapping_newid_colname_chk])]
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

            null_count = pd.isnull(input_df[colname]).sum()
            if null_count == len(input_df):
                FastTripsLogger.debug("Dropping null column [%s]" % colname)
                input_df.drop(colname, axis=1, inplace=inplace)

        return input_df

    @staticmethod
    def datetime64_formatter(x):
        """
        Formatter to convert :py:class:`numpy.datetime64` to string that looks like `HH:MM:SS`
        """
        return pd.to_datetime(x).strftime('%Y-%m-%d %H:%M:%S.%f') if pd.notnull(x) else ""

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
        return '%.2f' % (pd.to_datetime(x).hour*60.0 + \
                         pd.to_datetime(x).minute + \
                         (pd.to_datetime(x).second/60.0))

    @staticmethod
    def timedelta_formatter(x):
        """
        Formatter to convert :py:class:`numpy.timedelta64` to string that looks like `4m 35.6s`
        """
        seconds = (x/np.timedelta64(1,'s'))
        minutes = int(seconds/60)
        seconds -= minutes*60
        return '%4dm %04.1fs' % (minutes,seconds)

    @staticmethod
    def read_time(x, end_of_day=False):
        from .Assignment import Assignment
        try:
            if x=='' or x.lower()=='default' or pd.isnull(x):
                x = '24:00:00' if end_of_day else '00:00:00'
        except:
            if pd.isnull(x):
                x = '24:00:00' if end_of_day else '00:00:00'
        time_split = x.split(':')
        hour = int(time_split[0])
        day = Assignment.NETWORK_BUILD_DATE
        if hour >= 24:
            time_split[0] = '%02d' %(hour-24)
            day += datetime.timedelta(days=1)
        x = ':'.join(time_split)
        return datetime.datetime.combine(day, datetime.datetime.strptime(x, '%H:%M:%S').time())

    @staticmethod
    def read_end_time(x):
        return Util.read_time(x, True)

    @staticmethod
    def parse_minutes_to_time(minutes):
        from .Assignment import Assignment
        elapsed_time = datetime.timedelta(minutes=minutes)
        return datetime.datetime.combine(Assignment.NETWORK_BUILD_DATE, datetime.time()) + elapsed_time


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
            df_file = open(output_file, 'rt')
            df_reader = csv.reader(df_file, delimiter=",")
            header_row = next(df_reader)
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
                    units = np.timedelta64(1,'ms')
                elif units_str == "min":
                    units = np.timedelta64(1,'m')
                elif units_str == "seconds":
                    units = np.timedelta64(1,'s')
                else:
                    raise Exception

                # if the column already exists, continue
                if new_colname in df_cols: continue

                # otherwise make the new one and add or replace it
                df_toprint[new_colname] = (df_toprint[old_colname]/units)
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
        radius = 3963.190592 # mi

        # assume these aren't in here
        dataframe["dist_lat" ] = np.radians(dataframe[destination_lat]-dataframe[origin_lat])
        dataframe["dist_lon" ] = np.radians(dataframe[destination_lon]-dataframe[origin_lon])
        dataframe["dist_hava"] = (np.sin(dataframe["dist_lat"]/2) * np.sin(dataframe["dist_lat"]/2)) + \
                                 (np.cos(np.radians(dataframe[origin_lat])) * np.cos(np.radians(dataframe[destination_lat])) * np.sin(dataframe["dist_lon"]/2.0) * np.sin(dataframe["dist_lon"]/2.0))
        dataframe["dist_havc"] = 2.0*np.arctan2(np.sqrt(dataframe["dist_hava"]), np.sqrt(1.0-dataframe["dist_hava"]))
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
    def get_process_mem_use_bytes():
        """
        Returns the process memory usage in bytes
        """
        try:
            import psutil

        except ImportError:
            FastTripsLogger.warn("Unknown; please install python package psutil")
            return -1

        p = psutil.Process()
        bytes = p.memory_info().rss
        return bytes

    @staticmethod
    def get_process_mem_use_str():
        """
        Returns a string representing the process memory use.
        Use SI prefixes (not binary prefixes).  1KB = 1000 bytes
        """
        bytes = Util.get_process_mem_use_bytes()

        if bytes < 1000:
            return "%d bytes" % bytes
        if bytes < 1000*1000:
            return "%.1f KB" % (bytes/1000.0)
        if bytes < 1000*1000*1000:
            return "%.1f MB" % (bytes/(1000.0*1000.0))
        return "%.1f GB" % (bytes/(1000.0*1000.0*1000.0))

    @staticmethod
    def merge_two_dicts(x, y):
        """A helper method for Python 2.7 to 'zip' two dictionary objects

        :param x: This dictionary will be copied and y appended to the copy.
        :type x: dict.
        :param y: This dictionary will be appended with a copy of x.
        :type y: dict.
        :returns: dict

        """
        z = x.copy()
        z.update(y)
        return z

    @staticmethod
    def parse_boolean(val):
        return val in ['true', 'True', 'TRUE', 1]

    @staticmethod
    def calculate_pathweight_costs(df, result_col):
        """
        Calculates the weighted cost for a given :py:class:`pandas.DataFrame` given a impedance function and
        value.

        Sets the result into the column called result_col.

        +-----------------------+--------------+---------------------------------------------------------------------------+
        | *column name*         | *column type*| *description*                                                             |
        +=======================+==============+===========================================================================+
        |``var_value``          | float64      | The value to weight                                                       |
        +-----------------------+--------------+---------------------------------------------------------------------------+
        |``growth_type``        |          str | one of ``constant``, ``exponential``, ``logarithmic``, ``logistic``       |
        +-----------------------+--------------+---------------------------------------------------------------------------+
        |``growth_log_base``    |      float64 | *logarithmic only* log base for logarithmic base value                    |
        +-----------------------+--------------+---------------------------------------------------------------------------+
        |``growth_logistic_max``|      float64 | *logistic only* Maximum assymtotic value for logistic curve               |
        +-----------------------+--------------+---------------------------------------------------------------------------+
        |``growth_logistic_mid``|      float64 | *logistic only* X-Axis location of the midpoint of the curve              |
        +-----------------------+--------------+---------------------------------------------------------------------------+

        """
        from fasttrips import PathSet

        # default is constant (constant weight)
        df[result_col] = df['var_value']*df[PathSet.WEIGHTS_COLUMN_WEIGHT_VALUE]

        if PathSet.EXP_GROWTH_MODEL in df[PathSet.WEIGHTS_GROWTH_TYPE].values:
            df.loc[df[PathSet.WEIGHTS_GROWTH_TYPE] == PathSet.EXP_GROWTH_MODEL,  result_col] = \
                Util.exponential_integration(df['var_value'], df[PathSet.WEIGHTS_COLUMN_WEIGHT_VALUE])

        if PathSet.LOGARITHMIC_GROWTH_MODEL in df[PathSet.WEIGHTS_GROWTH_TYPE].values:
            assert {'var_value', PathSet.WEIGHTS_GROWTH_LOG_BASE}.issubset(df), "Logarithmic pathweight growth_type formula specified. Missing var_value, or growth_log_base."
            df.loc[df[PathSet.WEIGHTS_GROWTH_TYPE] == PathSet.LOGARITHMIC_GROWTH_MODEL, result_col] = \
                Util.logarithmic_integration(df['var_value'], df[PathSet.WEIGHTS_COLUMN_WEIGHT_VALUE], df[PathSet.WEIGHTS_GROWTH_LOG_BASE])

        if PathSet.LOGISTIC_GROWTH_MODEL in df[PathSet.WEIGHTS_GROWTH_TYPE].values:
            assert {'var_value', PathSet.WEIGHTS_GROWTH_LOGISTIC_MAX, PathSet.WEIGHTS_GROWTH_LOGISTIC_MID}.issubset(df), "Logistic pathweight growth_type formula specified. Missing var_value, growth_logistic_max, or growth_logistic_mid."
            df.loc[df[PathSet.WEIGHTS_GROWTH_TYPE] == PathSet.LOGISTIC_GROWTH_MODEL, result_col] = \
                Util.logistic_integration(df['var_value'], df[PathSet.WEIGHTS_COLUMN_WEIGHT_VALUE], df[PathSet.WEIGHTS_GROWTH_LOGISTIC_MAX], df[PathSet.WEIGHTS_GROWTH_LOGISTIC_MID])

        # TODO: option: make these more subtle?
        # missed_xfer has huge cost
        if 'missed_xfer' in df:
            df.loc[df['missed_xfer']==1, result_col] = PathSet.HUGE_COST

        # bump iter means over capacity
        if 'bump_iter' in df:
            df.loc[df['bump_iter']>=0, result_col] = PathSet.HUGE_COST

        # negative cost is invalid
        if (df[result_col] < 0).any():
            FastTripsLogger.warn("---Pathweight costs has negative values. Setting to zero.---\n{}".format(
                df[df[result_col] < 0].to_string())
            )
            df.loc[ df[result_col] < 0, result_col ] = 0.0


    @staticmethod
    def exponential_integration(penalty_min, growth_rate):
        """
        Returns the integrated value of an exponential function.
        Growth Function: (1 + Growth Rate) ** Penalty Minutes
        Integrated Growth Function: ((1 + Growth Rate) ** Penalty Minutes - 1) / LN(1 + Growth Rate), dx=Penalty Minutes
        :param penalty_min: float or :py:class:`pandas.Series` of floats
        :param growth_rate: float: Exponetial growth factor
        :return: float or :py:class:`pandas.Series` of floats depending on inputs
        """
        return (np.power(1.0 + growth_rate, penalty_min) - 1)/ np.log(1.0 + growth_rate)


    @staticmethod
    def logarithmic_integration(penalty_min, growth_rate, log_base=np.exp(1)):
        """
        Returns the integrated value of an logarithmic function.
        # Growth Function: Growth Rate * LOG((Penalty Minutes + 1), Log Base)
        # Integrated Growth Function: Growth Rate * ((Penalty Minutes + 1) * LN(Penalty Minutes + 1) - Penalty Minutes) / LN(Penalty Log Base), dx=Penalty Minutes
        :param penalty_min: float or :py:class:`pandas.Series` of floats
        :param growth_rate: log growth factor
        :param log_base: log base to impact shape of curve
        :return: float or :py:class:`pandas.Series` of floats depending on inputs
        """
        return growth_rate * ((penalty_min + 1) * np.log(penalty_min + 1) - penalty_min) / np.log(log_base)


    @staticmethod
    def logistic_integration(penalty_minute, growth_rate, max_logit, sigmoid_mid):
        """
        Returns the integrated value of an logistic function.
        Growth Function: Max Value / (1 + e^(-Growth Rate*(Penalty Min - Sigmoid Mid)))
        Integrated Growth Function: Upper Bound Integral - Lower Bound Integral (lower=0)
        Upper Bound Integral: (Max Logit / Growth Rate) * ln(e^(Growth Rate * Penalty Min) + e^(Growth Rate * Sigmoid Mid))
        Lower Bound Integral: (Max Value / Growth Rate) * ln(1 + e^(Growth Rate * Sigmoid Mid))
        :param penalty_minute: float or :py:class:`pandas.Series` of floats
        :param growth_rate: log growth factor
        :param max_logit: assymtotic max value of curve
        :param sigmoid_mid: x-midpoint of curve
        :return: float or :py:class:`pandas.Series` of floats depending on inputs
        """

        max_integral = ((max_logit/ growth_rate)) * np.log(np.exp(growth_rate * penalty_minute) + np.exp(growth_rate * sigmoid_mid))
        min_integral = ((max_logit/ growth_rate)) * np.log(1 + np.exp(growth_rate * sigmoid_mid))

        return max_integral - min_integral


    @staticmethod
    def get_fast_trips_config():
        """
        Adds additional nodes to the Partridge graph to
        support Fast Trip extension files.

        :return: Partridge configuration customized for
                 Fast-Trips (_ft) loads and type casting.
        """
        from .Route import Route
        from .Stop import Stop
        from .TAZ import TAZ
        from .Transfer import Transfer
        from .Trip import Trip

        config = ptg.config.default_config()
        config.add_nodes_from([
            (TAZ.INPUT_DRIVE_ACCESS_FILE, {
                'converters': {
                    TAZ.DRIVE_ACCESS_COLUMN_COST: vparse_numeric,
                    TAZ.DRIVE_ACCESS_COLUMN_TRAVEL_TIME: vparse_numeric,
                    TAZ.DRIVE_ACCESS_COLUMN_DISTANCE: vparse_numeric,
                    TAZ.DRIVE_ACCESS_COLUMN_START_TIME: np.vectorize(Util.read_time),
                    TAZ.DRIVE_ACCESS_COLUMN_END_TIME: np.vectorize(Util.read_end_time)
                }
            }),
            (TAZ.INPUT_DAP_FILE, {
                'converters': {
                    TAZ.DAP_COLUMN_LOT_LATITUDE: vparse_numeric,
                    TAZ.DAP_COLUMN_LOT_LONGITUDE: vparse_numeric,
                    TAZ.DAP_COLUMN_CAPACITY: vparse_numeric
                }
            }),
            (Route.INPUT_FARE_ATTRIBUTES_FILE, {
                'converters': {
                    Route.FARE_ATTR_COLUMN_PAYMENT_METHOD: vparse_numeric,
                    Route.FARE_ATTR_COLUMN_PRICE: vparse_numeric,
                    Route.FARE_ATTR_COLUMN_TRANSFERS: vparse_numeric,
                    Route.FARE_ATTR_COLUMN_TRANSFER_DURATION: vparse_numeric
                }
            }),
            (Route.INPUT_FARE_PERIODS_FILE, {
                'converters': {
                    Route.FARE_RULES_COLUMN_START_TIME: np.vectorize(Util.read_time),
                    Route.FARE_RULES_COLUMN_END_TIME: np.vectorize(Util.read_end_time),
                }
            }),
            (Route.INPUT_FARE_TRANSFER_RULES_FILE, {
                'converters': {
                    Route.FARE_TRANSFER_RULES_COLUMN_AMOUNT: vparse_numeric
                }
            }),
            (Route.INPUT_ROUTES_FILE, {
                'converters': {
                    Route.ROUTES_COLUMN_PROOF_OF_PAYMENT: np.vectorize(Util.parse_boolean)
                }
            }),
            (Stop.INPUT_STOPS_FILE, {}),
            (Transfer.INPUT_TRANSFERS_FILE, {
                'converters': {
                    Transfer.TRANSFERS_COLUMN_DISTANCE: vparse_numeric,
                    Transfer.TRANSFERS_COLUMN_ELEVATION_GAIN: vparse_numeric,
                }
            }),
            (Trip.INPUT_TRIPS_FILE, {
                'converters': {}
            }),
            (Trip.INPUT_VEHICLES_FILE, {
                'converters': {
                    Trip.VEHICLES_COLUMN_ACCELERATION: vparse_numeric,
                    Trip.VEHICLES_COLUMN_DECELERATION: vparse_numeric,
                    Trip.VEHICLES_COLUMN_MAXIMUM_SPEED: vparse_numeric,
                    Trip.VEHICLES_COLUMN_SEATED_CAPACITY: vparse_numeric,
                    Trip.VEHICLES_COLUMN_STANDING_CAPACITY: vparse_numeric,
                }
            }),
            (TAZ.INPUT_WALK_ACCESS_FILE, {
                'converters': {
                    TAZ.WALK_ACCESS_COLUMN_DIST: vparse_numeric,
                    TAZ.WALK_ACCESS_COLUMN_ELEVATION_GAIN: vparse_numeric,
                    TAZ.WALK_ACCESS_COLUMN_POPULATION_DENSITY: vparse_numeric,
                    TAZ.WALK_ACCESS_COLUMN_EMPLOYMENT_DENSITY: vparse_numeric,
                    TAZ.WALK_ACCESS_COLUMN_AUTO_CAPACITY: vparse_numeric,
                    TAZ.WALK_ACCESS_COLUMN_INDIRECTNESS: vparse_numeric,
                }
            })
        ])

        return config
