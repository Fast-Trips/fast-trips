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

import numpy
import pandas

from .Logger import FastTripsLogger

class Util:
    """
    Util class.

    Collect useful stuff here that doesn't belong in any particular existing class.
    """

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
    def add_numeric_id(input_df, id_colname, numeric_newcolname,
                       mapping_df, mapping_id_colname, mapping_numeric_colname):
        """
        Passing a :py:class:`pandas.DataFrame` *input_df* with an ID column called *id_colname*,
        adds the numeric id as a column named *numeric_newcolname* and returns it.

        *mapping_df* is defines the mapping from an ID (*mapping_id_colname*) 
        to a numeric ID (*mapping_numeric_colname*).
        """
        # add the numeric stop id column
        return_df = pandas.merge(left=input_df, right=mapping_df,
                                 how='left',
                                 left_on=id_colname,
                                 right_on=mapping_id_colname)

        # make sure all ids were mapped to numbers
        assert(pandas.isnull(return_df[mapping_numeric_colname]).sum() == 0)

        # remove the redundant id column if necessary (it's redundant)
        if id_colname != mapping_id_colname:
            return_df.drop(mapping_id_colname, axis=1, inplace=True)

        # rename it as requested (if necessary)
        if numeric_newcolname != mapping_numeric_colname:
            return_df.rename(columns={mapping_numeric_colname:numeric_newcolname}, inplace=True)

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
        Formatter to convert :py:class:`numpy.datetime64` to string that looks like `HH:MM.SS`
        """
        return pandas.to_datetime(x).strftime('%H:%M.%S')

    @staticmethod
    def datetime64_min_formatter(x):
        """
        Formatter to convert :py:class:`numpy.datetime64` to minutes after minutes
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