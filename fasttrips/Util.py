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

import pandas

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