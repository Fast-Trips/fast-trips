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
import collections,datetime,os,sys
import pandas

from .Logger import FastTripsLogger

class TAZ:
    """
    TAZ class.

    One instance represents all of the Transportation Analysis Zones as well as their access links.

    Stores TAZ information in :py:attr:`TAZ.zones_df`, an instance of :py:class:`pandas.DataFrame`
    and access link information in :py:attr:`TAZ.access_links_df`, another instance of
    :py:class:`pandas.DataFrame`.
    """

    #: File with TAZ (Transporation Analysis Zone) data.
    #: This is a tab-delimited file with required columns specified by
    #: :py:attr:`TAZ.ZONES_COLUMN_ID`, :py:attr:`TAZ.ZONES_COLUMN_LATITUDE` and
    #: :py:attr:`TAZ.ZONES_COLUMN_LONGITUDE`.
    INPUT_TAZ_FILE          = "ft_input_zones.dat"

    #: Zones column name: Unique identifier. This will be the index of the zones table.
    ZONES_COLUMN_ID         = 'ID'
    #: Zones column name: Latitude
    ZONES_COLUMN_LATITUDE   = 'Lat'
    #: Zones column name: Longitude
    ZONES_COLUMN_LONGITUDE  = 'ID'

    #: File with access links
    #: This is a tab-delimited file with required columns specified by
    #: :py:attr:`TAZ.ACCLINKS_COLUMN_TAZ`, :py:attr:`TAZ.ACCLINKS_COLUMN_STOP`,
    #: :py:attr:`TAZ.ACCLINKS_COLUMN_DIST` and :py:attr:`TAZ.ACCLINKS_COLUMN_TIME`.
    INPUT_ACCESS_LINKS_FILE = "ft_input_accessLinks.dat"

    #: Access links column name: TAZ identifier
    ACCLINKS_COLUMN_TAZ     = 'TAZ'
    #: Access links column name: Stop identifier
    ACCLINKS_COLUMN_STOP    = 'stop'
    #: Access links column name: Link walk distance
    ACCLINKS_COLUMN_DIST    = 'dist'
    #: Access links column name: Link walk time.  This is a TimeDelta
    ACCLINKS_COLUMN_TIME    = 'time'
    #: Access links column name: Link walk time in seconds.  This is float.
    ACCLINKS_COLUMN_TIME_SEC= 'time_sec'

    def __init__(self, input_dir):
        """
        Constructor.  Reads the TAZ data from the input files in *input_dir*.
        """
        #: Zones table
        self.zones_df = pandas.read_csv(os.path.join(input_dir, TAZ.INPUT_TAZ_FILE), sep="\t")
        # verify required columns are present
        zone_cols = list(self.zones_df.columns.values)
        assert(TAZ.ZONES_COLUMN_ID          in zone_cols)
        assert(TAZ.ZONES_COLUMN_LATITUDE    in zone_cols)
        assert(TAZ.ZONES_COLUMN_LONGITUDE   in zone_cols)
        self.zones_df.set_index(TAZ.ZONES_COLUMN_ID, inplace=True, verify_integrity=True)

        FastTripsLogger.debug("=========== TAZS ===========\n" + str(self.zones_df.head()))
        FastTripsLogger.debug("\n"+str(self.zones_df.index.dtype)+"\n"+str(self.zones_df.dtypes))
        FastTripsLogger.info("Read %7d TAZs" % len(self.zones_df))

        #: Access links table
        self.access_links_df = pandas.read_csv(os.path.join(input_dir, TAZ.INPUT_ACCESS_LINKS_FILE), sep="\t")
        # verify required columns are present
        access_links_cols = list(self.access_links_df.columns.values)
        assert(TAZ.ACCLINKS_COLUMN_TAZ      in access_links_cols)
        assert(TAZ.ACCLINKS_COLUMN_STOP     in access_links_cols)
        assert(TAZ.ACCLINKS_COLUMN_DIST     in access_links_cols)
        assert(TAZ.ACCLINKS_COLUMN_TIME     in access_links_cols)

        # printing this before setting index
        FastTripsLogger.debug("=========== ACCESS LINKS ===========\n" + str(self.access_links_df.head()))
        FastTripsLogger.debug("As read\n"+str(self.access_links_df.dtypes))

        self.access_links_df.set_index([TAZ.ACCLINKS_COLUMN_TAZ,
                                     TAZ.ACCLINKS_COLUMN_STOP], inplace=True, verify_integrity=True)
        # keep the seconds column as float
        self.access_links_df[TAZ.ACCLINKS_COLUMN_TIME_SEC] = self.access_links_df[TAZ.ACCLINKS_COLUMN_TIME]
        # convert time column from float to timedelta
        self.access_links_df[TAZ.ACCLINKS_COLUMN_TIME] = \
            self.access_links_df[TAZ.ACCLINKS_COLUMN_TIME].map(lambda x: datetime.timedelta(minutes=x))

        FastTripsLogger.debug("Final\n"+str(self.access_links_df.dtypes))
        FastTripsLogger.info("Read %7d access links" % len(self.access_links_df))