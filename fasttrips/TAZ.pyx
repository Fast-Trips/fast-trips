# cython: profile=True
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
    TAZ class.  Documentation forthcoming.
    """

    #: File with TAZ (Transporation Analysis Zone) data
    #: TODO document format
    INPUT_TAZ_FILE          = "ft_input_zones.dat"

    #: File with access links
    #: TODO document format
    INPUT_ACCESS_LINKS_FILE = "ft_input_accessLinks.dat"

    ACCESS_LINK_IDX_DIST    = 0  #: For accessing parts of :py:attr:`TAZ.access_links`
    ACCESS_LINK_IDX_TIME    = 1  #: For accessing parts of :py:attr:`TAZ.access_links`

    def __init__(self, taz_record):
        """
        Constructor from dictionary mapping attributes to value.
        """
        #: unique TAZ identifier
        self.taz_id         = taz_record['ID']
        self.latitude       = taz_record['Lat']
        self.longitude      = taz_record['Lon']

        #: The stops that have access from this TAZ.
        #: This is a :py:class:`dict` mapping a *stop_id* to (walk_dist, walk_time)
        #: where *walk_dist* is in miles and *walk_time* is a
        #: :py:class:`datetime.timedelta` instance.
        #: Use :py:attr:`TAZ.ACCESS_LINK_IDX_DIST` and :py:attr:`TAZ.ACCESS_LINK_IDX_TIME`
        self.access_links   = collections.OrderedDict()

    def add_access_link(self, access_link_record):
        """
        Add the given access link to this TAZ.
        """
        self.access_links[access_link_record['stop']] = (access_link_record['dist'],
                                                         datetime.timedelta(minutes=access_link_record['time']))

    @staticmethod
    def read_TAZs(input_dir):
        """
        Read the TAZ data from the input file in *input_dir*.
        """
        zones_df = pandas.read_csv(os.path.join(input_dir, TAZ.INPUT_TAZ_FILE), sep="\t")
        FastTripsLogger.debug("=========== TAZS ===========\n" + str(zones_df.head()))
        FastTripsLogger.debug("\n"+str(zones_df.dtypes))

        taz_id_to_taz = collections.OrderedDict()
        taz_records = zones_df.to_dict(orient='records')
        for taz_record in taz_records:
            taz = TAZ(taz_record)
            taz_id_to_taz[taz.taz_id] = taz

        FastTripsLogger.info("Read %7d TAZs" % len(taz_id_to_taz))
        return taz_id_to_taz

    @staticmethod
    def read_access_links(input_dir, taz_id_to_taz, stop_id_to_stop):
        """
        Read the access links from the input file in *input_dir*.

        .. todo:: This loses int types for stop_ids, etc.
        """
        access_links_df = pandas.read_csv(os.path.join(input_dir, TAZ.INPUT_ACCESS_LINKS_FILE), sep="\t")
        FastTripsLogger.debug("=========== ACCESS LINKS ===========\n" + str(access_links_df.head()))
        FastTripsLogger.debug("\n"+str(access_links_df.dtypes))

        access_link_records = access_links_df.to_dict(orient='records')
        for access_link_record in access_link_records:
            taz = taz_id_to_taz[access_link_record['TAZ']]
            taz.add_access_link(access_link_record)

            stop = stop_id_to_stop[access_link_record['stop']]
            stop.add_access_link(access_link_record)

        FastTripsLogger.info("Read %7d access links" % len(access_links_df))