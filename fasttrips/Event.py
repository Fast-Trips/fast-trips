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
import datetime

from .Logger import FastTripsLogger

class Event:
    """
    An event is an arrival or departure of a vehicle.
    """

    EVENT_TYPE_ARRIVAL      = 'a'
    EVENT_TYPE_DEPARTURE    = 'd'
    def __init__(self, stop_time_record, event_type):
        """
        Constructor from dictionary mapping attribute to value.
        """
        self.trip_id    = stop_time_record['tripId']
        self.stop_id    = stop_time_record['stopId']
        self.event_type = event_type
        self.sequence   = stop_time_record['sequence']

        if self.event_type == Event.EVENT_TYPE_DEPARTURE:
            self.event_time = datetime.time(hour = int(stop_time_record['departureTime']/10000) % 24,
                                            minute = int((stop_time_record['departureTime'] % 10000)/100),
                                            second = int(stop_time_record['departureTime'] % 100))
        else:
            self.event_time = datetime.time(hour = int(stop_time_record['arrivalTime']/10000) % 24,
                                            minute = int((stop_time_record['arrivalTime'] % 10000)/100),
                                            second = int(stop_time_record['arrivalTime'] % 100))
