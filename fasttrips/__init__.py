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

from .FastTrips import FastTrips
from .Logger import FastTripsLogger, setupLogging
from .Route import Route
from .Stop import Stop
from .Trip import Trip

__all__ = [
    'FastTrips',
    'FastTripsLogger','setupLogging',
    'Route',
    'Stop',
    'Trip',
]
