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

import logging

__all__ = ['FastTripsLogger', 'setupLogging']

#: This is the instance of :py:class:`Logger` that gets used for all dta logging needs!
FastTripsLogger = logging.getLogger("FastTripsLogger")

def setupLogging(infoLogFilename, debugLogFilename, logToConsole=True):
    """
    Sets up the logger.

    :param infoLogFilename: info log file, will receive log messages of level INFO or more important.
       Pass None for no info log file.
    :param debugLogFilename: debug log file, will receive log messages of level DEBUG or more important.
       Pass None for no debug log file.
    :param logToConsole: if true, INFO and above will also go to the console.
    """
    # already setup -- clear and set again
    while len(FastTripsLogger.handlers) > 0:
        h = FastTripsLogger.handlers[0]
        FastTripsLogger.removeHandler(h)

    # create a logger
    FastTripsLogger.setLevel(logging.DEBUG)

    if infoLogFilename:
        infologhandler = logging.StreamHandler(open(infoLogFilename, 'w'))
        infologhandler.setLevel(logging.INFO)
        infologhandler.setFormatter(logging.Formatter('%(asctime)s  %(message)s', '%Y-%m-%d %H:%M:%S'))
        FastTripsLogger.addHandler(infologhandler)

    if debugLogFilename:
        debugloghandler = logging.StreamHandler(open(debugLogFilename,'w'))
        debugloghandler.setLevel(logging.DEBUG)
        debugloghandler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s', '%Y-%m-%d %H:%M:%S'))
        FastTripsLogger.addHandler(debugloghandler)

    if logToConsole:
        consolehandler = logging.StreamHandler()
        consolehandler.setLevel(logging.INFO)
        consolehandler.setFormatter(logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s'))
        FastTripsLogger.addHandler(consolehandler)