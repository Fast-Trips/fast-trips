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
import multiprocessing


__all__ = ['FastTripsLogger', 'setupLogging']

#: This is the instance of :py:class:`Logger` that gets used for all dta logging needs!
FastTripsLogger = multiprocessing.get_logger()

def setupLogging(infoLogFilename, debugLogFilename, logToConsole=True, append=False):
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
        infologhandler = logging.StreamHandler(open(infoLogFilename, 'a' if append else 'w'))
        infologhandler.setLevel(logging.INFO)
        infologhandler.setFormatter(logging.Formatter('%(asctime)s %(processName)s %(message)s', '%Y-%m-%d %H:%M:%S'))
        FastTripsLogger.addHandler(infologhandler)
        FastTripsInfoLogFilename = infoLogFilename

    if debugLogFilename:
        debugloghandler = logging.StreamHandler(open(debugLogFilename, 'a' if append else 'w'))
        debugloghandler.setLevel(logging.DEBUG)
        debugloghandler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s/%(processName)s] %(message)s', '%Y-%m-%d %H:%M:%S'))
        FastTripsLogger.addHandler(debugloghandler)
        FastTripsDebugLogFilename = debugLogFilename

    if logToConsole:
        consolehandler = logging.StreamHandler()
        consolehandler.setLevel(logging.INFO)
        consolehandler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s/%(processName)s] %(message)s', '%Y-%m-%d %H:%M:%S'))
        FastTripsLogger.addHandler(consolehandler)
    FastTripsLogToConsole = logToConsole

