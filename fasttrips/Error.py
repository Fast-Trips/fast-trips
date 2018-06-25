"""
This file will define the various exceptions that can be raised in the course of running Fast-Trips.
"""

__copyright__ = "Copyright 2016 Contributing Entities"
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

class Error(Exception):
    """
    Base class for exceptions in fast-trips.
    """
    pass

class NetworkInputError(Error):
    """
    Exception raised for errors in the network input.

    Attributes:
       expr  -- the input file in which the error occurred
       msg   -- explanation of the error
    """

    def __init__(self, filename, msg):
        self.expr     = filename
        self.msg      = msg

class DemandInputError(Error):
    """
    Exception raised for errors in the demand input.

    Attributes:
       expr  -- the input file in which the error occurred
       msg   -- explanation of the error
    """

    def __init__(self, filename, msg):
        self.expr     = filename
        self.msg      = msg

class ConfigurationError(Error):
    """
    Exception raised for errors in configuration.

    Attributes:
       expr  -- the input file in which the error occurred
       msg   -- explanation of the error
    """

    def __init__(self, filename, msg):
        self.expr     = filename
        self.msg      = msg

class NotImplementedError(Error):
    """
    Exception raised for something expected but not implemented.

    Attributes:
       msg   -- explanation of the error
    """
    def __init__(self, msg):
        self.msg      = msg

class UnexpectedError(Error):
    """
    Exception raised for something unexpected.

    Attributes:
       msg   -- explanation of the error
    """
    def __init__(self, msg):
        self.msg      = msg
