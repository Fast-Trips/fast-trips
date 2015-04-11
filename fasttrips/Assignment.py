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

class Assignment:
    """
    Assignment class.  Documentation forthcoming.
    """

    #: Configuration: Maximum number of iterations to remove capacity violations. When
    #: the transit system is not crowded or when capacity constraint is
    #: relaxed the model will terminate after the first iteration
    ITERATION_FLAG                  = 1

    #: Configuration: Assignment Type
    #: 'Simulation Only' - No Assignment (only simulation, given paths in the input)
    #: 'Deterministic Assignment'
    #: 'Stochastic Assignment'
    ASSIGNMENT_TYPE                 = 'Deterministic Assignment'

    #: Configuration: Simulation flag. It should be on for iterative assignment. In a one shot
    #: assignment with simulation flag off, the passengers are assigned to
    #: paths but are not loaded to the network.
    SIMULATION_FLAG                 = True

    #: Configuration: Passenger trajectory output flag. Passengers' path and time will be
    #: reported if this flag is on. Note that the simulation flag should be on for
    #: passengers' time.
    OUTPUT_PASSENGER_TRAJECTORIES   = True

    #: Configuration: Path time-window. This is the time in which the paths are generated.
    #: E.g. with a typical 30 min window, any path within 30 min of the
    #: departure time will be checked.
    PATH_TIME_WINDOW                = 30

    #: Configuration: Create skims flag. This is specific to the travel demand models
    #: (not working in this version)
    CREATE_SKIMS                    = False

    #: Configuration: Beginning of the time period for which the skim is required.
    #: (in minutes from start of day)
    #: TODO: change to time?
    SKIM_START_TIME                 = 300

    #: Configuration: End of the time period for which the skim is required
    #: (minutes from start of day)
    SKIM_END_TIME                   = 600

    #: Route choice configuration: Weight of in-vehicle time
    IN_VEHICLE_TIME_WEIGHT          = 1.0

    #: Route choice configuration: Weight of waiting time
    WAIT_TIME_WEIGHT                = 1.77

    #: Route choice configuration: Weight of access walk time
    WALK_ACCESS_TIME_WEIGHT         = 3.93

    #: Route choice configuration: Weight of egress walk time
    WALK_EGRESS_TIME_WEIGHT         = 3.93

    #: Route choice configuration: Weight of transfer walking time
    WALK_TRANSFER_TIME_WEIGHT       = 3.93

    #: Route choice configuration: Weight transfer penalty (minutes)
    TRANSFER_PENALTY                = 47.73

    #: Route choice configuration: Weight of schedule delay (0 - no penalty)
    SCHEDULE_DELAY_WEIGHT           = 0.0

    #: Route choice configuration: Fare in dollars per boarding (with no transfer credit)
    FARE_PER_BOARDING               = 0.0

    #: Route choice configuration: Value of time (dollars per hour)
    VALUE_OF_TIME                   = 999

    #: Route choice configuration: Dispersion parameter in the logit function.
    #: Higher values result in less stochasticity. Must be nonnegative. 
    #: If unknown use a value between 0.5 and 1
    DISPERSION_PARAMETER            = 1.0

    #: Route choice configuration: Use vehicle capacity constraints
    CAPACITY_CONSTRAINT             = False

    def read_configuration():
        """
        Read the configuration parameters and override the above
        """
        pass