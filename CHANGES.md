## Changelog

# 1.0b2

 * Reduce copies of data to downsize memory needs  
 * Updates tests  
 * Adds a learning variable for iterations to remove jumpiness  
 * Exit labeling loop if find a negative cost  
 * Re-implements Utility scaling for labeling to avoid negative costs  
 * Python 3 compliant and backward compatible to 2.6 using future  
 * Add compiler flags and adjust some code so that it works with modern CPP  
 * Update TRAVIS and add auto creation and deployment of documentation at end of successful tests  
 * Move majority of documentation to sphinx-based github site  

# 1.0b1

 * Test scenarios: new scenarios, re-organization, and notebooks to view  
 * Flexible preferred departure and arrival times with
 * Ability to add functional forms to variables  
 * Add updateStopStatesForTrips

# Alpha
Major changes to fast-trips since the original FAST-TrIPs (https://github.com/MetropolitanTransportationCommission/FAST-TrIPs-1)

To be filled in further but including:
* Added pathfinding iterations to looping (so pathfinding_iteration=1 finds paths for everyone, and subsequently just find paths for people who don't have a valid path. Break when max or we don't find anymore)
* Added time-period based drive access links (10/2016)
* Added link distance to extension as part of StopState (10/2016)
* Implemented overlap pathsize correction (8/2016)
* Add purpose segmentation to cost weighting (7/2016)
* Output pathsets in addition to chosen paths (4/2016)
* Update transit trip vehicle times based on boards, alights and vehicle-configured accleration, deceleration and dwell formulas (4/2016)
* Output performance measures (pathfinding and path enumeration times, number of stops processed) (3/2016)
* Stop order update to pathfinding: when a stop state is updated, mark other reachable stops for reprocessing (3/2016) [details][stop-order-details-url]
* Support KNR and PNR access (11/2015)
* Read user-class based cost weighting (11/2015)
* Switch input format to GTFS-plus network (10/2015)
* Move path finding to C++ extension (9/2015)
* Parallelized path finding with multiprocessing (7/2015)
* Port original FAST-TrIPs codebase to python with debug tracing (5/2015)
