Fast-Trips Task 7
=================

Small Test Network - Test Demand
--------------------------------


Initial test demand includes trip information without associated HH or person info. As such `person_id` is
set to zero for all trips in the trip list. One trip occurs every 10 seconds on regular intervals. Each
trip has a random origin and a different random destination. If trips are evenly distributed the transit
network should be able to manage this level of demand, but some buses may fill. Demand starts at 3:15 PM
and ends at 5:15 PM. This means that there is a buffer time when transit will run without any demand at
the beginning and end of the test period. Fields and relevant explanations are explained below.

trip_list.txt
-------------

Field         | Type   | Values              | Notes
--------------|--------|---------------------|--------------------------------------------------------------------------------------------------------
person_id     |str     |                     |Emtpy for trips that are not associated with a person, start with simple dmd
o_taz         |str     |Z1-Z5                |I used str format for taz in network design, e.g. Z1, Z2, etc.
d_taz         |str	   |Z1-Z5                |Same as otaz
mode          |str     |transit              |Test net assumes user can choose any trn mode. Propose using  single "transit" for initial test demand
purpose       |str     |work, other          |For test net, just use "work" or "other"
departure_time|HH:MM:SS|15:00:00 to 17:30:00 |For test net, choose any time between 3:00p and 5:30p
arrival_time  |HH:MM:SS|15:00:00 to 18:00:00 |For test net, set at departure time plus 30 minutes
time_target   |str     |arrival, departure   |Randomly assign "arrival" or "departure"
vot	          |float   |1 to 30              |VOT in $/hr, for test net choose random number between 1 and 30
pnr_ids	      |        |                     |Optional, empty list implies any accessible PNR can be used
