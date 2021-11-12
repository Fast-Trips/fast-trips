How Fast-Trips Works
========================

Dynamic Passenger Assignment(DPA) Algorithms
------------------------------------------------
Fast-Trips is based on the work of Alireza Khani, Mark Hickman, and others while at University of Arizona.  Associated academic papers include:

 * `Models and solution algorithms for transit and intermodal passenger assignment (development of FAST-TrIPs model) <http://arizona.openrepository.com/arizona/handle/10150/306074>`_
 * `Hyperpaths in networks based on transit schedules <http://trrjournalonline.trb.org/doi/10.3141/2284-04>`_

Some other useful resources include:

 * `Teaching Materials <https://drive.google.com/open?id=0Bz-oz0TqHWtNQVdFNXV5eGwtbms>`_ (currently in draft form) for the Fast-Trips algorithm.
 * `Term Glossary <https://drive.google.com/open?id=1usCw5FAjAXL44UavBKmCmdr7jFbAnQ-2meMlJwnEl5Y>`_ (currently being developed collaboratively)


System Design
------------------
One of the best ways to understand what is happening internally in Fast-Trips is to trace a single passenger through a simulation.  This can be done by using the :code:`run_trace.py` script found in the :code:`/scripts` directory.


You can also review the following `code flow chart <https://docs.google.com/presentation/d/1ReNqDJP4O_2m882G3NI-4xjnsd6ORjOcDCxOQNGZN4c/edit#slide=id.p>`_ (which is a work in progress)



Skimming
------------------
Skims are average level-of-service indicators.

Skims are average level-of-service indicators. Given that fasttrips is trip-based and dynamic, we need to define an
averaging scheme. Since fasttrips is zone-based, we do not need to worry about spatial aggregation. We are
therefore left with the following dimensions:
 1) temporal
 2) personal attributes:
    - value of time
    - user class (purpose)
    - access/egress mode

Regarding 1), the user has to specify a skimming start time, a skimming end time, and a sample interval. For each
sampling interval, we build a shortest path from each origin to all destinations, calculate common skims,
and then average them by taking the mean.



Implemented components
^^^^^^^^^^^^^^^^^^^^^^

    'fare',
    'num_transfers',
    'invehicle_time',
    'access_time',
    'egress_time',
    'transfer_time',
    'wait_time',
    'adaption_time',
    'gen_cost'


Pathfinding details
^^^^^^^^^^^^^^^^^^^

Note that we use deterministic pathfinding, where we changed the implementation where necessary such that we build a
shortest path tree per origin (i.e. one origin to all destinations). This means the deterministic
pathfinder determines what constitutes a shortest path and at the moment this is always with respect to time,
not generalised cost.


Output format and location
^^^^^^^^^^^^^^^^^^^^^^^^^^

Skims are saved to the specified output directory in a sub-directory called skims. Currently, each combination of
user class, purpose, access_mode, transit_mode, egress_mode for which skims have been requested will have its own
sub-directory, with each component a separate omx file in that directory. Each omx file contains the data and several
attributes: Skim start time (start_time), skim end time (end_time), skim sampling period (sample_interval), the name
of the skim (name), the number of zones ('num_zones'), and lastly an attribute called 'index_to_zone_ids'. This array
encodes the mapping from skim index (0-based numpy indexing) to the zone identifier used in the input data. The
position in the array corresponds to the index of the zone identifier in the skim matrix.



